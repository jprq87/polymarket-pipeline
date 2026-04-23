/* @bruin

name: reports.fct_daily_category_concentration
type: bq.sql
description: |
  Calculates per-market tick share and concentration metrics within each daily
  category slice. Serves as the primary source for category concentration
  heatmaps and market-share drill-down views in the dashboard.
  Grain: one row per (date, category, market_id). Category-level summary
  metrics (top_market_share, top_3_market_share, hhi_concentration_index) are
  repeated on every market row within the same (date, category) partition via
  window functions — deliberate denormalization to simplify BI consumption
  without requiring a secondary join.
  Source: reports.fct_daily_market_history joined to
  reports.fct_daily_category_activity for category-level tick totals.
  Markets in categories with zero total ticks are excluded via
  AND c.tick_count > 0 to prevent division by zero in SAFE_DIVIDE.
connection: bruin_gcp

materialization:
  type: table
  strategy: delete+insert
  partition_by: date
  cluster_by:
    - category
    - market_id
  incremental_key: date

depends:
  - reports.fct_daily_market_history
  - reports.fct_daily_category_activity

columns:
  - name: date
    type: date
    description: Trading activity date for the category concentration slice.
    checks:
      - name: not_null
  - name: category
    type: string
    description: |
      Category label inherited from reports.fct_daily_market_history. Includes
      'Unknown / Deleted' when market metadata is missing. Never NULL — sourced
      from a COALESCE in fct_daily_market_history.
    checks:
      - name: not_null
  - name: market_id
    type: string
    description: Polymarket condition ID for the market contributing to this category-day.
    checks:
      - name: not_null
  - name: question
    type: string
    description: |
      Market question text from reports.fct_daily_market_history. NULL or
      'Market metadata unavailable' for markets with no Gamma API match.
  - name: market_tick_count
    type: integer
    description: This market's daily tick_count within the category-day.
    checks:
      - name: positive
  - name: category_tick_count
    type: integer
    description: |
      Total daily tick_count across all markets in the same category-day,
      sourced from fct_daily_category_activity. Used as the denominator for
      market_tick_share — guaranteed > 0 by the WHERE c.tick_count > 0 filter.
    checks:
      - name: positive
  - name: active_markets
    type: integer
    description: Number of distinct markets active in the same category-day.
    checks:
      - name: positive
  - name: market_tick_share
    type: float
    description: |
      This market's share of category_tick_count. Computed via SAFE_DIVIDE —
      NULL-safe even if category_tick_count is 0 (filtered upstream).
      Bounded to [0, 1]. All shares within a (date, category) sum to 1.0
      within floating point tolerance (validated by custom check).
    checks:
      - name: non_negative
      - name: max
        value: 1
  - name: market_rank_in_category
    type: integer
    description: |
      Descending rank of this market by market_tick_count within the
      category-day. Ties broken by market_id for determinism. Rank 1 = most
      active market in category that day.
    checks:
      - name: positive
  - name: avg_spread
    type: float
    description: |
      This market's avg_spread on this date, carried from
      fct_daily_market_history. Provides spread context alongside tick share
      for drill-down analysis. In probability units [0, 1].
    checks:
      - name: non_negative
      - name: max
        value: 1
  - name: spread_volatility
    type: float
    description: |
      This market's spread_volatility on this date, carried from
      fct_daily_market_history. NULL for markets with only one tick that day.
    checks:
      - name: non_negative
  - name: top_market_share
    type: float
    description: |
      Largest single-market market_tick_share observed in the category-day.
      Repeated on every row within the (date, category) partition. Close to 1
      indicates one market dominates the category that day.
    checks:
      - name: non_negative
      - name: max
        value: 1
  - name: top_3_market_share
    type: float
    description: |
      Combined market_tick_share of the top 3 ranked markets in the
      category-day. Repeated on every row within the (date, category)
      partition. Computed as a partitioned SUM with a rank <= 3 mask — not
      a conditional aggregate.
    checks:
      - name: non_negative
      - name: max
        value: 1
  - name: hhi_concentration_index
    type: float
    description: |
      Herfindahl-Hirschman style concentration index: SUM(share^2) across all
      markets in the category-day. Range [0, 1]. Approaches 1 when one market
      captures all activity (monopoly), approaches 0 when activity is evenly
      distributed. Repeated on every row within the (date, category) partition.
    checks:
      - name: non_negative
      - name: max
        value: 1

custom_checks:
  - name: table is not empty for loaded dates
    description: Ensures the incremental load inserted concentration rows for the target dates.
    value: 0
    blocking: true
    query: |
      SELECT CASE
        WHEN COUNT(*) > 0 THEN 0
        ELSE 1
      END
      FROM reports.fct_daily_category_concentration
      WHERE date >= DATE('{{ start_date }}') AND date < DATE('{{ end_date }}')

  - name: market shares sum to one inside each category-day
    description: |
      Validates that all market_tick_share values within each (date, category)
      sum to 1.0 within floating point tolerance (0.0001). A gap indicates a
      join or SAFE_DIVIDE issue corrupting market-share math across the entire
      category-day.
    value: 0
    blocking: true
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          date,
          category,
          ABS(SUM(market_tick_share) - 1.0) AS share_gap
        FROM reports.fct_daily_category_concentration
        WHERE date >= DATE('{{ start_date }}') AND date < DATE('{{ end_date }}')
        GROUP BY date, category
        HAVING share_gap > 0.0001
      )

  - name: hhi is bounded within valid range
    description: |
      Warns if any hhi_concentration_index value falls outside [0, 1].
      Mathematically impossible given share^2 inputs bounded to [0, 1], but
      catches floating point accumulation edge cases on very large category-days.
    value: 0
    blocking: false
    query: |
      SELECT COUNT(*)
      FROM reports.fct_daily_category_concentration
      WHERE date >= DATE('{{ start_date }}') AND date < DATE('{{ end_date }}')
        AND (hhi_concentration_index < 0 OR hhi_concentration_index > 1.0001)

@bruin */

-- ==============================================================================
-- BASE MARKET AND CATEGORY COUNTS
-- ==============================================================================
WITH base AS (
  SELECT
    m.date,
    m.category,
    m.market_id,
    m.question,
    m.tick_count      AS market_tick_count,
    m.avg_spread,
    m.spread_volatility,
    c.tick_count      AS category_tick_count
  FROM reports.fct_daily_market_history m
  INNER JOIN reports.fct_daily_category_activity c
    ON  m.date     = c.date
    AND m.category = c.category
  WHERE m.date >= DATE('{{ start_date }}')
    AND m.date <  DATE('{{ end_date }}')
    -- Exclude zero-tick categories to prevent division by zero in SAFE_DIVIDE
    AND c.tick_count > 0
),

-- ==============================================================================
-- MARKET SHARES & RANKS
-- ==============================================================================
ranked AS (
  SELECT
    date,
    category,
    market_id,
    question,
    market_tick_count,
    category_tick_count,
    avg_spread,
    spread_volatility,
    COUNT(*) OVER (PARTITION BY date, category)    AS active_markets,
    SAFE_DIVIDE(market_tick_count, category_tick_count) AS market_tick_share,
    ROW_NUMBER() OVER (
      PARTITION BY date, category
      ORDER BY market_tick_count DESC, market_id   -- market_id breaks ties deterministically
    ) AS market_rank_in_category
  FROM base
)

-- ==============================================================================
-- CATEGORY CONCENTRATION METRICS
-- ==============================================================================
SELECT
  date,
  category,
  market_id,
  question,
  market_tick_count,
  category_tick_count,
  active_markets,
  market_tick_share,
  market_rank_in_category,
  avg_spread,
  spread_volatility,

  -- Largest single-market share in the category-day
  MAX(market_tick_share) OVER (
    PARTITION BY date, category
  ) AS top_market_share,

  -- Combined share of top 3 markets: partitioned SUM with rank mask,
  -- not a conditional aggregate — all rows evaluate the full window
  SUM(
    CASE WHEN market_rank_in_category <= 3 THEN market_tick_share ELSE 0 END
  ) OVER (
    PARTITION BY date, category
  ) AS top_3_market_share,

  -- HHI: sum of squared shares — approaches 1 for monopoly, 0 for perfect distribution
  SUM(POW(market_tick_share, 2)) OVER (
    PARTITION BY date, category
  ) AS hhi_concentration_index

FROM ranked