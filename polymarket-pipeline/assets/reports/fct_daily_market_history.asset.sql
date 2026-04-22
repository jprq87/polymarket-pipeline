/* @bruin
name: reports.fct_daily_market_history
type: bq.sql
connection: bruin_gcp
description: >
  Provides a daily granular snapshot of volume, price action, and spread
  volatility at the individual market level. Includes time-series correlation
  for sentiment analysis.
  Grain: one row per (date, market_id). Markets with no match in stg_markets
  land with COALESCE defaults ('Market metadata unavailable', 'Unknown / Deleted'
  etc.) via the LEFT JOIN — same pattern as fct_daily_category_activity.
  price_trend_corr is NULL for markets with only one YES tick in the day, as
  CORR() requires at least two distinct time values.
materialization:
  type: table
  strategy: delete+insert
  incremental_key: date
  partition_by: date
  cluster_by:
    - category
    - market_id
depends:
  - staging.stg_price_changes
  - staging.stg_markets

columns:
  - name: date
    type: date
    description: "Partition key - the date of the trading activity"
  - name: market_id
    type: string
    description: "Polymarket condition ID"
    checks:
      - name: not_null
  - name: question
    type: string
    description: "Market question text from stg_markets. Defaults to 'Market metadata unavailable' if no match."
  - name: event_title
    type: string
    description: "Parent event title from stg_markets. Defaults to 'Unknown Event' if no match."
  - name: category
    type: string
    description: "Category label from stg_markets. Defaults to 'Unknown / Deleted' if no match via LEFT JOIN."
  - name: category_slug
    type: string
    description: "URL-safe category slug. Defaults to 'unknown-deleted' if no match."
  - name: end_date
    type: timestamp
    description: "Market resolution deadline from stg_markets. NULL if not set."
  - name: closed
    type: boolean
    description: "Whether the market is closed per stg_markets. NULL if no match."
  - name: resolution_status
    type: string
    description: "UMA resolution status from stg_markets. NULL if unresolved or no match."
  - name: tick_count
    type: integer
    description: "Total number of price changes for this market on this day"
    checks:
      - name: positive
  - name: yes_avg_bid
    type: float
    description: "Average YES token bid price across the day."
  - name: yes_avg_ask
    type: float
    description: "Average YES token ask price across the day."
  - name: yes_min_bid
    type: float
    description: "Minimum YES token bid price recorded during the day."
  - name: yes_max_ask
    type: float
    description: "Maximum YES token ask price recorded during the day."
  - name: avg_spread
    type: float
    description: "Average bid-ask spread across the day."
    checks:
      - name: non_negative
      - name: max
        value: 1.0
  - name: spread_volatility
    type: float
    description: "Standard deviation of the spread. High values indicate turbulent uncertainty."
  - name: min_spread
    type: float
    description: "Tightest spread recorded for this market on this day."
  - name: max_spread
    type: float
    description: "Widest spread recorded for this market on this day."
  - name: price_trend_corr
    type: float
    description: >
      Pearson correlation between UNIX_SECONDS(timestamp_received) and YES
      best_bid, computed over all YES-side ticks in the day. +1 = monotonic
      YES price increase across the day, -1 = monotonic decrease, ~0 = no
      directional trend. NULL if fewer than two distinct YES ticks exist.

custom_checks:
  - name: table is not empty for loaded dates
    query: |
      SELECT CASE 
        WHEN COUNT(*) > 0 THEN 0 
        ELSE 1 
      END
      FROM reports.fct_daily_market_history
      WHERE date >= DATE('{{ start_date }}') AND date < DATE('{{ end_date }}')
    value: 0
    blocking: true
    description: "Ensures the incremental load actually inserted data for the target dates."

@bruin */

-- ==============================================================================
-- MARKET HISTORY & VOLATILITY SNAPSHOT
-- ==============================================================================
SELECT
  f.date,
  f.market_id,
  
  -- Metadata defaults
  COALESCE(d.question,     'Market metadata unavailable') AS question,
  COALESCE(d.event_title,  'Unknown Event')               AS event_title,
  COALESCE(d.category,     'Unknown / Deleted')           AS category,
  COALESCE(d.category_slug,'unknown-deleted')             AS category_slug,
  d.end_date,
  d.closed,
  d.resolution_status,

  -- Global Volume
  COUNT(*)                                                AS tick_count,

  -- YES Token Pricing
  AVG(CASE WHEN f.side = 'YES' THEN f.best_bid END)       AS yes_avg_bid,
  AVG(CASE WHEN f.side = 'YES' THEN f.best_ask END)       AS yes_avg_ask,
  MIN(CASE WHEN f.side = 'YES' THEN f.best_bid END)       AS yes_min_bid,
  MAX(CASE WHEN f.side = 'YES' THEN f.best_ask END)       AS yes_max_ask,

  -- Global Spread & Uncertainty
  AVG(f.spread)                                           AS avg_spread,
  STDDEV(f.spread)                                        AS spread_volatility,
  MIN(f.spread)                                           AS min_spread,
  MAX(f.spread)                                           AS max_spread,

  -- Sentiment: Pearson correlation between time and YES bid price
  CORR(
    CASE WHEN f.side = 'YES' THEN UNIX_SECONDS(f.timestamp_received) END,
    CASE WHEN f.side = 'YES' THEN f.best_bid END
  )                                                       AS price_trend_corr

-- ==============================================================================
-- DIMENSION JOINS & FILTERING
-- ==============================================================================
FROM staging.stg_price_changes f
LEFT JOIN staging.stg_markets d
  ON f.market_id = d.market_id
WHERE f.spread >= 0
  -- COST OPTIMIZATION: Process data incrementally
  AND f.date >= DATE('{{ start_date }}') 
  AND f.date < DATE('{{ end_date }}')
GROUP BY
  f.date,
  f.market_id,
  d.question,
  d.event_title,
  d.category,
  d.category_slug,
  d.end_date,
  d.closed,
  d.resolution_status