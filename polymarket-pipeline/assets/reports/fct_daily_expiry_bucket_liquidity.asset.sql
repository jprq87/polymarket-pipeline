/* @bruin

name: reports.fct_daily_expiry_bucket_liquidity
type: bq.sql
description: |
  Aggregates daily market liquidity metrics into time-to-expiry buckets for
  dashboarding. Serves as the primary source for the expiry segmentation
  visualization.
  Grain: one row per (date, expiry_bucket) — maximum 5 rows per date, one per
  bucket. Markets without an end_date are excluded because they cannot be placed
  on the expiry curve.
  Source: reports.fct_daily_market_history — each source row represents one
  market on one day before bucketing.
  avg_spread_volatility may be NULL for buckets where all constituent markets
  had only one tick that day, as STDDEV of a single value is undefined.
connection: bruin_gcp

materialization:
  type: table
  strategy: delete+insert
  partition_by: date
  cluster_by:
    - expiry_bucket
  incremental_key: date

depends:
  - reports.fct_daily_market_history

columns:
  - name: date
    type: date
    description: Trading activity date for the aggregated bucket.
    checks:
      - name: not_null
  - name: expiry_bucket
    type: string
    description: |
      Ordered time-to-expiry bucket derived from DATE(end_date) - date. Exactly
      one of:
        '0-1d'    days_to_expiry BETWEEN 0 AND 1
        '2-7d'    days_to_expiry BETWEEN 2 AND 7
        '8-30d'   days_to_expiry BETWEEN 8 AND 30
        '31d+'    days_to_expiry > 30
        'expired' days_to_expiry < 0
    checks:
      - name: not_null
  - name: expiry_bucket_order
    type: integer
    description: |
      Numeric sort key for expiry_bucket. Explicitly mapped — not derived from
      ELSE — so label and order are guaranteed in sync by the custom check.
        1 = 0-1d
        2 = 2-7d
        3 = 8-30d
        4 = 31d+
        5 = expired
    checks:
      - name: not_null
      - name: positive
      - name: max
        value: 5
  - name: market_count
    type: integer
    description: Number of distinct markets that landed in this expiry bucket on this date.
    checks:
      - name: positive
  - name: total_tick_count
    type: integer
    description: Total price changes across all markets in this expiry bucket on this date.
    checks:
      - name: positive
  - name: avg_market_tick_count
    type: float
    description: Average per-market tick_count within this expiry bucket on this date.
    checks:
      - name: non_negative
  - name: avg_spread
    type: float
    description: Average of per-market avg_spread within this expiry bucket on this date. In probability units [0, 1].
    checks:
      - name: non_negative
      - name: max
        value: 1
  - name: avg_spread_volatility
    type: float
    description: |
      Average of per-market spread_volatility within this expiry bucket on this
      date. NULL if all constituent markets had only one tick that day (STDDEV
      of a single value is undefined and propagates as NULL through AVG).
    checks:
      - name: non_negative
  - name: avg_days_to_expiry
    type: float
    description: |
      Average integer days to expiry for markets in this bucket on this date.
      Computed as AVG(EXTRACT(DAY FROM DATE(end_date) - date)). Precision is
      whole-day intervals — sub-day granularity is not available since end_date
      is stored as a date string in dim_markets.

custom_checks:
  - name: table is not empty for loaded dates
    description: Ensures the incremental load inserted at least one expiry bucket row for the target dates.
    value: 0
    blocking: true
    query: |
      SELECT CASE
        WHEN COUNT(*) > 0 THEN 0
        ELSE 1
      END
      FROM reports.fct_daily_expiry_bucket_liquidity
      WHERE date >= DATE('{{ start_date }}') AND date < DATE('{{ end_date }}')

  - name: bucket labels and sort keys are valid
    description: |
      Protects dashboard ordering and ensures every row lands in a recognized
      expiry bucket with the correct sort key. Catches any label/order drift
      if the CASE logic is modified.
    value: 0
    blocking: true
    query: |
      SELECT COUNT(*)
      FROM reports.fct_daily_expiry_bucket_liquidity
      WHERE date >= DATE('{{ start_date }}') AND date < DATE('{{ end_date }}')
        AND (
          expiry_bucket NOT IN ('0-1d', '2-7d', '8-30d', '31d+', 'expired')
          OR (expiry_bucket = '0-1d'    AND expiry_bucket_order != 1)
          OR (expiry_bucket = '2-7d'    AND expiry_bucket_order != 2)
          OR (expiry_bucket = '8-30d'   AND expiry_bucket_order != 3)
          OR (expiry_bucket = '31d+'    AND expiry_bucket_order != 4)
          OR (expiry_bucket = 'expired' AND expiry_bucket_order != 5)
        )

  - name: bucket count does not exceed mathematical maximum
    description: |
      Warns if more than 5 distinct buckets appear for any single date —
      indicates an unrecognized label slipped through the label/sort check.
    value: 0
    blocking: false
    query: |
      SELECT CASE
        WHEN MAX(daily_buckets) <= 5 THEN 0
        ELSE 1
      END
      FROM (
        SELECT date, COUNT(*) AS daily_buckets
        FROM reports.fct_daily_expiry_bucket_liquidity
        WHERE date >= DATE('{{ start_date }}') AND date < DATE('{{ end_date }}')
        GROUP BY date
      )

@bruin */

-- ==============================================================================
-- DAILY EXPIRY BUCKET AGGREGATION
-- ==============================================================================
SELECT
  date,

  -- Explicit bucket label — mirrors expiry_bucket_order branch for branch
  CASE
    WHEN EXTRACT(DAY FROM DATE(end_date) - date) < 0              THEN 'expired'
    WHEN EXTRACT(DAY FROM DATE(end_date) - date) BETWEEN 0 AND 1  THEN '0-1d'
    WHEN EXTRACT(DAY FROM DATE(end_date) - date) BETWEEN 2 AND 7  THEN '2-7d'
    WHEN EXTRACT(DAY FROM DATE(end_date) - date) BETWEEN 8 AND 30 THEN '8-30d'
    ELSE '31d+'
  END AS expiry_bucket,

  -- Explicit sort key — every branch named, no ELSE fallback
  CASE
    WHEN EXTRACT(DAY FROM DATE(end_date) - date) < 0              THEN 5
    WHEN EXTRACT(DAY FROM DATE(end_date) - date) BETWEEN 0 AND 1  THEN 1
    WHEN EXTRACT(DAY FROM DATE(end_date) - date) BETWEEN 2 AND 7  THEN 2
    WHEN EXTRACT(DAY FROM DATE(end_date) - date) BETWEEN 8 AND 30 THEN 3
    ELSE 4
  END AS expiry_bucket_order,

  COUNT(*)                                                           AS market_count,
  SUM(tick_count)                                                    AS total_tick_count,
  AVG(tick_count)                                                    AS avg_market_tick_count,
  AVG(avg_spread)                                                    AS avg_spread,
  AVG(spread_volatility)                                             AS avg_spread_volatility,
  AVG(EXTRACT(DAY FROM DATE(end_date) - date))                       AS avg_days_to_expiry

FROM reports.fct_daily_market_history

-- ==============================================================================
-- INCREMENTAL FILTERING
-- ==============================================================================
-- end_date IS NOT NULL: markets with no resolution date cannot be bucketed
WHERE end_date IS NOT NULL
  AND date >= DATE('{{ start_date }}')
  AND date < DATE('{{ end_date }}')
GROUP BY 1, 2, 3