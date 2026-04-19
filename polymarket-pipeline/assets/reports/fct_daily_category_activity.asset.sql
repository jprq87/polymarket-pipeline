/* @bruin
name: reports.fct_daily_category_activity
type: bq.sql
connection: bruin_gcp
description: "Aggregates daily market activity, volume (ticks), and average bid-ask spreads grouped by category. Serves as the primary data source for the Looker Studio Macro View dashboard."
materialization:
  type: table
  strategy: delete+insert
  incremental_key: date
  partition_by: date
  cluster_by:
    - category
depends:
  - staging.stg_price_changes
  - staging.stg_markets

columns:
  - name: date
    type: date
    description: "Date of the trading activity"
    checks:
      - name: not_null
  - name: category
    type: string
    description: "Market category name from dim_markets"
    checks:
      - name: not_null
  - name: market_count
    type: integer
    description: "Number of distinct markets that recorded activity in this category today"
    checks:
      - name: positive
  - name: tick_count
    type: integer
    description: "Total number of price changes recorded in this category today"
    checks:
      - name: positive
  - name: avg_spread
    type: float
    description: "Average bid-ask spread across all ticks in this category today"
    checks:
      - name: non_negative
      - name: max
        value: 1.0

custom_checks:
  - name: table is not empty for today
    query: |
      SELECT CASE 
        WHEN COUNT(*) > 0 THEN 0 
        ELSE 1 
      END
      FROM reports.fct_daily_category_activity
      WHERE date >= DATE('{{ start_date }}') AND date < DATE('{{ end_date }}')
    value: 0
    blocking: true

@bruin */

-- ==============================================================================
-- AGGREGATED CATEGORY METRICS
-- ==============================================================================
SELECT
  f.date,
  COALESCE(d.category, 'Unknown / Deleted')  AS category,
  COUNT(DISTINCT f.market_id)                AS market_count,
  COUNT(*)                                   AS tick_count,
  AVG(f.spread)                              AS avg_spread
FROM staging.stg_price_changes f

-- ==============================================================================
-- DIMENSION JOINS & FILTERING
-- ==============================================================================
LEFT JOIN staging.stg_markets d
  ON f.market_id = d.market_id
WHERE f.spread >= 0
  -- Idempotent date boundaries for safe incremental loads
  AND f.date >= DATE('{{ start_date }}') 
  AND f.date < DATE('{{ end_date }}')
GROUP BY 1, 2