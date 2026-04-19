/* @bruin
name: reports.fct_daily_market_history
type: bq.sql
connection: bruin_gcp
description: "Provides a daily granular snapshot of volume, price action, and spread volatility at the individual market level. Includes time-series correlation for sentiment analysis."
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
  - name: tick_count
    type: integer
    description: "Total number of price changes for this market on this day"
    checks:
      - name: positive
  - name: yes_avg_bid
    type: float
    description: "Average YES token bid price across the day"
  - name: yes_avg_ask
    type: float
    description: "Average YES token ask price across the day"
  - name: avg_spread
    type: float
    description: "Average bid-ask spread across the day"
    checks:
      - name: non_negative
      - name: max
        value: 1.0
  - name: spread_volatility
    type: float
    description: "Standard deviation of the spread. High values indicate turbulent uncertainty."
  - name: price_trend_corr
    type: float
    description: "Pearson correlation between time and YES bid price. +1 = strong YES trend, -1 = strong NO trend."

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