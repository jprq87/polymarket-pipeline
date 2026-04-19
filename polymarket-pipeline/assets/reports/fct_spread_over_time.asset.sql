/* @bruin
name: reports.fct_spread_over_time
type: bq.sql
connection: bruin_gcp
description: "Calculates hourly time-series aggregations of market spreads and prices. Drives the intraday volatility and platform heartbeat charts."
materialization:
  type: table
  strategy: delete+insert
  incremental_key: date
  partition_by: date
  cluster_by:
    - hour
depends:
  - staging.stg_price_changes

columns:
  - name: date
    type: date
    description: "Partition key - the calendar date of the trading activity"
    checks:
      - name: not_null
  - name: hour
    type: timestamp
    description: "The specific hour block for this aggregation"
    checks:
      - name: not_null
  - name: yes_avg_bid
    type: float
    description: "Average YES best bid price during this hour"
  - name: yes_avg_ask
    type: float
    description: "Average YES best ask price during this hour"
  - name: avg_spread
    type: float
    description: "Average bid-ask spread during this hour"
    checks:
      - name: not_null
      - name: non_negative
      - name: max
        value: 1.0
  - name: stddev_spread
    type: float
    description: "Standard deviation of the spread during this hour"
  - name: tick_count
    type: integer
    description: "Total number of price changes processed during this hour"
    checks:
      - name: not_null
      - name: positive

custom_checks:
  - name: table is not empty for loaded dates
    query: |
      SELECT CASE 
        WHEN COUNT(*) > 0 THEN 0 
        ELSE 1 
      END
      FROM reports.fct_spread_over_time
      WHERE date >= DATE('{{ start_date }}') AND date < DATE('{{ end_date }}')
    value: 0
    blocking: true
    description: "Ensures the incremental load actually inserted data for the target dates."

@bruin */

-- ==============================================================================
-- HOURLY TIME-SERIES AGGREGATION
-- ==============================================================================
SELECT
  date,
  DATE_TRUNC(timestamp_received, HOUR)                    AS hour,
  AVG(CASE WHEN side = 'YES' THEN best_bid END)           AS yes_avg_bid,
  AVG(CASE WHEN side = 'YES' THEN best_ask END)           AS yes_avg_ask,
  AVG(spread)                                             AS avg_spread,
  STDDEV(spread)                                          AS stddev_spread,
  COUNT(*)                                                AS tick_count
FROM staging.stg_price_changes

-- ==============================================================================
-- DATA QUALITY & INCREMENTAL FILTERING
-- ==============================================================================
WHERE best_bid IS NOT NULL
  AND best_ask IS NOT NULL
  AND spread >= 0
  -- COST OPTIMIZATION: Process data incrementally
  AND date >= DATE('{{ start_date }}') 
  AND date < DATE('{{ end_date }}')
GROUP BY 1, 2