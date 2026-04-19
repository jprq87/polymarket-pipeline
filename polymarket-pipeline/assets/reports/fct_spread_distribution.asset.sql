/* @bruin
name: reports.fct_spread_distribution
type: bq.sql
connection: bruin_gcp
description: "Categorizes daily bid-ask spreads into discrete quality tiers (Tight, Medium, Wide). Powers the 100% Stacked Bar Chart for liquidity quality tracking."
materialization:
  type: table
  strategy: delete+insert
  incremental_key: date
  partition_by: date
depends:
  - staging.stg_price_changes

columns:
  - name: date
    type: date
    description: "Partition key - the date of the trading activity"
    checks:
      - name: not_null
  - name: spread_tier
    type: string
    description: "Categorical bucket representing the quality of the spread (Tight, Medium, Wide)."
    checks:
      - name: not_null
  - name: tick_count
    type: integer
    description: "Number of price changes that fell into this specific spread tier today."
    checks:
      - name: positive

custom_checks:
  - name: table is not empty for loaded dates
    query: |
      SELECT CASE 
        WHEN COUNT(*) > 0 THEN 0 
        ELSE 1 
      END
      FROM reports.fct_spread_distribution
      WHERE date >= DATE('{{ start_date }}') AND date < DATE('{{ end_date }}')
    value: 0
    blocking: true
    description: "Ensures the incremental load actually inserted data."

  - name: tier count does not exceed mathematical maximum
    query: |
      SELECT CASE 
        WHEN MAX(daily_tiers) <= 3 THEN 0 
        ELSE 1 
      END
      FROM (
        SELECT date, COUNT(*) as daily_tiers
        FROM reports.fct_spread_distribution
        WHERE date >= DATE('{{ start_date }}') AND date < DATE('{{ end_date }}')
        GROUP BY date
      )
    value: 0
    blocking: false
    description: "We only have 3 predefined tiers. Fails if unexpected values slip through."

@bruin */

-- ==============================================================================
-- SPREAD TIER BUCKETING
-- ==============================================================================
SELECT
  date,
  
  -- Force chronological sorting in Looker Studio by prepending 1, 2, 3
  CASE 
    WHEN spread <= 0.02 THEN '1. Tight (< $0.02)'
    WHEN spread <= 0.05 THEN '2. Medium ($0.02 - $0.05)'
    ELSE '3. Wide (> $0.05)'
  END AS spread_tier,
  
  COUNT(*)          AS tick_count
FROM staging.stg_price_changes

-- ==============================================================================
-- INCREMENTAL FILTERING
-- ==============================================================================
WHERE spread >= 0
  AND spread <= 1
  -- COST OPTIMIZATION: Process data incrementally
  AND date >= DATE('{{ start_date }}') 
  AND date < DATE('{{ end_date }}')
GROUP BY 1, 2