/* @bruin
name: staging.stg_price_changes
type: bq.sql
connection: bruin_gcp
description: "Incrementally parses, flattens, and type-casts price change events from the raw JSON orderbook data. Applies strict data quality shields to filter out invalid or inverted spreads."
materialization:
  type: table
  strategy: delete+insert
  incremental_key: date
  partition_by: date
  cluster_by:
    - market_id
depends:
  - staging.stg_orderbook

columns:
  - name: timestamp_received
    type: timestamp
    description: "When the price change was received"
  - name: market_id
    type: string
    description: "Polymarket condition ID"
  - name: token_id
    type: string
    description: "Specific token ID for the asset"
  - name: side
    type: string
    description: "YES or NO token side"
  - name: best_bid
    type: float
    description: "Best bid price, represented as a probability"
  - name: best_ask
    type: float
    description: "Best ask price, represented as a probability"
  - name: spread
    type: float
    description: "Ask minus bid — mathematically defined in query"
  - name: change_size
    type: float
    description: "Magnitude of the orderbook size update"
  - name: change_side
    type: string
    description: "Side of the orderbook that experienced the size change"
  - name: date
    type: date
    description: "Partition key derived from timestamp_received"

custom_checks:
  - name: loaded partitions are not empty
    query: |
      SELECT CASE 
        WHEN COUNT(*) > 0 THEN 0 
        ELSE 1 
      END
      FROM staging.stg_price_changes
      WHERE date >= DATE('{{ start_date }}') AND date < DATE('{{ end_date }}')
    value: 0
    blocking: true
    description: "Fails if the load resulted in 0 rows across the target interval."

  - name: data quality constraints for loaded partitions
    query: |
      SELECT COUNT(*)
      FROM staging.stg_price_changes
      WHERE date >= DATE('{{ start_date }}') AND date < DATE('{{ end_date }}')
        AND (
          timestamp_received IS NULL OR
          market_id IS NULL OR
          side IS NULL OR side NOT IN ('YES', 'NO') OR
          best_bid IS NULL OR best_bid < 0.0 OR best_bid > 1.0 OR
          best_ask IS NULL OR best_ask < 0.0 OR best_ask > 1.0 OR
          spread IS NULL OR best_ask < best_bid
        )
    value: 0
    blocking: true
    description: "Consolidated check for nulls, probability bounds [0,1], accepted values, and inverted markets (ask < bid). Scans only loaded data."

@bruin */

-- ==============================================================================
-- JSON PARSING & FLATTENING
-- ==============================================================================
SELECT
  timestamp_received,
  market_id,
  JSON_VALUE(data, '$.token_id')    AS token_id,
  JSON_VALUE(data, '$.side')        AS side,
  CAST(JSON_VALUE(data, '$.best_bid')  AS FLOAT64) AS best_bid,
  CAST(JSON_VALUE(data, '$.best_ask')  AS FLOAT64) AS best_ask,
  
  -- Calculate dynamic spread
  CAST(JSON_VALUE(data, '$.best_ask')  AS FLOAT64) 
    - CAST(JSON_VALUE(data, '$.best_bid') AS FLOAT64) AS spread,
    
  CAST(JSON_VALUE(data, '$.change_size') AS FLOAT64) AS change_size,
  JSON_VALUE(data, '$.change_side') AS change_side,
  DATE(timestamp_received)          AS date
FROM staging.stg_orderbook

-- ==============================================================================
-- DATA QUALITY SHIELDS & INCREMENTAL FILTERING
-- ==============================================================================
WHERE update_type = 'price_change'
  AND DATE(timestamp_received) >= DATE('{{ start_date }}')
  AND DATE(timestamp_received) < DATE('{{ end_date }}')
  
  -- Ensure core pricing fields exist before casting
  AND JSON_VALUE(data, '$.best_bid') IS NOT NULL
  AND JSON_VALUE(data, '$.best_ask') IS NOT NULL
  
  -- Data Quality Integrity Checks
  AND JSON_VALUE(data, '$.side') IN ('YES', 'NO')
  AND CAST(JSON_VALUE(data, '$.best_bid') AS FLOAT64) BETWEEN 0.0 AND 1.0
  AND CAST(JSON_VALUE(data, '$.best_ask') AS FLOAT64) BETWEEN 0.0 AND 1.0
  
  -- Filter out inverted markets (where bid is higher than ask)
  AND CAST(JSON_VALUE(data, '$.best_ask') AS FLOAT64) >= CAST(JSON_VALUE(data, '$.best_bid') AS FLOAT64)