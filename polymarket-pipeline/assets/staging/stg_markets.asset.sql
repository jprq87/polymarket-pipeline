/* @bruin
name: staging.stg_markets
type: bq.sql
connection: bruin_gcp
description: >
  Cleansed dimension view over staging.dim_markets. Filters out ghost markets
  and applies COALESCE defaults for categorical fields. Serves as the primary
  join key for downstream reporting assets.
  Grain: one row per market_id — ghost markets (is_ghost = TRUE) are excluded,
  so row count will be less than dim_markets. All four report assets join against
  this view, never against dim_markets directly.
materialization:
  type: view
depends:
  - staging.fetch_markets

columns:
  - name: market_id
    type: string
    description: "Polymarket condition ID — primary key for this dimension and join key for all downstream report assets"
    primary_key: true
    checks:
      - name: not_null
  - name: question
    type: string
    description: "Full question text as returned by the Gamma API (e.g. 'Will X happen by date Y?')"
  - name: event_title
    type: string
    description: "Title of the parent event this market belongs to, from events[0].title in the Gamma API response"
  - name: category
    type: string
    description: >
      Canonical category label inherited from dim_markets. One of: Politics,
      Finance, Crypto, Sports, Games, Tech, Culture, Geopolitics. COALESCE
      defaults NULL to 'Unknown'. Never NULL in this view.
      Example: "Politics"
  - name: category_slug
    type: string
    description: "URL slug for the market, sourced from the Gamma API slug field. Defaulted to 'unknown' via COALESCE."
  - name: end_date
    type: timestamp
    description: "Market resolution deadline, SAFE_CAST from the endDateIso string returned by the Gamma API. NULL if the API returned no end date."
  - name: closed
    type: boolean
    description: "Whether the market is closed per the Gamma API. NULL for ghost markets (filtered out by this view)."
  - name: resolution_status
    type: string
    description: "UMA resolution status string from the Gamma API (e.g. 'resolved', 'unresolved'). NULL if not yet resolved."

custom_checks:
  - name: no empty string market_ids
    query: |
      SELECT COUNT(*)
      FROM staging.stg_markets
      WHERE TRIM(market_id) = ''
    value: 0
    blocking: true
    description: "An empty-string market_id can enter dim_markets via the ghost path if stg_orderbook contains one — the no_null check in fetch_markets does not catch empty strings. A blank join key would silently corrupt all downstream LEFT JOINs."

@bruin */

-- ==============================================================================
-- DIMENSION CLEANSING & STANDARDIZATION
-- ==============================================================================
SELECT
  CAST(market_id AS STRING)                AS market_id,
  question,
  event_title,
  
  -- Enforce default values to prevent downstream NULL propagation
  COALESCE(category, 'Unknown')            AS category,
  COALESCE(category_slug, 'unknown')       AS category_slug,
  SAFE_CAST(end_date AS TIMESTAMP)         AS end_date,
  
  closed,
  resolution_status
FROM staging.dim_markets

-- ==============================================================================
-- EXCLUSIONS
-- ==============================================================================
WHERE is_ghost = FALSE