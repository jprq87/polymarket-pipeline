""" @bruin
name: staging.fetch_markets
type: python
image: python:3.12
description: >
  Fetches market metadata from the Polymarket Gamma API concurrently and merges
  it into staging.dim_markets.
  Grain: one row per market_id (condition ID). Insert-only MERGE — existing rows
  are never overwritten, making the table append-safe across backfills.
  Ghost markets: market IDs the Gamma API cannot resolve are written with
  is_ghost = TRUE to preserve referential integrity downstream. Filtered out in
  stg_markets before any report joins.
  Category resolution: canonical priority list of 8 known tag IDs
  (Politics, Finance, Crypto, Sports, Games, Tech, Culture, Geopolitics).
  Falls back to first non-rewards tag label if none match.
secrets:
  - key: bruin_gcp
depends:
  - staging.stg_orderbook

columns:
  - name: market_id
    type: string
    description: "Polymarket condition ID. Primary key — join target for all report assets via stg_markets."
  - name: question
    type: string
    description: "Human-readable market question from the Gamma API. ASCII-sanitized."
  - name: event_title
    type: string
    description: "Title of the parent event this market belongs to."
  - name: category
    type: string
    description: >
      Canonical category label. One of: Politics, Finance, Crypto, Sports,
      Games, Tech, Culture, Geopolitics, or Unknown. Resolved via priority
      list — see CANONICAL_PRIORITY in asset code.
      Example: "Politics"
  - name: category_slug
    type: string
    description: >
      URL-safe slug for the category. Example: "pop-culture" for Culture,
      "unknown" for unresolved markets.
  - name: end_date
    type: string
    description: "Market resolution date in YYYY-MM-DD format. NULL if not provided by the API."
  - name: closed
    type: boolean
    description: "Whether the market is closed at time of fetch."
  - name: resolution_status
    type: string
    description: "UMA resolution status string from the API. NULL if unresolved or empty."
  - name: is_ghost
    type: boolean
    description: >
      TRUE if the Gamma API returned no data for this market_id. Ghost rows
      exist solely to prevent broken foreign keys downstream. Filtered out
      in stg_markets. A ghost ratio above 10% triggers a non-blocking warning.

custom_checks:
  - name: dim_markets is not empty after merge
    query: |
      SELECT CASE
        WHEN COUNT(*) > 0 THEN 0
        ELSE 1
      END
      FROM `de-final-project-jprq.staging.dim_markets`
    value: 0
    blocking: true
    description: "Returns 0 (pass) if rows exist in dim_markets after the merge, 1 (fail) if the table is empty — indicates an API or credential failure."

  - name: ghost market ratio is not suspiciously high
    query: |
      SELECT COUNT(*) FROM (
        SELECT
          SAFE_DIVIDE(
            COUNTIF(is_ghost = TRUE),
            COUNT(*)
          ) AS ghost_ratio
        FROM `de-final-project-jprq.staging.dim_markets`
        HAVING ghost_ratio > 0.10
      )
    value: 0
    blocking: false
    description: "Warns if more than 10% of markets are ghosts — may indicate Gamma API degradation or connectivity issues."

  - name: no null market_ids
    query: |
      SELECT COUNT(*)
      FROM `de-final-project-jprq.staging.dim_markets`
      WHERE market_id IS NULL
    value: 0
    blocking: true
    description: "A null market_id would fan out every downstream join with stg_price_changes, corrupting all report tables."

@bruin """

import os
import sys
import json
import time
import logging
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

# ==============================================================================
# LOGGING CONFIGURATION
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ==============================================================================
# CONFIGURATION
# ==============================================================================
BATCH_SIZE            = 20
MAX_WORKERS           = 8
SLEEP_BETWEEN_BATCHES = 0.1
TARGET_TABLE          = "de-final-project-jprq.staging.dim_markets"
TEMP_TABLE            = "de-final-project-jprq.staging.dim_markets_tmp"

# Canonical mappings for platform-level tags
CANONICAL_TAG_LABELS = {
    "2":      "Politics",
    "120":    "Finance",
    "21":     "Crypto",
    "1":      "Sports",
    "100639": "Games",
    "1401":   "Tech",
    "596":    "Culture",
    "100265": "Geopolitics",
}

CANONICAL_TAG_SLUGS = {
    "2":      "politics",
    "120":    "finance",
    "21":     "crypto",
    "1":      "sports",
    "100639": "games",
    "1401":   "tech",
    "596":    "pop-culture",
    "100265": "geopolitics",
}

# Priority queue to resolve conflicting multi-tags
CANONICAL_PRIORITY = [
    "596",    # Culture
    "120",    # Finance
    "2",      # Politics
    "100265", # Geopolitics
    "21",     # Crypto
    "100639", # Games
    "1",      # Sports
    "1401",   # Tech
]

# ==============================================================================
# CORE FUNCTIONS
# ==============================================================================
def pick_category(tags: list) -> tuple:
    # Resolve the most prominent category based on predefined platform priority
    tag_ids = {str(tag.get("id", "")) for tag in tags}
    result_id = None
    for tag_id in CANONICAL_PRIORITY:
        if tag_id in tag_ids:
            result_id = tag_id
    if result_id:
        return CANONICAL_TAG_LABELS[result_id], CANONICAL_TAG_SLUGS[result_id]

    # Fallback: Find the first valid non-rewards label
    for tag in tags:
        label = tag.get("label", "")
        if label and "rewards" not in label.lower():
            return label, label.lower().replace(" ", "-")

    return "Unknown", "unknown"

def sanitize_text(text):
    # Strip erratic unicode chars and newlines from API string payloads
    if not isinstance(text, str):
        return text
    text = re.sub(r'[\n\r\u2028\u2029]+', ' ', text).strip()
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore').decode('ascii')
    return re.sub(r' +', ' ', text).strip()

# ==============================================================================
# THREAD-SAFE SESSION OPTIMIZATION
# ==============================================================================
# Connection pooling and automated retries for API resilience
session = requests.Session()
adapter = HTTPAdapter(
    pool_connections=MAX_WORKERS, 
    pool_maxsize=MAX_WORKERS,
    max_retries=Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
)
session.mount("https://", adapter)

def process_batch(batch_ids, batch_num, total_batches):
    """The function that each worker thread will execute independently."""
    
    if batch_num % 10 == 0 or batch_num == total_batches:
        logger.info(f"Processing Batch {batch_num}/{total_batches} (Concurrent)...")

    batch_records = []
    fetched_this_batch = set()
    local_event_cache = {}

    # Attempt to fetch market batch
    try:
        r = session.get(
            "https://gamma-api.polymarket.com/markets",
            params={"condition_ids": batch_ids},
            timeout=15
        )
        r.raise_for_status()
        markets = r.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error on batch {batch_num}: {e}")
        markets = []

    # Parse and structure valid market responses
    if isinstance(markets, list):
        for m in markets:
            if not isinstance(m, dict):
                continue
            
            market_id = m.get("conditionId")
            if not market_id:
                continue

            fetched_this_batch.add(str(market_id))
            events_list = m.get("events", [])
            event = events_list[0] if isinstance(events_list, list) and events_list and isinstance(events_list[0], dict) else {}
            
            event_id = event.get("id")
            tags_list = []

            # Resolve secondary event tags if missing from initial payload
            if event_id:
                if event_id not in local_event_cache:
                    try:
                        tag_res = session.get(f"https://gamma-api.polymarket.com/events/{event_id}/tags", timeout=10)
                        if tag_res.status_code == 200:
                            local_event_cache[event_id] = tag_res.json()
                        else:
                            local_event_cache[event_id] = []
                            
                        # RATE LIMIT PROTECTION: Sleep 0.1s after hitting the individual event tag endpoint
                        time.sleep(0.1)
                        
                    except Exception as e:
                        logger.warning(f"Failed to fetch tags for event {event_id}: {e}")
                        local_event_cache[event_id] = []
                
                tags_list = local_event_cache[event_id]
            else:
                tags_list = event.get("tags", [])

            category, category_slug = pick_category(tags_list)

            # Extract safe values for dates and resolution status
            raw_end_date = m.get("endDateIso") or m.get("endDate")
            raw_res_status = m.get("umaResolutionStatus") or m.get("umaResolutionStatuses")
            if raw_res_status in ("[]", "", [], None):
                raw_res_status = None
            elif isinstance(raw_res_status, list):
                raw_res_status = raw_res_status[0]

            batch_records.append({
                "market_id":          str(market_id),
                "question":           sanitize_text(m.get("question"))          or "Unknown",
                "event_title":        sanitize_text(event.get("title"))         or "Unknown",
                "category":           category,
                "category_slug":      category_slug,
                "end_date":           raw_end_date,
                "closed":             bool(m.get("closed", False)),
                "resolution_status":  raw_res_status,
                "is_ghost":           False,
            })

    # Impute missing markets as "Ghosts" to preserve referential integrity
    for ghost_id in set(str(b) for b in batch_ids) - fetched_this_batch:
        batch_records.append({
            "market_id":          ghost_id,
            "question":           "Ghost Market - no API data",
            "event_title":        "Unknown",
            "category":           "Unknown",
            "category_slug":      "unknown",
            "end_date":           None,
            "closed":             pd.NA,
            "resolution_status":  None,
            "is_ghost":           True,
        })

    time.sleep(SLEEP_BETWEEN_BATCHES)
    return batch_records

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
def main():
    # Setup BigQuery client and credentials
    serv_acc = json.loads(os.environ["bruin_gcp"])
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(serv_acc["service_account_json"])
    )
    bq_client = bigquery.Client(credentials=credentials, project=serv_acc["project_id"])

    # Determine required IDs by diffing staging against the target dimension
    logger.info("Fetching distinct market_ids from staging.stg_orderbook...")
    market_ids = [
        row.market_id for row in bq_client.query("""
            SELECT DISTINCT CAST(market_id AS STRING) AS market_id
            FROM staging.stg_orderbook
            WHERE market_id IS NOT NULL
        """).result()
    ]
    logger.info(f"Found {len(market_ids):,} unique market IDs.")

    if not market_ids:
        sys.exit(0)

    try:
        existing_ids = {
            row.market_id for row in bq_client.query(f"SELECT DISTINCT market_id FROM `{TARGET_TABLE}`").result()
        }
        logger.info(f"{len(existing_ids):,} market IDs already exist in dim_markets.")
    except NotFound:
        existing_ids = set()
        logger.info("dim_markets does not exist yet — performing full fetch.")
    
    market_ids_to_fetch = [m for m in market_ids if m not in existing_ids]
    logger.info(f"{len(market_ids_to_fetch):,} new market IDs to fetch from Gamma API.")

    if not market_ids_to_fetch:
        logger.info("dim_markets is up to date. Nothing to fetch.")
        sys.exit(0)

    # Initialize ThreadPool and dispatch batches
    all_records = []
    batches = [
        market_ids_to_fetch[i:i + BATCH_SIZE] 
        for i in range(0, len(market_ids_to_fetch), BATCH_SIZE)
    ]
    total_batches = len(batches)

    logger.info(f"Firing up ThreadPoolExecutor with {MAX_WORKERS} workers...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {
            executor.submit(process_batch, batch, idx + 1, total_batches): batch 
            for idx, batch in enumerate(batches)
        }
        
        for future in as_completed(future_to_batch):
            try:
                result_records = future.result()
                all_records.extend(result_records)
            except Exception as exc:
                logger.error(f"A batch generated an exception: {exc}")

    if not all_records:
        logger.error("No records returned from threads.")
        sys.exit(1)

    logger.info(f"Fetched {len(all_records):,} records ({sum(1 for r in all_records if r['is_ghost'])} ghosts).")

    # Format dataframe for insertion
    df = pd.DataFrame(all_records)
    df["closed"] = df["closed"].astype(pd.BooleanDtype())

    # Safely parse all dates and standardize them to YYYY-MM-DD format
    # errors='coerce' turns unparseable garbage into NaT (which BigQuery reads as NULL)
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.strftime('%Y-%m-%d')

    # BigQuery Upload & Merge Operation
    try:
        logger.info(f"Writing {len(df):,} records to temp table...")
        job = bq_client.load_table_from_dataframe(
            df, TEMP_TABLE,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_IF_NEEDED",
            )
        )
        job.result()
        logger.info("Temp table written successfully.")

        bq_client.query(f"""
            CREATE TABLE IF NOT EXISTS `{TARGET_TABLE}` (
                market_id          STRING,
                question           STRING,
                event_title        STRING,
                category           STRING,
                category_slug      STRING,
                end_date           STRING,
                closed             BOOL,
                resolution_status  STRING,
                is_ghost           BOOL
            )
        """).result()
        logger.info("Target table ready.")

        # Upsert records ensuring no duplicate metadata exists
        bq_client.query(f"""
            MERGE `{TARGET_TABLE}` AS target
            USING `{TEMP_TABLE}` AS source
            ON target.market_id = source.market_id
            WHEN NOT MATCHED THEN
                INSERT (
                    market_id, question, event_title, category, category_slug,
                    end_date, closed, resolution_status, is_ghost
                )
                VALUES (
                    source.market_id, source.question, source.event_title, source.category,
                    source.category_slug, source.end_date, source.closed,
                    source.resolution_status, source.is_ghost
                )
        """).result()
        logger.info("Merge into staging.dim_markets complete.")

        bq_client.delete_table(TEMP_TABLE, not_found_ok=True)

    except Exception as e:
        logger.critical(f"Failed during BigQuery write/merge: {e}")
        try:
            bq_client.delete_table(TEMP_TABLE, not_found_ok=True)
        except Exception:
            pass
        sys.exit(1)

    logger.info("staging.fetch_markets completed successfully.")

if __name__ == "__main__":
    main()