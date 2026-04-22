""" @bruin
name: staging.stg_orderbook
type: python
image: python:3.12
description: >
  Loads raw hourly Polymarket Parquet files from GCS into staging.stg_orderbook,
  one partition per date, for every date in [start_date, end_date).
  Uses LOAD DATA INTO ... OVERWRITE PARTITIONS for idempotent partition-level
  reloads — safe to re-run. Handles both daily scheduled runs (single date) and
  multi-day backfills (date range) without any external scripts.
  Source: gs://polymarket-raw-parquet/raw/orderbook/date={date}/*.parquet
  Grain: one row per token per orderbook event. Binary markets produce two rows
  per event (YES token + NO token), so row count is ~2x event count.
secrets:
  - key: bruin_gcp
depends:
  - raw.upload_to_gcs

columns:
  - name: timestamp_received
    type: timestamp
    description: "When the orderbook update was received by the archiver"
  - name: timestamp_created_at
    type: timestamp
    description: "When the orderbook update was created by Polymarket"
  - name: market_id
    type: string
    description: "Polymarket condition ID"
  - name: update_type
    type: string
    description: "Either price_change or book_snapshot"
  - name: data
    type: string
    description: >
      Raw JSON payload from the orderbook. For price_change rows the object
      contains: update_type (string), market_id (hex condition ID), token_id
      (integer string), side (YES|NO), best_bid (decimal string), best_ask
      (decimal string), timestamp (float unix), change_price (decimal string),
      change_size (integer string), change_side (BUY|SELL).
      book_snapshot rows have a different shape — an array of price levels.
      Parsed downstream in stg_price_changes via JSON_VALUE().
      Example (price_change, YES side):
        {"update_type": "price_change",
         "market_id": "0x00000977...ff707",
         "token_id": "44554681...248",
         "side": "YES",
         "best_bid": "0.014",
         "best_ask": "0.016",
         "timestamp": 1773446521.696,
         "change_price": "0.189",
         "change_size": "0",
         "change_side": "SELL"}

custom_checks:
  - name: no partitions are empty after load
    query: |
      SELECT CASE
        WHEN COUNT(*) > 0 THEN 0
        ELSE 1
      END
      FROM staging.stg_orderbook
      WHERE DATE(timestamp_received) >= DATE('{{ start_date }}') AND DATE(timestamp_received) < DATE('{{ end_date }}')
    value: 0
    blocking: true
    description: >
      Returns 0 (pass) if any rows exist across the loaded date range,
      1 (fail) if the entire range is empty — indicates a source or credential failure.

  - name: data quality constraints across loaded range
    query: |
      SELECT COUNT(*)
      FROM staging.stg_orderbook
      WHERE DATE(timestamp_received) >= DATE('{{ start_date }}') AND DATE(timestamp_received) < DATE('{{ end_date }}')
        AND (
          timestamp_received IS NULL OR
          market_id          IS NULL OR
          update_type        IS NULL OR
          data               IS NULL OR
          update_type NOT IN ('price_change', 'book_snapshot')
        )
    value: 0
    blocking: true
    description: >
      Checks for nulls and invalid update_type values across all loaded partitions.
      Scans only the loaded date range — no full-table bytes billed.

  - name: both update_type values are present across loaded range
    query: |
      SELECT COUNT(*) FROM (
        SELECT update_type
        FROM staging.stg_orderbook
        WHERE DATE(timestamp_received) >= DATE('{{ start_date }}') AND DATE(timestamp_received) < DATE('{{ end_date }}')
        GROUP BY update_type
        HAVING update_type IN ('price_change', 'book_snapshot')
      )
    value: 2
    blocking: false
    description: >
      Warns if only one update_type landed across the entire loaded range.
      Non-blocking: a missing book_snapshot type is unusual but not pipeline-fatal.

@bruin """

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, timedelta
from typing import Iterator

from google.cloud import bigquery
from google.oauth2 import service_account

# ==============================================================================
# LOGGING CONFIGURATION
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ==============================================================================
# CONSTANTS & SQL TEMPLATES
# ==============================================================================
BUCKET        = "polymarket-raw-parquet"
BQ_TABLE      = "staging.stg_orderbook"
PARTITION_COL = "timestamp_received"

# Template for idempotent partition overwrite
LOAD_SQL = """\
LOAD DATA INTO {table} OVERWRITE
PARTITIONS ({col} = '{date}')
PARTITION BY DATE({col})
CLUSTER BY market_id, update_type
FROM FILES (
  uris   = ['gs://{bucket}/raw/orderbook/date={date}/*.parquet'],
  format = 'PARQUET'
)
"""

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
def daterange(start: date, end: date) -> Iterator[date]:
    # Yields inclusive dates sequentially across the defined period
    current = start
    while current < end:
        yield current
        current += timedelta(days=1)

def partition_row_count(client: bigquery.Client, d: date) -> int:
    """Return the number of rows already in stg_orderbook for this date."""
    row = next(client.query(f"""
        SELECT COUNT(*) AS n
        FROM {BQ_TABLE}
        WHERE DATE({PARTITION_COL}) = DATE('{d.isoformat()}')
    """).result())
    return int(row.n)

def load_partition(client: bigquery.Client, d: date) -> None:
    """Execute LOAD DATA for a single date partition."""
    sql = LOAD_SQL.format(
        table=BQ_TABLE,
        col=PARTITION_COL,
        date=d.isoformat(),
        bucket=BUCKET,
    )
    log.info("Running LOAD DATA for partition %s ...", d)
    client.query(sql).result()
    log.info("OK  Partition %s loaded.", d)

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
def main() -> None:
    # Setup BigQuery client and credentials
    serv_acc    = json.loads(os.environ["bruin_gcp"])
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(serv_acc["service_account_json"])
    )
    client = bigquery.Client(
        credentials=credentials,
        project=serv_acc["project_id"],
    )

    # Establish the ingestion range using Bruin environment variables
    start = date.fromisoformat(os.environ["BRUIN_START_DATE"][:10])
    end   = date.fromisoformat(os.environ["BRUIN_END_DATE"][:10])

    all_dates = list(daterange(start, end))
    total     = len(all_dates)

    log.info(
        "staging.stg_orderbook — loading %d partition%s  [%s -> %s]",
        total, "s" if total != 1 else "", start, end,
    )

    # Sequentially process each date partition
    failed: list[str] = []

    for i, d in enumerate(all_dates, start=1):
        date_str = d.isoformat()
        log.info("-" * 60)
        log.info("[%d/%d]  %s", i, total, date_str)

        try:
            existing = partition_row_count(client, d)
            if existing > 0:
                log.info("NOTE  Partition %s already has %d rows — overwriting.", date_str, existing)
        except Exception as exc:
            # stg_orderbook may not exist yet on the very first run
            log.info("Could not check existing rows (%s) — proceeding with load.", exc)

        try:
            load_partition(client, d)
        except Exception as exc:
            log.error("FAIL  Partition %s — %s", date_str, exc)
            failed.append(date_str)

    # Summarize execution outcome
    log.info("=" * 60)
    log.info(
        "Load complete.  loaded=%d  failed=%d",
        total - len(failed), len(failed),
    )

    if failed:
        log.error("Failed partitions: %s", ", ".join(failed))
        sys.exit(1)

if __name__ == "__main__":
    main()