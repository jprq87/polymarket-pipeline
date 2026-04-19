"""@bruin
name: raw.upload_to_gcs
type: python
image: python:3.12
description: "Streams pmxt hourly Parquet files into GCS without buffering the full file in memory. Source: https://r2.pmxt.dev/polymarket_orderbook_{YYYY-MM-DDTHH}.parquet. Files land at gs://polymarket-raw-parquet/raw/orderbook/date={date}/{hour}.parquet. Idempotent: skips any blob that already exists. Runs up to 4 concurrent uploads. Exits non-zero if any slot fails, blocking downstream stg_orderbook from running on incomplete data."
secrets:
    - key: bruin_gcp
@bruin"""

import os
import sys
import json
import requests
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

# ==============================================================================
# CONFIGURATION
# ==============================================================================
BASE_URL    = "https://r2.pmxt.dev/polymarket_orderbook_{}.parquet"
BUCKET_NAME = "polymarket-raw-parquet"
MAX_WORKERS = 4

# ==============================================================================
# INITIALIZATION (GCS CLIENT)
# ==============================================================================
# Parse injected GCP credentials from Bruin environment variables
serv_acc    = json.loads(os.environ["bruin_gcp"])
credentials = service_account.Credentials.from_service_account_info(
    json.loads(serv_acc["service_account_json"])
)
client = storage.Client(credentials=credentials, project=serv_acc["project_id"])

# ==============================================================================
# CORE FUNCTIONS
# ==============================================================================
def upload_stream_to_gcs(hour_str: str) -> tuple:
    # Extract date partition and construct target GCS path
    date_str  = hour_str[:10]
    blob_name = f"raw/orderbook/date={date_str}/{hour_str}.parquet"
    bucket    = client.bucket(BUCKET_NAME)

    # Idempotency check: Skip download if file already exists in GCS
    if bucket.blob(blob_name).exists():
        print(f"  EXISTS {hour_str} - skipping")
        return hour_str, True

    url = BASE_URL.format(hour_str)
    try:
        # Stream the download to avoid memory bloat on large parquet files
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            blob = bucket.blob(blob_name)
            blob.upload_from_file(
                r.raw,
                content_type="application/octet-stream",
                timeout=600
            )
        print(f"  OK {hour_str} -> gs://{BUCKET_NAME}/{blob_name}")
        return hour_str, True
    except Exception as e:
        print(f"  FAIL {hour_str} - {e}")
        return hour_str, False

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
# Retrieve run dates from Bruin orchestrator
start_date = os.environ["BRUIN_START_DATE"]
end_date   = os.environ["BRUIN_END_DATE"]

start_dt = pd.to_datetime(start_date, utc=True)
end_dt   = pd.to_datetime(end_date,   utc=True)

# Generate hourly slots for the target time window
slots, current = [], start_dt
while current < end_dt:
    slots.append(current.strftime("%Y-%m-%dT%H"))
    current += timedelta(hours=1)

print(f"Uploading {len(slots)} hourly files ({start_date} -> {end_date}) "
      f"with {MAX_WORKERS} workers...")

failed = []
# Execute concurrent uploads using a ThreadPool
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(upload_stream_to_gcs, s): s for s in slots}
    for future in as_completed(futures):
        hour_str, success = future.result()
        if not success:
            failed.append(hour_str)

print(f"\nDone. {len(slots) - len(failed)}/{len(slots)} files uploaded or already existed.")

# Halt pipeline execution if any hours failed to download
if failed:
    print("Failed slots:", sorted(failed))
    sys.exit(1)