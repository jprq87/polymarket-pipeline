import json
from google.cloud import storage
from google.oauth2 import service_account

with open("bruin_gcp.json") as f:
    sa = json.load(f)

credentials = service_account.Credentials.from_service_account_info(sa)
client = storage.Client(credentials=credentials, project=sa["project_id"])

bucket = client.bucket("polymarket-raw-parquet")
blobs  = list(bucket.list_blobs(prefix="raw/orderbook/"))

print(f"Checking {len(blobs)} files...")

corrupted = []
for blob in blobs:
    size = blob.size

    # Download only first 4 and last 4 bytes
    header = blob.download_as_bytes(start=0,        end=3)
    footer = blob.download_as_bytes(start=size - 4, end=size - 1)

    if header != b"PAR1" or footer != b"PAR1":
        print(f"  CORRUPTED: {blob.name} (size: {size:,} bytes)")
        corrupted.append(blob.name)
    else:
        print(f"  OK: {blob.name}")

print(f"\nFound {len(corrupted)} corrupted files.")
for name in corrupted:
    print(f"  -> {name}")

# Delete corrupted files so they can be re-uploaded
if corrupted:
    print("\nDeleting corrupted files...")
    for name in corrupted:
        bucket.blob(name).delete()
        print(f"  Deleted: {name}")
    print("Done. Re-run the upload asset to replace them.")