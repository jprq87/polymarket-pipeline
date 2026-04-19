# Polymarket Pipeline — Infrastructure

This folder contains the Terraform configuration that provisions the full GCP infrastructure for the Polymarket Pipeline. A single `terraform apply` creates the GCS bucket, BigQuery datasets, Secret Manager secret containers, all IAM bindings, and the Compute VM that runs the pipeline on a daily cron schedule.

---

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) v1.0+
- [gcloud CLI](https://cloud.google.com/sdk/docs/install) — authenticated with `gcloud auth login`
- A GCP project with billing enabled

---

## Step 1 — Authenticate with GCP

Log in with your personal Google account and set up Application Default Credentials. Terraform uses ADC to create resources on your behalf.

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project polymarket-pulse-2026
```

---

## Step 2 — Enable required GCP APIs

These APIs must be enabled before Terraform can create any resources.

```bash
gcloud services enable \
  compute.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  secretmanager.googleapis.com \
  iam.googleapis.com \
  --project=polymarket-pulse-2026
```

> Enabling the Compute Engine API can take 1–3 minutes to initialize the default VPC networks. Wait for it to complete before running `terraform apply`.

---

## Step 3 — Create a service account

The pipeline runs as a dedicated service account (SA). This is a non-human identity that the VM assumes when making GCP API calls — your personal credentials are never used on the VM.

```bash
gcloud iam service-accounts create polymarket-pipeline-sa \
  --display-name="Polymarket Pipeline SA" \
  --project=polymarket-pulse-2026
```

Download the SA key file:

```bash
gcloud iam service-accounts keys create ./polymarket-sa.json \
  --iam-account=polymarket-pipeline-sa@polymarket-pulse-2026.iam.gserviceaccount.com
```

Keep `polymarket-sa.json` safe — you will need it in Steps 6 and 8. Never commit this file to git.

**You do not need to assign any roles manually.** Terraform handles all IAM bindings during `terraform apply`. The SA receives exactly these permissions, scoped to only the resources it touches:

| Role | Scope | Purpose |
|---|---|---|
| `roles/storage.objectAdmin` | `polymarket-raw-parquet` bucket | Read/write raw Parquet files |
| `roles/bigquery.dataEditor` | Project level | Read/write staging and reports tables |
| `roles/bigquery.jobUser` | Project level | Execute BigQuery queries and load jobs |
| `roles/secretmanager.secretAccessor` | Both secrets | Fetch credentials at VM startup |

---

## Step 4 — Fill in `main.tf`

Open `main.tf` and update the two empty fields in the `locals` block:

```hcl
locals {
  pipeline_sa_email = ""   # ← polymarket-pipeline-sa@polymarket-pulse-2026.iam.gserviceaccount.com
  image_name        = ""   # ← your Docker image, e.g. jprq/polymarket-pipeline:latest
}
```

---

## Step 5 — Check `startup-script.sh` line endings

The startup script must be saved with **LF line endings**, not CRLF. Windows editors often default to CRLF, which causes `bash` to fail silently on the VM.

In VS Code, check the bottom-right status bar. If it says `CRLF`, click it and switch to `LF` before saving.

Alternatively, convert with:

```bash
sed -i 's/\r//' startup-script.sh
```

---

## Step 6 — Prepare `.bruin.yml`

Your `.bruin.yml` must include a `prod` environment pointing to `./bruin_gcp.json`. The pipeline runs under `--environment prod` on the VM.

```yaml
default_environment: dev

environments:
  dev:
    connections:
      google_cloud_platform:
        - name: "bruin_gcp"
          service_account_file: "./polymarket-sa.json"
          project_id: "polymarket-pulse-2026"
          location: "us-central1"
      gcs:
        - name: "bruin_gcs"
          service_account_file: "./polymarket-sa.json"

  prod:
    connections:
      google_cloud_platform:
        - name: "bruin_gcp"
          service_account_file: "./bruin_gcp.json"
          project_id: "polymarket-pulse-2026"
          location: "us-central1"
      gcs:
        - name: "bruin_gcs"
          service_account_file: "./bruin_gcp.json"
```

> Note the difference: `dev` uses `./polymarket-sa.json` (your local key filename), `prod` uses `./bruin_gcp.json` (the filename the VM writes after fetching from Secret Manager).

---

## Step 7 — Run Terraform

```bash
terraform init
terraform plan    # review what will be created
terraform apply   # type 'yes' when prompted
```

This provisions:
- `polymarket-raw-parquet` GCS bucket
- `staging` and `reports` BigQuery datasets
- `bruin-gcp-credentials` and `bruin-yml-config` Secret Manager containers
- All IAM bindings for the service account
- `polymarket-docker-host` Compute VM (`e2-medium`, Ubuntu 24.04, `us-central1-a`)

> The VM boots immediately after apply but the startup script will fail at the secrets step — the containers are empty at this point. This is expected. You will fix it in the next step.

---

## Step 8 — Upload secrets

Upload the actual file contents into the Secret Manager containers Terraform just created. Run these commands from the directory containing `polymarket-sa.json` and `.bruin.yml`.

```bash
gcloud secrets versions add bruin-gcp-credentials \
  --data-file=./polymarket-sa.json \
  --project=polymarket-pulse-2026

gcloud secrets versions add bruin-yml-config \
  --data-file=./.bruin.yml \
  --project=polymarket-pulse-2026
```

**Why not upload secrets through Terraform?** Terraform stores all resource state in a plain-text file (`terraform.tfstate`). Uploading secret values through Terraform would expose your credentials unencrypted in that file. Secret versions are always managed manually via `gcloud`.

---

## Step 9 — Reset the VM

Force the startup script to re-run now that the secrets have content:

```bash
gcloud compute instances reset polymarket-docker-host \
  --zone us-central1-a
```

The startup script takes approximately 5 minutes to complete. It will:

1. Install system packages, Docker, and the gcloud SDK
2. Fetch both secrets from Secret Manager
3. Pull the Docker image from Docker Hub
4. Register the daily cron job

---

## Step 10 — Verify

SSH into the VM and confirm everything is in place:

```bash
gcloud compute ssh polymarket-docker-host --zone us-central1-a
```

Run the following checks:

```bash
# Startup script completed successfully
cat /var/log/startup-script.log

# Both secret files were fetched to the expected paths
ls -la /opt/polymarket/

# Cron job is registered
cat /etc/cron.d/polymarket-pipeline

# Docker image is available
sudo docker images
```

The last line of the startup log should read:

```
Startup script complete: <timestamp>
```

---

## Step 11 — Run the pipeline manually

Before relying on the cron schedule, trigger a manual run to confirm the pipeline works end to end:

```bash
sudo docker run --rm \
  -v /opt/polymarket/.bruin.yml:/app/.bruin.yml:ro \
  -v /opt/polymarket/bruin_gcp.json:/app/bruin_gcp.json:ro \
  -v /var/log/pipeline:/app/logs \
  jprq/polymarket-pipeline:latest \
  --environment prod \
  --start-date 2026-04-01 \
  --end-date 2026-04-02
```

---

## Cron schedule

The pipeline runs automatically every day at **17:30 UTC (12:30 PM Lima, UTC-5)**. It processes data for the current calendar day using dynamic dates injected at runtime.

To check pipeline run logs:

```bash
cat /var/log/pipeline-cron.log
tail -f /var/log/pipeline-cron.log   # live tail during a run
```

---

## Rotating credentials

To update the SA key or `.bruin.yml` without reprovisioning the VM:

```bash
# Add a new secret version — the VM picks it up on next restart
gcloud secrets versions add bruin-gcp-credentials \
  --data-file=./new_polymarket-sa.json \
  --project=polymarket-pulse-2026
```

To apply the new version immediately without restarting the VM:

```bash
gcloud compute ssh polymarket-docker-host --zone us-central1-a

sudo gcloud secrets versions access latest \
  --secret=bruin-gcp-credentials \
  --project=polymarket-pulse-2026 \
  > /opt/polymarket/bruin_gcp.json

sudo chmod 600 /opt/polymarket/bruin_gcp.json
```

---

## VM cost management

The VM incurs compute charges while running. Stop it when the pipeline is not needed:

```bash
# Stop (no compute charges; disk is retained)
gcloud compute instances stop polymarket-docker-host --zone us-central1-a

# Start again (cron resumes normally; startup script does not re-run)
gcloud compute instances start polymarket-docker-host --zone us-central1-a
```

---

## File reference

| File | Purpose |
|---|---|
| `main.tf` | All GCP infrastructure defined as code |
| `startup-script.sh` | VM bootstrap — runs once on first boot |
| `polymarket-sa.json` | Service account key — **never commit to git** |
| `.bruin.yml` | Bruin connection config — **never commit to git** |
| `terraform.tfstate` | Terraform state — **never commit to git** |