# ============================================================
# Polymarket Pipeline — GCP Infrastructure
# Project: de-final-project-jprq
# Region:  us-central1
#
# Resources managed:
#   - GCS bucket (data lake)
#   - BigQuery datasets (staging, reports)
#   - Secret Manager secrets (bruin credentials, bruin yml)
#   - IAM bindings (scoped per resource, least privilege)
#   - Compute VM (Docker host + cron scheduler)
#
# Usage:
#   terraform init
#   terraform import
#   terraform plan
#   terraform apply
# ============================================================

# ============================================================
# LOCALS — All configurable values in one place
# Update this block when migrating projects or renaming resources
# ============================================================

locals {
  # Identity
  pipeline_sa_email = ""

  # Project
  project_id        = ""
  zone              = "us-central1-a"
  region            = "us-central1"
  bq_location       = "US-CENTRAL1"  # Must match existing datasets and bucket

  # Resource names
  bucket_name       = "polymarket-raw-parquet"
  staging_dataset   = "staging"
  reports_dataset   = "reports"
  secret_creds      = "bruin-gcp-credentials"
  secret_bruin_yml  = "bruin-yml-config"

  # Pipeline
  image_name        = "jprq/polymarket-pipeline:latest"
  # 00:30 UTC = 19:30 Lima time (UTC-5)
  cron_schedule     = "30 0 * * *"
}

# ============================================================
# PROVIDER
# ============================================================

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project = local.project_id
  region  = local.region
  zone    = local.zone
}

# ============================================================
# GCS BUCKET — Raw parquet data lake
#
# Stores raw Polymarket orderbook parquet files before
# ingestion into BigQuery staging layer.
#
# Import existing:
#   terraform import google_storage_bucket.raw_parquet \
#     de-final-project-jprq/polymarket-raw-parquet
# ============================================================

resource "google_storage_bucket" "raw_parquet" {
  name          = local.bucket_name
  project       = local.project_id
  location      = local.bq_location
  storage_class = "STANDARD"
  force_destroy = false

  # Block all public access — internal pipeline use only
  public_access_prevention    = "enforced"
  uniform_bucket_level_access = true
}

# ============================================================
# BIGQUERY — Datasets
#
# staging: Raw ingestion layer.
#
# reports: Presentation layer. Contains all fact tables
#          consumed by Looker Studio dashboards.
#
# Import existing:
#   terraform import google_bigquery_dataset.staging \
#     de-final-project-jprq/staging
#   terraform import google_bigquery_dataset.reports \
#     de-final-project-jprq/reports
# ============================================================

resource "google_bigquery_dataset" "staging" {
  dataset_id  = local.staging_dataset
  project     = local.project_id
  location    = local.bq_location
  description = "Staging layer"

  # Prevent accidental table deletion via terraform destroy
  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "reports" {
  dataset_id  = local.reports_dataset
  project     = local.project_id
  location    = local.bq_location
  description = "Reports layer"

  # Prevent accidental table deletion via terraform destroy
  delete_contents_on_destroy = false
}

# ============================================================
# SECRET MANAGER — Pipeline secrets
#
# Secrets are uploaded manually once via gcloud:
#   gcloud secrets create bruin-gcp-credentials \
#     --data-file=./bruin_gcp.json \
#     --project=de-final-project-jprq
#
#   gcloud secrets create bruin-yml-config \
#     --data-file=./.bruin.yml \
#     --project=de-final-project-jprq
#
# To rotate credentials, add a new version:
#   gcloud secrets versions add bruin-gcp-credentials \
#     --data-file=./new_bruin_gcp.json
#
# Import existing:
#   terraform import google_secret_manager_secret.bruin_credentials \
#     projects/de-final-project-jprq/secrets/bruin-gcp-credentials
#   terraform import google_secret_manager_secret.bruin_yml \
#     projects/de-final-project-jprq/secrets/bruin-yml-config
# ============================================================

resource "google_secret_manager_secret" "bruin_credentials" {
  secret_id = local.secret_creds
  project   = local.project_id

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "bruin_yml" {
  secret_id = local.secret_bruin_yml
  project   = local.project_id

  replication {
    auto {}
  }
}

# ============================================================
# IAM — Least privilege bindings scoped per resource
#
# Principle: grant only what the pipeline SA needs,
# only on the specific resources it touches.
# ============================================================

# GCS — read/write objects in the raw parquet bucket only
resource "google_storage_bucket_iam_member" "sa_gcs" {
  bucket = google_storage_bucket.raw_parquet.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${local.pipeline_sa_email}"
}

# BigQuery — read/write data in staging dataset only
resource "google_bigquery_dataset_iam_member" "sa_staging" {
  dataset_id = google_bigquery_dataset.staging.dataset_id
  project    = local.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${local.pipeline_sa_email}"
}

# BigQuery — read/write data in reports dataset only
resource "google_bigquery_dataset_iam_member" "sa_reports" {
  dataset_id = google_bigquery_dataset.reports.dataset_id
  project    = local.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${local.pipeline_sa_email}"
}

# BigQuery — project-level job execution (required to run queries and loads)
resource "google_project_iam_member" "sa_bq_jobs" {
  project = local.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${local.pipeline_sa_email}"
}

# Secret Manager — fetch bruin SA credentials at VM startup
resource "google_secret_manager_secret_iam_member" "credentials_access" {
  secret_id = google_secret_manager_secret.bruin_credentials.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${local.pipeline_sa_email}"
  project   = local.project_id
}

# Secret Manager — fetch .bruin.yml config at VM startup
resource "google_secret_manager_secret_iam_member" "bruin_yml_access" {
  secret_id = google_secret_manager_secret.bruin_yml.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${local.pipeline_sa_email}"
  project   = local.project_id
}

# ============================================================
# VM INSTANCE — Docker host and cron scheduler
#
# Runs the Polymarket pipeline Docker image daily via cron.
# Bootstraps itself entirely from Secret Manager on first boot
# via startup-script.sh — no manual configuration needed.
#
# To SSH in:
#   gcloud compute ssh polymarket-docker-host --zone us-central1-a
#
# To check startup log:
#   cat /var/log/startup-script.log
#
# To check pipeline run log:
#   cat /var/log/pipeline-cron.log
# ============================================================

resource "google_compute_instance" "polymarket-docker-host" {
  name         = "polymarket-docker-host"
  machine_type = "e2-medium"
  zone         = local.zone

  boot_disk {
    auto_delete = true
    device_name = "polymarket-docker-host"
    initialize_params {
      # Ubuntu 24.04 LTS minimal — stable, Docker-supported
      image = "projects/ubuntu-os-cloud/global/images/ubuntu-minimal-2404-noble-amd64-v20260325"
      size  = 30
      type  = "pd-ssd"
    }
    mode = "READ_WRITE"
  }

  can_ip_forward      = false
  deletion_protection = false
  enable_display      = false

  # The VM assumes this SA identity for all GCP API calls.
  # Credentials are never stored on disk — ADC handles auth
  # transparently for any process running on the machine.
  service_account {
    email  = local.pipeline_sa_email
    scopes = ["cloud-platform"]
  }

  # startup-script.sh installs Docker, fetches secrets from
  # Secret Manager, pulls the pipeline image, and registers
  # the cron job. Stored as a separate file to avoid Windows
  # CRLF line ending issues with heredocs.
  metadata = {
    enable-osconfig = "FALSE"
    startup-script  = file("startup-script.sh")
  }

  network_interface {
    access_config {
      network_tier = "PREMIUM"
    }
    queue_count = 0
    stack_type  = "IPV4_ONLY"
    subnetwork  = "projects/${local.project_id}/regions/${local.region}/subnetworks/default"
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    preemptible         = false
    provisioning_model  = "STANDARD"
  }

  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_secure_boot          = true
    enable_vtpm                 = true
  }

  # No firewall tags — this VM only makes outbound connections,
  # it never receives incoming traffic.

  # Ensure all permissions are in place before the VM boots
  # and the startup script attempts to access GCP resources.
  depends_on = [
    google_secret_manager_secret_iam_member.credentials_access,
    google_secret_manager_secret_iam_member.bruin_yml_access,
    google_storage_bucket_iam_member.sa_gcs,
    google_bigquery_dataset_iam_member.sa_staging,
    google_bigquery_dataset_iam_member.sa_reports,
    google_project_iam_member.sa_bq_jobs
  ]
}
