# ==============================================================================
# Polymarket Pulse — Makefile
# ==============================================================================
# Usage:
#   make <target>
#   make run                           # today's data
#   make run DATE=2026-03-14           # specific date
#   make backfill START=2026-03-14 END=2026-03-20
#   make asset PATH=assets/ingestion/upload_to_gcs.py DATE=2026-03-14
# ==============================================================================

# ── Configuration ──────────────────────────────────────────────────────────────
PROJECT_ID   := polymarket-pulse-2026
REGION       := us-central1
ZONE         := us-central1-a
VM_NAME      := polymarket-docker-host
SA_EMAIL     := polymarket-pipeline-sa@$(PROJECT_ID).iam.gserviceaccount.com

IMAGE_REPO   := jprq
IMAGE_NAME   := polymarket-pipeline
IMAGE_TAG    := latest
IMAGE        := $(IMAGE_REPO)/$(IMAGE_NAME):$(IMAGE_TAG)

PIPELINE_DIR := ./polymarket-pipeline
TF_DIR       := ./terraform

# Default date window: today → tomorrow (used by 'make run')
TODAY        := $(shell date +%Y-%m-%d)
TOMORROW     := $(shell date -d "+1 day" +%Y-%m-%d 2>/dev/null || date -v+1d +%Y-%m-%d)

# Parameterizable date (override with DATE=YYYY-MM-DD for a single-day run)
DATE         ?= $(TODAY)
DATE_END     := $(shell date -d "$(DATE) +1 day" +%Y-%m-%d 2>/dev/null || date -j -f "%Y-%m-%d" "$(DATE)" -v+1d +%Y-%m-%d)

# Backfill parameters
START        ?= $(TODAY)
END          ?= $(TOMORROW)

# Single-asset path (override with PATH=assets/...)
PATH         ?=

# ── Phony targets ──────────────────────────────────────────────────────────────
.PHONY: help \
        install \
        validate \
        run backfill asset \
        docker-build docker-push docker-run \
        tf-init tf-plan tf-apply tf-destroy \
        upload-secrets rotate-credentials \
        ssh logs vm-stop vm-start vm-reset \
        check-gcs check-bq

# ── Default target ─────────────────────────────────────────────────────────────
.DEFAULT_GOAL := help

help:
	@echo ""
	@echo "  Polymarket Pulse — available targets"
	@echo ""
	@echo "  LOCAL DEVELOPMENT"
	@echo "    make install                         Install Python dependencies (uv sync)"
	@echo "    make validate                        Validate pipeline assets without running"
	@echo "    make run                             Run pipeline for today"
	@echo "    make run DATE=2026-03-14             Run pipeline for a specific date"
	@echo "    make backfill START=... END=...      Backfill a date range"
	@echo "    make asset PATH=assets/...           Run a single asset for today"
	@echo "    make asset PATH=assets/... DATE=...  Run a single asset for a specific date"
	@echo ""
	@echo "  DOCKER"
	@echo "    make docker-build                    Build the Docker image"
	@echo "    make docker-push                     Push image to Docker Hub"
	@echo "    make docker-run                      Run pipeline in Docker (prod, today)"
	@echo "    make docker-run DATE=2026-03-14      Run pipeline in Docker for a specific date"
	@echo ""
	@echo "  INFRASTRUCTURE (Terraform)"
	@echo "    make tf-init                         Initialize Terraform"
	@echo "    make tf-plan                         Preview infrastructure changes"
	@echo "    make tf-apply                        Apply infrastructure changes"
	@echo "    make tf-destroy                      Destroy all infrastructure (asks for confirmation)"
	@echo ""
	@echo "  SECRETS"
	@echo "    make upload-secrets                  Upload polymarket-sa.json + .bruin.yml to Secret Manager"
	@echo "    make rotate-credentials              Upload new polymarket-sa.json and re-fetch on the VM"
	@echo ""
	@echo "  VM"
	@echo "    make ssh                             SSH into the pipeline VM"
	@echo "    make logs                            Tail cron log on the VM"
	@echo "    make vm-stop                         Stop the VM (saves compute cost)"
	@echo "    make vm-start                        Start the VM"
	@echo "    make vm-reset                        Reset VM (re-runs startup script)"
	@echo ""
	@echo "  VERIFY"
	@echo "    make check-gcs DATE=2026-03-14       List GCS files for a date partition"
	@echo "    make check-bq  DATE=2026-03-14       Count rows in stg_orderbook for a date"
	@echo ""

# ==============================================================================
# LOCAL DEVELOPMENT
# ==============================================================================

install:
	uv sync --frozen

validate:
	bruin validate $(PIPELINE_DIR)

run:
	bruin run $(PIPELINE_DIR) \
	  --start-date $(DATE) \
	  --end-date $(DATE_END) \
	  --environment dev

backfill:
	@if [ "$(START)" = "$(TODAY)" ] && [ "$(END)" = "$(TOMORROW)" ]; then \
	  echo "ERROR: provide START and END dates, e.g. make backfill START=2026-03-14 END=2026-03-20"; \
	  exit 1; \
	fi
	bruin run $(PIPELINE_DIR) \
	  --start-date $(START) \
	  --end-date $(END) \
	  --environment dev

asset:
	@if [ -z "$(PATH)" ]; then \
	  echo "ERROR: provide PATH, e.g. make asset PATH=assets/ingestion/upload_to_gcs.py"; \
	  exit 1; \
	fi
	bruin run $(PIPELINE_DIR)/$(PATH) \
	  --start-date $(DATE) \
	  --end-date $(DATE_END) \
	  --environment dev

# ==============================================================================
# DOCKER
# ==============================================================================

docker-build:
	docker build -t $(IMAGE) .

docker-push:
	docker push $(IMAGE)

docker-run:
	docker run --rm \
	  -v $(PWD)/.bruin.yml:/app/.bruin.yml:ro \
	  -v $(PWD)/polymarket-sa.json:/app/bruin_gcp.json:ro \
	  -v $(PWD)/logs:/app/logs \
	  $(IMAGE) \
	  --environment prod \
	  --start-date $(DATE) \
	  --end-date $(DATE_END)

# ==============================================================================
# TERRAFORM
# ==============================================================================

tf-init:
	cd $(TF_DIR) && terraform init

tf-plan:
	cd $(TF_DIR) && terraform plan

tf-apply:
	cd $(TF_DIR) && terraform apply

tf-destroy:
	@echo "WARNING: this will destroy all GCP infrastructure including the VM, GCS bucket, and BigQuery datasets."
	@read -p "Type 'yes' to continue: " confirm && [ "$$confirm" = "yes" ]
	cd $(TF_DIR) && terraform destroy

# ==============================================================================
# SECRETS
# ==============================================================================

upload-secrets:
	@echo "Uploading polymarket-sa.json to Secret Manager..."
	gcloud secrets versions add bruin-gcp-credentials \
	  --data-file=./polymarket-sa.json \
	  --project=$(PROJECT_ID)
	@echo "Uploading .bruin.yml to Secret Manager..."
	gcloud secrets versions add bruin-yml-config \
	  --data-file=./.bruin.yml \
	  --project=$(PROJECT_ID)
	@echo "Done. Reset the VM with 'make vm-reset' to apply the new secrets."

rotate-credentials:
	@echo "Uploading new polymarket-sa.json..."
	gcloud secrets versions add bruin-gcp-credentials \
	  --data-file=./polymarket-sa.json \
	  --project=$(PROJECT_ID)
	@echo "Re-fetching credentials on the VM..."
	gcloud compute ssh $(VM_NAME) --zone=$(ZONE) --command=" \
	  sudo gcloud secrets versions access latest \
	    --secret=bruin-gcp-credentials \
	    --project=$(PROJECT_ID) \
	    > /opt/polymarket/bruin_gcp.json && \
	  sudo chmod 600 /opt/polymarket/bruin_gcp.json && \
	  echo 'Credentials rotated successfully.'"

# ==============================================================================
# VM
# ==============================================================================

ssh:
	gcloud compute ssh $(VM_NAME) --zone=$(ZONE)

logs:
	gcloud compute ssh $(VM_NAME) --zone=$(ZONE) \
	  --command="tail -f /var/log/pipeline-cron.log"

vm-stop:
	gcloud compute instances stop $(VM_NAME) --zone=$(ZONE)

vm-start:
	gcloud compute instances start $(VM_NAME) --zone=$(ZONE)

vm-reset:
	gcloud compute instances reset $(VM_NAME) --zone=$(ZONE)
	@echo "VM is resetting. Startup script takes ~5 minutes."
	@echo "Check progress with: make ssh then: cat /var/log/startup-script.log"

# ==============================================================================
# VERIFY
# ==============================================================================

check-gcs:
	gcloud storage ls gs://polymarket-raw-parquet/raw/orderbook/date=$(DATE)/

check-bq:
	bq query --nouse_legacy_sql \
	  "SELECT DATE(timestamp_received) AS date, COUNT(*) AS rows \
	   FROM staging.stg_orderbook \
	   WHERE DATE(timestamp_received) = '$(DATE)' \
	   GROUP BY 1"
