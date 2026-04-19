#!/bin/bash
set -e
exec >> /var/log/startup-script.log 2>&1

echo "============================================"
echo "Startup script begin: $(date)"
echo "============================================"

# ---- System packages ----
apt-get update
apt-get install -y \
  ca-certificates curl gnupg lsb-release git \
  htop sysstat ncdu \
  dnsutils iputils-ping netcat-openbsd traceroute \
  jq less vim logrotate \
  python3-pip apt-transport-https

echo "System packages installed."

# ---- Docker ----
mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu noble stable" \
  | tee /etc/apt/sources.list.d/docker.list > /dev/null

apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

systemctl enable docker
systemctl start docker

echo "Docker installed and running."

# ---- gcloud SDK ----
curl https://sdk.cloud.google.com | bash -s -- --disable-prompts --install-dir=/usr/local
ln -sf /usr/local/google-cloud-sdk/bin/gcloud   /usr/bin/gcloud
ln -sf /usr/local/google-cloud-sdk/bin/bq       /usr/bin/bq
ln -sf /usr/local/google-cloud-sdk/bin/gsutil   /usr/bin/gsutil

echo "gcloud SDK installed."

# ---- logrotate for pipeline logs ----
cat > /etc/logrotate.d/pipeline-cron << 'LOGROTATE'
/var/log/pipeline-cron.log {
  daily
  rotate 7
  compress
  missingok
  notifempty
}
LOGROTATE

# ---- Fetch secrets from Secret Manager ----
mkdir -p /opt/polymarket

gcloud secrets versions access latest \
  --secret=bruin-gcp-credentials \
  --project=de-final-project-jprq \
  > /opt/polymarket/bruin_gcp.json

gcloud secrets versions access latest \
  --secret=bruin-yml-config \
  --project=de-final-project-jprq \
  > /opt/polymarket/.bruin.yml

# Restrict file permissions — readable by root only
chmod 600 /opt/polymarket/bruin_gcp.json
chmod 600 /opt/polymarket/.bruin.yml

echo "Secrets fetched from Secret Manager."

# ---- Pull Docker image ----
docker pull jprq/polymarket-pipeline:latest

echo "Docker image pulled."

# ---- Create log directory for pipeline ----
mkdir -p /var/log/pipeline

# ---- Register cron job ----
# 17:30 UTC = 12:30 PM Lima time (UTC-5)
cat > /etc/cron.d/polymarket-pipeline << 'CRONEOF'
30 17 * * * root docker run --rm \
  -v /opt/polymarket/.bruin.yml:/app/.bruin.yml:ro \
  -v /opt/polymarket/bruin_gcp.json:/app/bruin_gcp.json:ro \
  -v /var/log/pipeline:/app/logs \
  jprq/polymarket-pipeline:latest \
  --environment prod >> /var/log/pipeline-cron.log 2>&1
CRONEOF
chmod 0644 /etc/cron.d/polymarket-pipeline

echo "Cron job registered: 17:30 UTC (12:30 PM Lima)"

echo "============================================"
echo "Startup script complete: $(date)"
echo "============================================"