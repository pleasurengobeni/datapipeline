#!/usr/bin/env bash
# =============================================================================
# install.sh — Fresh server installation for WASAC Analytics Pipeline
#
# Usage (new server):
#   1. Manually create ~/.airflow with all real values  (see INSTALL_LINUX.md)
#   2. mkdir -p ~/datapipeline/airflow && cd ~/datapipeline/airflow
#   3. git clone git@github.com:pleasurengobeni/datapipeline.git .
#   4. bash install.sh
#
# What this does:
#   1. Sources ~/.airflow (aborts with instructions if missing / incomplete)
#   2. Installs Docker Engine + docker compose plugin (skips if already present)
#   3. Creates required host directories with correct permissions
#   4. Writes .env by expanding all vars from ~/.airflow (no hardcoded values)
#   5. Starts the full stack
# =============================================================================
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_USER="$(whoami)"
SERVER_UID="$(id -u)"
SERVER_GID="$(id -g)"

AIRFLOW_ENV="$HOME/.airflow"

# ── SOURCE ~/.airflow FIRST ───────────────────────────────────────────────────
# This must happen before ANYTHING else.  All env vars come from here — the
# .env file is generated from these values and no defaults are hardcoded.
if [[ ! -f "$AIRFLOW_ENV" ]]; then
    echo ""
    echo "  ERROR: $AIRFLOW_ENV not found."
    echo ""
    echo "  Before running install.sh you must create ~/.airflow with all required"
    echo "  values.  A complete template is in INSTALL_LINUX.md (Step 5)."
    echo ""
    echo "  Quick-start:"
    echo "    nano ~/.airflow       # paste + fill in the template from INSTALL_LINUX.md"
    echo "    chmod 600 ~/.airflow"
    echo "    bash install.sh       # re-run this script"
    echo ""
    exit 1
fi

set -a; source "$AIRFLOW_ENV"; set +a
echo "✓  Sourced $AIRFLOW_ENV"

# Verify no CHANGE_ME_ placeholders remain
if grep -q "CHANGE_ME_" "$AIRFLOW_ENV" 2>/dev/null; then
    echo ""
    echo "  ERROR: $AIRFLOW_ENV still contains CHANGE_ME_ placeholder values."
    echo "  Edit it now:  nano ~/.airflow"
    echo ""
    exit 1
fi

PROJECT_NAME="${PROJECT_NAME:-Analytics Pipeline}"
PROJECT_NAME_LOWER="${PROJECT_NAME_LOWER:-${PROJECT_NAME,,}}"

echo ""
echo "============================================================"
echo "  $PROJECT_NAME — Server Installation"
echo "  User: $SERVER_USER  UID: $SERVER_UID  GID: $SERVER_GID"
echo "  Directory: $REPO_DIR"
echo "============================================================"
echo ""

# ── 1. Install Docker ────────────────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
    echo "▶  Installing Docker..."
    sudo apt-get update -qq
    sudo apt-get install -y ca-certificates curl gnupg lsb-release

    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg

    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
        | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    sudo apt-get update -qq
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

    sudo systemctl enable --now docker
    echo "✓  Docker installed"
else
    echo "✓  Docker already installed ($(docker --version))"
fi

# Add current user to docker group so they can run docker without sudo
if ! groups "$SERVER_USER" | grep -q docker; then
    echo "▶  Adding $SERVER_USER to docker group..."
    sudo usermod -aG docker "$SERVER_USER"
    echo "  ⚠  Log out and back in (or run: newgrp docker) for group change to take effect"
fi

# ── 2. Create required directories with correct permissions ──────────────────
echo "▶  Creating required directories..."
mkdir -p \
    "$REPO_DIR/data_dumps/incoming" \
    "$REPO_DIR/data_dumps/archive" \
    "$REPO_DIR/logs" \
    "$REPO_DIR/dags/config" \
    "$REPO_DIR/dags/etl" \
    "$REPO_DIR/dags/sql" \
    "$REPO_DIR/plugins"

# Airflow containers run as the same UID as the server user (AIRFLOW_UID=UID).
# Setting ownership here ensures both the host user and containers can read/write.
sudo chown -R "$SERVER_UID:$SERVER_GID" \
    "$REPO_DIR/dags" \
    "$REPO_DIR/data_dumps" \
    "$REPO_DIR/logs" \
    "$REPO_DIR/plugins"

chmod -R u+rwX,go+rX \
    "$REPO_DIR/dags" \
    "$REPO_DIR/data_dumps" \
    "$REPO_DIR/logs"

echo "✓  Directories created and permissions set"

# ── 3. Write .env by expanding vars from ~/.airflow ─────────────────────────
# docker compose reads .env at startup.  We write real values here so the file
# is always in sync with ~/.airflow even when compose is invoked outside make.
echo "▶  Generating .env from ~/.airflow values..."
cat > "$REPO_DIR/.env" << DOTENV
# =============================================================================
# .env — generated by install.sh from ~/.airflow — DO NOT EDIT BY HAND
# Re-run  bash install.sh  (or  make  which also sources ~/.airflow) to regenerate.
# =============================================================================

# Project identity
PROJECT_NAME=${PROJECT_NAME:-}
PROJECT_SERVER_IP=${PROJECT_SERVER_IP:-}
SERVER_SSH_PORT=${SERVER_SSH_PORT:-22}
SERVER_SSH_USER=${SERVER_SSH_USER:-ubuntu}
SERVER_SSH_KEY_PATH=${SERVER_SSH_KEY_PATH:-}

# Docker Compose — host settings
AIRFLOW_SERVER_IP=${PROJECT_SERVER_IP:-localhost}
AIRFLOW_PROJ_DIR=.
_PIP_ADDITIONAL_REQUIREMENTS=

# PostgreSQL — Airflow metadata DB
POSTGRES_USER=${POSTGRES_USER:-airflow}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-}
POSTGRES_PORT=55432

# PostgreSQL — Data Lake DB
POSTGRES_DATA_USER=${POSTGRES_DATA_USER:-}
POSTGRES_DATA_PWD=${POSTGRES_DATA_PWD:-}
POSTGRES_DATA_PORT=${POSTGRES_DATA_PORT:-5432}
POSTGRES_DATA_HOST=${POSTGRES_DATA_HOST:-}
POSTGRES_DATA_DB=${POSTGRES_DATA_DB:-}

# PgAdmin
PGADMIN_DEFAULT_EMAIL=${PGADMIN_DEFAULT_EMAIL:-}
PGADMIN_DEFAULT_PASSWORD=${PGADMIN_DEFAULT_PASSWORD:-}
PGADMIN_PORT=${PGADMIN_PORT:-5050}

# Airflow — core
AIRFLOW_UID=${AIRFLOW_UID:-1000}
AIRFLOW_GID=${AIRFLOW_GID:-0}
DOCKER_AIRFLOW_HOME=${DOCKER_AIRFLOW_HOME:-/opt/airflow}
AIRFLOW__CORE__FERNET_KEY=${AIRFLOW__CORE__FERNET_KEY:-}
AIRFLOW__CORE__DEFAULT_TIMEZONE=${AIRFLOW__CORE__DEFAULT_TIMEZONE:-UTC}
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=${AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT:-1000}
AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT=${AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT:-1000}
AIRFLOW__CORE__SQL_ALCHEMY_CONN=${AIRFLOW__CORE__SQL_ALCHEMY_CONN:-}
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-}
AIRFLOW__CELERY__RESULT_BACKEND=${AIRFLOW__CELERY__RESULT_BACKEND:-}
AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
AIRFLOW__WEBSERVER__SECRET_KEY=${AIRFLOW__WEBSERVER__SECRET_KEY:-}
AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT=${AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT:-1000}
AIRFLOW__WEBSERVER__WARN_DEPLOYMENT_EXPOSURE=False
AIRFLOW__SCHEDULER__PARSING_PROCESSES=${AIRFLOW__SCHEDULER__PARSING_PROCESSES:-2}
AIRFLOW__LOGGING__LOG_CLEANUP=True
AIRFLOW_LOG_RETENTION_DAYS=10
SQLALCHEMY_SILENCE_UBER_WARNING=${SQLALCHEMY_SILENCE_UBER_WARNING:-1}

# Airflow admin UI account
_AIRFLOW_WWW_USER_USERNAME=${_AIRFLOW_WWW_USER_USERNAME:-admin}
_AIRFLOW_WWW_USER_PASSWORD=${_AIRFLOW_WWW_USER_PASSWORD:-}

AIRFLOW_CONN_AIRFLOW=${AIRFLOW_CONN_AIRFLOW:-}

# OpenMetadata (optional)
OPENMENTA_USER=${OPENMENTA_USER:-}
OPENMENTA_PASSWORD=${OPENMENTA_PASSWORD:-}

# Airflow Variables — ETL paths & runtime
AIRFLOW_VAR_environment=${AIRFLOW_VAR_ENVIRONMENT:-dev}
AIRFLOW_VAR_modules_path=${AIRFLOW_VAR_MODULES_PATH:-/opt/airflow/dags}
AIRFLOW_VAR_config_path=${AIRFLOW_VAR_CONFIG_PATH:-/opt/airflow/dags/config}
AIRFLOW_VAR_sql_path=${AIRFLOW_VAR_SQL_PATH:-/opt/airflow/dags/sql}
AIRFLOW_VAR_etl_path=${AIRFLOW_VAR_ETL_PATH:-/opt/airflow/dags/etl}
AIRFLOW_VAR_template_path=${AIRFLOW_VAR_TEMPLATE_PATH:-/opt/airflow/dags/templates}
AIRFLOW_VAR_data_dump=${AIRFLOW_VAR_DATA_DUMP:-/opt/airflow/data_dump}
AIRFLOW_VAR_logs_path=${AIRFLOW_VAR_LOGS_PATH:-/opt/airflow/logs}
AIRFLOW_VAR_dag_home=${AIRFLOW_VAR_DAG_HOME:-/opt/airflow/dags}
AIRFLOW_VAR_slack_token=${AIRFLOW_VAR_SLACK_TOKEN:-}

# Airflow Variables — Data Warehouse
AIRFLOW_VAR_dw_config_path=${AIRFLOW_VAR_DW_CONFIG_PATH:-/opt/airflow/dags/dw_config}
AIRFLOW_VAR_dw_template_path=${AIRFLOW_VAR_DW_TEMPLATE_PATH:-/opt/airflow/dags/templates}
AIRFLOW_VAR_dw_etl_path=${AIRFLOW_VAR_DW_ETL_PATH:-/opt/airflow/dags/dw_etl}
AIRFLOW_VAR_dw_sql_path=${AIRFLOW_VAR_DW_SQL_PATH:-/opt/airflow/dags/dw_sql}
AIRFLOW_VAR_dw_default_schema=${AIRFLOW_VAR_DW_DEFAULT_SCHEMA:-edw_core}
AIRFLOW_VAR_dw_log_retention_days=${AIRFLOW_VAR_DW_LOG_RETENTION_DAYS:-30}

# Web UI (Flask — port 5001)
WEBUI_SECRET_KEY=${WEBUI_SECRET_KEY:-}
WEBUI_ADMIN_USER=${WEBUI_ADMIN_USER:-admin}
WEBUI_ADMIN_PASS=${WEBUI_ADMIN_PASS:-}
WEBUI_SESSION_COOKIE_NAME=${WEBUI_SESSION_COOKIE_NAME:-etl_manager_session}

# Pipeline Monitor dashboard DB
METRICS_DB_USER=${METRICS_DB_USER:-}
METRICS_DB_PASS=${METRICS_DB_PASS:-}
METRICS_DB_HOST=${METRICS_DB_HOST:-}
METRICS_DB_PORT=${METRICS_DB_PORT:-5432}
METRICS_DB_NAME=${METRICS_DB_NAME:-}

# AI API keys (optional)
GOOGLE_AI_API_KEY=${GOOGLE_AI_API_KEY:-}
GROQ_API_KEY=${GROQ_API_KEY:-}
MISTRAL_API_KEY=${MISTRAL_API_KEY:-}
DEEPSEEK_API_KEY=${DEEPSEEK_API_KEY:-}
OPENROUTER_API_KEY=${OPENROUTER_API_KEY:-}
CEREBRAS_API_KEY=${CEREBRAS_API_KEY:-}
SAMBANOVA_API_KEY=${SAMBANOVA_API_KEY:-}
DOTENV
echo "✓  .env generated"

# ── 4. Start the stack ───────────────────────────────────────────────────────

echo "▶  Starting WASAC Analytics stack..."
cd "$REPO_DIR"
docker compose up -d

echo ""
echo "▶  Waiting for containers to start (up to 120s)..."
TIMEOUT=120
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
    STARTING=$(docker compose ps --format '{{.Status}}' 2>/dev/null | grep -c "starting" || true)
    [ "$STARTING" -eq 0 ] && break
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    echo -n "."
done
echo ""

# ── Ensure pipeline.etl_metrics exists (idempotent) ──────────────────────────
echo "▶  Initialising pipeline.etl_metrics table..."
cat "$REPO_DIR/etl_metrics.sql" | docker compose exec -T postgres psql -U "${POSTGRES_USER:-airflow}" -d airflow && echo "✓  pipeline.etl_metrics ready" || echo "⚠  Could not init pipeline.etl_metrics — run: make init-db"

# ── Post-install health check ─────────────────────────────────────────────────
SERVER_IP="$(hostname -I | awk '{print $1}')"

echo ""
echo "============================================================"
echo "  $PROJECT_NAME — Post-Install Health Check"
echo "============================================================"
echo ""

# Per-container status
echo "  Container status:"
docker compose ps --format '{{.Name}}\t{{.Status}}' 2>/dev/null \
    | while IFS=$'\t' read -r NAME STATUS; do
        if echo "$STATUS" | grep -qi "unhealthy"; then
            ICON="✗"
        elif echo "$STATUS" | grep -qi "healthy\|up"; then
            ICON="✓"
        else
            ICON="?"
        fi
        printf "    %s  %-45s %s\n" "$ICON" "$NAME" "$STATUS"
    done

echo ""
echo "  Port / HTTP reachability:"

# Each entry: "label|port|path"
CHECKS=(
    "Airflow UI      |8090|/health"
    "ETL Manager     |5001|/login"
    "Pipeline Monitor|8050|/"
    "Jenkins CI/CD   |9090|/login"
    "Analytics       |8501|/"
    "PostgreSQL      |55432|"
)

ALL_PASS=true
for ENTRY in "${CHECKS[@]}"; do
    LABEL=$(echo "$ENTRY" | cut -d'|' -f1)
    PORT=$(echo "$ENTRY"  | cut -d'|' -f2)
    PATH=$(echo "$ENTRY"  | cut -d'|' -f3)

    # TCP check first
    if ! (echo >/dev/tcp/localhost/$PORT) 2>/dev/null; then
        printf "    ✗  %-18s →  http://%s:%s  (port not open)\n" "$LABEL" "$SERVER_IP" "$PORT"
        ALL_PASS=false
        continue
    fi

    # HTTP check (skip for postgres)
    if [[ -n "$PATH" ]]; then
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
            --max-time 5 "http://localhost:${PORT}${PATH}" 2>/dev/null || echo "000")
        if [[ "$HTTP_CODE" == "000" ]]; then
            STATUS_MSG="no response"
            ICON="✗"; ALL_PASS=false
        elif [[ "$HTTP_CODE" =~ ^(2|3) ]]; then
            STATUS_MSG="HTTP $HTTP_CODE"
            ICON="✓"
        else
            STATUS_MSG="HTTP $HTTP_CODE"
            ICON="~"
        fi
        printf "    %s  %-18s →  http://%s:%s  (%s)\n" "$ICON" "$LABEL" "$SERVER_IP" "$PORT" "$STATUS_MSG"
    else
        printf "    ✓  %-18s →  %s:%s  (port open)\n" "$LABEL" "$SERVER_IP" "$PORT"
    fi
done

echo ""
if $ALL_PASS; then
    echo "  ✓  All services are up and reachable."
else
    echo "  ⚠  One or more services may still be starting — wait 30s and re-check:"
    echo "     cd $REPO_DIR && docker compose ps"
fi

echo ""
echo "  Airflow login   →  admin / ${_AIRFLOW_WWW_USER_PASSWORD:-see ~/.airflow}"
echo "  ETL Manager     →  ${WEBUI_ADMIN_USER:-admin} / ${WEBUI_ADMIN_PASS:-see ~/.airflow}"
echo "============================================================"
