#!/usr/bin/env bash
# =============================================================================
# wipe_and_reinstall.sh — Wipe remote server and do a fresh install
#
# Reads all connection details and credentials from .airflow in this directory.
# Steps:
#   1. Reads local .airflow for connection details + secrets
#   2. SSHes to the server:
#      a. Stops ALL compose stacks in ~/datapipeline (any old install)
#      b. Force-removes any containers still holding our ports or named jenkins
#      c. Removes project images and project directory
#   3. Rsyncs local project (excluding .airflow, webui.db, logs, .pyc)
#   4. Uploads a server-adapted ~/.airflow (correct AIRFLOW_PROJ_DIR/UID/GID)
#   5. Sets permissions and runs install.sh
#   6. Waits for all services to become healthy before reporting URLs
#
# Usage:
#   bash wipe_and_reinstall.sh
#   make wipe-server
#   make install-server
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_AIRFLOW="$SCRIPT_DIR/.airflow"

if [[ ! -f "$LOCAL_AIRFLOW" ]]; then
    echo "Error: $LOCAL_AIRFLOW not found. Cannot read connection details."
    exit 1
fi

# Load all vars from the project .airflow
set -a; source "$LOCAL_AIRFLOW"; set +a

# ── Connection details ────────────────────────────────────────────────────────
HOST="${PROJECT_SERVER_IP:?PROJECT_SERVER_IP not set in .airflow}"
PORT="${SERVER_SSH_PORT:-22}"
RUSER="${SERVER_SSH_USER:-ubuntu}"
KEY="${SERVER_SSH_KEY_PATH:?SERVER_SSH_KEY_PATH not set in .airflow}"
PNAME="${PROJECT_NAME:?PROJECT_NAME not set in .airflow}"
PNAME_LOWER="${PROJECT_NAME_LOWER:-${PNAME,,}}"

# Server path — tilde is intentional: remote shell will expand it
SERVER_PATH_TILDE="~/datapipeline/${PNAME}"

SSH_OPTS="-i ${KEY} -p ${PORT} -o StrictHostKeyChecking=no -o BatchMode=yes"
SCP_OPTS="-i ${KEY} -P ${PORT} -o StrictHostKeyChecking=no"

echo ""
echo "============================================================"
echo "  ${PNAME} — Remote Wipe & Fresh Install"
echo "  Server : ${RUSER}@${HOST}:${PORT}"
echo "  Target : ${SERVER_PATH_TILDE}"
echo "============================================================"
echo ""
echo "  This will:"
echo "    • Stop all running containers on the server"
echo "    • Delete all project images and the project directory"
echo "    • Rsync fresh code from local and run a full install"
echo ""
echo "  Named volumes (postgres-db-volume, jenkins_home, metabase-*)"
echo "  are PRESERVED — database and Jenkins config survive."
echo ""
echo "  ⚠  To continue you must confirm TWICE."
echo ""

read -rp "  Type 'delete' to confirm you want to wipe the server: " CONFIRM1
if [[ "${CONFIRM1}" != "delete" ]]; then
    echo ""
    echo "  Aborted — you must type exactly 'delete'."
    exit 1
fi

read -rp "  Type 'yes' to proceed with the wipe and reinstall:   " CONFIRM2
if [[ "${CONFIRM2}" != "yes" ]]; then
    echo ""
    echo "  Aborted — you must type exactly 'yes'."
    exit 1
fi

echo ""

# ── Step 1: Wipe the old stack on the server ─────────────────────────────────
echo "▶  Step 1/5  Wiping existing project on server..."

# Ports used by the stack — any container holding these will block startup
REQUIRED_PORTS=(55432 8090 5001 8501 8050 9090 50000)

# shellcheck disable=SC2087
ssh ${SSH_OPTS} "${RUSER}@${HOST}" bash << REMOTE_WIPE
set -e
PNAME="${PNAME}"
PNAME_LOWER="${PNAME_LOWER}"
RPATH="\$HOME/datapipeline/\${PNAME}"

echo "   ...stopping ALL compose stacks in ~/datapipeline (old installs)"
for COMPOSE_FILE in "\$HOME"/datapipeline/*/docker-compose.yaml; do
    [[ -f "\$COMPOSE_FILE" ]] || continue
    STACK_DIR="\$(dirname "\$COMPOSE_FILE")"
    echo "      stopping stack in: \$STACK_DIR"
    # Do NOT pass --volumes: named volumes (jenkins_home, postgres-db-volume, etc.)
    # must survive redeploys so Jenkins stays configured and DB data is preserved.
    docker compose -f "\$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true
done

echo "   ...removing containers with project prefix (\${PNAME_LOWER}*)"
docker ps -a --format '{{.Names}}' 2>/dev/null \
    | grep -i "^\${PNAME_LOWER}" \
    | xargs -r docker rm -f 2>/dev/null || true

echo "   ...removing stale airflow-* / deploy-* containers from old installs"
docker ps -a --format '{{.Names}}' 2>/dev/null \
    | grep -iE "^(airflow|deploy)-" \
    | xargs -r docker rm -f 2>/dev/null || true

echo "   ...removing any container holding ports we need: ${REQUIRED_PORTS[*]}"
for PORT in ${REQUIRED_PORTS[*]}; do
    # find the container ID listening on this port (host or container side)
    CID=\$(docker ps --format '{{.ID}} {{.Ports}}' 2>/dev/null \
           | grep ":\${PORT}->" | awk '{print \$1}' | head -1)
    if [[ -n "\$CID" ]]; then
        CNAME=\$(docker inspect --format '{{.Name}}' "\$CID" 2>/dev/null | tr -d '/')
        echo "      killing \${CNAME:-\$CID} (held port \$PORT)"
        docker rm -f "\$CID" 2>/dev/null || true
    fi
done

echo "   ...removing project images (\${PNAME_LOWER}*)"
docker images --format '{{.Repository}}:{{.Tag}}' 2>/dev/null \
    | grep -i "^\${PNAME_LOWER}" \
    | xargs -r docker rmi -f 2>/dev/null || true

echo "   ...removing old project directory \$RPATH"
rm -rf "\$RPATH"

echo "   ...ensuring ~/datapipeline exists"
mkdir -p "\$HOME/datapipeline"

echo "   Wipe complete."
REMOTE_WIPE

echo "✓  Server wiped"

# ── Step 2: Rsync local project to server ─────────────────────────────────────
echo ""
echo "▶  Step 2/5  Syncing project files to server..."
echo "   Local : ${SCRIPT_DIR}"
echo "   Remote: ${RUSER}@${HOST}:${SERVER_PATH_TILDE}"

# Note: rsync treats ~/path correctly when passed to remote
rsync -az --progress \
    --exclude='.git' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.airflow' \
    --exclude='logs/' \
    --exclude='data_dumps/incoming/*.csv' \
    --exclude='web_ui/webui.db' \
    -e "ssh ${SSH_OPTS}" \
    "${SCRIPT_DIR}/" \
    "${RUSER}@${HOST}:${SERVER_PATH_TILDE}/"

echo "✓  Files synced"

# Belt-and-suspenders: delete any stale webui.db on the server so that
# install.sh creates a fresh DB with the correct admin password from .airflow.
# (rsync excludes it from being overwritten with the local dev DB, but an old
#  server-side DB surviving a partial wipe must not be re-used.)
ssh ${SSH_OPTS} "${RUSER}@${HOST}" \
    "rm -f ~/datapipeline/${PNAME}/web_ui/webui.db && echo '   stale webui.db removed (if any)'"

# ── Step 3: Generate server .airflow and upload ───────────────────────────────
echo ""
echo "▶  Step 3/5  Preparing server ~/.airflow..."

TMPFILE=$(mktemp)
trap "rm -f ${TMPFILE}" EXIT

# Get the actual expanded server path for AIRFLOW_PROJ_DIR
# We pass PROJECT_NAME so remote can expand $HOME properly; we embed it as a sentinel
# and replace on the remote side. Simpler: compute it here via ssh.
REMOTE_HOME=$(ssh ${SSH_OPTS} "${RUSER}@${HOST}" 'echo $HOME')
REMOTE_PROJ_DIR="${REMOTE_HOME}/datapipeline/${PNAME}"

# Build the server .airflow:
# - Replace AIRFLOW_PROJ_DIR with the actual server path
# - Replace AIRFLOW_UID / AIRFLOW_GID with the server user's IDs
REMOTE_UID=$(ssh ${SSH_OPTS} "${RUSER}@${HOST}" 'id -u')
REMOTE_GID=$(ssh ${SSH_OPTS} "${RUSER}@${HOST}" 'id -g')

sed \
    -e "s|^export AIRFLOW_PROJ_DIR=.*|export AIRFLOW_PROJ_DIR=\"${REMOTE_PROJ_DIR}\"|" \
    -e "s|^export AIRFLOW_UID=.*|export AIRFLOW_UID=${REMOTE_UID}|" \
    -e "s|^export AIRFLOW_GID=.*|export AIRFLOW_GID=${REMOTE_GID}|" \
    "${LOCAL_AIRFLOW}" > "${TMPFILE}"

# Upload to server
scp ${SCP_OPTS} "${TMPFILE}" "${RUSER}@${HOST}:~/.airflow"
ssh ${SSH_OPTS} "${RUSER}@${HOST}" 'chmod 600 ~/.airflow'

echo "✓  Server ~/.airflow uploaded (AIRFLOW_PROJ_DIR=${REMOTE_PROJ_DIR})"

# ── Step 4: Fix permissions on synced project dir ────────────────────────────
echo ""
echo "▶  Step 4/5  Setting permissions..."

ssh ${SSH_OPTS} "${RUSER}@${HOST}" bash << REMOTE_PERMS
set -e
RPATH="\$HOME/datapipeline/${PNAME}"
sudo chown -R \$(id -u):\$(id -g) "\$RPATH" 2>/dev/null || true
chmod +x "\$RPATH/install.sh" "\$RPATH/deploy.sh" "\$RPATH/wipe_and_reinstall.sh" 2>/dev/null || true
echo "   Permissions set."
REMOTE_PERMS

echo "✓  Permissions set"

# ── Step 5: Run install.sh on server ─────────────────────────────────────────
echo ""
echo "▶  Step 5/5  Running install.sh on server..."
echo "   (This installs Docker if needed, creates dirs, and starts the stack)"
echo ""

ssh ${SSH_OPTS} "${RUSER}@${HOST}" "bash ~/datapipeline/${PNAME}/install.sh"

echo ""
echo "============================================================"
echo "  Waiting for services to become healthy (up to 3 min)..."
echo "============================================================"

EXPECTED_SERVICES=(
    "postgres"
    "redis"
    "airflow-webserver"
    "airflow-scheduler"
    "airflow-worker"
    "airflow-triggerer"
    "${PNAME_LOWER}_web_ui"
    "${PNAME_LOWER}_analytics"
    "pipeline-monitor"
    "jenkins"
)

MAX_WAIT=180
INTERVAL=10
ELAPSED=0

while true; do
    # Get running container names from the server
    RUNNING=$(ssh ${SSH_OPTS} "${RUSER}@${HOST}" \
        "cd ~/datapipeline/${PNAME} && docker compose ps --format '{{.Name}} {{.Status}}' 2>/dev/null" || true)

    ALL_UP=true
    for SVC in "${EXPECTED_SERVICES[@]}"; do
        STATUS=$(echo "$RUNNING" | grep -i "$SVC" | awk '{print $2}' | head -1)
        if [[ "$STATUS" != "running" && "$STATUS" != "Up" ]] && ! echo "$STATUS" | grep -qi "Up"; then
            ALL_UP=false
            break
        fi
    done

    if $ALL_UP; then
        echo "   All services running after ${ELAPSED}s."
        break
    fi

    if (( ELAPSED >= MAX_WAIT )); then
        echo "   Timeout: not all services are up after ${MAX_WAIT}s (may still be starting)"
        break
    fi

    echo "   Waiting... ${ELAPSED}s elapsed"
    sleep $INTERVAL
    ELAPSED=$(( ELAPSED + INTERVAL ))
done

# Final per-service status table
echo ""
echo "============================================================"
echo "  ${PNAME} — Service Health"
echo "============================================================"
RUNNING=$(ssh ${SSH_OPTS} "${RUSER}@${HOST}" \
    "cd ~/datapipeline/${PNAME} && docker compose ps --format '{{.Name}} {{.Status}}' 2>/dev/null" || true)
while IFS= read -r LINE; do
    CNAME=$(echo "$LINE" | awk '{print $1}')
    CSTATUS=$(echo "$LINE" | awk '{$1=""; print $0}' | xargs)
    printf "  %-40s %s\n" "$CNAME" "$CSTATUS"
done <<< "$RUNNING"

echo ""
echo "============================================================"
echo "  ✓  ${PNAME} deployed to ${HOST}"
echo ""
echo "  Airflow UI       →  http://${HOST}:8090"
echo "  ETL Manager      →  http://${HOST}:5001"
echo "  Analytics        →  http://${HOST}:8501"
echo "  Pipeline Monitor →  http://${HOST}:8050"
echo "  Jenkins CI/CD    →  http://${HOST}:9090"
echo "  PostgreSQL       →  ${HOST}:55432"
echo "============================================================"
