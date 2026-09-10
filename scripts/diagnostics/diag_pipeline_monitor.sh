#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

# Load project-local and user-level exported vars (same order as Makefile)
set -a
[[ -f "$ROOT_DIR/.airflow" ]] && source "$ROOT_DIR/.airflow"
[[ -f "$HOME/.airflow" ]] && source "$HOME/.airflow"
set +a

DC=(docker compose --project-directory "$ROOT_DIR" --env-file "$ROOT_DIR/.env")

section() {
  printf "\n\033[1;34m== %s ==\033[0m\n" "$1"
}

mask() {
  local val="${1:-}"
  if [[ -z "$val" ]]; then
    echo "<empty>"
  else
    local n=${#val}
    if (( n <= 4 )); then
      echo "****"
    else
      echo "${val:0:2}****${val:n-2:2}"
    fi
  fi
}

section "Host + Docker basics"
echo "Time: $(date)"
echo "PWD:  $PWD"
if ! docker info >/dev/null 2>&1; then
  echo "FAIL: Docker daemon is not reachable"
  exit 1
fi
echo "OK: Docker daemon reachable"

docker --version

awk '/^METRICS_DB_(USER|PASS|HOST|PORT|NAME)=/ {print}' "$ROOT_DIR/.env" || true

section "Runtime env presence (shell)"
echo "METRICS_DB_USER=$(mask "${METRICS_DB_USER:-}")"
echo "METRICS_DB_PASS=$(mask "${METRICS_DB_PASS:-}")"
echo "METRICS_DB_HOST=${METRICS_DB_HOST:-<empty>}"
echo "METRICS_DB_PORT=${METRICS_DB_PORT:-<empty>}"
echo "METRICS_DB_NAME=${METRICS_DB_NAME:-<empty>}"

section "Compose service status"
"${DC[@]}" ps pipeline-monitor || true

section "Bring up pipeline-monitor (idempotent)"
"${DC[@]}" up -d pipeline-monitor

CID=$("${DC[@]}" ps -q pipeline-monitor)
if [[ -z "$CID" ]]; then
  echo "FAIL: pipeline-monitor container was not created"
  exit 2
fi
echo "Container ID: $CID"

docker inspect "$CID" --format 'State={{.State.Status}} Health={{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}} StartedAt={{.State.StartedAt}}'

section "Recent container logs"
"${DC[@]}" logs --tail=120 pipeline-monitor || true

section "Container env snapshot"
docker exec "$CID" /bin/sh -c 'echo "DB_USER=${DB_USER:+set}"; echo "DB_PASS=${DB_PASS:+set}"; echo "DB_HOST=${DB_HOST}"; echo "DB_PORT=${DB_PORT}"; echo "DB_NAME=${DB_NAME}"'

section "HTTP check"
HTTP_CODE=$(curl -sS -o /tmp/pipeline_monitor_home.html -w "%{http_code}" "http://localhost:8050/" || true)
echo "GET / => HTTP ${HTTP_CODE}"
if [[ "$HTTP_CODE" != "200" ]]; then
  echo "WARN: dashboard endpoint not returning 200"
fi

section "DB connectivity from inside container"
set +e
docker exec -i "$CID" python - <<'PY'
import os
import socket
from sqlalchemy import create_engine, text

user=os.getenv("DB_USER")
pwd=os.getenv("DB_PASS")
host=os.getenv("DB_HOST")
port=int(os.getenv("DB_PORT") or "5432")
name=os.getenv("DB_NAME")

if not user or not pwd or not host or not name:
    print("FAIL: missing one or more DB_* env vars")
    raise SystemExit(3)

try:
    s=socket.create_connection((host, port), timeout=5)
    s.close()
    print(f"OK: TCP connect to {host}:{port}")
except Exception as e:
    print(f"FAIL: TCP connect to {host}:{port} -> {e}")
    raise SystemExit(4)

url=f"postgresql://{user}:{pwd}@{host}:{port}/{name}"
try:
    eng=create_engine(url, pool_pre_ping=True)
    with eng.connect() as c:
        one=c.execute(text("select 1")).scalar()
        exists=c.execute(text("""
            select exists (
              select 1
              from information_schema.tables
              where table_schema='pipeline' and table_name='etl_metrics'
            )
        """)).scalar()
        print(f"OK: select 1 => {one}")
        print(f"pipeline.etl_metrics exists => {exists}")
except Exception as e:
    print(f"FAIL: SQLAlchemy/DB query failed -> {e}")
    raise SystemExit(5)
PY
STATUS=$?
set -e

if [[ $STATUS -ne 0 ]]; then
  section "Diagnosis summary"
  echo "FAIL: DB validation failed. Most common causes:"
  echo "  1) METRICS_DB_* not exported in ~/.airflow or project .airflow"
  echo "  2) DB host not reachable from Docker network"
  echo "  3) Wrong credentials or database name"
  exit $STATUS
fi

section "Diagnosis summary"
echo "OK: pipeline-monitor container, HTTP endpoint, and DB checks passed"
echo "If UI still blank, inspect browser console and Dash callback errors in logs"
