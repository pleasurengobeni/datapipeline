#!/usr/bin/env bash
# Wrapper entrypoint: read Docker secret files → export as env vars → exec Airflow.
# Mounted at /run/airflow-entrypoint.sh inside each Airflow container.
set -euo pipefail

_read_secret() {
    local file="/run/secrets/$1"
    [ -f "$file" ] && cat "$file" || true
}

# ── Read secrets from files (if present) ─────────────────────────────────────
PG_USER="$(_read_secret postgres_user)"
PG_PASS="$(_read_secret postgres_password)"
FERNET="$(_read_secret airflow_fernet_key)"
WS_KEY="$(_read_secret airflow_webserver_secret_key)"
WW_PASS="$(_read_secret airflow_www_user_password)"

# Only override if the secret file was non-empty
[ -n "$PG_USER" ]  && export POSTGRES_USER="$PG_USER"
[ -n "$PG_PASS" ]  && export POSTGRES_PASSWORD="$PG_PASS"
[ -n "$FERNET" ]   && export AIRFLOW__CORE__FERNET_KEY="$FERNET"
[ -n "$WS_KEY" ]   && export AIRFLOW__WEBSERVER__SECRET_KEY="$WS_KEY"
[ -n "$WW_PASS" ]  && export _AIRFLOW_WWW_USER_PASSWORD="$WW_PASS"

# Reconstruct DB URIs from (possibly just-updated) POSTGRES_* vars
if [ -n "${POSTGRES_USER:-}" ] && [ -n "${POSTGRES_PASSWORD:-}" ]; then
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres/airflow"
    export AIRFLOW__CORE__SQL_ALCHEMY_CONN="$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
    export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres/airflow"
fi

# Hand off to the real Airflow entrypoint with all original arguments
exec /entrypoint "$@"
