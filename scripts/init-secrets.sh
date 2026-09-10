#!/usr/bin/env bash
# Create Docker secret files from ~/.airflow
# Run once on first deploy, or after rotating credentials.
# Secret files live in ./secrets/ (gitignored) and are mounted read-only
# into each container at /run/secrets/<name>.
#
# Usage:
#   make init-secrets
#
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SECRETS_DIR="$PROJECT_ROOT/secrets"
AIRFLOW_ENV="${HOME}/.airflow"

if [ ! -f "$AIRFLOW_ENV" ]; then
    echo "✗  ~/.airflow not found. Cannot initialise secrets."
    exit 1
fi

set -a
# shellcheck source=/dev/null
source "$AIRFLOW_ENV"
set +a

mkdir -p "$SECRETS_DIR"
chmod 700 "$SECRETS_DIR"

write_secret() {
    local name="$1"
    local value="$2"
    if [ -z "$value" ]; then
        echo "  ⚠  Skipping $name — value is empty"
        return
    fi
    # A previous run leaves this file chmod 444 (read-only) — without
    # restoring write permission first, overwriting it on a re-run (e.g.
    # after rotating a credential, or adding a new secret to ~/.airflow)
    # fails with "Permission denied" instead of updating the file.
    if [ -f "$SECRETS_DIR/$name" ]; then
        chmod u+w "$SECRETS_DIR/$name"
    fi
    printf '%s' "$value" > "$SECRETS_DIR/$name"
    # 0444: read-only, any user (needed so container UID ≠ host UID can read)
    # Host-level access is restricted by the parent dir (chmod 700 above)
    chmod 444 "$SECRETS_DIR/$name"
    echo "  ✓  $name"
}

echo "Writing secrets to $SECRETS_DIR/ ..."
write_secret "postgres_password"              "${POSTGRES_PASSWORD:-}"
write_secret "postgres_user"                  "${POSTGRES_USER:-}"
write_secret "airflow_fernet_key"             "${AIRFLOW__CORE__FERNET_KEY:-}"
write_secret "airflow_webserver_secret_key"   "${AIRFLOW__WEBSERVER__SECRET_KEY:-}"
write_secret "airflow_www_user_password"      "${_AIRFLOW_WWW_USER_PASSWORD:-}"
write_secret "webui_secret_key"               "${WEBUI_SECRET_KEY:-}"
write_secret "webui_admin_user"               "${WEBUI_ADMIN_USER:-admin}"
write_secret "webui_admin_pass"               "${WEBUI_ADMIN_PASS:-}"
write_secret "webui_fernet_key"               "${WEBUI_FERNET_KEY:-}"

echo ""
echo "✓  Secrets written. Files are chmod 600 and gitignored."
echo "   Run 'make up' (or 'make restart') to apply."
