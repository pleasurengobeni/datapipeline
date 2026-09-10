#!/usr/bin/env bash
# Security scan for the datapipeline stack.
# Checks: pip package CVEs, Docker image ages, pinned versions.
#
# Usage:
#   make security-scan         → run all checks
#   ./scripts/security-scan.sh → run directly
#
# Requirements: Docker must be running. pip-audit runs inside a temp container.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PASS=0
FAIL=0
WARN=0

_ok()   { echo "  ✓  $*"; ((PASS++)) || true; }
_fail() { echo "  ✗  $*"; ((FAIL++)) || true; }
_warn() { echo "  ⚠  $*"; ((WARN++)) || true; }
_head() { echo ""; echo "── $* ──────────────────────────────────────────"; }

# ── 1. Pinned image versions ──────────────────────────────────────────────────
_head "Image version pinning"

check_pinned() {
    local file="$1"
    local label="$2"
    while IFS= read -r line; do
        img=$(echo "$line" | awk '{print $2}')
        # Ignore AS aliases (multi-stage FROM ... AS name)
        img=$(echo "$img" | sed 's/ AS.*//i')
        if echo "$img" | grep -qiE ':latest$'; then
            _fail "$label: $img — uses :latest, pin to a specific version"
        elif echo "$img" | grep -qvE ':'; then
            # No colon at all — no tag
            _warn "$label: $img — no version tag, consider pinning"
        else
            _ok "$label: $img"
        fi
    done < <(grep -iE '^FROM\s' "$file" || true)
}

check_pinned "$PROJECT_ROOT/Dockerfile"                       "Airflow Dockerfile"
check_pinned "$PROJECT_ROOT/web_ui/Dockerfile"                "web_ui Dockerfile"
check_pinned "$PROJECT_ROOT/analytics/Dockerfile"             "analytics Dockerfile"
check_pinned "$PROJECT_ROOT/data_pipeline_monitor/Dockerfile" "monitor Dockerfile"

# Check docker-compose for :latest on PULLED (non-local-build) images
_head "docker-compose image tags"
# Lines like `image: ${PROJECT_NAME_LOWER:-...}:latest` are locally built — skip them
pulled_latest=$(grep -E '^\s+image:.*:latest' "$PROJECT_ROOT/docker-compose.yaml" 2>/dev/null \
  | grep -v 'PROJECT_NAME_LOWER' || true)
if [ -n "$pulled_latest" ]; then
    _fail "docker-compose.yaml: pulled image(s) use :latest — pin them:"
    echo "$pulled_latest"
else
    _ok "docker-compose.yaml: no pulled images use :latest"
fi

# ── 2. Pip CVE scan (pip-audit) ───────────────────────────────────────────────
_head "Pip package CVE scan (pip-audit)"

REQ_FILES=(
    "$PROJECT_ROOT/requirements.txt"
    "$PROJECT_ROOT/web_ui/requirements.txt"
    "$PROJECT_ROOT/analytics/requirements.txt"
    "$PROJECT_ROOT/data_pipeline_monitor/requirements.txt"
)

_run_pip_audit() {
    local req="$1"
    local label="$2"
    local result exit_code=0
    # pip-audit exits 1 when vulns are found, 0 when clean
    result=$(pip-audit -r "$req" --progress-spinner=off -S 2>&1) || exit_code=$?
    if [ "$exit_code" -eq 0 ]; then
        _ok "$label: no known CVEs"
    else
        _fail "$label: vulnerabilities found:"
        echo "$result" | grep -vE "^(Collecting|Downloading|Installing|Successfully|WARNING|Notice)" | head -30
    fi
}

if ! command -v pip-audit &>/dev/null; then
    echo "  pip-audit not found locally — running in a temporary Docker container..."
    for req in "${REQ_FILES[@]}"; do
        [ -f "$req" ] || continue
        label="${req#$PROJECT_ROOT/}"
        exit_code=0
        result=$(docker run --rm \
            -v "$req:/tmp/requirements.txt:ro" \
            python:3.11.9-slim-bookworm \
            sh -c "pip install --quiet --disable-pip-version-check pip-audit \
                   && pip-audit -r /tmp/requirements.txt --progress-spinner=off -S" 2>&1) || exit_code=$?
        if [ "$exit_code" -eq 0 ]; then
            _ok "$label: no known CVEs"
        else
            clean=$(echo "$result" | grep -vE "^(Collecting|Downloading|Installing|Successfully|WARNING|NOTICE|\[notice\])" || true)
            if echo "$clean" | grep -qi "error\|exception\|usage:"; then
                _warn "$label: pip-audit error — ${clean:0:200}"
            else
                _fail "$label: vulnerabilities found:"
                echo "$clean" | head -30
            fi
        fi
    done
else
    for req in "${REQ_FILES[@]}"; do
        [ -f "$req" ] || continue
        _run_pip_audit "$req" "${req#$PROJECT_ROOT/}"
    done
fi

# ── 3. Secrets in .env ────────────────────────────────────────────────────────
_head "Weak / default secrets in .env"

ENV_FILE="$PROJECT_ROOT/.env"
if [ -f "$ENV_FILE" ]; then
    while IFS='=' read -r key val; do
        [[ "$key" =~ ^# ]] && continue
        [[ -z "$key" ]] && continue
        val="${val//\"/}"
        val="${val//\'/}"
        case "$key" in
            *PASSWORD*|*SECRET*|*KEY*|*PASS*)
                if [ -z "$val" ]; then
                    _fail "Empty secret: $key"
                elif echo "$val" | grep -qiE '^(airflow|admin|password|changeme|secret|1234|test)$'; then
                    _fail "Weak default secret: $key=$val"
                else
                    _ok "$key: non-empty, non-default"
                fi
                ;;
        esac
    done < "$ENV_FILE"
else
    _warn ".env not found — run 'make gen-env' first"
fi

# ── 4. Port binding ───────────────────────────────────────────────────────────
_head "Port binding (must be 127.0.0.1, not 0.0.0.0)"

# Any port binding that is NOT prefixed with 127.0.0.1: is a public binding
if grep -E '^\s+-\s+"?[0-9]+:[0-9]+"?' "$PROJECT_ROOT/docker-compose.yaml" | \
   grep -v '127\.0\.0\.1' 2>/dev/null; then
    _fail "Found ports bound to 0.0.0.0 (public) — prefix with 127.0.0.1:"
else
    _ok "All mapped ports are bound to 127.0.0.1"
fi

# ── 5. security_opt / no-new-privileges ──────────────────────────────────────
_head "Container security options"

if grep -q "no-new-privileges:true" "$PROJECT_ROOT/docker-compose.yaml"; then
    _ok "no-new-privileges:true is set"
else
    _fail "no-new-privileges:true is not set in docker-compose.yaml"
fi

if grep -q "cap_drop:" "$PROJECT_ROOT/docker-compose.yaml"; then
    _ok "cap_drop is configured on at least one service"
else
    _warn "cap_drop not found — consider dropping ALL caps on Python services"
fi

# ── 6. FLASK_ENV ─────────────────────────────────────────────────────────────
_head "Flask production mode"

if grep -q "FLASK_ENV: production" "$PROJECT_ROOT/docker-compose.yaml" || \
   grep -q "FLASK_DEBUG: \"0\"" "$PROJECT_ROOT/docker-compose.yaml"; then
    _ok "Flask is set to production mode"
else
    _fail "FLASK_ENV is not set to production — debugger may be exposed"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════"
echo "  Security scan summary"
echo "  PASS: $PASS   WARN: $WARN   FAIL: $FAIL"
echo "════════════════════════════════════"
echo ""

[ "$FAIL" -eq 0 ]
