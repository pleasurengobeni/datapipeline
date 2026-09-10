#!/usr/bin/env bash
# Delete Airflow task logs and scheduler logs older than RETAIN_DAYS (default 30).
# Safe to run while Airflow is running — completed task logs are never re-read.
#
# Usage:
#   make clean-logs                   → delete logs older than 30 days (dry-run first)
#   RETAIN_DAYS=7 make clean-logs     → delete logs older than 7 days
#   ./scripts/clean-logs.sh --dry-run → preview what would be deleted
#
# Server cron (recommended):
#   0 2 * * * cd ~/datapipeline/airflow && make clean-logs >> logs/clean-logs.cron.log 2>&1
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGS_DIR="${AIRFLOW_PROJ_DIR:-$PROJECT_ROOT}/logs"
RETAIN_DAYS="${RETAIN_DAYS:-30}"
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        --days=*)  RETAIN_DAYS="${arg#*=}" ;;
    esac
done

if [ ! -d "$LOGS_DIR" ]; then
    echo "Logs directory not found: $LOGS_DIR"
    exit 0
fi

echo "Log cleanup — files older than ${RETAIN_DAYS} days in $LOGS_DIR"
echo "Dry run: $DRY_RUN"
echo ""

# Count and size before deletion
BEFORE_COUNT=$(find "$LOGS_DIR" -type f -name "*.log" -mtime +"$RETAIN_DAYS" 2>/dev/null | wc -l | tr -d ' ')
BEFORE_SIZE=$(find  "$LOGS_DIR" -type f -name "*.log" -mtime +"$RETAIN_DAYS" 2>/dev/null \
              -exec du -ch {} + 2>/dev/null | tail -1 | awk '{print $1}' || echo "0")

echo "Files to delete: $BEFORE_COUNT ($BEFORE_SIZE)"

if [ "$BEFORE_COUNT" -eq 0 ]; then
    echo "Nothing to clean."
    exit 0
fi

if [ "$DRY_RUN" = true ]; then
    echo ""
    echo "DRY RUN — no files deleted. Remove --dry-run to actually clean."
    find "$LOGS_DIR" -type f -name "*.log" -mtime +"$RETAIN_DAYS" 2>/dev/null | head -20
    [ "$BEFORE_COUNT" -gt 20 ] && echo "  ... and $((BEFORE_COUNT - 20)) more"
    exit 0
fi

# Delete old log files
find "$LOGS_DIR" -type f -name "*.log" -mtime +"$RETAIN_DAYS" -delete 2>/dev/null
# Remove empty directories left behind (but keep the top-level logs/ dir)
find "$LOGS_DIR" -mindepth 1 -type d -empty -delete 2>/dev/null || true

AFTER_COUNT=$(find "$LOGS_DIR" -type f -name "*.log" 2>/dev/null | wc -l | tr -d ' ')
echo "Done. Removed $BEFORE_COUNT files ($BEFORE_SIZE). Remaining: $AFTER_COUNT files."
echo "Completed at $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
