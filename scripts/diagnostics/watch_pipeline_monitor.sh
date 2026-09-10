#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

DC=(docker compose --project-directory "$ROOT_DIR" --env-file "$ROOT_DIR/.env")

echo "Watching pipeline-monitor every 5s. Press Ctrl+C to stop."
while true; do
  echo
  date
  "${DC[@]}" ps pipeline-monitor || true
  curl -sS -o /dev/null -w "HTTP %{http_code} | connect=%{time_connect}s | total=%{time_total}s\n" http://localhost:8050/ || echo "curl failed"
  "${DC[@]}" logs --tail=8 pipeline-monitor | tail -8 || true
  sleep 5
done
