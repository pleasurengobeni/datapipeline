#!/usr/bin/env python3
"""
summarize_falco_alerts.py
--------------------------
Maintains a compact daily summary (alert count per day per rule) so the
Falco dashboard page can show a weekly trend without re-parsing
potentially-large raw alerts.log/rotated files on every dashboard load —
alerts.log rotates at 50MB (see rotate_falco_alerts.py) and under noisy
conditions that can happen every 30 minutes, so the kept rotated copies
alone don't reliably cover a full week. Runs on the HOST via cron.

Checkpoints by the last-seen alert *timestamp* (not a byte offset or file
identity), so it's naturally rotation-safe: alert timestamps keep moving
forward regardless of which physical file they land in, so "everything
with time > last_seen" is correct whether or not alerts.log got rotated
since the last run. This only works because it runs BEFORE rotation in
the same cron tick (see crontab) — if rotation ran first, alerts written
to the just-rotated-away file would never be read from anywhere.

Usage (cron, every 5 minutes, before rotate_falco_alerts.py):
    */5 * * * * /usr/bin/python3 .../scripts/summarize_falco_alerts.py && /usr/bin/python3 .../scripts/rotate_falco_alerts.py
"""
import json
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ALERTS_LOG = REPO_ROOT / "logs" / "falco" / "alerts.log"
SUMMARY_PATH = REPO_ROOT / "data_pipeline_monitor" / "data" / "falco_daily_summary.json"
# A "now" view of recent alerts, written here rather than mounting the raw
# logs/falco directory into pipeline-monitor — that container is
# deliberately kept to the minimum access needed (see collect_host_
# metrics.py's docstring for why), same reasoning applies here: only ever
# hand it a small, controlled file, not direct access to the real log dir.
RECENT_PATH = REPO_ROOT / "data_pipeline_monitor" / "data" / "falco_recent_alerts.jsonl"
RECENT_MAX_LINES = 200
RETENTION_DAYS = 30


def _load_summary():
    if not SUMMARY_PATH.is_file():
        return {"last_seen": None, "days": {}}
    try:
        return json.loads(SUMMARY_PATH.read_text())
    except Exception:
        return {"last_seen": None, "days": {}}


def main():
    if not ALERTS_LOG.is_file():
        return

    summary = _load_summary()
    last_seen = summary.get("last_seen")
    days = summary.get("days", {})
    max_time_seen = last_seen
    recent = deque(maxlen=RECENT_MAX_LINES)  # "now" view — every valid line seen this pass, not just new ones

    with ALERTS_LOG.open() as f:
        for line in f:
            try:
                entry = json.loads(line)
                ts = entry["time"]
                rule = entry["rule"]
                priority = entry.get("priority", "unknown")
            except Exception:
                continue  # one corrupt/partial line (e.g. truncated mid-write) shouldn't block the rest

            recent.append(line if line.endswith("\n") else line + "\n")

            if last_seen and ts <= last_seen:
                continue
            day = ts[:10]  # "2026-08-12T12:34:56..." -> "2026-08-12"
            bucket = days.setdefault(day, {"total": 0, "by_rule": {}, "by_priority": {}})
            bucket["total"] += 1
            bucket["by_rule"][rule] = bucket["by_rule"].get(rule, 0) + 1
            bucket["by_priority"][priority] = bucket["by_priority"].get(priority, 0) + 1
            if max_time_seen is None or ts > max_time_seen:
                max_time_seen = ts

    RECENT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_recent = RECENT_PATH.with_suffix(".jsonl.tmp")
    tmp_recent.write_text("".join(recent))
    tmp_recent.replace(RECENT_PATH)

    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
    days = {d: v for d, v in days.items() if d >= cutoff}

    payload = {"last_seen": max_time_seen, "days": days}
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = SUMMARY_PATH.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2))
    tmp_path.replace(SUMMARY_PATH)


if __name__ == "__main__":
    main()
