#!/usr/bin/env python3
"""
collect_host_metrics.py
------------------------
Runs on the HOST (via cron — never inside a container) and writes a small
JSON snapshot of server health to data_pipeline_monitor/data/host_metrics.json.

Why this exists rather than reading /proc directly from the dashboard
container: pipeline-monitor is the one unauthenticated, internet-facing
container in this stack (see docker-compose.yaml's comments on it) —
mounting host /proc, /sys, or the Docker socket into it to read this data
live would hand that same access to anyone who reaches the dashboard. This
script runs as a normal host process with normal host permissions, and the
container only ever reads the JSON file it produces (already inside the
existing read-write ./data_pipeline_monitor/data mount) — no new access
granted to the container.

Usage (cron, every 2 minutes):
    */2 * * * * /usr/bin/python3 /home/cenfri/datapipeline/airflow/scripts/collect_host_metrics.py
"""
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = REPO_ROOT / "data_pipeline_monitor" / "data" / "host_metrics.json"
HISTORY_PATH = REPO_ROOT / "data_pipeline_monitor" / "data" / "host_metrics_history.jsonl"
HISTORY_RETENTION_DAYS = 30
# Prune (rewrite, keeping only recent entries) only once the file gets big
# enough to be worth it — appending every 2 min is cheap; re-reading and
# rewriting the whole file every run for no reason isn't. At ~300 bytes/
# line and one line/2min, 30 days is ~6MB — this triggers well before that
# grows unbounded over months.
HISTORY_PRUNE_THRESHOLD_BYTES = 8 * 1024 * 1024

# Thresholds — deliberately simple and visible here rather than buried in
# the dashboard, so anyone reading this script sees exactly what green/
# yellow/red means.
DISK_YELLOW_PCT = 70
DISK_RED_PCT = 90
RAM_YELLOW_PCT = 70
RAM_RED_PCT = 90


def _worst(*statuses):
    order = {"red": 2, "yellow": 1, "green": 0, "unknown": 1}
    return max(statuses, key=lambda s: order.get(s, 1))


def _pct_status(pct, yellow, red):
    if pct is None:
        return "unknown"
    if pct >= red:
        return "red"
    if pct >= yellow:
        return "yellow"
    return "green"


def collect_server():
    try:
        loadavg = Path("/proc/loadavg").read_text().split()[:3]
        loadavg = [float(x) for x in loadavg]
    except Exception:
        loadavg = None
    try:
        uptime_seconds = float(Path("/proc/uptime").read_text().split()[0])
    except Exception:
        uptime_seconds = None
    return {
        "status": "green" if loadavg is not None else "unknown",
        "load_avg": loadavg,
        "uptime_seconds": uptime_seconds,
    }


def collect_disk():
    try:
        out = subprocess.run(
            ["df", "-B1", "--output=size,used,pcent", "/"],
            capture_output=True, text=True, timeout=10, check=True,
        ).stdout.strip().splitlines()
        total_bytes, used_bytes, pcent_str = out[-1].split()
        pct = float(pcent_str.rstrip("%"))
        return {
            "status": _pct_status(pct, DISK_YELLOW_PCT, DISK_RED_PCT),
            "total_gb": round(int(total_bytes) / 1e9, 1),
            "used_gb": round(int(used_bytes) / 1e9, 1),
            "percent_used": pct,
            "mount": "/",
        }
    except Exception as exc:
        return {"status": "unknown", "error": str(exc)}


def collect_ram():
    try:
        meminfo = {}
        for line in Path("/proc/meminfo").read_text().splitlines():
            m = re.match(r"^(\w+):\s+(\d+)\s*kB", line)
            if m:
                meminfo[m.group(1)] = int(m.group(2))
        total_kb = meminfo.get("MemTotal", 0)
        avail_kb = meminfo.get("MemAvailable", meminfo.get("MemFree", 0))
        used_kb = total_kb - avail_kb
        pct = (used_kb / total_kb * 100) if total_kb else None
        return {
            "status": _pct_status(pct, RAM_YELLOW_PCT, RAM_RED_PCT),
            "total_gb": round(total_kb / 1e6, 1),
            "used_gb": round(used_kb / 1e6, 1),
            "percent_used": round(pct, 1) if pct is not None else None,
        }
    except Exception as exc:
        return {"status": "unknown", "error": str(exc)}


def collect_network():
    """Primary interface's cumulative error/drop counters (from boot) and
    whether it's up. Cumulative counts, not a rate — any non-zero count is
    flagged yellow so a human can judge whether it's old/stale or growing,
    rather than the script guessing a rate threshold."""
    try:
        route = subprocess.run(
            ["ip", "route", "get", "1.1.1.1"], capture_output=True, text=True, timeout=5,
        ).stdout
        m = re.search(r"dev (\S+)", route)
        iface = m.group(1) if m else None
        if not iface:
            return {"status": "unknown", "error": "could not determine primary interface"}

        operstate = Path(f"/sys/class/net/{iface}/operstate").read_text().strip()
        stats_dir = Path(f"/sys/class/net/{iface}/statistics")
        rx_errors = int((stats_dir / "rx_errors").read_text())
        tx_errors = int((stats_dir / "tx_errors").read_text())
        rx_dropped = int((stats_dir / "rx_dropped").read_text())
        tx_dropped = int((stats_dir / "tx_dropped").read_text())
        # Cumulative since boot, not a rate — the dashboard computes rates
        # itself by diffing consecutive history entries, so raw counters
        # here are all it needs regardless of how far apart runs are.
        rx_bytes = int((stats_dir / "rx_bytes").read_text())
        tx_bytes = int((stats_dir / "tx_bytes").read_text())
        total_issues = rx_errors + tx_errors + rx_dropped + tx_dropped

        if operstate != "up":
            status = "red"
        elif total_issues > 0:
            status = "yellow"
        else:
            status = "green"

        return {
            "status": status,
            "interface": iface,
            "link_state": operstate,
            "rx_errors": rx_errors,
            "tx_errors": tx_errors,
            "rx_dropped": rx_dropped,
            "tx_dropped": tx_dropped,
            "rx_bytes": rx_bytes,
            "tx_bytes": tx_bytes,
        }
    except Exception as exc:
        return {"status": "unknown", "error": str(exc)}


def collect_security():
    """Falco is the runtime security monitor this stack is meant to run
    (see Makefile's falco-up target) — not yet running as of writing this.
    Red/yellow if it isn't, rather than trying to invent a broader security
    score with no real signal behind it."""
    try:
        out = subprocess.run(
            ["docker", "ps", "--filter", "name=falco", "--format", "{{.Status}}"],
            capture_output=True, text=True, timeout=10,
        ).stdout.strip()
        running = out.lower().startswith("up")
        return {
            "status": "green" if running else "red",
            "falco_running": running,
            "detail": "Falco runtime security monitor is running" if running
                       else "Falco is not running — see `make falco-up`",
        }
    except Exception as exc:
        return {"status": "unknown", "falco_running": None, "detail": str(exc)}


def main():
    server = collect_server()
    disk = collect_disk()
    ram = collect_ram()
    network = collect_network()
    security = collect_security()

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "server": server,
        "disk": disk,
        "ram": ram,
        "network": network,
        "security": security,
        "overall_status": _worst(
            server["status"], disk["status"], ram["status"],
            network["status"], security["status"],
        ),
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = OUT_PATH.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2))
    tmp_path.replace(OUT_PATH)  # atomic — dashboard never sees a half-written file

    # Compact history line (no indent) — one entry per run, appended.
    # Server page charts (disk/RAM/network over time) read this.
    with HISTORY_PATH.open("a") as f:
        f.write(json.dumps(payload) + "\n")
    _prune_history_if_large()


def _prune_history_if_large():
    if not HISTORY_PATH.is_file():
        return
    if HISTORY_PATH.stat().st_size < HISTORY_PRUNE_THRESHOLD_BYTES:
        return
    cutoff = datetime.now(timezone.utc).timestamp() - HISTORY_RETENTION_DAYS * 86400
    kept = []
    with HISTORY_PATH.open() as f:
        for line in f:
            try:
                entry = json.loads(line)
                ts = datetime.fromisoformat(entry["generated_at"]).timestamp()
            except Exception:
                continue  # drop unparseable lines rather than let one bad line block pruning
            if ts >= cutoff:
                kept.append(line)
    tmp_path = HISTORY_PATH.with_suffix(".jsonl.tmp")
    tmp_path.write_text("".join(kept))
    tmp_path.replace(HISTORY_PATH)


if __name__ == "__main__":
    main()
