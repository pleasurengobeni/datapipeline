#!/usr/bin/env python3
"""
rotate_falco_alerts.py
-----------------------
Falco explicitly does not rotate its own file_output — confirmed in
Falco's own default config comments: "Falco does not perform log rotation
for this file." Runs on the HOST via cron.

Doesn't need root/sudo even though the file itself is root-owned (Falco
runs privileged): renaming only requires write permission on the
containing directory, which is cenfri-owned, not on the file itself —
same principle already used elsewhere in this repo for root-owned DAG
files. Falco reopens alerts.log fresh on every write (file_output.
keep_alive: false in falco.yaml), so renaming it away is enough — Falco
transparently recreates it at the same path on its very next alert, no
restart or signal needed. The rename happens first and is atomic, so it's
safe even if Falco is mid-write at that instant.

Usage (cron, every 5 minutes):
    */5 * * * * /usr/bin/python3 /home/cenfri/datapipeline/airflow/scripts/rotate_falco_alerts.py
"""
import gzip
import shutil
from pathlib import Path

LOG_PATH = Path(__file__).resolve().parents[1] / "logs" / "falco" / "alerts.log"
MAX_SIZE_BYTES = 50 * 1024 * 1024  # matches Falco's own docker-log max-size for this service
MAX_ROTATED_FILES = 10             # matches Falco's own docker-log max-file for this service


def main():
    if not LOG_PATH.is_file():
        return
    if LOG_PATH.stat().st_size < MAX_SIZE_BYTES:
        return

    oldest = LOG_PATH.parent / f"{LOG_PATH.name}.{MAX_ROTATED_FILES}.gz"
    if oldest.exists():
        oldest.unlink()
    for i in range(MAX_ROTATED_FILES - 1, 0, -1):
        src = LOG_PATH.parent / f"{LOG_PATH.name}.{i}.gz"
        if src.exists():
            src.rename(LOG_PATH.parent / f"{LOG_PATH.name}.{i + 1}.gz")

    # Atomic rename first — safe regardless of what Falco is doing at this
    # instant. Anything that opens LOG_PATH after this gets a fresh file.
    rotated = LOG_PATH.parent / f"{LOG_PATH.name}.rotating"
    LOG_PATH.rename(rotated)

    # Compress at leisure now that it's a static snapshot, no rush.
    dest = LOG_PATH.parent / f"{LOG_PATH.name}.1.gz"
    with open(rotated, "rb") as f_in, gzip.open(dest, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    rotated.unlink()


if __name__ == "__main__":
    main()
