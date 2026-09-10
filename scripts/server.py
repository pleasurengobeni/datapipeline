#!/usr/bin/env python3
"""
server.py
---------
Small paramiko wrapper for running commands against the deployed server,
reading connection details from the user's ~/.ssh/config (host alias
"wasac" by default) instead of hardcoding host/user/key here.

Usage:
    python scripts/server.py "docker ps"
    python scripts/server.py --host wasac "docker exec airflow-airflow-scheduler-1 airflow dags list"

Can also be imported:
    from scripts.server import run
    out, err, code = run("docker ps")
"""

import argparse
import sys
from pathlib import Path

import paramiko


def _load_host_config(alias: str) -> dict:
    """Resolve an ssh_config host alias to connection kwargs, the same way
    the `ssh <alias>` CLI would (HostName/User/IdentityFile/Port)."""
    config_path = Path.home() / ".ssh" / "config"
    cfg = paramiko.SSHConfig()
    if config_path.is_file():
        with config_path.open() as fh:
            cfg.parse(fh)
    host_cfg = cfg.lookup(alias)
    return {
        "hostname": host_cfg.get("hostname", alias),
        "username": host_cfg.get("user"),
        "port": int(host_cfg.get("port", 22)),
        "key_filename": host_cfg.get("identityfile", [None])[0],
    }


def connect(alias: str = "wasac") -> paramiko.SSHClient:
    conn = _load_host_config(alias)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=conn["hostname"],
        username=conn["username"],
        port=conn["port"],
        key_filename=conn["key_filename"],
    )
    return client


def run(command: str, alias: str = "wasac", timeout: int = 60):
    """Run one command on the remote host. Returns (stdout, stderr, exit_code)."""
    client = connect(alias)
    try:
        _, stdout, stderr = client.exec_command(command, timeout=timeout)
        out = stdout.read().decode(errors="replace")
        err = stderr.read().decode(errors="replace")
        code = stdout.channel.recv_exit_status()
        return out, err, code
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description="Run a command on the deployed server via paramiko.")
    parser.add_argument("command", help="Shell command to run remotely")
    parser.add_argument("--host", default="wasac", help="ssh_config host alias (default: wasac)")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    out, err, code = run(args.command, alias=args.host, timeout=args.timeout)
    if out:
        sys.stdout.write(out)
    if err:
        sys.stderr.write(err)
    sys.exit(code)


if __name__ == "__main__":
    main()
