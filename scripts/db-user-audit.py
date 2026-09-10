#!/usr/bin/env python3
"""
Scan every Airflow connection for unexpected database users.

Reads all connections from the Airflow metadata DB, connects to each
Postgres/MySQL/MSSQL target, lists database users, and flags any that are
not in the configured allowlist. Optionally drops the unexpected users.

Usage (run via `make db-audit` or directly inside the web-ui container):
  python scripts/db-user-audit.py
  python scripts/db-user-audit.py --drop-unknown   # drop unexpected users
  python scripts/db-user-audit.py --allowlist etl_user,readonly_user

Environment (read automatically from container env):
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN  — Airflow metadata DB URI
  AIRFLOW__CORE__FERNET_KEY            — Fernet key to decrypt stored passwords
  DB_AUDIT_ALLOWLIST                   — comma-separated list of allowed usernames
                                         (merged with --allowlist arg)
"""
import argparse
import os
import sys

# ── Fernet decryption ─────────────────────────────────────────────────────────

def _decrypt(password_enc: str) -> str:
    if not password_enc:
        return ""
    fernet_key = os.getenv("AIRFLOW__CORE__FERNET_KEY", "")
    if fernet_key:
        try:
            from cryptography.fernet import Fernet
            return Fernet(fernet_key.encode()).decrypt(password_enc.encode()).decode()
        except Exception:
            pass
    return password_enc  # not encrypted or wrong key — use as-is


# ── Build engine ──────────────────────────────────────────────────────────────

def _build_uri(conn_type: str, host: str, port, schema: str,
               login: str, password: str) -> str:
    type_map = {
        "postgres":   "postgresql+psycopg2",
        "postgresql": "postgresql+psycopg2",
        "mysql":      "mysql+pymysql",
        "mssql":      "mssql+pyodbc",
    }
    dialect = type_map.get((conn_type or "").lower())
    if not dialect:
        return ""
    port_str = f":{port}" if port else ""
    return f"{dialect}://{login}:{password}@{host}{port_str}/{schema}"


# ── Per-DB user queries ───────────────────────────────────────────────────────

SUPERUSER_QUERIES = {
    "postgresql+psycopg2": (
        "SELECT usename, usesuper, usecreatedb, usecreaterole "
        "FROM pg_catalog.pg_user ORDER BY usename",
        ["usename", "usesuper", "usecreatedb", "usecreaterole"],
    ),
    "mysql+pymysql": (
        "SELECT User, Host, Super_priv, Create_priv FROM mysql.user ORDER BY User",
        ["user", "host", "super_priv", "create_priv"],
    ),
    "mssql+pyodbc": (
        "SELECT name, type_desc, is_disabled FROM sys.server_principals "
        "WHERE type IN ('S','U') ORDER BY name",
        ["name", "type_desc", "is_disabled"],
    ),
}

def _primary_name_col(dialect: str) -> str:
    return {"postgresql+psycopg2": "usename", "mysql+pymysql": "user",
            "mssql+pyodbc": "name"}.get(dialect, "name")


# ── Drop queries ──────────────────────────────────────────────────────────────

def _drop_sql(dialect: str, username: str) -> str:
    if dialect == "postgresql+psycopg2":
        return f'DROP USER IF EXISTS "{username}"'
    if dialect == "mysql+pymysql":
        return f"DROP USER IF EXISTS '{username}'@'%'"
    if dialect == "mssql+pyodbc":
        return f"DROP LOGIN [{username}]"
    return ""


# ── Main audit ────────────────────────────────────────────────────────────────

def audit(allowlist: set[str], drop_unknown: bool) -> int:
    from sqlalchemy import create_engine, text

    airflow_db = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN") or \
                 os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
    if not airflow_db:
        print("ERROR: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN is not set.")
        return 1

    meta_engine = create_engine(airflow_db)
    with meta_engine.connect() as mc:
        rows = mc.execute(text(
            "SELECT conn_id, conn_type, host, port, schema, login, password "
            "FROM connection ORDER BY conn_id"
        )).fetchall()

    print(f"\nFound {len(rows)} Airflow connections to audit.\n")
    issues_found = 0

    for conn_id, conn_type, host, port, schema, login, password_enc in rows:
        if not conn_type or conn_type.lower() not in (
            "postgres", "postgresql", "mysql", "mssql"
        ):
            continue

        password = _decrypt(password_enc or "")
        uri = _build_uri(conn_type, host, port, schema, login, password)
        if not uri:
            continue

        dialect = uri.split("://")[0]
        query, cols = SUPERUSER_QUERIES.get(dialect, (None, None))
        if not query:
            continue

        print(f"── {conn_id}  ({host}/{schema}) ──────────────────")
        try:
            eng = create_engine(uri, connect_args={"connect_timeout": 5})
            with eng.connect() as c:
                users = c.execute(text(query)).fetchall()
        except Exception as exc:
            print(f"  SKIP: cannot connect — {exc}\n")
            continue

        name_col = _primary_name_col(dialect)
        col_idx  = cols.index(name_col)

        for row in users:
            uname = row[col_idx]
            status = "OK" if uname in allowlist else "UNEXPECTED"
            is_super = any(str(row[i]).upper() in ("TRUE", "1", "YES", "Y")
                           for i, c in enumerate(cols) if "super" in c or "priv" in c)
            flag = " [SUPERUSER]" if is_super else ""
            marker = "  ✓" if status == "OK" else "  ✗ UNEXPECTED"
            print(f"{marker}  {uname}{flag}")
            if status == "UNEXPECTED":
                issues_found += 1
                if drop_unknown:
                    sql = _drop_sql(dialect, uname)
                    if sql:
                        try:
                            with eng.begin() as c:
                                c.execute(text(sql))
                            print(f"     → DROPPED {uname}")
                        except Exception as exc:
                            print(f"     → DROP FAILED: {exc}")
        print()

    if issues_found == 0:
        print("✓  No unexpected users found across all connections.")
    else:
        verb = "dropped" if drop_unknown else "found (run with --drop-unknown to remove)"
        print(f"✗  {issues_found} unexpected user(s) {verb}.")

    return 1 if (issues_found and not drop_unknown) else 0


def main():
    parser = argparse.ArgumentParser(description="Audit DB users in Airflow connections.")
    parser.add_argument("--drop-unknown", action="store_true",
                        help="Drop users not in the allowlist (use with caution).")
    parser.add_argument("--allowlist", default="",
                        help="Additional comma-separated usernames to allow.")
    args = parser.parse_args()

    env_allowlist  = set(filter(None, os.getenv("DB_AUDIT_ALLOWLIST", "").split(",")))
    arg_allowlist  = set(filter(None, args.allowlist.split(",")))
    # Always allow the Airflow connection user and common system users
    system_users   = {"airflow", "postgres", "rdsadmin", "rdsrepladmin",
                      "cloudsqlsuperuser", "azure_superuser"}
    allowlist = env_allowlist | arg_allowlist | system_users

    print("DB User Audit")
    print("=============")
    print(f"Allowlist: {sorted(allowlist)}")
    print(f"Drop unknown: {args.drop_unknown}")

    sys.exit(audit(allowlist, args.drop_unknown))


if __name__ == "__main__":
    main()
