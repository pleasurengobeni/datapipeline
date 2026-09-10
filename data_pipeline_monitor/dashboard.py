import json
import os
import re
import time
import traceback
from datetime import timedelta
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

CONFIG_DIR = Path(os.getenv("CONFIG_DIR", "/app/dags/config"))


# Read from /run/secrets/<name> first (Docker secrets), fall back to env var.
# Same convention as web_ui's _secret() — a secret file mounted read-only
# under /run/secrets never appears in `docker inspect`; a ${VAR} baked into
# environment: does. This container resolves the Fernet key (decrypts every
# Airflow connection password) and the Airflow metadata DB credentials, so
# it gets the same treatment, not the plaintext-env-var shortcut.
def _secret(secret_name: str, env_var: str = None) -> str:
    path = Path("/run/secrets") / secret_name
    if path.is_file():
        return path.read_text().strip()
    return os.getenv(env_var or secret_name.upper(), "")


def _build_airflow_db_url():
    # Prefer an explicit full connection string if one's set (e.g. pointing
    # at a non-default host/db) — otherwise build it from the same
    # postgres_user/postgres_password secrets airflow-common and web_ui use,
    # against the same fixed postgres/airflow metadata DB every other
    # service in this stack already targets.
    explicit = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN") or os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
    if explicit:
        return explicit
    pg_user = _secret("postgres_user", "POSTGRES_USER")
    pg_pass = _secret("postgres_password", "POSTGRES_PASSWORD")
    if not (pg_user and pg_pass):
        return None
    return f"postgresql+psycopg2://{quote_plus(pg_user)}:{quote_plus(pg_pass)}@postgres/airflow"


AIRFLOW_DB = _build_airflow_db_url()
AIRFLOW_FERNET_KEY = _secret("airflow_fernet_key", "AIRFLOW__CORE__FERNET_KEY")

st.set_page_config(page_title="ETL Pipeline Monitoring Dashboard", layout="wide")


def _masked_value(val):
    if not val:
        return "<empty>"
    return "****"


def _render_load_error(title, exc):
    st.error(f"{title}: failed to load data")
    st.code(str(exc))
    with st.expander("Runtime DB configuration"):
        st.write(f"DB_USER: {_masked_value(os.getenv('DB_USER', ''))}")
        st.write(f"DB_PASS: {_masked_value(os.getenv('DB_PASS', ''))}")
        st.write(f"DB_HOST: {os.getenv('DB_HOST', '<empty>')}")
        st.write(f"DB_PORT: {os.getenv('DB_PORT', '<empty>')}")
        st.write(f"DB_NAME: {os.getenv('DB_NAME', '<empty>')}")
    with st.expander("Technical traceback"):
        st.code(traceback.format_exc())


def _get_lookback_days():
    raw = os.getenv("METRICS_LOOKBACK_DAYS", "30")
    try:
        return int(raw)
    except Exception:
        return 30


# Non-production sources (test/demo/sandbox DAGs, scratch tables) matched by
# name against dag_id/table_name/source_system and dropped everywhere in the
# dashboard — a customer-facing overview showing "test_customer_import"
# alongside real pipelines undermines trust in the real numbers. Override
# with EXCLUDE_NAME_PATTERN if a legitimate source happens to collide.
EXCLUDE_NAME_PATTERN = os.getenv(
    "EXCLUDE_NAME_PATTERN",
    # Underscore-delimited, not \b — dag/table names use "_" as the word
    # separator (e.g. "hybrid_wasac_dw_dw_customer_test"), and \b doesn't
    # split on "_" since it's a word character; \btest\b would silently
    # never match any of these real names.
    r"(?i)(?:^|_)(?:test|demo|sample|sandbox|dummy|poc|scratch)(?:_|$)",
)
_EXCLUDE_RE = re.compile(EXCLUDE_NAME_PATTERN)


def _is_excluded_name(*values) -> bool:
    return any(v and _EXCLUDE_RE.search(str(v)) for v in values)


def _build_engine():
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_host = os.getenv("DB_HOST")
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_name = os.getenv("DB_NAME")

    missing = [k for k, v in {
        "DB_USER": db_user,
        "DB_PASS": db_pass,
        "DB_HOST": db_host,
        "DB_NAME": db_name,
    }.items() if not v]
    if missing:
        raise EnvironmentError("Pipeline monitor DB env is incomplete. Missing: " + ", ".join(missing))

    if (
        db_name == "airflow"
        and db_host in {"postgres", "localhost", "127.0.0.1"}
        and os.getenv("ALLOW_METRICS_ON_AIRFLOW_DB", "0") != "1"
    ):
        raise EnvironmentError(
            "Refusing to use Airflow metadata DB for pipeline-monitor. "
            "Set METRICS_DB_* to your DW DB (or set ALLOW_METRICS_ON_AIRFLOW_DB=1 to override)."
        )

    url = f"postgresql://{quote_plus(db_user)}:{quote_plus(db_pass)}@{db_host}:{db_port}/{db_name}"
    return create_engine(url, pool_pre_ping=True)


def _distinct_target_conn_ids():
    """Every target_db_conn_id referenced by any job config (dev or prod).

    pipeline.etl_metrics is not one shared table — each job's
    collect_metrics_fn writes it to ITS OWN target database (whatever
    that job's target_db_conn_id resolves to). A single hardcoded
    METRICS_DB_* connection only ever sees jobs that happen to target that
    one database and silently hides every other job — confirmed concretely:
    hybrid_cenfri_dev_con_july_new_connections (targets an RDS instance)
    never showed up here while a same-named DAG targeting the local dev DB
    did, purely because of which DB happened to be configured. This has to
    visit every distinct target instead.
    """
    conn_ids = set()
    if not CONFIG_DIR.is_dir():
        return conn_ids
    for path in CONFIG_DIR.rglob("*.json"):
        try:
            cfg = json.loads(path.read_text())
        except Exception:
            continue
        for env in ("dev", "prod"):
            cid = (cfg.get("target", {}).get(env, {}) or {}).get("target_db_conn_id")
            if cid:
                conn_ids.add(cid)
    return conn_ids


def _engine_for_conn_id(conn_id: str):
    """Build a SQLAlchemy engine from an Airflow connection row, same
    approach web_ui's _engine_for_conn_id uses — resolve each job's real
    target from Airflow's own connection metadata rather than a single
    hardcoded METRICS_DB_* connection."""
    meta_engine = create_engine(AIRFLOW_DB)
    with meta_engine.connect() as mc:
        row = mc.execute(
            text("SELECT conn_type, host, port, schema, login, password FROM connection WHERE conn_id = :cid"),
            {"cid": conn_id},
        ).fetchone()
    if not row:
        raise ValueError(f"Connection '{conn_id}' not found in Airflow")
    conn_type, host, port, schema, login, password_enc = row
    password = ""
    if password_enc:
        if AIRFLOW_FERNET_KEY:
            try:
                from cryptography.fernet import Fernet
                password = Fernet(AIRFLOW_FERNET_KEY.encode()).decrypt(password_enc.encode()).decode()
            except Exception:
                password = password_enc
        else:
            password = password_enc
    type_map = {
        "postgres": "postgresql+psycopg2",
        "postgresql": "postgresql+psycopg2",
        "mysql": "mysql+pymysql",
        "mssql": "mssql+pyodbc",
        "sqlite": "sqlite",
    }
    dialect = type_map.get((conn_type or "").lower(), conn_type)
    port_str = f":{port}" if port else ""
    return create_engine(f"{dialect}://{login}:{password}@{host}{port_str}/{schema}", pool_pre_ping=True)


# ─────────────────────────────────────────────────────────────────────────────
# Table-metadata-based freshness — the Overview tab's real source of truth
# ─────────────────────────────────────────────────────────────────────────────
# pipeline.etl_metrics is a log of *runs*, and it has real gaps: hybrid_load
# never populates target_today_records at all (confirmed concretely —
# hybrid_wasac_dw_invoice loaded 31,484 rows on a day etl_metrics reported 0
# for "today"), and a job that polls every minute floods it with
# "no_new_rows" rows that tell you nothing about the data itself. Every
# target table carries its own _loaded_at/_pipeline_inserted_at pipeline
# meta columns now (see the universal self-heal fix), so querying the table
# directly for "how many rows have _loaded_at = today" is accurate
# regardless of which pipeline_type last wrote it — no per-engine gaps to
# work around.
#
# Which tables appear is driven by what jobs actually recorded in
# pipeline.etl_metrics, not by a hand-maintained list in this file. Every
# template now writes a metrics row on every run (including the freshness
# columns — see the collect_metrics fix), so "has this pipeline ever loaded
# anything" is a question the data answers on its own.
#
# The previous hardcoded allowlist drifted by construction: a new production
# job needed a code change and a monitor redeploy before it would ever show
# up, which nobody remembers to do, and the page then reports "couldn't read
# any known production tables" while the pipelines run fine.
#
# Scoping is still possible, but it is now an explicit opt-OUT set in the job
# config (`"monitor": false`) rather than an opt-in list here — a scratch or
# one-off job is known to be one by whoever creates it, whereas a future
# production job is not known to this file.
def _excluded_dag_ids():
    """dag_ids whose config explicitly sets "monitor": false — test/scratch
    jobs the business page should not show. Anything else counts."""
    excluded = set()
    if not CONFIG_DIR.is_dir():
        return excluded
    for path in CONFIG_DIR.rglob("*.json"):
        try:
            cfg = json.loads(path.read_text())
        except Exception:
            continue
        if cfg.get("monitor") is False and cfg.get("dag_id"):
            excluded.add(cfg["dag_id"])
    return excluded


def _env_block(cfg: dict, key: str) -> dict:
    """Config files nest source/target under {"dev": {...}, "prod": {...}}
    for most jobs but not all — same fallback load_config() in every
    template uses."""
    block = cfg.get(key, {}) or {}
    for env in ("prod", "dev"):
        if env in block:
            return block[env] or {}
    return block


def _monitored_tables():
    """One entry per physical (conn_id, schema, table), merging every
    dag_id that writes to it — db_wasac_dw_cms_invoice and
    hybrid_wasac_dw_invoice both feed wasac_dw.cms.invoice, so "is invoice
    up to date" should reflect the table, not report as two disconnected
    sources for the same data.

    Every job with a resolvable target is included except those explicitly
    opted out via "monitor": false (see _excluded_dag_ids). Config is the
    source of truth for WHERE a table lives (conn_id/schema/table), which
    etl_metrics does not record; whether the job has actually run is then
    answered by the table query itself."""
    excluded = _excluded_dag_ids()
    tables = {}
    if not CONFIG_DIR.is_dir():
        return []
    for path in CONFIG_DIR.rglob("*.json"):
        try:
            cfg = json.loads(path.read_text())
        except Exception:
            continue
        dag_id = cfg.get("dag_id")
        if not dag_id or dag_id in excluded:
            continue
        tgt = _env_block(cfg, "target")
        src = _env_block(cfg, "source")
        conn_id = tgt.get("target_db_conn_id")
        schema = tgt.get("target_schema") or tgt.get("target_table_schema")
        table = tgt.get("target_table") or tgt.get("target_table_name")
        if not (conn_id and schema and table):
            continue
        key = (conn_id, schema, table)
        entry = tables.setdefault(key, {
            "conn_id": conn_id, "schema": schema, "table": table,
            "dag_ids": [], "business_date_column": None,
        })
        entry["dag_ids"].append(dag_id)
        # An explicitly configured business date wins over the incremental
        # column. They are often the same field, but not always: the
        # incremental column is whatever the pipeline pages through (an
        # updated_at, a surrogate id), while the business date is the one
        # that means "how current is this data" to someone reading the
        # dashboard. Only DB-style sources have an incremental column at
        # all, so without the explicit field file/API jobs could never show
        # a business date.
        business_col = (
            src.get("business_date_column")
            or src.get("incremental_column")
            or src.get("hwm_column")
        )
        if business_col and not entry["business_date_column"]:
            entry["business_date_column"] = business_col
    return list(tables.values())


def _query_table_freshness(entry):
    """Direct read of one target table's own pipeline meta columns — the
    numbers a non-technical user actually wants (loaded today/this week,
    how current the data itself is), sourced from the data, not a run log.

    Each period is paired with the one before it (yesterday, previous week)
    so the page can say whether today is normal for this table rather than
    just how many rows arrived — a bare "0 rows today" means nothing without
    knowing what yesterday looked like.
    """
    engine = _engine_for_conn_id(entry["conn_id"])
    business_col = entry.get("business_date_column")

    def _sql(business_expr):
        return text(f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE _loaded_at::date = CURRENT_DATE)     AS loaded_today,
                COUNT(*) FILTER (WHERE _loaded_at::date = CURRENT_DATE - 1) AS loaded_yesterday,
                COUNT(*) FILTER (WHERE _loaded_at >= now() - interval '7 days') AS loaded_week,
                COUNT(*) FILTER (
                    WHERE _loaded_at >= now() - interval '14 days'
                      AND _loaded_at <  now() - interval '7 days'
                ) AS loaded_prev_week,
                MAX(_loaded_at) AS last_loaded,
                {business_expr} AS business_date
            FROM "{entry['schema']}"."{entry['table']}"
        """)

    try:
        with engine.connect() as conn:
            row = conn.execute(
                _sql(f'MAX("{business_col}")' if business_col else "NULL")
            ).mappings().fetchone()
        return dict(row) if row else {}
    except Exception:
        if not business_col:
            raise
        # business_date_column came from config and may not match the
        # table's actual (cleaned) column name — retry without it rather
        # than losing today/week/last_loaded over one bad column name.
        with engine.connect() as conn:
            row = conn.execute(_sql("NULL")).mappings().fetchone()
        return dict(row) if row else {}


def _query_table_daily(entry, lookback_days=14):
    """Rows loaded per day for one table over the lookback window — feeds
    the weekly trend line, sourced the same way as the freshness numbers."""
    engine = _engine_for_conn_id(entry["conn_id"])
    query = text(f"""
        SELECT _loaded_at::date AS load_date, COUNT(*) AS rows_loaded
        FROM "{entry['schema']}"."{entry['table']}"
        WHERE _loaded_at >= CURRENT_DATE - INTERVAL '{int(lookback_days)} days'
        GROUP BY 1
        ORDER BY 1
    """)
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn)


@st.cache_data(ttl=1800)
def load_table_metrics(lookback_days=14):
    """Table-metadata-based numbers for the Overview tab. Returns
    (summary_df, daily_df, status) — summary_df has one row per physical
    table with today/week/overall/business-date/last-loaded; daily_df has
    one row per (table, date) for the trend line. A table that fails to
    query (unreachable connection, schema drift) is skipped, not fatal —
    matches load_and_process_data's "one bad connection can't hide every
    other job" rule."""
    tables = _monitored_tables()
    summary_rows = []
    daily_frames = []
    skipped = []
    for entry in tables:
        label = f'{entry["schema"]}.{entry["table"]}'
        try:
            freshness = _query_table_freshness(entry)
            daily = _query_table_daily(entry, lookback_days=lookback_days)
        except Exception as exc:
            # Skipping one unreachable table must not hide every other job,
            # but discarding WHY made a real outage indistinguishable from
            # "no jobs configured" — both surfaced as the same blank page.
            skipped.append((label, entry["conn_id"], f"{type(exc).__name__}: {exc}"))
            continue
        summary_rows.append({
            "table_label": label,
            "conn_id": entry["conn_id"],
            "schema": entry["schema"],
            "table": entry["table"],
            "dag_ids": ", ".join(sorted(entry["dag_ids"])),
            **freshness,
        })
        if not daily.empty:
            daily["table_label"] = label
            daily_frames.append(daily)

    summary_df = pd.DataFrame(summary_rows)
    daily_df = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame(
        columns=["load_date", "rows_loaded", "table_label"]
    )
    diagnostics = {"tables_considered": len(tables), "skipped": skipped}
    if summary_df.empty:
        return summary_df, daily_df, "empty", diagnostics

    summary_df["last_loaded"] = _to_naive_utc(summary_df["last_loaded"])
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    summary_df["hours_since_last_load"] = (now - summary_df["last_loaded"]).dt.total_seconds() / 3600

    def _freshness_label(hrs):
        if pd.isna(hrs):
            return "⚪ Unknown"
        if hrs <= 24:
            return "🟢 Up to date"
        if hrs <= 72:
            return "🟡 Slightly delayed"
        return "🔴 Needs attention"

    summary_df["freshness"] = summary_df["hours_since_last_load"].apply(_freshness_label)
    summary_df = summary_df.sort_values("last_loaded", ascending=False)
    return summary_df, daily_df, "ok", diagnostics


BASE_QUERY = """
SELECT
    COALESCE(NULLIF(schema, ''), dag_id) AS source_system,
    schema AS target_schema,
    dag_id,
    table_name,
    created_at,
    target_today_records,
    target_total_records,
    source_total_records,
    target_max_upload_date,
    target_max_incremental,
    source_max_incremental,
    source_loaded_to_max_target,
    rows_loaded_now,
    perc_loaded,
    run_id,
    run_start,
    run_end,
    run_duration_secs,
    run_status,
    error_msg,
    batch_size,
    COALESCE(engine_type, 'pandas') AS engine_type,
    COALESCE(source_type, 'db') AS source_type
FROM pipeline.etl_metrics
"""


def _build_metrics_query(include_lookback=True):
    lookback_days = _get_lookback_days()
    if include_lookback and lookback_days > 0:
        return (
            BASE_QUERY
            + "\nWHERE COALESCE(created_at, run_end, run_start) >= "
            + f"now() - interval '{lookback_days} days'"
            + "\nORDER BY COALESCE(created_at, run_end, run_start) DESC"
        )
    return BASE_QUERY + "\nORDER BY COALESCE(created_at, run_end, run_start) DESC"


def _format_duration(seconds):
    if pd.isna(seconds) or seconds is None:
        return "N/A"
    seconds = int(seconds)
    # Past a couple of days, hours stop being readable — a file job whose
    # sensor waited weeks for its file rendered as "1247h 45m", which nobody
    # parses as "about 52 days".
    if seconds >= 172800:  # 48h
        d, r = divmod(seconds, 86400)
        h = r // 3600
        return f"{d}d {h}h"
    if seconds >= 3600:
        h, r = divmod(seconds, 3600)
        m = r // 60
        return f"{h}h {m}m"
    if seconds >= 60:
        m, s = divmod(seconds, 60)
        return f"{m}m {s}s"
    return f"{seconds}s"


def _format_business_date(value):
    """Render a business date as a date, whatever the column's storage type.

    The configured column is frequently not a timestamp: an API-sourced
    table commonly lands it as epoch milliseconds in a bigint (ArcGIS does
    exactly this), and a raw-text landing keeps it as a string. MAX() then
    hands back 1816826400000, which is worse than useless on a page whose
    entire point is "how current is this data".

    Only reinterprets integers big enough to be unambiguous epochs; a plain
    year like 2024 is left alone rather than being read as a 1970 timestamp.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    try:
        if pd.isna(value):
            return "N/A"
    except (TypeError, ValueError):
        pass
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        n = float(value)
        unit = "ms" if abs(n) >= 1e11 else ("s" if abs(n) >= 1e9 else None)
        if unit:
            try:
                return str(pd.to_datetime(n, unit=unit).date())
            except Exception:
                return str(value)
        return str(value)
    ts = pd.to_datetime(value, errors="coerce")
    return str(ts.date()) if not pd.isna(ts) else str(value)


def _to_naive_utc(series):
    """Normalize a datetime column to naive UTC regardless of source dtype.

    Different target DBs return TIMESTAMP vs TIMESTAMPTZ columns, so the same
    logical field can come back tz-aware from one connection and tz-naive
    from another (or after pd.concat across connections). Any later
    comparison against a plain pd.Timestamp — e.g. "loaded in the last N
    days" — raises TypeError: Invalid comparison between dtype=...UTC... and
    Timestamp the moment a tz-aware value shows up. Stripping tz consistently
    right after parsing avoids that regardless of which connection it came from.
    """
    s = pd.to_datetime(series, errors="coerce")
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_convert("UTC").dt.tz_localize(None)
    return s


def _format_timedelta(hours):
    if pd.isna(hours):
        return "N/A"
    total_seconds = int(hours * 3600)
    days = total_seconds // (24 * 3600)
    if days > 0:
        return f"{days} day{'s' if days > 1 else ''}"
    rem = total_seconds % (24 * 3600)
    h = rem // 3600
    m = (rem % 3600) // 60
    if h > 0:
        return f"{h}h {m}m" if m > 0 else f"{h}h"
    return f"{m}m"


def _load_from_conn_ids(conn_ids, include_lookback):
    """Query pipeline.etl_metrics on every given conn_id's own database,
    skipping any that are unreachable or don't have the table yet — one
    bad/missing connection must not hide every other job's data."""
    frames = []
    query = _build_metrics_query(include_lookback=include_lookback)
    for conn_id in conn_ids:
        try:
            engine = _engine_for_conn_id(conn_id)
            frame = pd.read_sql_query(query, engine)
        except Exception:
            continue
        if not frame.empty:
            frame["_conn_id"] = conn_id
            frames.append(frame)
    return frames


@st.cache_data(ttl=1800)
def load_and_process_data():
    conn_ids = sorted(_distinct_target_conn_ids())
    frames = []

    if AIRFLOW_DB and conn_ids:
        frames = _load_from_conn_ids(conn_ids, include_lookback=True)

    if not frames:
        # No job configs / Airflow metadata reachable from this container,
        # or every per-job connection came back empty — fall back to the
        # single legacy METRICS_DB_* connection so the dashboard still works
        # in a leaner deployment that only sets that.
        try:
            engine = _build_engine()
            frame = pd.read_sql_query(_build_metrics_query(include_lookback=True), engine)
            if not frame.empty:
                frames = [frame]
        except Exception:
            pass

    if not frames and _get_lookback_days() > 0:
        # Retry unscoped (no lookback window) across the same connections,
        # in case everything's just older than the lookback window.
        if AIRFLOW_DB and conn_ids:
            frames = _load_from_conn_ids(conn_ids, include_lookback=False)
        if not frames:
            try:
                engine = _build_engine()
                frame = pd.read_sql_query(_build_metrics_query(include_lookback=False), engine)
                if not frame.empty:
                    frames = [frame]
            except Exception:
                pass

    if not frames:
        return pd.DataFrame(), pd.DataFrame(), "empty"

    df = pd.concat(frames, ignore_index=True, sort=False)

    optional = {
        "source_system": "",
        "table_name": "unknown_table",
        "created_at": pd.NaT,
        "engine_type": "pandas",
        "source_type": "db",
        "run_start": pd.NaT,
        "run_end": pd.NaT,
        "run_duration_secs": None,
        "error_msg": "",
        "source_loaded_to_max_target": None,
        "target_max_incremental": None,
        "perc_loaded": None,
        "rows_loaded_now": 0,
        "target_today_records": 0,
        "target_total_records": None,
        "source_total_records": None,
        "target_max_upload_date": pd.NaT,
        "run_status": "unknown",
    }
    for col, default in optional.items():
        if col not in df.columns:
            df[col] = default

    if "dag_id" in df.columns:
        df["source_system"] = df["source_system"].where(
            df["source_system"].notna() & (df["source_system"] != ""),
            df["dag_id"],
        )

    df["source_system"] = df["source_system"].fillna("unknown")
    df["table_name"] = df["table_name"].fillna("unknown_table").replace("", "unknown_table")
    df["engine_type"] = df["engine_type"].fillna("pandas").replace("", "pandas")
    df["source_type"] = df["source_type"].fillna("db").replace("", "db")

    # Drop test/demo/sandbox sources everywhere — see EXCLUDE_NAME_PATTERN —
    # and anything a job config opted out of with "monitor": false. Both
    # mechanisms mean "don't show this job", so they're applied at the same
    # point: a job hidden from the business Overview but still charted on the
    # Pipeline page would be a confusing half-exclusion.
    opted_out = _excluded_dag_ids()
    test_mask = df.apply(
        lambda r: _is_excluded_name(r.get("dag_id"), r.get("table_name"), r.get("source_system"))
        or r.get("dag_id") in opted_out,
        axis=1,
    )
    df = df[~test_mask].copy()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), "empty"

    df["created_at"] = _to_naive_utc(df["created_at"])
    df["run_start"] = _to_naive_utc(df["run_start"])
    df["run_end"] = _to_naive_utc(df["run_end"])
    df["target_max_upload_date"] = _to_naive_utc(df["target_max_upload_date"])
    df["target_max_upload_date"] = df["target_max_upload_date"].where(
        df["target_max_upload_date"].isna(),
        df["target_max_upload_date"] + timedelta(hours=2),
    )

    df["perc_loaded"] = pd.to_numeric(df["perc_loaded"], errors="coerce")
    df["perc_left"] = (100 - df["perc_loaded"]).clip(lower=0)
    df["table_label"] = df["source_system"] + "." + df["table_name"]
    df["duration_str"] = df["run_duration_secs"].apply(_format_duration)

    df_sorted = df.sort_values(["source_system", "table_name", "created_at"])
    df_sorted["load_diff"] = df_sorted.groupby(["source_system", "table_name"])["created_at"].diff()
    df_sorted["load_diff_hrs"] = df_sorted["load_diff"].dt.total_seconds() / 3600

    df_latest = (
        df_sorted.sort_values("created_at")
        .groupby(["source_system", "table_name"], as_index=False)
        .last()
    )
    avg_intervals = (
        df_sorted.groupby(["source_system", "table_name"])["load_diff_hrs"]
        .mean()
        .reset_index(name="avg_load_interval_hrs")
    )
    avg_duration = (
        df_sorted.groupby(["source_system", "table_name"])["run_duration_secs"]
        .mean()
        .reset_index(name="avg_duration_secs")
    )
    df_latest = df_latest.merge(avg_intervals, on=["source_system", "table_name"], how="left")
    df_latest = df_latest.merge(avg_duration, on=["source_system", "table_name"], how="left")
    df_latest["avg_load_interval_str"] = df_latest["avg_load_interval_hrs"].apply(_format_timedelta)
    df_latest["avg_duration_str"] = df_latest["avg_duration_secs"].apply(_format_duration)
    df_latest = df_latest.sort_values("created_at", ascending=False)

    return df, df_latest, "ok"


def _last_loaded_str(row, col="created_at"):
    ts = row[col]
    if pd.isna(ts):
        return "Never"
    return f'{ts.strftime("%Y-%m-%d %H:%M")} ({_format_timedelta(row["hours_since_last_load"])} ago)'


HOST_METRICS_PATH = Path(os.getenv("HOST_METRICS_PATH", "/app/data/host_metrics.json"))
HOST_METRICS_HISTORY_PATH = Path(
    os.getenv("HOST_METRICS_HISTORY_PATH", "/app/data/host_metrics_history.jsonl")
)
_STATUS_BADGE = {"green": "🟢", "yellow": "🟡", "red": "🔴", "unknown": "⚪"}
_STATUS_LABEL = {"green": "All well", "yellow": "Needs attention", "red": "Critical", "unknown": "No data"}


def _load_host_metrics():
    """Read the JSON snapshot scripts/collect_host_metrics.py writes on the
    HOST via cron. Deliberately not collected live from inside this
    container — see that script's docstring for why (pipeline-monitor is
    the one unauthenticated, internet-facing container in this stack)."""
    if not HOST_METRICS_PATH.is_file():
        return None
    try:
        return json.loads(HOST_METRICS_PATH.read_text())
    except Exception:
        return None


@st.cache_data(ttl=1800)
def _load_host_metrics_history():
    """One row per collector run (every 2 min via cron, see
    collect_host_metrics.py) — same file, kept as history rather than
    overwritten. Bandwidth is computed here as a rate (bytes/sec) between
    consecutive snapshots; the file only stores cumulative-since-boot
    counters, which aren't directly chartable as a trend."""
    if not HOST_METRICS_HISTORY_PATH.is_file():
        return pd.DataFrame()

    rows = []
    with HOST_METRICS_HISTORY_PATH.open() as f:
        for line in f:
            try:
                entry = json.loads(line)
            except Exception:
                continue  # one corrupt line (e.g. truncated by a crash mid-write) shouldn't drop the rest
            rows.append({
                "generated_at": entry.get("generated_at"),
                "disk_pct": (entry.get("disk") or {}).get("percent_used"),
                "ram_pct": (entry.get("ram") or {}).get("percent_used"),
                "load_1m": ((entry.get("server") or {}).get("load_avg") or [None])[0],
                "rx_bytes": (entry.get("network") or {}).get("rx_bytes"),
                "tx_bytes": (entry.get("network") or {}).get("tx_bytes"),
            })
    if not rows:
        return pd.DataFrame()

    history = pd.DataFrame(rows)
    history["generated_at"] = _to_naive_utc(history["generated_at"])
    history = history.sort_values("generated_at").reset_index(drop=True)

    dt_seconds = history["generated_at"].diff().dt.total_seconds()
    # clip(lower=0): a counter reset (interface reset, host reboot) would
    # otherwise show as a nonsensical negative rate for one data point.
    history["rx_bps"] = (history["rx_bytes"].diff() / dt_seconds).clip(lower=0)
    history["tx_bps"] = (history["tx_bytes"].diff() / dt_seconds).clip(lower=0)
    return history


def _server_monitoring_section():
    st.subheader("Current Status")
    data = _load_host_metrics()
    if data is None:
        st.info(
            "No server metrics yet — the host-side collector "
            "(scripts/collect_host_metrics.py, cron every 2 min) hasn't "
            "written data_pipeline_monitor/data/host_metrics.json yet."
        )
        return

    generated_at = pd.to_datetime(data.get("generated_at"), errors="coerce")
    if pd.notna(generated_at):
        age_minutes = (pd.Timestamp.now(tz="UTC") - generated_at).total_seconds() / 60
        if age_minutes > 10:
            st.warning(
                f"Server metrics are {age_minutes:.0f} minutes old — the host "
                "collector may have stopped running (check its cron job on the server)."
            )

    def badge(section):
        s = data.get(section, {}) or {}
        status = s.get("status", "unknown")
        return _STATUS_BADGE.get(status, "⚪"), s

    c1, c2, c3, c4, c5 = st.columns(5)

    icon, s = badge("server")
    with c1:
        st.markdown(f"**{icon} Server**")
        if s.get("uptime_seconds") is not None:
            st.caption(f"Up {_format_timedelta(s['uptime_seconds'] / 3600)}")
        if s.get("load_avg"):
            la = s["load_avg"]
            st.caption(f"Load: {la[0]:.2f}, {la[1]:.2f}, {la[2]:.2f}")

    icon, s = badge("disk")
    with c2:
        st.markdown(f"**{icon} Disk**")
        if s.get("percent_used") is not None:
            st.caption(f"{s['percent_used']:.0f}% used ({s['used_gb']:.0f}/{s['total_gb']:.0f} GB)")
        else:
            st.caption(s.get("error", "No data"))

    icon, s = badge("ram")
    with c3:
        st.markdown(f"**{icon} RAM**")
        if s.get("percent_used") is not None:
            st.caption(f"{s['percent_used']:.0f}% used ({s['used_gb']:.0f}/{s['total_gb']:.0f} GB)")
        else:
            st.caption(s.get("error", "No data"))

    icon, s = badge("network")
    with c4:
        st.markdown(f"**{icon} Network**")
        if s.get("interface"):
            st.caption(f'{s["interface"]}: {s.get("link_state", "?")}')
            issues = (s.get("rx_dropped", 0) + s.get("tx_dropped", 0)
                      + s.get("rx_errors", 0) + s.get("tx_errors", 0))
            st.caption("No errors/drops" if issues == 0 else f"{issues:,} errors/drops (since boot)")
        else:
            st.caption(s.get("error", "No data"))

    icon, s = badge("security")
    with c5:
        st.markdown(f"**{icon} Security**")
        st.caption(s.get("detail", _STATUS_LABEL.get(s.get("status"), "No data")))


def _failed_today_section(df):
    """Failed jobs today, job name + error, for the production allowlist —
    surfaced separately from the freshness/rows numbers above since a table
    can look "up to date" from an earlier successful run while today's run
    of the same job failed (e.g. hybrid_wasac_dw_payment: today's data still
    hasn't loaded, but the table's last-loaded stamp is from a prior day's
    success, not from today's failure)."""
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    today_start = now.normalize()
    # df is already scoped by load_and_process_data (name pattern + the
    # "monitor": false opt-out), so no extra filtering is needed here.
    failed_today = df[
        (df["run_status"] == "failed")
        & (df["created_at"] >= today_start)
    ].copy()

    st.subheader(f"Failed Today ({len(failed_today)})")
    if failed_today.empty:
        st.success("No failed jobs today.")
        return

    # A failure that's already been resolved (e.g. someone reran the job
    # and it succeeded) shouldn't read as an open incident — confirmed
    # concretely against exactly this scenario: hybrid_wasac_dw_payment
    # failed today, was rerun, and succeeded, but a bare failure list
    # would still show it exactly the same as one nobody's touched yet.
    success_by_dag = df[df["run_status"] == "success"][["dag_id", "created_at"]]

    def _fixed_status(row):
        later_success = success_by_dag[
            (success_by_dag["dag_id"] == row["dag_id"])
            & (success_by_dag["created_at"] > row["created_at"])
        ]
        if later_success.empty:
            return "🔴 Still failing"
        fixed_at = later_success["created_at"].max()
        return f'✅ Fixed at {fixed_at.strftime("%Y-%m-%d %H:%M")}'

    failed_today["Fixed"] = failed_today.apply(_fixed_status, axis=1)
    failed_today = failed_today.sort_values("created_at", ascending=False)
    failed_today["error_msg"] = failed_today["error_msg"].fillna("(no error message recorded)")
    display = failed_today[["dag_id", "table_name", "created_at", "Fixed", "error_msg"]].rename(columns={
        "dag_id": "Job",
        "table_name": "Table",
        "created_at": "Failed At",
        "error_msg": "Error",
    })
    st.dataframe(display, use_container_width=True, hide_index=True)


def _page_header(title):
    """Shared top-of-page bar — Refresh Now + lookback caption — repeated
    on every page rather than shown once, since st.navigation only runs
    the currently-selected page's function on each render; each page is
    its own self-contained script section now, not a tab sharing one
    header."""
    st.title(title)
    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Refresh Now"):
            st.cache_data.clear()
            st.rerun()
    with c2:
        st.caption(
            f"Lookback days: {_get_lookback_days()} (set METRICS_LOOKBACK_DAYS=0 for all history) "
            "· auto-refreshes every 30 min, or click Refresh Now"
        )


def _load_dashboard_data():
    """Cached data any page might need. Loaded fresh (i.e. from cache,
    ttl=1800) on whichever page is actually rendered — st.navigation only
    calls the selected page's function, so a page that doesn't need a
    particular dataset never pays for it, unlike the old single-page-with-
    tabs design that computed everything for every tab on every render
    regardless of which was actually open."""
    errors = []
    try:
        table_summary, table_daily, table_status, table_diag = load_table_metrics()
    except Exception as exc:
        errors.append(("Overview", exc))
        table_summary, table_daily, table_status = pd.DataFrame(), pd.DataFrame(), "error"
        table_diag = {"tables_considered": 0, "skipped": []}
    try:
        df, df_latest, status = load_and_process_data()
    except Exception as exc:
        errors.append(("Dashboard", exc))
        df, df_latest, status = pd.DataFrame(), pd.DataFrame(), "error"
    return {
        "table_summary": table_summary, "table_daily": table_daily, "table_status": table_status,
        "table_diag": table_diag,
        "df": df, "df_latest": df_latest, "status": status, "errors": errors,
    }


def _delta_vs(current, previous, period_label):
    """(delta_string_for_st_metric, plain_english_caption) comparing two
    periods. st.metric colours a delta red when it starts with "-", which
    is the behaviour we want: a sharp drop in rows loaded is the signal
    that something upstream has gone quiet.

    Returns delta=None where a percentage would be meaningless or
    misleading — no history to compare against, or a jump from zero, which
    is not "+infinity%" but simply "it started loading".
    """
    current = int(current or 0)
    previous = int(previous or 0)
    if previous == 0 and current == 0:
        return None, f"none {period_label} either"
    if previous == 0:
        return None, f"nothing {period_label} — this is new activity"
    pct = (current - previous) / previous * 100
    # Don't let rounding change the meaning. 5 rows against 1,683 is -99.7%,
    # and printing that as "-100%" reads as "nothing loaded at all" — a
    # different and more alarming claim than the truth. The mirror case is a
    # small change rounding to "-0%", which reads as broken. In both cases
    # fall back to a decimal place rather than a qualitatively wrong figure.
    rounded = round(pct)
    misleading = (
        (rounded == 0 and pct != 0)
        or (abs(rounded) == 100 and abs(abs(pct) - 100) > 1e-9)
    )
    pct_txt = f"{abs(pct):,.1f}" if misleading else f"{abs(pct):,.0f}"
    sign = "+" if pct >= 0 else "-"
    return f"{sign}{pct_txt}% vs {period_label}", f"{previous:,} {period_label}"


def _table_signals(table_summary, table_daily):
    """Plain-language "this looks off" findings, one row per affected table.

    Judged against each table's OWN recent rhythm rather than a fixed rule,
    because the tables here don't share one: flagging "nothing loaded today"
    globally would cry wolf on every weekly or monthly job. A table is only
    expected to load today if it has actually been loading most days.
    """
    findings = []
    if table_summary.empty:
        return pd.DataFrame(columns=["Table", "What's off", "Detail"])

    by_table = {}
    if not table_daily.empty:
        for label, grp in table_daily.groupby("table_label"):
            by_table[label] = grp

    for _, row in table_summary.iterrows():
        label = row["table_label"]
        hrs = row.get("hours_since_last_load")
        today = int(row.get("loaded_today") or 0)
        grp = by_table.get(label)
        active_days = int(grp["load_date"].nunique()) if grp is not None and not grp.empty else 0
        typical = float(grp["rows_loaded"].median()) if grp is not None and not grp.empty else 0.0
        loads_most_days = active_days >= 7          # 7+ of the last 14

        if pd.notna(hrs) and hrs > 72:
            findings.append((label, "Not loading",
                             f"Last load was {_humanize_hours(hrs)} ago."))
        elif pd.notna(hrs) and hrs > 24:
            findings.append((label, "Falling behind",
                             f"Last load was {_humanize_hours(hrs)} ago."))
        elif loads_most_days and today == 0:
            findings.append((label, "Nothing today",
                             f"Loaded on {active_days} of the last 14 days, "
                             f"but nothing has arrived today."))
        elif loads_most_days and typical > 0 and today < typical * 0.4:
            findings.append((label, "Volume down",
                             f"{today:,} rows today against a typical "
                             f"{int(typical):,} a day."))

    return pd.DataFrame(findings, columns=["Table", "What's off", "Detail"])


def _humanize_hours(hrs):
    """'3 days' / '5 hours' — the table is read by people who do not want
    to convert 78.4 hours in their head."""
    try:
        hrs = float(hrs)
    except (TypeError, ValueError):
        return "an unknown time"
    if hrs < 1:
        return "less than an hour"
    if hrs < 48:
        return f"{int(round(hrs))} hour{'s' if int(round(hrs)) != 1 else ''}"
    days = int(round(hrs / 24))
    return f"{days} day{'s' if days != 1 else ''}"


def render_main():
    """First page anyone in the business opens: which real production
    tables are we loading, is each one up to date, and how much came in
    today/this week — read straight from each table's own pipeline meta
    columns (see load_table_metrics) rather than the run-log table, which
    has real per-engine gaps. No engine internals, no raw percentages —
    those live on the Pipeline page for whoever needs them."""
    _page_header("ETL Pipeline Monitoring Dashboard")
    data = _load_dashboard_data()
    for title, exc in data["errors"]:
        _render_load_error(title, exc)

    table_summary, table_status, df = data["table_summary"], data["table_status"], data["df"]
    table_diag = data.get("table_diag") or {}

    if table_status != "ok" or table_summary.empty:
        # Say which of the two very different causes this actually is —
        # "no jobs are configured here" and "every job's database is
        # unreachable" used to render as the same sentence, which made the
        # page useless for diagnosing either.
        considered = table_diag.get("tables_considered", 0)
        skipped = table_diag.get("skipped") or []
        if not considered:
            st.warning(
                "No job configs with a resolvable target were found, so there "
                "is nothing to report on. Check that this container can see "
                f"the job configs (CONFIG_DIR={CONFIG_DIR})."
            )
        else:
            st.warning(
                f"Found {considered} configured table(s) but couldn't read any "
                "of them. This is a connection or schema problem, not a "
                "configuration one — the per-table reasons are below."
            )
        if skipped:
            with st.expander(f"Why each table was skipped ({len(skipped)})"):
                st.dataframe(
                    pd.DataFrame(skipped, columns=["table", "conn_id", "error"]),
                    use_container_width=True, hide_index=True,
                )
        st.caption("The Pipeline page shows the run-log view, which may still work.")
    else:
        needs_attention = int((table_summary["freshness"] == "🔴 Needs attention").sum())
        table_daily = data.get("table_daily")
        if table_daily is None:
            table_daily = pd.DataFrame()
        signals = _table_signals(table_summary, table_daily)

        # Lead with the answer to the only question most readers have, in a
        # sentence, before any number: is anything wrong right now?
        if signals.empty:
            st.success(
                f"Everything looks normal — all {len(table_summary)} tables are "
                "loading on their usual rhythm."
            )
        else:
            st.warning(
                f"{len(signals)} of {len(table_summary)} tables need a look — "
                "see “What needs attention” below."
            )
        st.caption(f"{len(table_summary)} production tables tracked")

        today_n = int(table_summary["loaded_today"].sum())
        yday_n = int(table_summary.get("loaded_yesterday", pd.Series(dtype=int)).sum() or 0)
        week_n = int(table_summary["loaded_week"].sum())
        prev_week_n = int(table_summary.get("loaded_prev_week", pd.Series(dtype=int)).sum() or 0)

        today_delta, today_cap = _delta_vs(today_n, yday_n, "yesterday")
        week_delta, week_cap = _delta_vs(week_n, prev_week_n, "last week")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Loaded Today", f"{today_n:,} rows", delta=today_delta)
        c1.caption(today_cap)
        c2.metric("Loaded This Week", f"{week_n:,} rows", delta=week_delta)
        c2.caption(week_cap)
        c3.metric("Loaded Overall", f"{int(table_summary['total_rows'].sum()):,} rows")
        c3.caption("every row ever loaded")
        c4.metric(
            "Needs Attention",
            f"{needs_attention} of {len(table_summary)}",
            delta=None if needs_attention == 0 else "check below",
            delta_color="inverse",
        )
        c4.caption("tables not loaded in over 3 days")

        if not signals.empty:
            st.subheader("What needs attention")
            st.caption(
                "Each table is judged against its own recent rhythm, not a fixed "
                "rule — a table that normally loads weekly isn't flagged for "
                "having nothing today."
            )
            st.dataframe(signals, use_container_width=True, hide_index=True)

        if not table_daily.empty:
            st.subheader("Rows Loaded Per Day — Last 14 Days")
            st.caption(
                "All tables combined. The dashed line is the daily average for "
                "the period, so a bar well below it is a day that under-delivered."
            )
            # Fill the whole window, including days nothing loaded. The query
            # only returns days that HAVE rows, so a day with no loads was
            # simply missing from the chart — silently hiding the exact event
            # this chart exists to show. A zero-height bar on a dense axis
            # says "nothing came in that day"; an absent bar says nothing.
            per_day = (
                table_daily.groupby("load_date", as_index=False)["rows_loaded"].sum()
            )
            per_day["load_date"] = pd.to_datetime(per_day["load_date"])
            full_range = pd.date_range(
                end=pd.Timestamp.now().normalize(), periods=14, freq="D"
            )
            per_day = (
                per_day.set_index("load_date")
                .reindex(full_range, fill_value=0)
                .rename_axis("load_date")
                .reset_index()
            )
            per_day["label"] = per_day["load_date"].dt.strftime("%d %b")
            fig = px.bar(per_day, x="label", y="rows_loaded")
            avg = float(per_day["rows_loaded"].mean())
            fig.add_hline(
                y=avg, line_dash="dash", line_color="#6b7a90",
                annotation_text=f"daily average {avg:,.0f}",
                annotation_position="top left",
            )
            fig.update_layout(
                xaxis_title="", yaxis_title="Rows Loaded",
                xaxis_type="category", bargap=0.25,
                margin=dict(t=30, b=0, l=0, r=0), height=280,
            )
            fig.update_traces(marker_color="#0ea5e9",
                              hovertemplate="%{x}<br>%{y:,} rows<extra></extra>")
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Sources We're Loading")
        st.caption(
            "Most recently loaded first. \"Business Date\" is the latest value of "
            "the business date column named in the job config — not when the "
            "pipeline ran, but how current the data itself is. Blank means the "
            "job doesn't name one."
        )

        display = table_summary.copy()
        display["Last Loaded"] = display.apply(lambda r: _last_loaded_str(r, "last_loaded"), axis=1)
        display["Business Date"] = display["business_date"].apply(_format_business_date)
        display = display.rename(columns={
            "table_label": "Table",
            "dag_ids": "Fed By",
            "freshness": "Status",
            "loaded_today": "Rows Today",
            "loaded_yesterday": "Rows Yesterday",
            "loaded_week": "Rows This Week",
            "total_rows": "Rows Overall",
        })
        st.dataframe(
            display[[
                "Table", "Status", "Last Loaded", "Business Date",
                "Rows Today", "Rows Yesterday", "Rows This Week", "Rows Overall", "Fed By",
            ]],
            use_container_width=True,
            hide_index=True,
        )

    if not df.empty:
        _failed_today_section(df)


def _run_reliability(df):
    """Per-table run record over the window: how often it ran, how often it
    failed, and how long it takes. This is the "why" behind a table that
    looks stale on the Overview — a job can be failing every run, or simply
    not running at all, and those need different responses."""
    if df.empty:
        return pd.DataFrame()
    grp = df.groupby("table_label")
    out = pd.DataFrame({
        "Runs": grp.size(),
        "Failed": grp["run_status"].apply(lambda s: int((s == "failed").sum())),
        "Rows Loaded": grp["rows_loaded_now"].sum(),
        "Last Run": grp["created_at"].max(),
        "Typical Duration": grp["run_duration_secs"].median().apply(_format_duration),
    }).reset_index().rename(columns={"table_label": "Table"})
    out["Failure Rate"] = (out["Failed"] / out["Runs"] * 100).round(0)
    out["Failure Rate"] = out["Failure Rate"].apply(lambda p: f"{p:,.0f}%")
    out["Last Run"] = out["Last Run"].apply(
        lambda t: "Never" if pd.isna(t) else t.strftime("%Y-%m-%d %H:%M")
    )
    return out.sort_values("Failed", ascending=False)


def render_pipeline():
    """The detail behind the Overview, in the same order someone reads it.

    This page used to be a wall of charts keyed on engine_type — a pie of
    pandas-vs-spark run counts, rows-loaded coloured by engine, a snapshot
    count plotted against run timestamps. Which engine ran a job is an
    implementation detail nobody acts on, and none of it connected back to
    the numbers on the Overview, so the page answered no question anyone
    actually had. It now follows the questions the Overview provokes:
    which table made up that total, how has each been running, and what
    broke. The engine internals are kept, but at the bottom and labelled
    as such.
    """
    _page_header("Pipeline Details")
    st.caption(
        "The detail behind the Overview — which tables make up those totals, "
        "how reliably each job runs, and what failed."
    )
    data = _load_dashboard_data()
    for title, exc in data["errors"]:
        _render_load_error(title, exc)

    if data["status"] == "empty":
        conn_ids = sorted(_distinct_target_conn_ids())
        if conn_ids:
            st.info(
                "No pipeline runs found across any job's target connection "
                f"({', '.join(conn_ids)}) in the current lookback window."
            )
        else:
            st.warning(
                "No job configs found under CONFIG_DIR and the legacy METRICS_DB_* "
                "connection returned nothing — check both are set correctly."
            )
        return

    table_daily, df, df_latest = data["table_daily"], data["df"], data["df_latest"]
    table_summary = data.get("table_summary")
    if table_summary is None:
        table_summary = pd.DataFrame()

    # ── 1. Who made up the Overview's totals ────────────────────────────
    # Read from table_summary, the same source the Overview uses, so the
    # per-table figures here always add up to the headline there.
    if not table_summary.empty:
        st.subheader("Which tables made up this week's rows")
        st.caption(
            "The Overview's “Loaded This Week” total, split by table. Each "
            "bar is the table's week, with today's share picked out in blue "
            "— a long bar with no blue is a table that has gone quiet today."
        )
        contrib = table_summary[["table_label", "loaded_today", "loaded_week"]].copy()
        contrib = contrib[contrib["loaded_week"] > 0].sort_values("loaded_week")
        if contrib.empty:
            st.info("No rows loaded by any table in the last 7 days.")
        else:
            # Stacked, not grouped: today is PART of the week, so drawing the
            # two side by side double-counts today and invites the reader to
            # add them together. Split the week into today + the rest so the
            # segments sum to the bar.
            contrib["Earlier this week"] = (
                contrib["loaded_week"] - contrib["loaded_today"]
            ).clip(lower=0)
            melted = contrib.melt(
                id_vars="table_label",
                value_vars=["loaded_today", "Earlier this week"],
                var_name="period", value_name="rows",
            ).replace({"loaded_today": "Today"})
            fig = px.bar(
                melted, x="rows", y="table_label", color="period",
                orientation="h", barmode="stack",
                color_discrete_map={"Today": "#0ea5e9", "Earlier this week": "#c7d2da"},
            )
            fig.update_layout(
                xaxis_title="Rows Loaded", yaxis_title="", legend_title="",
                height=max(240, 42 * len(contrib)),
                margin=dict(t=10, b=0, l=0, r=0),
            )
            st.plotly_chart(fig, use_container_width=True)

    # ── 2. The Overview's trend line, split per table ───────────────────
    st.subheader("Rows Loaded Per Day, By Table — Last 14 Days")
    st.caption(
        "The Overview's daily chart broken out per table. A line that drops "
        "to the floor and stays there is the job to look at first."
    )
    if table_daily.empty:
        st.info("No rows loaded (by _loaded_at) in this window for any tracked table.")
    else:
        # Same dense-axis treatment as the Overview chart: the query only
        # returns days a table actually loaded on, so without filling the
        # gaps a table with two active days renders as two points on an axis
        # labelled in hours, and "dropped to the floor" is invisible because
        # the floor isn't drawn.
        per_table = table_daily.copy()
        per_table["load_date"] = pd.to_datetime(per_table["load_date"])
        full_range = pd.date_range(end=pd.Timestamp.now().normalize(), periods=14, freq="D")
        per_table = (
            per_table.pivot_table(index="load_date", columns="table_label",
                                  values="rows_loaded", aggfunc="sum")
            .reindex(full_range, fill_value=0)
            .fillna(0)
            .rename_axis("load_date")
            .reset_index()
            .melt(id_vars="load_date", var_name="table_label", value_name="rows_loaded")
        )
        per_table["label"] = per_table["load_date"].dt.strftime("%d %b")
        fig = px.line(
            per_table.sort_values("load_date"),
            x="label", y="rows_loaded", color="table_label", markers=True,
        )
        fig.update_layout(
            xaxis_title="", yaxis_title="Rows Loaded", legend_title="Table",
            xaxis_type="category", margin=dict(t=10, b=0, l=0, r=0),
        )
        fig.update_traces(hovertemplate="%{x}<br>%{y:,} rows<extra></extra>")
        st.plotly_chart(fig, use_container_width=True)

    # ── 3. Is each job actually running, and succeeding? ────────────────
    st.subheader("How each job has been running")
    st.caption(
        "A table can look stale on the Overview for two very different "
        "reasons: the job is failing, or it isn't running at all. "
        "Failure rate and last run separate the two."
    )
    reliability = _run_reliability(df)
    if reliability.empty:
        st.info("No runs recorded in this window.")
    else:
        st.dataframe(
            reliability[["Table", "Runs", "Failed", "Failure Rate",
                         "Rows Loaded", "Typical Duration", "Last Run"]],
            use_container_width=True, hide_index=True,
        )

    # ── 4. Is anything getting slower? ──────────────────────────────────
    runs_timed = df[df["run_duration_secs"].notna() & df["created_at"].notna()]
    if not runs_timed.empty and runs_timed["table_label"].nunique() <= 25:
        st.subheader("Run duration over time")
        st.caption(
            "A job taking steadily longer each run is usually on its way to "
            "a timeout — worth catching before it starts failing."
        )
        fig = px.line(
            runs_timed.sort_values("created_at"),
            x="created_at", y="run_duration_secs", color="table_label", markers=True,
        )
        fig.update_layout(
            xaxis_title="", yaxis_title="Seconds", legend_title="Table",
            margin=dict(t=10, b=0, l=0, r=0),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── 5. What actually broke ──────────────────────────────────────────
    st.subheader("Failures")
    _failures_tab(df)

    # ── 6. Engine internals, for whoever needs them ─────────────────────
    with st.expander("Engine internals and coverage (technical)"):
        st.caption(
            "Which engine ran what, and high-water-mark coverage for the "
            "DB-source jobs that track one. Kept for debugging; nothing here "
            "is needed to answer “is the data OK”."
        )
        try:
            _quality_tab(df)
        except Exception as exc:
            _render_load_error("Data Quality", exc)
        _analytics_tab(df, df_latest)


def _analytics_tab(df, df_latest):
    c1, c2, c3, c4 = st.columns(4)
    failed_tables = int(df[df["run_status"] == "failed"]["table_name"].nunique())
    c1.metric("Tables Monitored", f"{len(df_latest)}")
    c2.metric("Source Systems", f"{df_latest['source_system'].nunique()}")
    c3.metric("Tables With Failures", f"{failed_tables}")
    avg_cov = df_latest["perc_loaded"].mean()
    c4.metric("Avg Coverage", "N/A" if pd.isna(avg_cov) else f"{avg_cov:.1f}%")

    left, right = st.columns([1, 2])
    with left:
        engine_counts = df.groupby("engine_type").size().reset_index(name="runs")
        fig = px.pie(engine_counts, names="engine_type", values="runs", title="Run Engine Distribution")
        st.plotly_chart(fig, use_container_width=True)
    with right:
        fig = px.bar(
            df_latest,
            x="table_label",
            y="rows_loaded_now",
            color="engine_type",
            title="Rows Loaded Last Run - by Engine",
        )
        fig.update_layout(xaxis_title="", yaxis_title="Rows Loaded")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("ETL Status Summary")
    st.dataframe(
        df_latest[
            [
                "source_system",
                "table_name",
                "source_type",
                "run_status",
                "duration_str",
                "avg_duration_str",
                "target_total_records",
                "source_total_records",
                "perc_loaded",
                "perc_left",
                "target_max_upload_date",
                "rows_loaded_now",
                "avg_load_interval_str",
                "engine_type",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Records Loaded Today")
    fig1 = px.bar(df_latest, x="table_label", y="target_today_records", color="engine_type")
    fig1.update_layout(xaxis_title="", yaxis_title="Records Loaded")
    st.plotly_chart(fig1, use_container_width=True)

    st.subheader("Loading Progress Over Time")
    fig2 = px.line(df.sort_values("created_at"), x="created_at", y="target_today_records", color="table_label")
    fig2.update_layout(xaxis_title="Time", yaxis_title="Records Loaded")
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Completion Status")
    fig3 = px.bar(df_latest, x="table_label", y="perc_left", color="engine_type")
    fig3.update_layout(xaxis_title="", yaxis_title="% Remaining")
    st.plotly_chart(fig3, use_container_width=True)


def _quality_tab(df):
    """HWM coverage, filtered from the already-loaded, already multi-DB
    combined `df` — not a fresh query against one hardcoded engine. Every
    column this used to SELECT is already present in BASE_QUERY, so this
    tab gets the same every-connection aggregation as the other tabs for
    free instead of falling back to a single database on its own."""
    st.subheader("HWM Coverage Check")
    df_hwm = df[df["target_max_incremental"].notna() & df["source_loaded_to_max_target"].notna()].copy()
    if df_hwm.empty:
        st.info("No HWM data available.")
    else:
        df_hwm = df_hwm.sort_values("created_at", ascending=False).groupby(["source_system", "table_name"], as_index=False).first()
        df_hwm["table_label"] = df_hwm["source_system"] + "." + df_hwm["table_name"]
        melted = df_hwm.melt(
            id_vars=["table_label"],
            value_vars=["source_loaded_to_max_target", "target_total_records"],
            var_name="metric",
            value_name="count",
        )
        fig = px.bar(melted, x="table_label", y="count", color="metric", barmode="group")
        fig.update_layout(xaxis_title="", yaxis_title="Rows")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(
            df_hwm[
                [
                    "source_system",
                    "table_name",
                    "engine_type",
                    "target_max_incremental",
                    "source_loaded_to_max_target",
                    "target_total_records",
                    "source_total_records",
                    "perc_loaded",
                    "run_status",
                    "created_at",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )


def _failures_tab(df):
    # Heading is supplied by the caller — this used to be a tab with its own
    # title and is now a section on the drill-down page.
    failed = df[df["run_status"] == "failed"].copy()
    if failed.empty:
        st.success("No failures in the current window.")
        return
    st.caption(
        "Every failed run in the window, newest first, with the error the "
        "task reported."
    )
    failed = failed.sort_values("created_at", ascending=False)

    st.dataframe(
        failed[
            [
                "run_id",
                "source_system",
                "table_name",
                "engine_type",
                "run_status",
                "error_msg",
                "run_start",
                "duration_str",
                "rows_loaded_now",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )
    # No frequency histogram here. Failures are few and bursty, so binning a
    # handful of events over a window they all fall inside renders an axis in
    # minutes and communicates nothing the table above doesn't already say
    # row by row — and per-table failure counts and rates are in "How each
    # job has been running".


def render_server():
    _page_header("Server Monitoring")
    _server_monitoring_section()

    st.subheader("Trends — Last 30 Days")
    history = _load_host_metrics_history()
    if len(history) < 2:
        st.info(
            "Not enough history yet to chart trends — the host-side "
            "collector runs every 2 minutes via cron, so this fills in "
            "over the next several minutes."
        )
        return

    fig_disk_ram = px.line(
        history, x="generated_at", y=["disk_pct", "ram_pct"],
        labels={"value": "% Used", "generated_at": "", "variable": "Metric"},
        title="Disk & RAM Usage",
    )
    fig_disk_ram.update_layout(yaxis_range=[0, 100])
    st.plotly_chart(fig_disk_ram, use_container_width=True)

    net = history.dropna(subset=["rx_bps", "tx_bps"])
    if not net.empty:
        fig_net = px.line(
            net, x="generated_at", y=["rx_bps", "tx_bps"],
            labels={"value": "Bytes/sec", "generated_at": "", "variable": "Direction"},
            title="Network Throughput",
        )
        st.plotly_chart(fig_net, use_container_width=True)

    if history["load_1m"].notna().any():
        fig_load = px.line(
            history, x="generated_at", y="load_1m",
            labels={"load_1m": "Load Average (1 min)", "generated_at": ""},
            title="Load Average",
        )
        st.plotly_chart(fig_load, use_container_width=True)


FALCO_SUMMARY_PATH = Path(os.getenv("FALCO_SUMMARY_PATH", "/app/data/falco_daily_summary.json"))
FALCO_RECENT_PATH = Path(os.getenv("FALCO_RECENT_PATH", "/app/data/falco_recent_alerts.jsonl"))


def _load_falco_summary():
    """Read the daily rollup scripts/summarize_falco_alerts.py maintains on
    the HOST via cron. Not parsed from raw alerts.log here — that file
    rotates at 50MB and can turn over within an hour under noisy
    conditions, so the kept rotated copies alone don't reliably cover a
    full week; the summary is a small running total that does."""
    if not FALCO_SUMMARY_PATH.is_file():
        return None
    try:
        return json.loads(FALCO_SUMMARY_PATH.read_text())
    except Exception:
        return None


def _load_falco_recent():
    """The "now" view — a small ring buffer of the most recent alerts the
    same summarizer script maintains, not a live read of logs/falco/
    itself. pipeline-monitor is the one unauthenticated, internet-facing
    container in this stack (see collect_host_metrics.py's docstring for
    the same reasoning) — it only ever gets this small controlled file,
    not direct access to the real log directory."""
    if not FALCO_RECENT_PATH.is_file():
        return pd.DataFrame()
    rows = []
    with FALCO_RECENT_PATH.open() as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    if not rows:
        return pd.DataFrame()
    recent = pd.DataFrame(rows)
    if "time" in recent.columns:
        recent["time"] = _to_naive_utc(recent["time"])
        recent = recent.sort_values("time", ascending=False)
    return recent


def render_falco():
    _page_header("Falco Security Reports")
    tab_now, tab_weekly = st.tabs(["Now", "Weekly Report"])

    with tab_now:
        recent = _load_falco_recent()
        if recent.empty:
            st.success("No recent alerts.")
        else:
            st.caption(f"Last {len(recent)} alerts (most recent first)")
            cols = [c for c in ["time", "priority", "rule", "output"] if c in recent.columns]
            st.dataframe(recent[cols], use_container_width=True, hide_index=True)

    with tab_weekly:
        summary = _load_falco_summary()
        days = (summary or {}).get("days") or {}
        if not days:
            st.info(
                "No alert history yet — scripts/summarize_falco_alerts.py "
                "runs every 5 minutes via cron."
            )
        else:
            daily = pd.DataFrame(
                [{"date": day, "total": bucket.get("total", 0)} for day, bucket in days.items()]
            ).sort_values("date")
            last7 = daily.tail(7)

            st.subheader("Alerts Per Day — Last 7 Days")
            fig = px.bar(last7, x="date", y="total")
            fig.update_layout(xaxis_title="", yaxis_title="Alert Count")
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("By Rule — Last 7 Days")
            rule_totals = {}
            for day in last7["date"]:
                for rule, count in days[day].get("by_rule", {}).items():
                    rule_totals[rule] = rule_totals.get(rule, 0) + count
            rule_df = pd.DataFrame(
                sorted(rule_totals.items(), key=lambda x: -x[1]), columns=["Rule", "Count"]
            )
            st.dataframe(rule_df, use_container_width=True, hide_index=True)


def main():
    pages = [
        st.Page(render_main, title="Main", icon="🏠", url_path="main", default=True),
        st.Page(render_pipeline, title="Pipeline", icon="📊", url_path="pipeline"),
        st.Page(render_server, title="Server", icon="🖥️", url_path="server"),
        st.Page(render_falco, title="Falco", icon="🛡️", url_path="falco"),
    ]
    pg = st.navigation(pages)
    pg.run()


main()
# NOTE: st.fragment(run_every=...) was tried here for a non-blanking
# background refresh and pulled instead — confirmed concretely on the
# deployed container: it crash-looped the whole process every ~25-30s
# (RestartCount climbing continuously, no traceback in logs, not OOM) while
# hammering multi-million-row tables (cms.invoice: 17M+ rows) on every
# cycle. Root cause not fully isolated (suspect: fragment auto-rerun scheduling
# with no active browser session attached, since this was only verified via
# curl) — needs a real browser-attached test before retrying. Back to a
# blocking full-page rerun, which is confirmed stable, until that's done.
#
# 30 min, not 60s: this Overview tab runs real COUNT(*)/MAX queries against
# every known production table directly (cms.invoice alone is 17M+ rows) —
# re-scanning those every minute has no benefit business data doesn't
# change that fast, and just adds load to the DW for no reason. Cache TTLs
# above match this interval.
time.sleep(1800)
st.rerun()