import copy
import json
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path

from flask import Flask, redirect, render_template, request, url_for, session
from werkzeug.security import check_password_hash, generate_password_hash
from sqlalchemy import create_engine, text
from dags.modules import naming, secrets_crypto


APP_ROOT = Path(__file__).resolve().parents[1]

# Read from /run/secrets/<name> first (Docker secrets), fall back to env var.
def _secret(secret_name: str, env_var: str = None) -> str:
    path = Path("/run/secrets") / secret_name
    if path.is_file():
        return path.read_text().strip()
    return os.getenv(env_var or secret_name.upper(), "")


CONFIG_DIR = Path(os.getenv("CONFIG_DIR", APP_ROOT / "dags" / "config"))
SQL_DIR = Path(os.getenv("SQL_DIR", APP_ROOT / "dags" / "sql"))
ETL_DIR = Path(os.getenv("ETL_DIR", APP_ROOT / "dags" / "etl"))
TEMPLATE_FILE            = Path(os.getenv("TEMPLATE_FILE",            APP_ROOT / "dags" / "templates" / "data_load.template"))
SPARK_TEMPLATE_FILE      = Path(os.getenv("SPARK_TEMPLATE_FILE",      APP_ROOT / "dags" / "templates" / "spark_load.template"))
FILE_TEMPLATE_FILE       = Path(os.getenv("FILE_TEMPLATE_FILE",       APP_ROOT / "dags" / "templates" / "file_load.template"))
SPARK_FILE_TEMPLATE_FILE = Path(os.getenv("SPARK_FILE_TEMPLATE_FILE", APP_ROOT / "dags" / "templates" / "spark_file_load.template"))
INCOMING_DIR             = Path(os.getenv("INCOMING_DIR",             APP_ROOT / "data_dumps" / "incoming"))
HYBRID_TEMPLATE_FILE     = Path(os.getenv("HYBRID_TEMPLATE_FILE",     APP_ROOT / "dags" / "templates" / "hybrid_load.template"))
# API jobs share HYBRID_TEMPLATE_FILE (pipeline_type "hybrid" + source_type "api").
# Path used to populate watch_path / archive_path in file-based DAG configs.
# On production this must match the Airflow Scheduler's filesystem view.
DATA_DUMP_PATH = os.getenv("AIRFLOW_VAR_DATA_DUMP", str(APP_ROOT / "data_dumps"))
WEBUI_DB = os.getenv("WEBUI_DB", str(APP_ROOT / "web_ui" / "webui.db"))

# Mirror the Airflow Variable so the UI can highlight the active environment
ACTIVE_ENV = os.getenv("AIRFLOW_VAR_ENVIRONMENT", os.getenv("AIRFLOW_VAR_environment", "dev"))

PROJECT_NAME = os.getenv("PROJECT_NAME", "ETL Manager")
AIRFLOW_SERVER_IP = os.getenv("AIRFLOW_SERVER_IP", "localhost")

def _get_app_version() -> str:
    """Return a short git commit SHA for cache-busting static assets.
    Falls back to a Unix timestamp if git is unavailable.
    """
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(APP_ROOT),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return str(int(datetime.utcnow().timestamp()))

APP_VERSION = _get_app_version()

AIRFLOW_DB = os.getenv(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN"),
)

app = Flask(__name__)
app.secret_key = _secret("webui_secret_key", "WEBUI_SECRET_KEY")
# Use a distinct cookie name so the Web UI session never collides with
# Airflow's own 'session' cookie (both run on the same browser/domain).
app.config["SESSION_COOKIE_NAME"] = os.getenv("WEBUI_SESSION_COOKIE_NAME", "etl_manager_session")


@app.context_processor
def inject_globals():
    return {
        "project_name": PROJECT_NAME,
        "airflow_server_ip": AIRFLOW_SERVER_IP,
        "app_version": APP_VERSION,
    }

SCHEDULE_SUGGESTIONS = [
    ("Every minute",        "* * * * *"),
    ("Every 5 minutes",     "*/5 * * * *"),
    ("Every 10 minutes",    "*/10 * * * *"),
    ("Every 30 minutes",    "*/30 * * * *"),
    ("Every hour",          "@hourly"),
    ("Every day at 2am",    "0 2 * * *"),
    ("Every day at midnight","0 0 * * *"),
    ("Every week (Mon 2am)","0 2 * * 1"),
    ("Every month (1st 2am)","0 2 1 * *"),
    ("Every year (Jan 1)",  "0 2 1 1 *"),
]


def _sql_dir(target_conn_id: str) -> Path:
    """Return SQL directory nested under target_conn_id subfolder."""
    return SQL_DIR / naming.clean_id(target_conn_id) if target_conn_id else SQL_DIR


# Columns injected by the ETL pipeline at load time — never user-defined,
# so they are excluded from Data Dictionary entries and dtype_override_formats.
PIPELINE_META_COLS = frozenset([
    "_pipeline_inserted_at", "_pipeline_run_id", "_source_system",
    "_loaded_at", "_row_checksum", "_source_file", "_file_checksum",
])


def load_file_config_files():
    return [cfg for cfg in load_config_files(all_types=True) if cfg.get("pipeline_type") == "file_based"]


def load_file_config(dag_id: str):
    return load_config(dag_id)


def save_file_config(config: dict, existing_config: dict = None):
    save_config(config, existing_config=existing_config)


def delete_file_config(dag_id: str):
    delete_config(dag_id)


def delete_file_dag_files(dag_id: str):
    delete_dag_files(dag_id)


def generate_file_dag_py(config: dict):
    generate_dag_py(config)


def _parse_data_dictionary(form) -> dict:
    """Parse dd_col[] / dd_desc[] / dd_dtype[] / dd_date_fmt[] form arrays into a data_dictionary dict."""
    def _getlist(key):
        """Works with Flask ImmutableMultiDict and plain dicts used in tests."""
        if hasattr(form, 'getlist'):
            return form.getlist(key)
        val = form.get(key)
        if val is None:
            return []
        return val if isinstance(val, list) else [val]

    cols      = _getlist("dd_col[]")
    descs     = _getlist("dd_desc[]")
    dtypes    = _getlist("dd_dtype[]")
    date_fmts = _getlist("dd_date_fmt[]")
    dd = {}
    for i, col in enumerate(cols):
        col = col.strip()
        if not col:
            continue
        entry = {"description": (descs[i].strip() if i < len(descs) else "")}
        dtype = dtypes[i].strip() if i < len(dtypes) else ""
        if dtype:
            entry["dtype"] = dtype
        date_fmt = date_fmts[i].strip() if i < len(date_fmts) else ""
        if date_fmt and dtype in ("date", "timestamp"):
            entry["date_format"] = date_fmt
        dd[col] = entry
    return dd


def build_file_config_from_form(form, apply_prod_same_as_dev: bool):
    """Build a file-based ETL config dict from a file_form.html POST."""

    def _parse_list(raw: str) -> list:
        return [v.strip() for v in (raw or "").split(",") if v.strip()]

    def _parse_hwm_columns(raw: str):
        """Parse the HWM column JSON array from the form.
        Returns a list (possibly empty), a single string for backward compat
        with legacy plain-string values, or None if nothing was selected."""
        if not raw:
            return None
        raw = raw.strip()
        try:
            v = json.loads(raw)
            if isinstance(v, list):
                cols = [str(c).strip() for c in v if str(c).strip()]
                if not cols:
                    return None
                return cols if len(cols) > 1 else cols[0]
            if v:
                return str(v).strip() or None
        except (ValueError, TypeError):
            pass
        return raw or None

    def src_block(prefix):
        raw_overrides = form.get(f"{prefix}_dtype_overrides_json", "") or "{}"
        try:
            dtype_overrides = json.loads(raw_overrides)
        except (ValueError, TypeError):
            dtype_overrides = {}
        raw_col_names = form.get(f"{prefix}_column_names", "") or ""
        column_names = [c.strip() for c in raw_col_names.split(",") if c.strip()] or list(dtype_overrides.keys())
        watch_path   = form.get(f"{prefix}_watch_path",  "").strip() or DATA_DUMP_PATH + "/incoming"
        archive_path = form.get(f"{prefix}_archive_path", "").strip() or DATA_DUMP_PATH + "/archive"
        file_name = form.get(f"{prefix}_file_name", "").strip()
        return {
            "watch_path":             watch_path,
            "file_name":              file_name,
            "file_format":            form.get(f"{prefix}_file_format", "csv"),
            "delimiter":              form.get(f"{prefix}_delimiter", ",") or ",",
            "encoding":               "auto",
            "has_header":             form.get(f"{prefix}_has_header") in ("on", "true", "1"),
            "skip_rows":              int(form.get(f"{prefix}_skip_rows") or 0),
            "date_columns":           _parse_list(form.get(f"{prefix}_date_columns", "")),
            "null_values":            _parse_list(form.get(f"{prefix}_null_values", "NULL,null,None,NA,N/A,#N/A")),
            "archive_path":           archive_path,
            "archive_retention_days": int(form.get(f"{prefix}_archive_retention_days") or 30),
            "column_names":           column_names,
            "dtype_overrides":        dtype_overrides,
            "incremental_column":     _parse_hwm_columns(form.get(f"{prefix}_hwm_column", "")),
            "business_date_column":   (form.get("business_date_column") or "").strip(),
        }

    def tgt_block(prefix):
        return {
            "target_db_conn_id": form.get(f"{prefix}_target_db_conn_id"),
            "db_type":           form.get(f"{prefix}_target_db_type", "postgresql"),
            "target_schema":     form.get(f"{prefix}_target_schema", "").strip().lower(),
            "target_table":      form.get(f"{prefix}_target_table", "").strip().lower(),
        }

    dev_src = src_block("dev")
    dev_tgt = tgt_block("dev")

    if apply_prod_same_as_dev:
        prod_src = dict(dev_src)
        prod_tgt = dict(dev_tgt)
    else:
        prod_src = src_block("prod")
        prod_tgt = tgt_block("prod")

    raw_tags   = form.get("extra_tags", "")
    extra_tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
    created_by = form.get("created_by", "")
    tags = list(dict.fromkeys([created_by] + extra_tags)) if created_by else extra_tags

    use_spark    = form.get("use_spark") == "on"
    has_existing = bool(form.get("_existing_spark_master"))
    spark_cfg = None
    if use_spark:
        spark_cfg = {
            "master":          form.get("spark_master")          or "local[*]",
            "app_name":        form.get("spark_app_name")        or form.get("dag_id") or "spark_file_etl",
            "driver_memory":   form.get("spark_driver_memory")   or "1g",
            "executor_memory": form.get("spark_executor_memory") or "2g",
            "executor_cores":  int(form.get("spark_executor_cores")  or 2),
            "num_executors":   int(form.get("spark_num_executors")    or 2),
        }
    elif has_existing:
        spark_cfg = {
            "master":          form.get("_existing_spark_master")          or "local[*]",
            "app_name":        form.get("_existing_spark_app_name")        or form.get("dag_id") or "spark_file_etl",
            "driver_memory":   form.get("_existing_spark_driver_memory")   or "1g",
            "executor_memory": form.get("_existing_spark_executor_memory") or "2g",
            "executor_cores":  int(form.get("_existing_spark_executor_cores") or 2),
            "num_executors":   int(form.get("_existing_spark_num_executors")   or 2),
        }

    config = {
        "pipeline_type":     "file_based",
        "schedule_interval": _norm_schedule(form.get("schedule_interval")),
        "start_date":        form.get("start_date") or "2023-01-01",
        "tags":              tags,
        "source":            {"dev": dev_src, "prod": prod_src},
        "target":            {"dev": dev_tgt, "prod": prod_tgt},
    }
    if spark_cfg:
        config["spark"] = spark_cfg

    dd = _parse_data_dictionary(form)
    if dd:
        dd = {col: info for col, info in dd.items() if col not in PIPELINE_META_COLS}
    if dd:
        config["data_dictionary"] = dd

    table_desc = (form.get("table_description") or "").strip()
    if table_desc:
        config["table_description"] = table_desc

    # Generate the final, canonical dag_id
    bare_name = (form.get("dag_id") or "").strip()
    dag_id_final = naming.generate_dag_id(config, bare_name)
    config["dag_id"] = dag_id_final

    return config


def load_config_files(all_types=False):
    """Load all config JSONs from the config directory."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    configs = []
    for path in sorted(CONFIG_DIR.rglob("*.json")):
        with path.open() as handle:
            try:
                cfg = json.load(handle)
                if all_types:
                    configs.append(cfg)
                # Default is to load only DB-to-DB jobs for the main /jobs page
                elif cfg.get("pipeline_type") != "file_based":
                    configs.append(cfg)
            except json.JSONDecodeError:
                pass  # Ignore malformed JSON
    return configs


def load_config(dag_id: str):
    """Load a single config by its canonical dag_id."""
    path = naming.find_config_path(dag_id, CONFIG_DIR)
    if not path or not path.exists():
        return None
    with path.open() as handle:
        return json.load(handle)


def save_config(config: dict, existing_config: dict = None):
    """Saves a config JSON and generates its corresponding DAG .py file."""
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    if existing_config:
        config["created_at"] = existing_config.get("created_at", now_str)
    else:
        config.setdefault("created_at", now_str)
    config["last_modified"] = now_str

    path = naming.get_config_path(config["dag_id"], config, CONFIG_DIR)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        json.dump(config, handle, indent=4)
    generate_dag_py(config)


def delete_config(dag_id: str):
    """Deletes a config JSON file by its canonical dag_id."""
    path = naming.find_config_path(dag_id, CONFIG_DIR)
    if path and path.exists():
        path.unlink()
        # Remove empty subfolder
        try:
            path.parent.rmdir()
        except OSError:
            pass


def delete_dag_files(dag_id: str):
    """Delete all .py and .sql files associated with a dag_id."""
    # This is imperfect as we don't have the full config to derive paths.
    # We glob for the dag_id as a suffix.
    for root_dir in (ETL_DIR, SQL_DIR):
        if not root_dir.exists(): continue
        for f in root_dir.rglob(f"{dag_id}*.py"):
            f.unlink(missing_ok=True)
        for f in root_dir.rglob(f"{dag_id}*.sql"):
            f.unlink(missing_ok=True)


def generate_dag_py(config: dict):
    """Generate the Airflow DAG .py file from the correct template into etl/<target_conn_id>/."""
    is_spark  = bool(config.get("spark"))
    pipe_type = config.get("pipeline_type")

    if pipe_type == "hybrid":
        # API jobs are pipeline_type "hybrid" + source_type "api" — a
        # standalone job type from the user's perspective, sharing this
        # same template (reuses its raw -> refined -> target code).
        tmpl_file = HYBRID_TEMPLATE_FILE
    elif pipe_type == "file_based":
        tmpl_file = SPARK_FILE_TEMPLATE_FILE if is_spark else FILE_TEMPLATE_FILE
    else:
        tmpl_file = SPARK_TEMPLATE_FILE if is_spark else TEMPLATE_FILE

    if not tmpl_file.exists():
        return

    dag_id = config["dag_id"]
    out_path = naming.get_dag_py_path(dag_id, config, ETL_DIR)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    content = tmpl_file.read_text()
    # The dag_id in the python file MUST match the canonical dag_id
    content = content.replace("<dag_name>", dag_id)
    out_path.write_text(content)



def read_sql_text(sql_file: str) -> str:
    if not sql_file:
        return ""
    # sql_file may be a plain filename or a relative path like conn_id/file.sql
    path = SQL_DIR / sql_file
    if path.exists():
        return path.read_text()
    # Fallback: search subfolders
    for found in SQL_DIR.rglob(Path(sql_file).name):
        return found.read_text()
    return ""


def persist_sql_from_form(form, dag_id: str, apply_prod_same_as_dev: bool, existing_config=None):
    dev_sql_text = (form.get("dev_sql_text") or "").strip()
    prod_sql_text = (form.get("prod_sql_text") or "").strip()

    # Determine target conn id to use as subfolder
    target_conn_id = form.get("dev_target_db_conn_id") or "default"
    sql_subdir = _sql_dir(target_conn_id)
    sql_subdir.mkdir(parents=True, exist_ok=True)

    # For DB-to-DB, SQL file name is based on dag_id
    # e.g., db_myconn_invoices.sql or db_myconn_invoices_prod.sql
    dev_filename = f"{dag_id}.sql" if apply_prod_same_as_dev else f"{dag_id}_dev.sql"
    dev_path = sql_subdir / dev_filename
    if dev_sql_text:
        dev_path.write_text(dev_sql_text)

    if apply_prod_same_as_dev:
        prod_path = dev_path
    else:
        prod_filename = f"{dag_id}_prod.sql"
        prod_path = sql_subdir / prod_filename

    if prod_sql_text:
        prod_path.write_text(prod_sql_text)

    return {
        "dev": str(dev_path.relative_to(SQL_DIR)),
        "prod": str(prod_path.relative_to(SQL_DIR)),
    }


def airflow_connections():
    if not AIRFLOW_DB:
        return []
    try:
        engine = create_engine(AIRFLOW_DB)
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT conn_id FROM connection ORDER BY conn_id")).fetchall()
        connections = [row[0] for row in rows]
        return connections
    except Exception:
        return []


def webui_engine():
    return create_engine(f"sqlite:///{WEBUI_DB}")


def _engine_for_conn_id(conn_id: str):
    """Return a SQLAlchemy engine built from an Airflow connection row. Raises on failure."""
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
        fernet_key = os.getenv("AIRFLOW__CORE__FERNET_KEY", "")
        if fernet_key:
            try:
                from cryptography.fernet import Fernet
                password = Fernet(fernet_key.encode()).decrypt(password_enc.encode()).decode()
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
    dialect = type_map.get(conn_type.lower(), conn_type)
    port_str = f":{port}" if port else ""
    return create_engine(f"{dialect}://{login}:{password}@{host}{port_str}/{schema}")


def init_webui_db():
    engine = webui_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS webui_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    is_admin INTEGER NOT NULL DEFAULT 0,
                    is_active INTEGER NOT NULL DEFAULT 1
                )
                """
            )
        )

        admin_user = _secret("webui_admin_user", "WEBUI_ADMIN_USER")
        admin_pass = _secret("webui_admin_pass", "WEBUI_ADMIN_PASS")
        if admin_user and admin_pass:
            # Always upsert so that a password change in .airflow/.env takes effect
            # on the next container/server restart without manually deleting webui.db.
            conn.execute(
                text(
                    """
                    INSERT INTO webui_users (username, password_hash, is_admin, is_active)
                    VALUES (:u, :p, 1, 1)
                    ON CONFLICT (username) DO UPDATE SET
                        password_hash = :p,
                        is_admin  = 1,
                        is_active = 1
                    """
                ),
                {"u": admin_user, "p": generate_password_hash(admin_pass)},
            )


def current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    engine = webui_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, username, is_admin, is_active FROM webui_users WHERE id = :id"
            ),
            {"id": user_id},
        ).fetchone()
    return row


def require_login():
    if not session.get("user_id"):
        return redirect(url_for("login"))
    return None


def require_admin():
    user = current_user()
    if not user or not user[2]:
        return redirect(url_for("jobs"))
    return None


def _norm_schedule(value):
    """Normalise a posted schedule_interval to a real cron string or None.

    A config with a null schedule renders into the form's value= as the
    literal text "None" (Jinja stringifies Python None), which posts straight
    back and is then saved as the STRING "None". Airflow parses that as a
    cron expression and the DAG dies with AirflowTimetableInvalid — a job
    that breaks purely by being opened and saved unchanged.
    """
    v = (value or "").strip()
    return None if v.lower() in ("", "none", "null") else v


def _bare_dag_id(config):
    """The user-facing name from a dag_id, i.e. dag_id minus its
    "<type-prefix>_<target_conn_id>_" lead.

    Strips the conn_id by its actual value rather than by a regex like
    `[^_]+`. Every conn_id in use contains underscores (local_dw_con,
    cenfri_dev_con, wasac_dw), so `[^_]+` matched only the first segment and
    left the rest in the "bare" name — editing and saving
    api_local_dw_con_water_production then re-derived it as
    api_local_dw_con_dw_con_water_production, silently creating a SECOND job
    pointing at the same target table. That is not hypothetical: it is how a
    duplicate DAG came to share gis.water_production and corrupt its row
    counts. Mirrors the two-step strip generate_dag_id() already does.
    """
    dag_id = (config or {}).get("dag_id", "") or ""
    bare = re.sub(r'^(spark_file|spark_db|hybrid|api|file|db)_', '', dag_id, flags=re.IGNORECASE)
    conn_id = ((config or {}).get("target", {}).get("dev", {}) or {}).get("target_db_conn_id") or ""
    if conn_id and bare.lower().startswith(conn_id.lower() + "_"):
        bare = bare[len(conn_id) + 1:]
    return bare


def _business_date_col(config):
    """business_date_column off a config's source block (dev preferred).

    Stored per-env alongside the rest of the source settings, but it is a
    single job-level answer — the column name doesn't differ between dev and
    prod — so the forms render one field and write it to both."""
    src = ((config or {}).get("source") or {})
    for env in ("dev", "prod"):
        val = (src.get(env) or {}).get("business_date_column")
        if val:
            return val
    return src.get("business_date_column") or ""


def build_config_from_form(form, apply_prod_same_as_dev: bool):
    def env_block(prefix):
        return {
            "source_db_conn_id": form.get(f"{prefix}_source_db_conn_id"),
            "db_type": form.get(f"{prefix}_source_db_type"),
            "incremental_column": (form.get(f"{prefix}_hwm_column") or "").strip(),
            "incremental_column_datatype": form.get(f"{prefix}_hwm_datatype"),
            "incremental_column_date_format": form.get(f"{prefix}_hwm_date_format", "").strip(),
            "batch_size": int(form.get(f"{prefix}_batch_size") or 0),
            "full_load": form.get(f"{prefix}_full_load") == "on",
            "business_date_column": (form.get("business_date_column") or "").strip(),
            "sql_file": None,  # populated by persist_sql_from_form after build
        }

    def target_block(prefix):
        return {
            "target_db_conn_id": form.get(f"{prefix}_target_db_conn_id"),
            "db_type": form.get(f"{prefix}_target_db_type"),
            "target_schema": (form.get(f"{prefix}_target_schema") or "").strip().lower() or None,
            "target_table": (form.get(f"{prefix}_target_table") or "").strip().lower() or None,
            "hwm_column": form.get(f"{prefix}_target_hwm_column"),
            "load_strategy": form.get(f"{prefix}_load_strategy"),
        }

    dev_source = env_block("dev")
    dev_target = target_block("dev")

    if apply_prod_same_as_dev:
        prod_source = dict(dev_source)
        prod_target = dict(dev_target)
    else:
        prod_source = env_block("prod")
        prod_target = target_block("prod")

    # Tags: always include the submitting user; merge with any extra tags typed
    raw_tags = form.get("extra_tags", "")
    extra_tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
    created_by = form.get("created_by", "")
    tags = list(dict.fromkeys([created_by] + extra_tags)) if created_by else extra_tags

    use_spark     = form.get("use_spark") == "on"
    has_existing  = bool(form.get("_existing_spark_master"))  # set by hidden inputs on edit
    spark_cfg = None
    if use_spark:
        # User explicitly toggled Spark on — read live form fields
        spark_cfg = {
            "master":          form.get("spark_master")          or "local[*]",
            "app_name":        form.get("spark_app_name")        or form.get("dag_id") or "etl_spark",
            "driver_memory":   form.get("spark_driver_memory")   or "1g",
            "executor_memory": form.get("spark_executor_memory") or "2g",
            "executor_cores":  int(form.get("spark_executor_cores")  or 2),
            "num_executors":   int(form.get("spark_num_executors")    or 2),
        }
    elif has_existing:
        # Spark was configured before; toggle not visible / not changed — preserve it
        spark_cfg = {
            "master":          form.get("_existing_spark_master")          or "local[*]",
            "app_name":        form.get("_existing_spark_app_name")        or form.get("dag_id") or "etl_spark",
            "driver_memory":   form.get("_existing_spark_driver_memory")   or "1g",
            "executor_memory": form.get("_existing_spark_executor_memory") or "2g",
            "executor_cores":  int(form.get("_existing_spark_executor_cores") or 2),
            "num_executors":   int(form.get("_existing_spark_num_executors")   or 2),
        }

    config = {
        "schedule_interval":_norm_schedule(form.get("schedule_interval")),
        "start_date":       form.get("start_date") or "2023-01-01",
        "batch_size":       int(form.get("batch_size") or 0),
        "read_chunk_size":  int(form.get("read_chunk_size") or 50000),
        "write_mode":       form.get("write_mode") or "append",
        "tags":             tags,
        "source":           {"dev": dev_source, "prod": prod_source},
        "target":           {"dev": dev_target, "prod": prod_target},
    }
    if spark_cfg:
        config["spark"] = spark_cfg

    dd = _parse_data_dictionary(form)
    if dd:
        # Strip pipeline-injected columns — they are managed by the ETL template
        # and should never appear in the user-facing Data Dictionary.
        dd = {col: info for col, info in dd.items() if col not in PIPELINE_META_COLS}
    if dd:
        config["data_dictionary"] = dd
        # Propagate dtype values from the data dictionary into dtype_overrides so
        # the ETL template enforces them on table creation and every write.
        dtype_overrides = {col: info["dtype"] for col, info in dd.items() if info.get("dtype")}
        if dtype_overrides:
            config["source"]["dev"]["dtype_overrides"] = dtype_overrides
            config["source"]["prod"]["dtype_overrides"] = dtype_overrides
        # Propagate date_format values for date/timestamp columns so the ETL
        # template knows the exact format to use when parsing source values.
        dtype_override_formats = {
            col: info["date_format"]
            for col, info in dd.items()
            if info.get("date_format") and info.get("dtype") in ("date", "timestamp")
        }
        if dtype_override_formats:
            config["source"]["dev"]["dtype_override_formats"] = dtype_override_formats
            config["source"]["prod"]["dtype_override_formats"] = dtype_override_formats

    table_desc = (form.get("table_description") or "").strip()
    if table_desc:
        config["table_description"] = table_desc

    # Generate the final, canonical dag_id
    bare_name = (form.get("dag_id") or "").strip()
    dag_id_final = naming.generate_dag_id(config, bare_name)
    config["dag_id"] = dag_id_final

    return config


@app.route("/")
def index():
    if not session.get("user_id"):
        return redirect(url_for("login"))
    configs = load_config_files(all_types=True)
    engine = webui_engine()
    with engine.connect() as conn:
        user_count = conn.execute(text("SELECT COUNT(*) FROM webui_users")).scalar()
    hybrid_configs = [c for c in configs if c.get("pipeline_type") == "hybrid"]
    return render_template(
        "index.html",
        user=current_user(),
        job_count=len(configs),
        user_count=user_count,
        # Legacy standalone types — no longer offered as a "+ New Job" entry
        # point (Hybrid Jobs, with File/DB/API as source_type choices, is
        # now the one job-creation surface) but existing configs of these
        # types keep running and stay reachable/editable at their own URLs.
        db_count=sum(1 for c in configs if c.get("pipeline_type") not in ("file_based", "hybrid")),
        file_count=sum(1 for c in configs if c.get("pipeline_type") == "file_based"),
        hybrid_count=len(hybrid_configs),
        hybrid_file_count=sum(1 for c in hybrid_configs if c.get("source_type", "file") == "file"),
        hybrid_db_count=sum(1 for c in hybrid_configs if c.get("source_type") == "db"),
        hybrid_api_count=sum(1 for c in hybrid_configs if c.get("source_type") == "api"),
    )


@app.route("/login", methods=["GET", "POST"])
def login():
    init_webui_db()
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        engine = webui_engine()
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT id, password_hash, is_active
                    FROM webui_users WHERE username = :u
                    """
                ),
                {"u": username},
            ).fetchone()
        if row and row[2] and check_password_hash(row[1], password):
            session["user_id"] = row[0]
            return redirect(url_for("index"))
        return render_template("login.html", error="Invalid credentials")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/profile", methods=["GET", "POST"])
def profile():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp

    user = current_user()
    if request.method == "POST":
        password = request.form.get("password")
        if password:
            engine = webui_engine()
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE webui_users SET password_hash = :p WHERE id = :id"),
                    {"p": generate_password_hash(password), "id": user[0]},
                )
        return redirect(url_for("index"))

    return render_template("profile.html", user=user)


@app.route("/jobs")
def jobs():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    configs = [cfg for cfg in load_config_files(all_types=True) if cfg.get("pipeline_type") != "file_based"]
    return render_template("jobs.html", jobs=configs, user=current_user(), pipeline_type="db")


@app.route("/jobs/new", methods=["GET", "POST"])
def new_job():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    if request.method == "POST":
        apply_prod_same_as_dev = request.form.get("apply_prod_same_as_dev") == "on"
        config = build_config_from_form(request.form, apply_prod_same_as_dev)
        sql_files = persist_sql_from_form(request.form, config["dag_id"], apply_prod_same_as_dev)
        config["source"]["dev"]["sql_file"] = sql_files["dev"]
        config["source"]["prod"]["sql_file"] = sql_files["prod"]
        save_config(config)
        return redirect(url_for("jobs"))

    _user = current_user()
    return render_template(
        "form.html",
        config=None,
        connections=airflow_connections(),
        dev_sql_text="",
        prod_sql_text="",
        schedule_suggestions=SCHEDULE_SUGGESTIONS,
        user=_user,
        current_username=_user[1] if _user else "",
        existing_tags=[],
        active_env=ACTIVE_ENV,
        data_dictionary_json="{}",
        table_description_val="",
        business_date_column_val="",
    )


@app.route("/jobs/<dag_id>/edit", methods=["GET", "POST"])
def edit_job(dag_id):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    config = load_config(dag_id)
    if not config:
        return redirect(url_for("jobs"))

    if request.method == "POST":
        apply_prod_same_as_dev = request.form.get("apply_prod_same_as_dev") == "on"
        new_config = build_config_from_form(request.form, apply_prod_same_as_dev)
        sql_files = persist_sql_from_form(
            request.form,
            new_config["dag_id"],
            apply_prod_same_as_dev,
            existing_config=config,
        )
        new_config["source"]["dev"]["sql_file"] = sql_files["dev"]
        new_config["source"]["prod"]["sql_file"] = sql_files["prod"]
        # If the dag_id changed (e.g. conn_id or table renamed), remove the old files first
        if new_config["dag_id"] != dag_id:
            delete_config(dag_id)
            delete_dag_files(dag_id)
        save_config(new_config, existing_config=config)
        return redirect(url_for("jobs"))

    _user = current_user()
    existing_tags = config.get("tags", [])
    # Extra tags = all tags except the owner username
    owner = existing_tags[0] if existing_tags else ""
    extra_tags = existing_tags[1:] if len(existing_tags) > 1 else []
    import json as _json
    return render_template(
        "form.html",
        config=config,
        connections=airflow_connections(),
        dev_sql_text=read_sql_text(config.get("source", {}).get("dev", {}).get("sql_file")),
        prod_sql_text=read_sql_text(config.get("source", {}).get("prod", {}).get("sql_file")),
        schedule_suggestions=SCHEDULE_SUGGESTIONS,
        user=_user,
        current_username=_user[1] if _user else "",
        existing_tags=existing_tags,
        extra_tags_str=", ".join(extra_tags),
        active_env=ACTIVE_ENV,
        data_dictionary_json=_json.dumps({
            k: v for k, v in (config.get("data_dictionary") or {}).items()
            if k not in PIPELINE_META_COLS
        }),
        table_description_val=config.get("table_description") or "",
        business_date_column_val=_business_date_col(config),
    )


@app.route("/jobs/<dag_id>/delete", methods=["POST"])
def delete_job(dag_id):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    delete_config(dag_id) # Deletes JSON
    delete_dag_files(dag_id)
    return redirect(url_for("jobs"))


@app.route("/users")
def users():
    redirect_resp = require_admin()
    if redirect_resp:
        return redirect_resp

    engine = webui_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, username, is_admin, is_active FROM webui_users ORDER BY id")
        ).fetchall()
    return render_template("users.html", users=rows, user=current_user())


@app.route("/users/new", methods=["POST"])
def create_user():
    redirect_resp = require_admin()
    if redirect_resp:
        return redirect_resp

    username = request.form.get("username")
    password = request.form.get("password")
    is_admin = 1 if request.form.get("is_admin") == "on" else 0
    if username and password:
        engine = webui_engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO webui_users (username, password_hash, is_admin, is_active)
                    VALUES (:u, :p, :a, 1)
                    """
                ),
                {"u": username, "p": generate_password_hash(password), "a": is_admin},
            )
    return redirect(url_for("users"))


@app.route("/users/<int:user_id>/toggle", methods=["POST"])
def toggle_user(user_id):
    redirect_resp = require_admin()
    if redirect_resp:
        return redirect_resp

    engine = webui_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE webui_users SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END WHERE id = :id"
            ),
            {"id": user_id},
        )
    return redirect(url_for("users"))


CLEANING_FILES_DIR = Path(os.getenv("CLEANING_FILES_DIR", APP_ROOT / "cleaning_files"))


@app.route("/api/cleaning-scripts")
def api_cleaning_scripts():
    from flask import jsonify
    try:
        scripts = sorted(
            f.name for f in CLEANING_FILES_DIR.iterdir()
            if f.suffix == ".py" and not f.name.startswith("_")
        ) if CLEANING_FILES_DIR.exists() else []
    except Exception as exc:
        return jsonify({"scripts": [], "error": str(exc)}), 500
    return jsonify({"scripts": scripts})


@app.route("/api/cleaning-functions")
def api_cleaning_functions():
    """Return top-level, non-private callable function names from a cleaning script."""
    import ast
    from flask import jsonify, request as req
    script_name = req.args.get("script", "").strip()
    if not script_name:
        return jsonify({"functions": []}), 400
    safe_name = Path(script_name).name
    path = CLEANING_FILES_DIR / safe_name
    if not path.exists():
        return jsonify({"error": f"Script '{safe_name}' not found"}), 404
    try:
        tree = ast.parse(path.read_text())
        fns = [
            node.name for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and not node.name.startswith("_")
        ]
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"functions": fns})


# ──────────────────────────────────────────────────────────────────────────────
# File-based ETL routes  (completely separate from DB-to-DB /jobs/* routes)
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/api/files")
def api_list_files():
    """Return the list of files available in INCOMING_DIR."""
    from flask import jsonify
    try:
        if INCOMING_DIR.exists():
            files = sorted(
                f.name for f in INCOMING_DIR.iterdir()
                if f.is_file() and not f.name.startswith(".")
            )
        else:
            files = []
    except Exception as exc:
        return jsonify({"files": [], "error": str(exc), "incoming_dir": str(INCOMING_DIR)}), 500
    return jsonify({"files": files, "incoming_dir": str(INCOMING_DIR)})


@app.route("/api/preview-columns", methods=["POST"])
def api_preview_columns():
    """
    Accept a file upload (or a server-side path) and return column names
    with suggested dtypes as JSON.  Used by the file_form wizard.
    """
    import io
    from flask import jsonify
    from web_ui.column_inference import infer_columns_from_dataframe

    try:
        import pandas as pd

        file_obj        = request.files.get("sample_file")
        server_filename = request.form.get("server_filename", "").strip()
        file_format  = request.form.get("file_format", "csv").lower()
        delimiter    = request.form.get("delimiter", ",") or ","
        has_header   = request.form.get("has_header", "true").lower() != "false"
        skip_rows    = int(request.form.get("skip_rows", 0) or 0)
        enc_raw      = request.form.get("encoding", "utf-8") or "utf-8"
        encoding     = "utf-8" if enc_raw.lower() in ("auto", "") else enc_raw
        null_values  = [v.strip() for v in request.form.get("null_values", "NULL,null,None,NA").split(",") if v.strip()]

        if server_filename:
            # Prevent path traversal — only allow a plain filename with no directory components
            safe_name = Path(server_filename).name
            if safe_name != server_filename:
                return jsonify({"error": "Invalid filename — directory traversal is not allowed."}), 400
            file_path = INCOMING_DIR / safe_name
            if not file_path.exists():
                return jsonify({"error": f"File '{safe_name}' was not found in the incoming folder ({INCOMING_DIR})."}), 404
            raw = file_path.read_bytes()
            # Auto-detect format from extension when not explicitly provided
            if not file_format or file_format in ("auto", "csv"):
                file_format = "parquet" if safe_name.lower().endswith(".parquet") else "csv"
        elif file_obj:
            raw = file_obj.read()
        else:
            return jsonify({"error": "No filename provided"}), 400

        # Auto-detect delimiter if set to 'auto' or empty
        detected_delimiter = delimiter
        if file_format != "parquet" and (not delimiter or delimiter == "auto"):
            import csv as _csv
            try:
                sample_text = raw[:4096].decode(encoding if encoding not in ("", "auto") else "utf-8", errors="replace")
                sniffer = _csv.Sniffer()
                detected_delimiter = sniffer.sniff(sample_text, delimiters=",;|\t").delimiter
            except Exception:
                detected_delimiter = ","
            delimiter = detected_delimiter

        if file_format == "parquet":
            df = pd.read_parquet(io.BytesIO(raw))
        else:
            # Try the requested encoding, fall back to latin-1
            try:
                text_io = io.StringIO(raw.decode(encoding, errors="replace"))
            except (LookupError, UnicodeDecodeError):
                text_io = io.StringIO(raw.decode("latin-1", errors="replace"))

            df = pd.read_csv(
                text_io,
                sep=delimiter,
                header=0 if has_header else None,
                skiprows=skip_rows if skip_rows else None,
                na_values=null_values,
                keep_default_na=True,
                nrows=500,      # sample only
                low_memory=False,
            )

        columns = infer_columns_from_dataframe(df)
        return jsonify({"columns": columns, "detected_delimiter": detected_delimiter})

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/file-jobs")
def file_jobs():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    configs = [cfg for cfg in load_config_files(all_types=True) if cfg.get("pipeline_type") == "file_based"]
    return render_template("jobs.html", jobs=configs, user=current_user(), pipeline_type="file_based")


@app.route("/file-jobs/new", methods=["GET", "POST"])
def new_file_job():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    _user = current_user()
    if request.method == "POST":
        try:
            apply_prod_same_as_dev = request.form.get("apply_prod_same_as_dev") == "on"
            config = build_file_config_from_form(request.form, apply_prod_same_as_dev)
            if not config.get("dag_id"):
                raise ValueError("DAG ID is required.")
            save_file_config(config)
            return redirect(url_for("file_jobs"))
        except Exception as exc:
            app.logger.exception("Error saving file job")
            return render_template(
                "file_form.html",
                config=None,
                connections=airflow_connections(),
                schedule_suggestions=SCHEDULE_SUGGESTIONS,
                user=_user,
                current_username=_user[1] if _user else "",
                existing_tags=[],
                active_env=ACTIVE_ENV,
                save_error=str(exc),
                form_data=request.form,
                data_dictionary_json=json.dumps(_parse_data_dictionary(request.form)),
                table_description_val=request.form.get("table_description", ""),
                business_date_column_val=request.form.get("business_date_column", ""),
                incoming_dir=DATA_DUMP_PATH + "/incoming",
                archive_dir=DATA_DUMP_PATH + "/archive",
            ), 422
    return render_template(
        "file_form.html",
        config=None,
        connections=airflow_connections(),
        schedule_suggestions=SCHEDULE_SUGGESTIONS,
        user=_user,
        current_username=_user[1] if _user else "",
        existing_tags=[],
        active_env=ACTIVE_ENV,
        data_dictionary_json="{}",
        table_description_val="",
        business_date_column_val="",
        incoming_dir=DATA_DUMP_PATH + "/incoming",
        archive_dir=DATA_DUMP_PATH + "/archive",
    )


@app.route("/file-jobs/<dag_id>/edit", methods=["GET", "POST"])
def edit_file_job(dag_id):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    config = load_file_config(dag_id)
    if not config:
        return redirect(url_for("file_jobs"))

    _user = current_user()
    existing_tags = config.get("tags", [])
    extra_tags = existing_tags[1:] if len(existing_tags) > 1 else []
    bare_dag_id = _bare_dag_id(config)
    if request.method == "POST":
        try:
            apply_prod_same_as_dev = request.form.get("apply_prod_same_as_dev") == "on"
            new_config = build_file_config_from_form(request.form, apply_prod_same_as_dev)
            # If the dag_id changed, remove the old files first
            if new_config["dag_id"] != dag_id:
                delete_file_config(dag_id)
                delete_file_dag_files(dag_id)
            save_file_config(new_config, existing_config=config)
            return redirect(url_for("file_jobs"))
        except Exception as exc:
            app.logger.exception("Error saving file job")
            return render_template(
                "file_form.html",
                config=config,
                connections=airflow_connections(),
                schedule_suggestions=SCHEDULE_SUGGESTIONS,
                user=_user,
                current_username=_user[1] if _user else "",
                existing_tags=existing_tags,
                extra_tags_str=", ".join(extra_tags),
                active_env=ACTIVE_ENV,
                save_error=str(exc),
                form_data=request.form,
                bare_dag_id=bare_dag_id,
                data_dictionary_json=json.dumps(_parse_data_dictionary(request.form)),
                table_description_val=request.form.get("table_description") or config.get("table_description") or "",
                business_date_column_val=request.form.get("business_date_column") or _business_date_col(config),
                incoming_dir=DATA_DUMP_PATH + "/incoming",
                archive_dir=DATA_DUMP_PATH + "/archive",
            ), 422
    import json as _json
    return render_template(
        "file_form.html",
        config=config,
        connections=airflow_connections(),
        schedule_suggestions=SCHEDULE_SUGGESTIONS,
        user=_user,
        current_username=_user[1] if _user else "",
        existing_tags=existing_tags,
        extra_tags_str=", ".join(extra_tags),
        active_env=ACTIVE_ENV,
        bare_dag_id=bare_dag_id,
        data_dictionary_json=_json.dumps({
            k: v for k, v in (config.get("data_dictionary") or {}).items()
            if k not in PIPELINE_META_COLS
        }),
        table_description_val=config.get("table_description") or "",
        business_date_column_val=_business_date_col(config),
        incoming_dir=DATA_DUMP_PATH + "/incoming",
        archive_dir=DATA_DUMP_PATH + "/archive",
    )


@app.route("/file-jobs/<dag_id>/delete", methods=["POST"])
def delete_file_job(dag_id):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    delete_config(dag_id)
    delete_file_dag_files(dag_id)
    return redirect(url_for("file_jobs"))


# ──────────────────────────────────────────────────────────────────────────────
# Data Catalog
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/catalog")
def catalog():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp

    configs = load_config_files(all_types=True)
    tables = []
    col_index = {}  # col_name -> [{dag_id, schema, table, conn}]

    for cfg in configs:
        dd = cfg.get("data_dictionary") or {}
        tgt = cfg.get("target", {}).get("dev", {})
        schema = tgt.get("target_schema", "")
        table  = tgt.get("target_table", "")
        conn   = tgt.get("target_db_conn_id", "")
        tables.append({
            "dag_id":          cfg.get("dag_id"),
            "pipeline_type":   cfg.get("pipeline_type", "db"),
            "connection":      conn,
            "schema":          schema,
            "table":           table,
            "data_dictionary": dd,
            "column_count":    len(dd),
            "has_dd":          bool(dd),
        })
        for col_name in dd:
            col_index.setdefault(col_name, []).append({
                "dag_id": cfg.get("dag_id"),
                "schema": schema,
                "table":  table,
                "conn":   conn,
            })

    # Potential joins: columns appearing in 2+ distinct tables
    join_suggestions = [
        {"column": col, "tables": refs}
        for col, refs in col_index.items()
        if len(refs) >= 2
    ]
    join_suggestions.sort(key=lambda x: len(x["tables"]), reverse=True)

    return render_template(
        "catalog.html",
        tables=tables,
        join_suggestions=join_suggestions[:30],
        user=current_user(),
        total_columns=sum(t["column_count"] for t in tables),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Columns from query  (SELECT * FROM (<sql>) LIMIT 0)
# ──────────────────────────────────────────────────────────────────────────────
@app.route("/api/columns-from-query", methods=["POST"])
def api_columns_from_query():
    from flask import jsonify

    data = request.get_json(force=True, silent=True) or {}
    conn_id = str(data.get("conn_id", "")).strip()
    sql     = str(data.get("sql", "")).strip()

    if not conn_id or not sql:
        return jsonify({"error": "conn_id and sql are required"}), 400

    if not AIRFLOW_DB:
        return jsonify({"error": "AIRFLOW_DB not configured"}), 400

    try:
        src_engine = _engine_for_conn_id(conn_id)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404
    except Exception as exc:
        return jsonify({"error": f"Failed to build connection: {exc}"}), 500

    try:
        wrapped = f"SELECT * FROM ({sql}) AS _q LIMIT 0"
        with src_engine.connect() as sc:
            result = sc.execute(text(wrapped))
            columns = [
                {"name": col, "type": str(result.cursor.description[i][1].__name__ if hasattr(result.cursor.description[i][1], '__name__') else result.cursor.description[i][1])}
                for i, col in enumerate(result.keys())
            ]
        return jsonify({"columns": [
            c for c in columns if c["name"] not in PIPELINE_META_COLS
        ]})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ──────────────────────────────────────────────────────────────────────────────
# API job — connection testing, response-format detection, column detection
# ──────────────────────────────────────────────────────────────────────────────

# A default User-Agent — many real-world APIs (anything behind Cloudflare or
# similar) reject urllib's default "Python-urllib/x.y" string outright with a
# 403, even for a perfectly legitimate, unauthenticated GET. Always lowest
# priority in a header merge so a user-configured header can still override it.
API_JOB_USER_AGENT = "ETL-Manager/1.0"


def _extract_error_envelope(parsed):
    """Return a human-readable error message if `parsed` looks like a common
    REST API error envelope (a top-level dict with an "error" key). Plenty of
    real-world APIs report failures this way with an HTTP 200 status — the
    error is only visible in the body, e.g. ArcGIS/Esri Server responding
    {"error": {"code": 498, "message": "Invalid Token"}} with a 200. Checking
    HTTP status alone misses these entirely. Returns None when `parsed`
    doesn't look like an error envelope.
    """
    if not isinstance(parsed, dict):
        return None
    err = parsed.get("error")
    if err is None:
        return None
    if isinstance(err, dict):
        msg  = err.get("message") or err.get("details") or str(err)
        code = err.get("code")
        return f"{msg} (code {code})" if code is not None else str(msg)
    return str(err)


def _get_nested_json(d, dotted_path):
    """Resolve a dotted path like 'data.results' against a parsed JSON value.
    A numeric segment indexes into a list, e.g. 'data.0.token'. Mirrors
    hybrid_load.template's _get_nested (used for the api source_type) — kept
    as a separate copy here rather than shared, matching this project's
    convention of every runtime (web UI process vs. Airflow worker) staying
    self-contained."""
    if not dotted_path:
        return d
    cur = d
    for part in dotted_path.split("."):
        if cur is None:
            return None
        if isinstance(cur, list):
            try:
                cur = cur[int(part)]
            except (ValueError, IndexError):
                return None
        elif isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def _resolve_test_auth_headers(auth_cfg, existing_auth):
    """Build the (headers, params) for a live 'Test Connection' / 'Detect
    Columns' request. Exactly one of the two is populated, depending on auth
    type — everything merges into the same request either way.

    A secret field is either a freshly-typed plaintext value or the mask
    placeholder (unchanged) — when masked, fall back to decrypting the
    previously-saved ciphertext from existing_auth (present when editing).
    The resolved plaintext is used only to build the outbound request; it is
    never included in a JSON response sent back to the browser.
    """
    auth_type = (auth_cfg or {}).get("type", "none")
    if auth_type == "none" or not auth_cfg:
        return {}, {}

    def _plaintext(field_name, enc_field_name):
        typed = (auth_cfg.get(field_name) or "").strip()
        if typed and not secrets_crypto.is_masked(typed):
            return typed
        return secrets_crypto.decrypt((existing_auth or {}).get(enc_field_name, ""))

    if auth_type == "api_key_header":
        header_name = auth_cfg.get("header_name") or "Authorization"
        key = _plaintext("api_key", "api_key_enc")
        if not key:
            raise ValueError("Enter an API key to test.")
        return {header_name: key}, {}

    if auth_type == "api_key_query_param":
        # Common for ArcGIS/Esri FeatureServer/MapServer REST endpoints and
        # similar services that expect a static token as a query string
        # value (e.g. "...&token=..."), not a header.
        param_name = auth_cfg.get("param_name") or "token"
        key = _plaintext("api_key", "api_key_enc")
        if not key:
            raise ValueError("Enter an API key/token to test.")
        return {}, {param_name: key}

    if auth_type == "login_token":
        import urllib.request
        token_endpoint = (auth_cfg.get("token_endpoint") or "").strip()
        if not token_endpoint:
            raise ValueError("A token endpoint is required for login_token auth.")
        username       = auth_cfg.get("token_username", "") or (existing_auth or {}).get("token_username", "")
        password       = _plaintext("token_password", "token_password_enc")
        username_field = auth_cfg.get("token_username_field", "username")
        password_field = auth_cfg.get("token_password_field", "password")
        body_type      = (auth_cfg.get("token_request_body_type") or "json").lower()
        payload        = {username_field: username, password_field: password}

        if body_type == "form":
            body    = urllib.parse.urlencode(payload).encode()
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
        else:
            body    = json.dumps(payload).encode()
            headers = {"Content-Type": "application/json"}

        req = urllib.request.Request(
            token_endpoint, data=body, headers={"User-Agent": API_JOB_USER_AGENT, **headers}, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp_body = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            raise ValueError(f"Failed to fetch token from {token_endpoint}: {exc}") from exc

        token_field = auth_cfg.get("token_response_field", "access_token")
        token = _get_nested_json(resp_body, token_field)
        if not token:
            raise ValueError(f"Token response did not contain a '{token_field}' field.")
        header_name = auth_cfg.get("token_header_name") or "Authorization"
        # `or`, not a .get() default — a present-but-empty value (e.g. the
        # wizard always sends this key, blank or not) must still default to
        # "Bearer ", since .get(key, default) only falls back when the key
        # itself is entirely absent.
        prefix      = auth_cfg.get("token_header_prefix") or "Bearer "
        return {header_name: f"{prefix}{token}"}, {}

    raise ValueError(f"Unknown auth type: {auth_type}")


def _existing_api_auth(dag_id, env):
    """Load the saved (encrypted) auth block for an API job being edited, so
    a masked secret field can be resolved against it. Returns {} for a new
    job or one that isn't an API job."""
    if not dag_id:
        return {}
    existing = load_config(dag_id)
    if not existing or not _is_api_job(existing):
        return {}
    return ((existing.get("source") or {}).get(env) or {}).get("auth", {}) or {}


def _existing_api_full_url(dag_id, env):
    """Load the saved (encrypted) full_url for an API job being edited, so a
    masked value can be resolved against it when testing. Returns "" for a
    new job or one that isn't an API job."""
    if not dag_id:
        return ""
    existing = load_config(dag_id)
    if not existing or not _is_api_job(existing):
        return ""
    return ((existing.get("source") or {}).get(env) or {}).get("full_url_enc", "") or ""


def _resolve_test_full_url(typed_value, existing_full_url_enc):
    """Same mask-or-typed resolution as _resolve_test_auth_headers's secret
    fields, for url_mode="full"'s one full-URL secret."""
    typed = (typed_value or "").strip()
    if typed and not secrets_crypto.is_masked(typed):
        return typed
    return secrets_crypto.decrypt(existing_full_url_enc or "")


def _detect_response_shape(body: bytes):
    """Heuristic auto-detection for Test Connection: returns
    (format_type, records_path, sample_records)."""
    text_body = body.decode("utf-8", errors="replace").strip()
    if not text_body:
        return "json_array", "", []

    try:
        parsed = json.loads(text_body)
    except (ValueError, TypeError):
        parsed = None

    if parsed is None:
        # Not one valid JSON document — maybe NDJSON (one JSON value per line).
        lines = [l for l in text_body.splitlines() if l.strip()]
        if len(lines) > 1:
            try:
                records = [json.loads(l) for l in lines]
                if all(isinstance(r, dict) for r in records):
                    return "ndjson", "", records
            except (ValueError, TypeError):
                pass
        return "json_array", "", []

    if isinstance(parsed, list):
        return "json_array", "", [r for r in parsed if isinstance(r, dict)]

    if isinstance(parsed, dict):
        # Depth-first search for the first key whose value is a non-empty
        # list of dicts — the common "{"data": {"results": [...]}}" shape.
        def _search(node, prefix=""):
            if isinstance(node, dict):
                for k, v in node.items():
                    path = f"{prefix}.{k}" if prefix else k
                    if isinstance(v, list) and v and all(isinstance(i, dict) for i in v):
                        return path, v
                    found = _search(v, path)
                    if found:
                        return found
            return None

        found = _search(parsed)
        if found:
            path, records = found
            return "json_object_path", path, records
        return "json_object_path", "", []

    return "json_array", "", []


def _extract_records_for_detection(body: bytes, response_format: dict):
    """Extract just the record list for column detection — same parsing
    rules as hybrid_load.template's _parse_page (used for the api
    source_type), minus the pagination-metadata return value detection
    doesn't need."""
    fmt = (response_format or {}).get("type", "json_array")
    text_body = body.decode("utf-8", errors="replace")

    if fmt == "ndjson":
        records = []
        for line in text_body.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                parsed_line = json.loads(line)
            except (ValueError, TypeError):
                continue
            records.append(parsed_line if isinstance(parsed_line, dict) else {"value": parsed_line})
        return records

    try:
        parsed = json.loads(text_body) if text_body.strip() else None
    except (ValueError, TypeError):
        parsed = None

    if fmt == "json_object_path":
        records = _get_nested_json(parsed, (response_format or {}).get("records_path", ""))
    else:
        records = parsed

    if records is None:
        records = []
    elif isinstance(records, dict):
        records = [records]
    if not isinstance(records, list):
        records = []
    return [r for r in records if isinstance(r, dict)]


@app.route("/api/api-test-connection", methods=["POST"])
def api_test_connection():
    """Build one outbound request from the wizard's current Connection-tab
    fields (resolving a masked secret against the saved config when editing)
    and fetch a small sample. Auto-detects the response format so the wizard
    can pre-fill the Response Format tab. Never echoes a secret back."""
    from flask import jsonify
    import urllib.request
    import urllib.error
    import urllib.parse

    data = request.get_json(force=True, silent=True) or {}
    http_method      = (data.get("http_method") or "GET").upper()
    request_params   = dict(data.get("request_params") or {})
    request_headers  = dict(data.get("request_headers") or {})
    timeout_seconds  = int(data.get("timeout_seconds") or 30)

    if data.get("url_mode") == "full":
        existing_full_url = _existing_api_full_url(data.get("dag_id"), data.get("env") or "dev")
        full_url = _resolve_test_full_url(data.get("full_url"), existing_full_url)
        if not full_url:
            return jsonify({"status": "error", "error": "Paste the full request URL to test."}), 400
        parts = urllib.parse.urlsplit(full_url)
        base_url = f"{parts.scheme}://{parts.netloc}{parts.path}"
        base_params = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
        base_params.update(request_params)
        auth_headers = {}
    else:
        base_url = (data.get("base_url") or "").rstrip("/")
        if not base_url:
            return jsonify({"status": "error", "error": "Base URL is required."}), 400
        base_url += data.get("data_endpoint") or ""
        auth_cfg      = data.get("auth") or {}
        existing_auth = _existing_api_auth(data.get("dag_id"), data.get("env") or "dev")
        try:
            auth_headers, auth_params = _resolve_test_auth_headers(auth_cfg, existing_auth)
        except Exception as exc:
            return jsonify({"status": "error", "error": str(exc)}), 200
        base_params = {**request_params, **auth_params}

    url   = base_url
    query = ("?" + urllib.parse.urlencode(base_params)) if base_params else ""
    req = urllib.request.Request(
        url + query, headers={"User-Agent": API_JOB_USER_AGENT, **request_headers, **auth_headers}, method=http_method
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            http_status = resp.status
            body = resp.read()
    except urllib.error.HTTPError as exc:
        return jsonify({"status": "error", "http_status": exc.code, "error": f"HTTP {exc.code}: {exc.reason}"}), 200
    except urllib.error.URLError as exc:
        return jsonify({"status": "error", "error": f"Could not reach {url}: {exc.reason}"}), 200
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 200

    try:
        parsed_body = json.loads(body.decode("utf-8", errors="replace"))
    except (ValueError, TypeError):
        parsed_body = None

    # Many real-world APIs (ArcGIS/Esri Server among them) report failures as
    # HTTP 200 with an error envelope in the body — a status-code-only check
    # would call this "ok" and hand the wizard a nonsense detected format.
    env_error = _extract_error_envelope(parsed_body)
    if env_error:
        return jsonify({"status": "error", "http_status": http_status, "error": f"API returned an error: {env_error}"})

    detected_format, detected_records_path, sample_records = _detect_response_shape(body)
    try:
        pretty = json.dumps(parsed_body, indent=2)[:4000] if parsed_body is not None else body[:2000].decode("utf-8", errors="replace")
    except (ValueError, TypeError):
        pretty = body[:2000].decode("utf-8", errors="replace")

    return jsonify({
        "status":                 "ok",
        "http_status":            http_status,
        "detected_format":        detected_format,
        "detected_records_path":  detected_records_path,
        "record_count_sample":    len(sample_records),
        "sample_raw":             pretty,
    })


@app.route("/api/api-detect-columns", methods=["POST"])
def api_detect_columns():
    """Fetch one sample page from the configured API and run column/dtype
    inference over the returned records — same response shape
    /api/preview-columns already returns, so the wizard's existing
    renderColumnTable() JS needs no changes."""
    from flask import jsonify
    import urllib.request
    import urllib.error
    import urllib.parse
    from web_ui.column_inference import infer_columns_from_records

    data = request.get_json(force=True, silent=True) or {}
    http_method     = (data.get("http_method") or "GET").upper()
    request_params  = dict(data.get("request_params") or {})
    request_headers = dict(data.get("request_headers") or {})
    timeout_seconds = int(data.get("timeout_seconds") or 30)
    response_format = data.get("response_format") or {"type": "json_array", "records_path": ""}

    if data.get("url_mode") == "full":
        existing_full_url = _existing_api_full_url(data.get("dag_id"), data.get("env") or "dev")
        full_url = _resolve_test_full_url(data.get("full_url"), existing_full_url)
        if not full_url:
            return jsonify({"error": "Paste the full request URL to test."}), 400
        parts = urllib.parse.urlsplit(full_url)
        base_url = f"{parts.scheme}://{parts.netloc}{parts.path}"
        sample_params = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
        sample_params.update(request_params)
        auth_headers = {}
    else:
        base_url = (data.get("base_url") or "").rstrip("/")
        if not base_url:
            return jsonify({"error": "Base URL is required."}), 400
        base_url += data.get("data_endpoint") or ""
        auth_cfg      = data.get("auth") or {}
        existing_auth = _existing_api_auth(data.get("dag_id"), data.get("env") or "dev")
        try:
            auth_headers, auth_params = _resolve_test_auth_headers(auth_cfg, existing_auth)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 200
        sample_params = dict(request_params)
        sample_params.update(auth_params)

    # HWM is intentionally excluded from detection sampling — there's no
    # target table yet to read a watermark from, and detection should reflect
    # a representative, unfiltered-by-HWM sample. Static filters ARE applied,
    # since they narrow toward the kind of records this job will actually load.
    for f in (data.get("filters") or []):
        api_param = f.get("api_param")
        if api_param and f.get("value") not in (None, ""):
            sample_params[api_param] = f["value"]

    # Sample a second page when pagination is configured. A field that is
    # absent or entirely null across page 1 — common with wide record
    # schemas, where not every record carries every field — would otherwise
    # never appear in the detected column list at all, and so would never be
    # mapped or loaded. Two pages is the deliberate cap: this runs
    # synchronously while the user waits on the wizard, so it buys the
    # coverage that matters without turning detection into a full pull.
    pagination = data.get("pagination") or {}
    strategy   = pagination.get("strategy") or "none"
    page_size  = int(pagination.get("page_size") or 100)

    page_params = [dict(sample_params)]
    if strategy == "offset_limit":
        second = dict(sample_params)
        second[pagination.get("offset_param") or "offset"] = page_size
        second[pagination.get("limit_param")  or "limit"]  = page_size
        page_params[0][pagination.get("limit_param") or "limit"] = page_size
        page_params.append(second)
    elif strategy == "page_number":
        start = int(pagination.get("start_page") or 1)
        second = dict(sample_params)
        second[pagination.get("page_param") or "page"] = start + 1
        page_params[0][pagination.get("page_param") or "page"] = start
        page_params.append(second)
    # cursor / next_link need the first response body to find the follow-on
    # location, so they're sampled one page only — the field that carries it
    # is what Test Connection is for.

    records = []
    for idx, params in enumerate(page_params):
        query = ("?" + urllib.parse.urlencode(params)) if params else ""
        req = urllib.request.Request(
            base_url + query,
            headers={"User-Agent": API_JOB_USER_AGENT, **request_headers, **auth_headers},
            method=http_method,
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                body = resp.read()
        except urllib.error.HTTPError as exc:
            if idx:      # page 1 already succeeded — detect on what we have
                break
            return jsonify({"error": f"HTTP {exc.code}: {exc.reason}"}), 200
        except urllib.error.URLError as exc:
            if idx:
                break
            return jsonify({"error": f"Could not reach {base_url}: {exc.reason}"}), 200
        except Exception as exc:
            if idx:
                break
            return jsonify({"error": str(exc)}), 200

        try:
            parsed_body = json.loads(body.decode("utf-8", errors="replace"))
        except (ValueError, TypeError):
            parsed_body = None
        env_error = _extract_error_envelope(parsed_body)
        if env_error:
            if idx:
                break
            return jsonify({"columns": [], "records_fetched": 0,
                            "error": f"API returned an error: {env_error}"})

        page_records = _extract_records_for_detection(body, response_format)
        if not page_records:
            break
        records.extend(page_records)

    if not records:
        return jsonify({
            "columns": [], "records_fetched": 0,
            "error": "No records found in the response — check the response format / records path.",
        })

    # Sample across both pages rather than the first 100 records overall,
    # so a page-2-only field still reaches the inference step.
    sample = records[:100] if len(page_params) == 1 else records[:50] + records[-50:]
    columns = infer_columns_from_records(sample)
    return jsonify({"columns": columns, "records_fetched": len(records)})


# ──────────────────────────────────────────────────────────────────────────────
# AI description suggestions
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/api/suggest-descriptions", methods=["POST"])
def api_suggest_descriptions():
    """Try multiple AI providers in order until one succeeds."""
    from flask import jsonify
    import json as _json
    import urllib.request
    import urllib.error

    data = request.get_json(force=True, silent=True) or {}
    columns = [str(c).strip() for c in data.get("columns", []) if str(c).strip()]
    table_context = str(data.get("table", "")).strip()
    describe_table = bool(data.get("describe_table", False))
    # API Job's Response Format card: an optional raw sample response body
    # (from Test Connection) the AI can inspect to suggest response_format
    # (type + records_path) — independent of columns/describe_table.
    sample_response = str(data.get("sample_response", "")).strip()[:3000]

    # Allow describe_table / sample_response even when no columns supplied yet
    if not columns and not describe_table and not sample_response:
        return jsonify({"suggestions": {}}), 200

    column_list = "\n".join(f"- {c}" for c in columns)
    table_prefix = ('The table is named "' + table_context + '". ') if table_context else ""

    if describe_table:
        col_hint = (" It contains these columns: " + ", ".join(columns) + ".") if columns else ""
        table_desc_prompt = (
            "You are a data engineer. "
            + table_prefix
            + "Write a single concise sentence (max 25 words) describing the business purpose "
            "of this table." + col_hint + " Respond with ONLY the sentence, no JSON, no markdown."
        )
    else:
        table_desc_prompt = None

    if columns:
        col_prompt = (
            "You are a data engineer. "
            + table_prefix
            + "For each column below, respond with a JSON object where each key is the column name "
            "and the value is an object with two fields: "
            '"description" (max 15 words, precise) and '
            '"dtype" (one of: text, integer, decimal, timestamp, boolean, date, json). '
            "Respond ONLY with the raw JSON object, no markdown.\n\n"
            "Columns:\n" + column_list + "\n\nJSON:"
        )
    else:
        col_prompt = None

    if sample_response:
        format_prompt = (
            "You are a data engineer analyzing a sample REST API response below. "
            "Determine how to extract the list of records from it. Respond with ONLY a raw "
            "JSON object (no markdown) with exactly two fields: "
            '"type" (one of: json_array, json_object_path, ndjson) and '
            '"records_path" (a dotted path such as "data.results" to the list of records inside '
            'the response object, only when type is json_object_path — otherwise an empty string).\n\n'
            "Sample response:\n" + sample_response + "\n\nJSON:"
        )
    else:
        format_prompt = None

    # Keep backward compat: single-prompt mode when only columns requested
    prompt = col_prompt or table_desc_prompt

    def _strip_fences(raw):
        if raw.startswith("```"):
            parts = raw.split("```")
            raw = parts[1]
            if raw.lower().startswith("json"):
                raw = raw[4:]
        return raw.strip()

    def _parse(raw):
        suggestions = _json.loads(_strip_fences(raw))
        cleaned = {}
        valid_dtypes = {'text', 'integer', 'decimal', 'timestamp', 'boolean', 'date', 'json'}
        for k, v in suggestions.items():
            if isinstance(v, dict):
                dtype = str(v.get('dtype', '')).lower().strip()
                cleaned[k] = {
                    'description': str(v.get('description', '')),
                    'dtype': dtype if dtype in valid_dtypes else ''
                }
            else:
                cleaned[k] = {'description': str(v), 'dtype': ''}
        return cleaned

    def _parse_format(raw):
        parsed = _json.loads(_strip_fences(raw))
        fmt_type = str(parsed.get("type", "")).lower().strip()
        valid_types = {"json_array", "json_object_path", "ndjson"}
        return {
            "type":         fmt_type if fmt_type in valid_types else "json_array",
            "records_path": str(parsed.get("records_path", "")).strip(),
        }

    def _post(url, payload_dict, headers):
        req = urllib.request.Request(
            url, data=_json.dumps(payload_dict).encode(),
            headers={"Content-Type": "application/json", **headers}, method="POST"
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            return _json.loads(r.read())

    def _chat_payload(model, p):
        return {"model": model, "messages": [{"role": "user", "content": p}], "temperature": 0.3, "max_tokens": 800}

    # ── Provider definitions (tried in order) ────────────────────────────────
    # Each requested piece (columns / table_description / response_format) is
    # attempted independently, per provider: a piece that fails (expired key,
    # malformed AI output, a transient network error, ...) never discards
    # another piece that DID succeed from that same provider, and moving to
    # the next provider only re-attempts whichever piece(s) are still
    # missing — never the whole request from scratch. One flaky/misconfigured
    # provider this way gets skipped over for just its failing piece(s)
    # instead of failing the suggestion outright.
    errors = []
    result = {}

    def _all_done():
        return (not col_prompt or "suggestions" in result) \
           and (not table_desc_prompt or "table_description" in result) \
           and (not format_prompt or "response_format" in result)

    def _try_provider(name, call):
        if col_prompt and "suggestions" not in result:
            try:
                result["suggestions"] = _parse(call(col_prompt))
            except Exception as e:
                errors.append(f"{name} (columns): {e}")
        if table_desc_prompt and "table_description" not in result:
            try:
                result["table_description"] = call(table_desc_prompt).strip()
            except Exception as e:
                errors.append(f"{name} (table description): {e}")
        if format_prompt and "response_format" not in result:
            try:
                result["response_format"] = _parse_format(call(format_prompt))
            except Exception as e:
                errors.append(f"{name} (response format): {e}")

    # 1. Google Gemini
    key = os.getenv("GOOGLE_AI_API_KEY")
    if key and not _all_done():
        def _gemini(p):
            res = _post(
                "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=" + key,
                {"contents": [{"parts": [{"text": p}]}], "generationConfig": {"temperature": 0.3, "maxOutputTokens": 800}},
                {}
            )
            return res["candidates"][0]["content"]["parts"][0]["text"]
        _try_provider("Gemini", _gemini)

    # 2. Groq
    key = os.getenv("GROQ_API_KEY")
    if key and not _all_done():
        def _groq(p):
            res = _post(
                "https://api.groq.com/openai/v1/chat/completions",
                _chat_payload("llama-3.3-70b-versatile", p),
                {"Authorization": "Bearer " + key}
            )
            return res["choices"][0]["message"]["content"]
        _try_provider("Groq", _groq)

    # 3. Mistral
    key = os.getenv("MISTRAL_API_KEY")
    if key and not _all_done():
        def _mistral(p):
            res = _post(
                "https://api.mistral.ai/v1/chat/completions",
                _chat_payload("mistral-small-latest", p),
                {"Authorization": "Bearer " + key}
            )
            return res["choices"][0]["message"]["content"]
        _try_provider("Mistral", _mistral)

    # 4. DeepSeek
    key = os.getenv("DEEPSEEK_API_KEY")
    if key and not _all_done():
        def _deepseek(p):
            res = _post(
                "https://api.deepseek.com/chat/completions",
                _chat_payload("deepseek-chat", p),
                {"Authorization": "Bearer " + key}
            )
            return res["choices"][0]["message"]["content"]
        _try_provider("DeepSeek", _deepseek)

    # 5. OpenRouter
    key = os.getenv("OPENROUTER_API_KEY")
    if key and not _all_done():
        def _openrouter(p):
            res = _post(
                "https://openrouter.ai/api/v1/chat/completions",
                {**_chat_payload("meta-llama/llama-3.3-70b-instruct", p)},
                {"Authorization": "Bearer " + key}
            )
            return res["choices"][0]["message"]["content"]
        _try_provider("OpenRouter", _openrouter)

    # 6. Cerebras
    key = os.getenv("CEREBRAS_API_KEY")
    if key and not _all_done():
        def _cerebras(p):
            res = _post(
                "https://api.cerebras.ai/v1/chat/completions",
                _chat_payload("llama-3.3-70b", p),
                {"Authorization": "Bearer " + key}
            )
            return res["choices"][0]["message"]["content"]
        _try_provider("Cerebras", _cerebras)

    # 7. SambaNova
    key = os.getenv("SAMBANOVA_API_KEY")
    if key and not _all_done():
        def _sambanova(p):
            res = _post(
                "https://api.sambanova.ai/v1/chat/completions",
                _chat_payload("Meta-Llama-3.3-70B-Instruct", p),
                {"Authorization": "Bearer " + key}
            )
            return res["choices"][0]["message"]["content"]
        _try_provider("SambaNova", _sambanova)

    if not result:
        if not errors:
            return jsonify({"error": "No AI API keys configured on the server."}), 400
        return jsonify({"error": "All providers failed: " + " | ".join(errors)}), 502
    # Partial success is still success: return whatever pieces were
    # obtained, even if one or more configured providers failed along the
    # way (their failures are simply not surfaced when something usable
    # came back — same as before, callers only ever checked for the
    # specific keys they asked for).
    return jsonify(result)


# ──────────────────────────────────────────────────────────────────────────────
# Hybrid pipeline — per-column cleaning SQL generation
# ──────────────────────────────────────────────────────────────────────────────

_RULE_ORDER = ["trim", "remove_special", "titlecase", "upper", "lower", "null_empty"]
_RULE_SQL   = {
    "trim":           "TRIM({expr})",
    "remove_special": "regexp_replace({expr}, '[^a-zA-Z0-9 ]', '', 'g')",
    "titlecase":      "INITCAP({expr})",
    "upper":          "UPPER({expr})",
    "lower":          "LOWER({expr})",
    "null_empty":     "NULLIF({expr}, '')",
}


def _col_clean_sql(col, rules, raw_table):
    """Return a single UPDATE statement for col given ordered rules, or None."""
    expr = col
    for rule in _RULE_ORDER:
        if rule in rules:
            expr = _RULE_SQL[rule].replace("{expr}", expr)
    return f"UPDATE {raw_table} SET {col} = {expr};" if expr != col else None


def build_hybrid_config_from_form(form, apply_prod_same_as_dev: bool, existing_config: dict = None):
    """Build a hybrid ETL config dict from hybrid_form.html POST.

    source_type is "file", "db", or "api" — API Job is a standalone job type
    from the user's perspective (own source-type choice in this same
    wizard) but shares this exact builder, since all three reuse
    hybrid_load.template's raw -> refined -> target code.

    `existing_config` is only needed for source_type "api" (when editing),
    to resolve a masked secret field the user left blank/unchanged — see
    secrets_crypto.resolve_secret().
    """
    def _getlist(key):
        if hasattr(form, "getlist"):
            return form.getlist(key)
        v = form.get(key)
        return (v if isinstance(v, list) else [v]) if v else []

    def _json_field(key, default):
        raw = form.get(key, "")
        try:
            return json.loads(raw) if raw else default
        except (ValueError, TypeError):
            return default

    source_type = form.get("source_type", "file")

    def file_src(prefix):
        return {
            "watch_path":             form.get(f"{prefix}_watch_path", "").strip() or DATA_DUMP_PATH + "/incoming",
            "file_name":              form.get(f"{prefix}_file_name", "").strip(),
            "file_format":            form.get(f"{prefix}_file_format", "csv"),
            "delimiter":              form.get(f"{prefix}_delimiter", ",") or ",",
            "encoding":               "auto",
            "has_header":             form.get(f"{prefix}_has_header") in ("on", "true", "1"),
            "skip_rows":              int(form.get(f"{prefix}_skip_rows") or 0),
            "null_values":            [v.strip() for v in (form.get(f"{prefix}_null_values") or "NULL,null,None,NA,N/A,#N/A").split(",") if v.strip()],
            "archive_path":           form.get(f"{prefix}_archive_path", "").strip() or DATA_DUMP_PATH + "/archive",
            "archive_retention_days": int(form.get(f"{prefix}_archive_retention_days") or 30),
            "business_date_column":   (form.get("business_date_column") or "").strip(),
        }

    def db_src(prefix):
        return {
            "source_db_conn_id":    form.get(f"{prefix}_source_db_conn_id", ""),
            "db_type":              form.get(f"{prefix}_source_db_type", "postgresql"),
            "sql_file":             None,
            "business_date_column": (form.get("business_date_column") or "").strip(),
        }

    filters = _json_field("filters_json", [])

    def existing_api_auth_for(prefix):
        if not existing_config:
            return {}
        env = "prod" if prefix == "prod" else "dev"
        return ((existing_config.get("source") or {}).get(env) or {}).get("auth", {}) or {}

    def existing_api_full_url_for(prefix):
        if not existing_config:
            return ""
        env = "prod" if prefix == "prod" else "dev"
        return ((existing_config.get("source") or {}).get(env) or {}).get("full_url_enc", "") or ""

    def api_auth_block(prefix):
        auth_type = form.get(f"{prefix}_auth_type", "none")
        existing  = existing_api_auth_for(prefix)
        block = {"type": auth_type}
        if auth_type == "api_key_header":
            block["header_name"] = form.get(f"{prefix}_auth_header_name", "").strip() or "Authorization"
            block["api_key_enc"] = secrets_crypto.resolve_secret(
                form.get(f"{prefix}_auth_api_key", ""), existing.get("api_key_enc", "")
            )
        elif auth_type == "api_key_query_param":
            block["param_name"]  = form.get(f"{prefix}_auth_param_name", "").strip() or "token"
            block["api_key_enc"] = secrets_crypto.resolve_secret(
                # Distinct field name from api_key_header's — both auth-type
                # panels are always present in the form (only one shown/
                # active at a time via CSS), so sharing one field name would
                # mean Werkzeug's form.get() silently reads whichever panel
                # comes first in the DOM instead of the one the user filled in.
                form.get(f"{prefix}_auth_api_key_param", ""), existing.get("api_key_enc", "")
            )
        elif auth_type == "login_token":
            block["token_endpoint"]          = form.get(f"{prefix}_auth_token_endpoint", "").strip()
            block["token_username_field"]    = form.get(f"{prefix}_auth_token_username_field", "").strip() or "username"
            block["token_username"]          = form.get(f"{prefix}_auth_token_username", "").strip()
            block["token_password_field"]    = form.get(f"{prefix}_auth_token_password_field", "").strip() or "password"
            block["token_password_enc"]      = secrets_crypto.resolve_secret(
                form.get(f"{prefix}_auth_token_password", ""), existing.get("token_password_enc", "")
            )
            block["token_request_body_type"] = form.get(f"{prefix}_auth_token_request_body_type", "json")
            block["token_response_field"]    = form.get(f"{prefix}_auth_token_response_field", "").strip() or "access_token"
            block["token_header_name"]       = form.get(f"{prefix}_auth_token_header_name", "").strip() or "Authorization"
            # `or`, not a form.get default — a submitted-but-empty field is
            # indistinguishable from "not present" via form.get(key, default)
            # since the key IS present with value "". Defaulting only on a
            # missing key would silently strip the "Bearer " prefix whenever
            # a user leaves this field blank.
            block["token_header_prefix"]     = form.get(f"{prefix}_auth_token_header_prefix") or "Bearer "
        return block

    def api_src(prefix):
        return {
            # "full": the whole request URL (query string, token/key embedded
            # and all — e.g. pasted straight out of Postman) is encrypted as
            # one secret rather than split into base_url + a separate auth
            # mechanism. "separate" (default) is the base_url + Auth Type
            # design below. See hybrid_load.template's fetch_api_pages().
            "url_mode":     form.get(f"{prefix}_url_mode", "separate"),
            "full_url_enc": secrets_crypto.resolve_secret(
                form.get(f"{prefix}_full_url", ""), existing_api_full_url_for(prefix)
            ),
            # Data Endpoint was folded into Base URL — base_url alone is the
            # complete request URL now. Kept as a (permanently empty) key so
            # hybrid_load.template's `base_url + data_endpoint` concatenation
            # still works unchanged for older configs that do have one set.
            "base_url":        (form.get(f"{prefix}_base_url") or "").strip(),
            "data_endpoint":   "",
            "http_method":     form.get(f"{prefix}_http_method", "GET"),
            "request_params":  _json_field(f"{prefix}_request_params_json", {}),
            "request_headers": _json_field(f"{prefix}_request_headers_json", {}),
            "timeout_seconds": int(form.get(f"{prefix}_timeout_seconds") or 30),
            "auth":            api_auth_block(prefix),
            "response_format": {
                "type":         form.get(f"{prefix}_response_format_type", "json_array"),
                "records_path": form.get(f"{prefix}_response_format_records_path", "").strip(),
            },
            "pagination": {
                "strategy":                 form.get(f"{prefix}_pagination_strategy", "none"),
                "page_size":                int(form.get(f"{prefix}_pagination_page_size") or 100),
                "offset_param":             form.get(f"{prefix}_pagination_offset_param", "offset"),
                "limit_param":              form.get(f"{prefix}_pagination_limit_param", "limit"),
                "page_param":               form.get(f"{prefix}_pagination_page_param", "page"),
                "page_size_param":          form.get(f"{prefix}_pagination_page_size_param", "page_size"),
                "start_page":               int(form.get(f"{prefix}_pagination_start_page") or 1),
                "cursor_param":             form.get(f"{prefix}_pagination_cursor_param", "cursor"),
                "cursor_response_field":    form.get(f"{prefix}_pagination_cursor_response_field", "").strip(),
                "next_link_response_field": form.get(f"{prefix}_pagination_next_link_response_field", "").strip() or "next",
                "max_pages":                int(form.get(f"{prefix}_pagination_max_pages") or 500),
                # A checkbox's key is entirely ABSENT from the POST when
                # unchecked (not present-with-empty-value), so a default
                # applied via form.get(key, default) only ever fires when the
                # field is missing from the form altogether — correctly
                # covers "unchecked" here.
                "stop_when_empty":          form.get(f"{prefix}_pagination_stop_when_empty") == "on",
            },
            "filters": filters,
            "hwm": {
                "column":          form.get(f"{prefix}_hwm_column", "").strip(),
                "column_datatype": form.get(f"{prefix}_hwm_column_datatype", ""),
                "date_format":     form.get(f"{prefix}_hwm_date_format", ""),
                "request_param":   form.get(f"{prefix}_hwm_request_param", "").strip(),
            },
            "business_date_column": (form.get("business_date_column") or "").strip(),
        }

    def tgt(prefix):
        return {
            "target_db_conn_id": form.get(f"{prefix}_target_db_conn_id", ""),
            "db_type":           form.get(f"{prefix}_target_db_type", "postgresql"),
            "target_schema":     (form.get(f"{prefix}_target_schema") or "").strip().lower(),
            "target_table":      (form.get(f"{prefix}_target_table") or "").strip().lower(),
        }

    src_fn  = {"file": file_src, "db": db_src, "api": api_src}.get(source_type, file_src)
    dev_src = src_fn("dev")
    dev_tgt = tgt("dev")

    if source_type == "api":
        # Unlike Target, the wizard never renders a prod-specific set of
        # Connection/Response-Format/Pagination/Filters/HWM fields for API —
        # in practice the same external API is used regardless of
        # environment, only the target warehouse legitimately differs. Always
        # mirror dev -> prod here regardless of apply_prod_same_as_dev (which
        # only ever governs Target for an API-sourced job), avoiding a
        # silently-broken prod source if that checkbox is ever unchecked.
        prod_src = dict(dev_src)
    elif apply_prod_same_as_dev:
        prod_src = dict(dev_src)
    else:
        prod_src = src_fn("prod")

    if apply_prod_same_as_dev:
        prod_tgt = dict(dev_tgt)
    else:
        prod_tgt = tgt("prod")

    # Raw/refined location is fixed, not user-configurable (see
    # dags/templates/hybrid_load.template) — raw always lives in schema
    # `raw`, refined always lives in the shared schema `pipeline` (both
    # table-name-prefixed with the target schema so different targets
    # never collide). Column-cleaning-rule SQL runs against refined (where
    # cleaning now happens), never against raw (which must stay an
    # untouched, permanent copy of exactly what was ingested).
    target_schema      = dev_tgt.get("target_schema", "").strip()
    target_table       = dev_tgt.get("target_table", "").strip()
    refined_table_ref  = (
        f"pipeline.{target_schema}_{target_table}_refined"
        if target_schema and target_table
        else "refined_table"
    )

    # Parse per-column actions JSON written by the browser
    try:
        col_actions = json.loads(form.get("col_actions_json") or "[]")
    except (ValueError, TypeError):
        col_actions = []

    sql_queries          = []
    dtype_overrides      = {}
    dtype_override_formats = {}

    for ca in col_actions:
        col = (ca.get("name") or "").strip()
        if not col or ca.get("exclude"):
            continue
        col_type = (ca.get("type") or "").strip()
        if col_type and col_type != "text":
            dtype_overrides[col] = col_type
        fmt = (ca.get("format") or "").strip()
        if fmt and col_type in ("date", "timestamp", "datetime"):
            dtype_override_formats[col] = fmt
        stmt = _col_clean_sql(col, ca.get("rules") or [], refined_table_ref)
        if stmt:
            sql_queries.append(stmt)

    for stmt in _getlist("manual_sql_query[]"):
        stmt = (stmt or "").strip()
        if stmt:
            sql_queries.append(stmt)

    enable_cleaning = form.get("enable_cleaning") == "on"
    cleaning = None
    if enable_cleaning:
        cleaning = {}
        if sql_queries:
            cleaning["sql_queries"] = sql_queries
        script_path = (form.get("cleaning_script_path") or "").strip()
        functions   = [f.strip() for f in _getlist("cleaning_function[]") if f.strip()]
        if script_path:
            cleaning["script_path"] = script_path
        if functions:
            cleaning["functions"] = functions
        if not cleaning:
            cleaning = None  # nothing configured — treat as pass-through

    raw_tags   = form.get("extra_tags", "")
    extra_tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
    created_by = form.get("created_by", "")
    tags = list(dict.fromkeys([created_by] + extra_tags)) if created_by else extra_tags

    config = {
        "pipeline_type":     "hybrid",
        "source_type":       source_type,
        "schedule_interval": _norm_schedule(form.get("schedule_interval")),
        "start_date":        form.get("start_date") or "2023-01-01",
        "tags":              tags,
        "source":            {"dev": dev_src, "prod": prod_src},
        "target":            {"dev": dev_tgt, "prod": prod_tgt},
    }

    # Only write non-default overrides to keep configs lean
    if form.get("create_dw_table_if_not_exists") != "on":
        config["create_dw_table_if_not_exists"] = False
    if cleaning:
        config["cleaning"] = cleaning

    if dtype_overrides:
        config["source"]["dev"]["dtype_overrides"] = dtype_overrides
        config["source"]["prod"]["dtype_overrides"] = dtype_overrides
    if dtype_override_formats:
        config["source"]["dev"]["dtype_override_formats"] = dtype_override_formats
        config["source"]["prod"]["dtype_override_formats"] = dtype_override_formats

    # Build data_dictionary from col_actions so edit prefilling can restore all columns + types.
    dd_from_actions = {
        ca["name"]: {"dtype": ca.get("type") or "text"}
        for ca in col_actions
        if (ca.get("name") or "").strip() and not ca.get("exclude")
    }
    dd = _parse_data_dictionary(form)
    if dd:
        dd = {c: v for c, v in dd.items() if c not in PIPELINE_META_COLS}
    # Merge: col_actions is authoritative for dtype; form dd adds description etc.
    merged_dd = {**dd_from_actions, **{c: {**dd_from_actions.get(c, {}), **v} for c, v in (dd or {}).items()}}
    if merged_dd:
        config["data_dictionary"] = {c: v for c, v in merged_dd.items() if c not in PIPELINE_META_COLS}

    table_desc = (form.get("table_description") or "").strip()
    if table_desc:
        config["table_description"] = table_desc

    bare_name     = (form.get("dag_id") or "").strip()
    config["dag_id"] = naming.generate_dag_id(config, bare_name)
    return config


# API's connection fields are nested one level deeper than file/db's
# (auth.type, response_format.type, pagination.strategy, hwm.column) — Jinja's
# default Undefined raises as soon as you chain an attribute PAST an
# already-missing key (config.source.dev.auth.type is two levels past
# "source"), even though referencing a single missing key directly
# (config.source.dev.file_name) safely renders as empty. File/db configs
# never have an "auth"/"response_format"/"pagination"/"hwm" key at all, so
# hybrid_form.html's API panel would 500 when editing a file/db-sourced job
# unless these are always present (even if empty) before rendering.
_API_SRC_DEFAULTS = {
    "base_url": "", "data_endpoint": "", "http_method": "GET",
    "request_params": {}, "request_headers": {}, "timeout_seconds": 30,
    "auth": {"type": "none"},
    "response_format": {"type": "json_array", "records_path": ""},
    "pagination": {
        "strategy": "none", "page_size": 100, "offset_param": "offset", "limit_param": "limit",
        "page_param": "page", "page_size_param": "page_size", "start_page": 1,
        "cursor_param": "cursor", "cursor_response_field": "", "next_link_response_field": "next",
        "max_pages": 500, "stop_when_empty": True,
    },
    "filters": [],
    "hwm": {"column": "", "column_datatype": "", "date_format": "", "request_param": ""},
}


def _with_api_render_defaults(config):
    """Render-only copy of `config` with source.dev/source.prod backfilled to
    always carry the full API-field shape — never mutates what's persisted;
    only used right before handing a config to hybrid_form.html. See
    _API_SRC_DEFAULTS for why this is needed."""
    if not config:
        return config
    cfg = copy.deepcopy(config)
    for env in ("dev", "prod"):
        env_src = cfg.setdefault("source", {}).setdefault(env, {})
        for k, v in _API_SRC_DEFAULTS.items():
            env_src.setdefault(k, copy.deepcopy(v))
    return cfg


# ──────────────────────────────────────────────────────────────────────────────
# Hybrid pipeline routes
# ──────────────────────────────────────────────────────────────────────────────

def _hybrid_render_ctx(config, user, extra_tags_str="", bare_dag_id="", save_error=None, form_data=None, col_actions_json="[]"):
    """Shared context dict for hybrid_form.html renders."""
    dd = {}
    if config:
        dd = {k: v for k, v in (config.get("data_dictionary") or {}).items() if k not in PIPELINE_META_COLS}
    existing_sql = (config.get("cleaning") or {}).get("sql_queries", []) if config else []
    return dict(
        config=_with_api_render_defaults(config),
        connections=airflow_connections(),
        schedule_suggestions=SCHEDULE_SUGGESTIONS,
        user=user,
        current_username=user[1] if user else "",
        existing_tags=(config or {}).get("tags", []),
        extra_tags_str=extra_tags_str,
        active_env=ACTIVE_ENV,
        save_error=save_error,
        form_data=form_data,
        bare_dag_id=bare_dag_id,
        data_dictionary_json=json.dumps(dd),
        table_description_val=(config or {}).get("table_description") or "",
        incoming_dir=DATA_DUMP_PATH + "/incoming",
        archive_dir=DATA_DUMP_PATH + "/archive",
        col_actions_json=col_actions_json,
        existing_sql_queries=existing_sql,
        secret_mask=secrets_crypto.MASK,
    )


@app.route("/hybrid-jobs")
def hybrid_jobs():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    # Hybrid Jobs is the one job-creation surface — File, DB, and API are all
    # source_type choices within it (api-sourced configs included here, not
    # split into a separate page — see _is_api_job for how a row is told
    # apart in the UI).
    configs = [c for c in load_config_files(all_types=True) if c.get("pipeline_type") == "hybrid"]
    return render_template("jobs.html", jobs=configs, user=current_user(), pipeline_type="hybrid")


@app.route("/hybrid-jobs/new", methods=["GET", "POST"])
def new_hybrid_job():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    _user = current_user()
    if request.method == "POST":
        try:
            apply_same = request.form.get("apply_prod_same_as_dev") == "on"
            config = build_hybrid_config_from_form(request.form, apply_same)
            if not config.get("dag_id"):
                raise ValueError("DAG ID is required.")
            save_config(config)
            return redirect(url_for("hybrid_jobs"))
        except Exception as exc:
            app.logger.exception("Error saving hybrid job")
            ctx = _hybrid_render_ctx(
                None, _user,
                save_error=str(exc),
                form_data=request.form,
                col_actions_json=request.form.get("col_actions_json", "[]"),
            )
            return render_template("hybrid_form.html", **ctx), 422
    return render_template("hybrid_form.html", **_hybrid_render_ctx(None, _user))


@app.route("/hybrid-jobs/<dag_id>/edit", methods=["GET", "POST"])
def edit_hybrid_job(dag_id):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    config = load_config(dag_id)
    if not config or config.get("pipeline_type") != "hybrid":
        return redirect(url_for("hybrid_jobs"))
    _user         = current_user()
    existing_tags = config.get("tags", [])
    extra_tags    = existing_tags[1:] if len(existing_tags) > 1 else []
    # An api-sourced job's dag_id carries the "api_" prefix (see naming.py),
    # not "hybrid_" — _bare_dag_id strips whichever applies, plus the conn_id.
    bare_dag_id = _bare_dag_id(config)

    # Pre-build col_actions_json from data_dictionary for editing.
    # If data_dictionary is empty (configs saved before the fix), fall back to
    # reading column names from the target DB table so the editor still shows columns.
    dd = {k: v for k, v in (config.get("data_dictionary") or {}).items() if k not in PIPELINE_META_COLS}
    _dev_src  = config.get("source", {}).get("dev", {})
    _dev_ovrd = _dev_src.get("dtype_overrides", {})
    _dt_fmts  = _dev_src.get("dtype_override_formats", {})

    if not dd and AIRFLOW_DB:
        try:
            tgt_dev    = config.get("target", {}).get("dev", {})
            tgt_conn   = tgt_dev.get("target_db_conn_id")
            tgt_schema = tgt_dev.get("target_schema") or tgt_dev.get("target_table_schema")
            tgt_table  = tgt_dev.get("target_table") or tgt_dev.get("target_table_name")
            if tgt_conn and tgt_table:
                from sqlalchemy import inspect as _sa_inspect
                _tgt_eng = _engine_for_conn_id(tgt_conn)
                _insp    = _sa_inspect(_tgt_eng)
                for col_info in _insp.get_columns(tgt_table, schema=tgt_schema):
                    col_name = col_info["name"]
                    if col_name not in PIPELINE_META_COLS:
                        dd[col_name] = {"dtype": _dev_ovrd.get(col_name, "text")}
        except Exception as _e:
            app.logger.warning("edit prefill: could not read target table columns: %s", _e)

    prefill_actions = json.dumps([
        {
            "name": col, "original": col,
            "type": info.get("dtype", "text"),
            "format": _dt_fmts.get(col, ""),
            "rules": [], "rename": "", "exclude": False,
        }
        for col, info in dd.items()
    ])

    if request.method == "POST":
        try:
            apply_same = request.form.get("apply_prod_same_as_dev") == "on"
            new_config = build_hybrid_config_from_form(request.form, apply_same, existing_config=config)
            if new_config["dag_id"] != dag_id:
                delete_config(dag_id)
                delete_dag_files(dag_id)
            save_config(new_config, existing_config=config)
            return redirect(url_for("hybrid_jobs"))
        except Exception as exc:
            app.logger.exception("Error saving hybrid job")
            ctx = _hybrid_render_ctx(
                config, _user,
                extra_tags_str=", ".join(extra_tags),
                bare_dag_id=bare_dag_id,
                save_error=str(exc),
                form_data=request.form,
                col_actions_json=request.form.get("col_actions_json", prefill_actions),
            )
            return render_template("hybrid_form.html", **ctx), 422

    ctx = _hybrid_render_ctx(
        config, _user,
        extra_tags_str=", ".join(extra_tags),
        bare_dag_id=bare_dag_id,
        col_actions_json=prefill_actions,
    )
    return render_template("hybrid_form.html", **ctx)


@app.route("/hybrid-jobs/<dag_id>/delete", methods=["POST"])
def delete_hybrid_job(dag_id):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    delete_config(dag_id)
    delete_dag_files(dag_id)
    return redirect(url_for("hybrid_jobs"))


# ──────────────────────────────────────────────────────────────────────────────
# API Job — a source_type choice ("file" | "db" | "api") within the Hybrid Job
# wizard/routes above, not a separate wizard. Still a standalone job type from
# the user's own perspective (its own "api_" dag_id prefix), just implemented
# entirely through build_hybrid_config_from_form / hybrid_form.html / the
# /hybrid-jobs* routes.
# ──────────────────────────────────────────────────────────────────────────────

def _is_api_job(config: dict) -> bool:
    """An API job is stored as pipeline_type "hybrid" + source_type "api" —
    the "API" choice in the same Hybrid Job wizard as File/DB, distinguished
    from a real file/db hybrid job (also pipeline_type "hybrid") only by
    source_type. Used wherever a row/config needs to know which of the
    three it is (dag_id prefix, masked-secret prefill, review display)."""
    return (config or {}).get("pipeline_type") == "hybrid" and (config or {}).get("source_type") == "api"


with app.app_context():
    init_webui_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5001")), debug=True)
