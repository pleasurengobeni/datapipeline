"""
dag_builder — Airflow DAG
Runs every 5 minutes inside the Airflow worker.
Reads every config JSON in dags/config/, picks the right template,
and writes the generated .py DAG file into dags/etl/<conn_id>/.

No extra container needed.
"""
import json
import logging
import os
import re
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
try:
    from modules import naming
except ImportError:
    from dags.modules import naming
from airflow.utils.task_group import TaskGroup

# ── Paths ─────────────────────────────────────────────────────────────────────
# File lives at  dags/custom_dags/dag_builder.py
# ROOT  →        repo root  (custom_dags → dags → root)
ROOT       = Path(__file__).resolve().parent.parent.parent
CONFIG_DIR = Path(os.getenv("CONFIG_DIR", str(ROOT / "dags" / "config")))
ETL_DIR    = Path(os.getenv("ETL_DIR",    str(ROOT / "dags" / "etl")))
TMPL_DIR   = ROOT / "dags" / "templates"

TEMPLATES = {
    "db_pandas":     TMPL_DIR / "data_load.template",
    "db_spark":      TMPL_DIR / "spark_load.template",
    "file_pandas":   TMPL_DIR / "file_load.template",
    "file_spark":    TMPL_DIR / "spark_file_load.template",
    # API jobs are pipeline_type "hybrid" + source_type "api" — a standalone
    # job type from the user's perspective, sharing this same template.
    "hybrid_pandas": TMPL_DIR / "hybrid_load.template",
}

log = logging.getLogger("dag_builder")


def _load_all_configs():
    configs = []
    for path in CONFIG_DIR.rglob("*.json"):
        try:
            configs.append(json.loads(path.read_text()))
        except Exception as exc:
            log.warning("Could not parse %s: %s", path, exc)
    return configs


def _generate_one(config):
    dag_id = config.get("dag_id")
    if not dag_id:
        log.warning("Config file missing dag_id, skipping: %s", config)
        return False

    is_spark  = bool(config.get("spark"))
    pipe_type = config.get("pipeline_type")
    if pipe_type == "hybrid":
        tmpl = TEMPLATES["hybrid_pandas"]
    elif pipe_type == "file_based":
        tmpl = TEMPLATES["file_spark"] if is_spark else TEMPLATES["file_pandas"]
    else:
        tmpl = TEMPLATES["db_spark"] if is_spark else TEMPLATES["db_pandas"]

    if not tmpl.exists():
        log.warning("Template missing: %s — skipping %s", tmpl, config.get("dag_id"))
        return False

    out = naming.get_dag_py_path(dag_id, config, ETL_DIR)
    out.parent.mkdir(parents=True, exist_ok=True)
    content = tmpl.read_text().replace("<dag_name>", dag_id)
    out.write_text(content)
    log.info("Generated %s  <- %s", str(out.relative_to(ROOT)), tmpl.name)
    return True


@dag(
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["system", "dag_builder"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Regenerates ETL DAG .py files from templates + config JSONs",
)
def dag_builder():
    configs = _load_all_configs()
    groups = {}
    for cfg in configs:
        dag_id = cfg.get("dag_id")
        if not dag_id: continue
        conn_id = naming.clean_id(cfg.get("target", {}).get("dev", {}).get("target_db_conn_id", "default"))
        groups.setdefault(conn_id, []).append(cfg)

    for conn_id, cfgs in groups.items():
        with TaskGroup(group_id=f"conn_{conn_id}") as tg:
            for cfg in cfgs:
                task_id = f"build_{naming.clean_id(cfg['dag_id'])}"
                task(task_id=task_id)(_generate_one)(cfg)
        tg

dag = dag_builder()
