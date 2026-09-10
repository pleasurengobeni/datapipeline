#!/usr/bin/env python3
"""
recreate_etl_dags.py
--------------------
Regenerate all Airflow DAG .py files from the current templates.

Run this after:
  - Modifying a template file (data_load.template, spark_load.template, etc.)
  - Adding new config JSON files outside the web UI
  - Recovering from a lost dags/etl/ tree

Usage:
    python scripts/recreate_etl_dags.py [--dry-run]

Options:
    --dry-run   Print what would be written without writing anything.
"""

import argparse
import json
import sys
from pathlib import Path

# Allow importing dags.modules without installing the package
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from dags.modules import naming

CONFIG_DIR            = REPO_ROOT / "dags" / "config"
ETL_DIR               = REPO_ROOT / "dags" / "etl"
TEMPLATE_FILE         = REPO_ROOT / "dags" / "templates" / "data_load.template"
SPARK_TEMPLATE_FILE   = REPO_ROOT / "dags" / "templates" / "spark_load.template"
FILE_TEMPLATE_FILE    = REPO_ROOT / "dags" / "templates" / "file_load.template"
SPARK_FILE_TEMPLATE_FILE = REPO_ROOT / "dags" / "templates" / "spark_file_load.template"
HYBRID_TEMPLATE_FILE  = REPO_ROOT / "dags" / "templates" / "hybrid_load.template"


def _pick_template(config: dict) -> Path:
    is_spark  = bool(config.get("spark"))
    pipe_type = config.get("pipeline_type")
    if pipe_type == "hybrid":
        return HYBRID_TEMPLATE_FILE
    if pipe_type == "file_based":
        return SPARK_FILE_TEMPLATE_FILE if is_spark else FILE_TEMPLATE_FILE
    return SPARK_TEMPLATE_FILE if is_spark else TEMPLATE_FILE


def _generate(config: dict, dry_run: bool) -> str:
    """Generate (or preview) the DAG .py for one config. Returns the output path."""
    tmpl_file = _pick_template(config)
    if not tmpl_file.exists():
        raise FileNotFoundError(f"Template not found: {tmpl_file}")

    dag_id   = config["dag_id"]
    out_path = naming.get_dag_py_path(dag_id, config, ETL_DIR)

    if dry_run:
        return str(out_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    content = tmpl_file.read_text()
    content = content.replace("<dag_name>", dag_id)
    out_path.write_text(content)
    return str(out_path)


def main():
    parser = argparse.ArgumentParser(description="Recreate all ETL DAG .py files from templates.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be generated without writing files.")
    args = parser.parse_args()

    config_files = list(CONFIG_DIR.rglob("*.json"))
    if not config_files:
        print(f"No config files found under {CONFIG_DIR}")
        sys.exit(1)

    ok = 0
    errors = 0

    for cfg_path in sorted(config_files):
        try:
            with cfg_path.open() as fh:
                config = json.load(fh)
        except Exception as exc:
            print(f"  [ERROR] Cannot parse {cfg_path.name}: {exc}")
            errors += 1
            continue

        dag_id = config.get("dag_id")
        if not dag_id:
            print(f"  [SKIP]  {cfg_path.name} — no dag_id")
            continue

        try:
            out = _generate(config, args.dry_run)
            verb = "Would write" if args.dry_run else "Written"
            print(f"  [OK]    {dag_id:55s}  →  {out}")
            ok += 1
        except Exception as exc:
            print(f"  [ERROR] {dag_id}: {exc}")
            errors += 1

    print()
    print(f"{'Dry run' if args.dry_run else 'Done'}: {ok} DAG(s) generated, {errors} error(s).")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
