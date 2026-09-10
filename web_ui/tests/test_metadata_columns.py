"""
Integration test: verify that all 5 pipeline metadata columns exist on every
DB-pipeline target table registered in pipeline.etl_metrics.

Run with:
    docker compose exec web-ui python3 -m pytest /app/web_ui/tests/test_metadata_columns.py -v
"""
import os

import pytest
from sqlalchemy import create_engine, text

DB_URL = os.getenv("CENFRI_DB_URL")
if not DB_URL:
    pytest.skip(
        "CENFRI_DB_URL env var not set — skipping integration test",
        allow_module_level=True,
    )

METADATA_COLS = [
    "_pipeline_inserted_at",
    "_pipeline_run_id",
    "_source_system",
    "_loaded_at",
    "_row_checksum",
]


@pytest.fixture(scope="module")
def engine():
    eng = create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})
    yield eng
    eng.dispose()


@pytest.fixture(scope="module")
def db_pipeline_tables(engine):
    """Return list of (schema, table_name) for all DB-pipeline targets."""
    sql = text("""
        SELECT DISTINCT
            COALESCE(NULLIF(schema, ''), split_part(dag_id, '_', 2)) AS tgt_schema,
            table_name
        FROM pipeline.etl_metrics
        WHERE source_type = 'db'
        ORDER BY 1, 2
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return [(r[0], r[1]) for r in rows]


def test_at_least_one_db_pipeline_table_found(db_pipeline_tables):
    """Sanity check: etl_metrics must contain at least one DB pipeline entry."""
    assert len(db_pipeline_tables) > 0, (
        "No DB-pipeline tables found in pipeline.etl_metrics — "
        "run at least one DB pipeline first."
    )


@pytest.mark.parametrize("meta_col", METADATA_COLS)
def test_metadata_column_present_on_all_tables(engine, db_pipeline_tables, meta_col):
    """Each metadata column must exist on every DB-pipeline target table."""
    missing_from = []

    col_sql = text("""
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name   = :table
          AND column_name  = :col
    """)

    for tgt_schema, table_name in db_pipeline_tables:
        with engine.connect() as conn:
            row = conn.execute(col_sql, {
                "schema": tgt_schema,
                "table":  table_name,
                "col":    meta_col,
            }).fetchone()
        if row is None:
            missing_from.append(f"{tgt_schema}.{table_name}")

    assert not missing_from, (
        f"Column '{meta_col}' is MISSING from: {', '.join(missing_from)}\n"
        "Fix: re-run the pipeline — create_target now calls ALTER TABLE ADD COLUMN IF NOT EXISTS."
    )


def test_metadata_columns_report(engine, db_pipeline_tables, capsys):
    """Print a human-readable report of column presence per table."""
    col_sql = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name   = :table
          AND column_name  = ANY(:cols)
    """)

    print(f"\n{'Table':<45} {'Present columns'}")
    print("-" * 80)
    all_pass = True
    for tgt_schema, table_name in db_pipeline_tables:
        with engine.connect() as conn:
            found = {
                r[0] for r in conn.execute(col_sql, {
                    "schema": tgt_schema,
                    "table":  table_name,
                    "cols":   METADATA_COLS,
                })
            }
        missing = set(METADATA_COLS) - found
        label = f"{tgt_schema}.{table_name}"
        if missing:
            all_pass = False
            print(f"  FAIL  {label:<40}  missing: {sorted(missing)}")
        else:
            print(f"  PASS  {label:<40}  all 5 columns present")

    print("-" * 80)
    print("Overall:", "ALL PASS" if all_pass else "FAILURES DETECTED")
    # Always passes — just for the report
    assert True
