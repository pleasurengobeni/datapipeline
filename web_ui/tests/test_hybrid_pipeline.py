"""
Tests for hybrid_load.template — the three-stage raw → refined → DW pipeline.

Covers:
  1. Helper functions extracted from the template:
       _clean_col, _universal_to_datetime, _cast_types, _add_meta,
       _find_file, _compute_md5, _ensure_table_from_df
  2. load_to_raw   — reads a CSV file, adds metadata, truncates-then-loads into raw table
  3. refine_data   — copies raw → refined, optionally running SQL cleaning queries
  4. load_to_dw    — reads refined, dedup-inserts into DW target; reruns are idempotent
  5. Cleanup logic — truncate_raw and truncate_refined leave tables empty
  6. End-to-end    — all three stages in sequence with and without cleaning config

All tests use SQLite (via SQLAlchemy) and temporary CSV files — no Airflow,
no PostgreSQL required.

Run with:  pytest web_ui/tests/test_hybrid_pipeline.py -v
"""
from __future__ import annotations

import csv
import hashlib
import os
import re
import tempfile
import textwrap
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect, text

# ─────────────────────────────────────────────────────────────────────────────
# Load helpers from the template
# ─────────────────────────────────────────────────────────────────────────────

TPL_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "dags" / "templates" / "hybrid_load.template"
)

EXTRACT_NAMES = [
    "_clean_col",
    "_detect_encoding",
    "_universal_to_datetime",
    "_read_file",
    "_cast_types",
    "_add_meta",
    "_find_file",
    "_compute_md5",
    "_PIPELINE_META",
]

PREAMBLE = textwrap.dedent("""
    import hashlib
    import logging
    import os
    import re
    import unicodedata
    from datetime import datetime
    from pathlib import Path
    import pandas as pd
""").strip()


def _extract_fn(src: str, name: str) -> str:
    """Extract a top-level 'def name' or 'name = ...' block from template source."""
    pattern = rf"^(def {name}\b.*?)(?=\ndef |\Z)"
    m = re.search(pattern, src, re.DOTALL | re.MULTILINE)
    if m:
        return m.group(1).rstrip()
    pattern2 = rf"^({name}\s*=.*?)(?=\n[A-Z_a-z]|\Z)"
    m2 = re.search(pattern2, src, re.DOTALL | re.MULTILINE)
    return m2.group(1).rstrip() if m2 else ""


@pytest.fixture(scope="module")
def helpers() -> dict:
    """Compile and exec all required helpers from hybrid_load.template once per module."""
    source = TPL_PATH.read_text()
    snippets = "\n\n".join(_extract_fn(source, n) for n in EXTRACT_NAMES)
    ns: dict = {}
    exec(compile(PREAMBLE + "\n\n" + snippets, str(TPL_PATH), "exec"), ns)  # noqa: S102
    return ns


# ─────────────────────────────────────────────────────────────────────────────
# SQLite engine fixture (in-memory, shared per test)
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def engine():
    """Fresh in-memory SQLite engine for each test."""
    eng = create_engine("sqlite:///:memory:", echo=False)
    yield eng
    eng.dispose()


# ─────────────────────────────────────────────────────────────────────────────
# 1. _clean_col
# ─────────────────────────────────────────────────────────────────────────────

class TestCleanCol:
    @pytest.mark.parametrize("raw,expected", [
        ("First Name",  "first_name"),
        ("  ID  ",      "id"),
        ("Café",        "cafe"),
        ("from",        "from_"),        # SQL reserved word
        ("select",      "select_"),
        ("123col",      "123col"),        # leading digit kept
        ("col!@#name",  "col_name"),
        ("",            "col"),           # empty → fallback
        ("A B C",       "a_b_c"),
    ])
    def test_clean_col(self, helpers, raw, expected):
        assert helpers["_clean_col"](raw) == expected


# ─────────────────────────────────────────────────────────────────────────────
# 2. _universal_to_datetime
# ─────────────────────────────────────────────────────────────────────────────

class TestUniversalToDatetime:
    @pytest.mark.parametrize("value,hint,expect_nat,expect_raise", [
        ("2024-03-15",              "YYYY-MM-DD",      False, False),
        ("15/03/2024",             "DD/MM/YYYY",      False, False),
        ("03/15/2024",             "MM/DD/YYYY",      False, False),
        ("2024-03-15 10:30:00",    None,              False, False),
        ("2024-03-15T10:30:00Z",   None,              False, False),
        (1704067200,               None,              False, False),  # epoch seconds
        ("",                       None,              True,  False),
        (None,                     None,              True,  False),
        ("not-a-date",             "YYYY-MM-DD",      False, True),
    ])
    def test_parse(self, helpers, value, hint, expect_nat, expect_raise):
        fn = helpers["_universal_to_datetime"]
        s  = pd.Series([value])
        if expect_raise:
            with pytest.raises(ValueError, match="Cannot parse"):
                fn(s, hint_fmt=hint)
            return
        result = fn(s, hint_fmt=hint)
        if expect_nat:
            assert pd.isna(result.iloc[0])
        else:
            assert not pd.isna(result.iloc[0])
            assert pd.api.types.is_datetime64_any_dtype(result)

    def test_dayfirst_31st(self, helpers):
        fn = helpers["_universal_to_datetime"]
        result = fn(pd.Series(["31/01/2024"]), hint_fmt="DD/MM/YYYY")
        assert result.iloc[0].month == 1
        assert result.iloc[0].day == 31


# ─────────────────────────────────────────────────────────────────────────────
# 3. _add_meta
# ─────────────────────────────────────────────────────────────────────────────

class TestAddMeta:
    def test_meta_columns_prepended(self, helpers):
        add_meta = helpers["_add_meta"]
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        result = add_meta(df.copy(), run_id="run-001", file_name="test.csv", checksum="abc123")
        assert "_pipeline_run_id" in result.columns
        assert "_pipeline_inserted_at" in result.columns
        assert "_source_file" in result.columns
        assert "_file_checksum" in result.columns
        assert "_loaded_at" in result.columns
        assert "_row_checksum" in result.columns
        assert (result["_pipeline_run_id"] == "run-001").all()
        assert (result["_source_file"] == "test.csv").all()
        assert (result["_file_checksum"] == "abc123").all()

    def test_row_checksum_unique_for_different_rows(self, helpers):
        add_meta = helpers["_add_meta"]
        df = pd.DataFrame({"val": ["x", "y", "z"]})
        result = add_meta(df.copy(), run_id="r1")
        assert result["_row_checksum"].nunique() == 3

    def test_row_checksum_identical_for_same_row(self, helpers):
        add_meta = helpers["_add_meta"]
        df = pd.DataFrame({"val": ["same", "same"]})
        result = add_meta(df.copy(), run_id="r1")
        assert result["_row_checksum"].nunique() == 1

    def test_existing_meta_cols_stripped_before_readd(self, helpers):
        add_meta = helpers["_add_meta"]
        df = pd.DataFrame({"name": ["Alice"], "_pipeline_run_id": ["old-run"]})
        result = add_meta(df.copy(), run_id="new-run")
        assert (result["_pipeline_run_id"] == "new-run").all()

    def test_none_file_name_allowed(self, helpers):
        add_meta = helpers["_add_meta"]
        df = pd.DataFrame({"col": [1]})
        result = add_meta(df.copy(), run_id="r1", file_name=None)
        assert result["_source_file"].iloc[0] is None

    def test_source_column_named_pipeline_inserted_at_cannot_hijack_watermark(self, helpers):
        """A source that legitimately carries a _pipeline_inserted_at column —
        most realistically a DB job selecting from a table THIS pipeline
        produced — must NOT have its upstream timestamps become this
        pipeline's watermark. If it did, any of its rows older than what is
        already in target would be skipped by refine forever. Ingest never
        opts into preservation, so the value is always re-stamped."""
        add_meta = helpers["_add_meta"]
        df = pd.DataFrame({"name": ["Alice"], "_pipeline_inserted_at": ["1999-01-01 00:00:00"]})
        result = add_meta(df.copy(), run_id="r1")
        assert str(result["_pipeline_inserted_at"].iloc[0]) != "1999-01-01 00:00:00"
        assert result["_pipeline_inserted_at"].iloc[0].year >= 2024

    def test_preserve_inserted_at_keeps_original_ingest_moment(self, helpers):
        """load_to_dw opts in, so the raw-ingest moment survives into target
        unchanged — that value IS the next run's refine watermark."""
        add_meta = helpers["_add_meta"]
        df = pd.DataFrame({"name": ["Alice"], "_pipeline_inserted_at": ["2024-01-02 03:04:05"]})
        result = add_meta(df.copy(), run_id="r1", preserve_inserted_at=True)
        assert str(result["_pipeline_inserted_at"].iloc[0]) == "2024-01-02 03:04:05"

    def test_preserved_value_is_datetime_dtype_not_string(self, helpers):
        """raw/refined store the column as TEXT, so a read-back is a string.
        Its pandas dtype drives a to_sql CREATE TABLE for the DW staging
        table — left as object it makes that column TEXT and breaks the
        INSERT into a target whose column is already TIMESTAMP."""
        add_meta = helpers["_add_meta"]
        df = pd.DataFrame({"name": ["Alice"], "_pipeline_inserted_at": ["2024-01-02 03:04:05"]})
        result = add_meta(df.copy(), run_id="r1", preserve_inserted_at=True)
        assert pd.api.types.is_datetime64_any_dtype(result["_pipeline_inserted_at"])

    def test_row_checksum_excludes_pipeline_metadata(self, helpers):
        """The same data must hash identically no matter what metadata rides
        along — otherwise cross-run dedup in raw and target would never
        match, since run_id and timestamps differ every run."""
        add_meta = helpers["_add_meta"]
        plain = add_meta(pd.DataFrame({"name": ["Alice"]}), run_id="r1",
                         file_name="a.csv", checksum="aaa")
        with_meta = add_meta(
            pd.DataFrame({"name": ["Alice"], "_pipeline_run_id": ["other"],
                          "_source_file": ["b.csv"], "_file_checksum": ["bbb"]}),
            run_id="r2", file_name="b.csv", checksum="bbb")
        assert plain["_row_checksum"].iloc[0] == with_meta["_row_checksum"].iloc[0]


# ─────────────────────────────────────────────────────────────────────────────
# 4. _find_file and _compute_md5
# ─────────────────────────────────────────────────────────────────────────────

class TestFindFileAndMd5:
    def test_find_existing_file(self, helpers, tmp_path):
        (tmp_path / "test.csv").write_text("a,b\n1,2\n")
        result = helpers["_find_file"](str(tmp_path), "test.csv")
        assert result is not None
        assert result.endswith("test.csv")

    def test_find_missing_file(self, helpers, tmp_path):
        result = helpers["_find_file"](str(tmp_path), "missing.csv")
        assert result is None

    def test_find_nonexistent_dir(self, helpers):
        result = helpers["_find_file"]("/no/such/dir", "anything.csv")
        assert result is None

    def test_md5_stable(self, helpers, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("col1,col2\nA,1\nB,2\n")
        h1 = helpers["_compute_md5"](str(f))
        h2 = helpers["_compute_md5"](str(f))
        assert h1 == h2
        assert len(h1) == 32

    def test_md5_differs_for_different_content(self, helpers, tmp_path):
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f1.write_text("hello")
        f2.write_text("world")
        assert helpers["_compute_md5"](str(f1)) != helpers["_compute_md5"](str(f2))


# ─────────────────────────────────────────────────────────────────────────────
# 5. _cast_types
# ─────────────────────────────────────────────────────────────────────────────

class TestCastTypes:
    def _cfg(self, overrides=None, date_cols=None, fmt_hints=None):
        return {
            "date_columns": date_cols or [],
            "dtype_overrides": overrides or {},
            "dtype_override_formats": fmt_hints or {},
        }

    def test_int_cast(self, helpers):
        cast = helpers["_cast_types"]
        df = pd.DataFrame({"amount": ["10", "20", "30"]})
        result = cast(df.copy(), self._cfg({"amount": "integer"}))
        assert pd.api.types.is_integer_dtype(result["amount"].dtype)

    def test_float_cast(self, helpers):
        cast = helpers["_cast_types"]
        df = pd.DataFrame({"price": ["9.99", "1.50", "0.01"]})
        result = cast(df.copy(), self._cfg({"price": "float"}))
        assert pd.api.types.is_float_dtype(result["price"].dtype)

    def test_bool_cast(self, helpers):
        cast = helpers["_cast_types"]
        df = pd.DataFrame({"active": ["true", "false", "yes", "no"]})
        result = cast(df.copy(), self._cfg({"active": "boolean"}))
        assert result["active"].tolist() == [True, False, True, False]

    def test_datetime_cast(self, helpers):
        cast = helpers["_cast_types"]
        df = pd.DataFrame({"created": ["2024-01-01", "2024-06-15"]})
        result = cast(df.copy(), self._cfg({"created": "timestamp"}))
        assert pd.api.types.is_datetime64_any_dtype(result["created"])

    def test_unknown_col_skipped_gracefully(self, helpers):
        cast = helpers["_cast_types"]
        df = pd.DataFrame({"known": ["1", "2"]})
        result = cast(df.copy(), self._cfg({"nonexistent": "integer"}))
        assert "nonexistent" not in result.columns

    def test_auto_numeric_inference(self, helpers):
        cast = helpers["_cast_types"]
        df = pd.DataFrame({"val": ["1", "2", "3", "4", "5"]})
        result = cast(df.copy(), self._cfg())
        assert pd.api.types.is_numeric_dtype(result["val"])


# ─────────────────────────────────────────────────────────────────────────────
# 6. End-to-end: raw → refined → DW  (SQLite, no Airflow)
# ─────────────────────────────────────────────────────────────────────────────

def _write_csv(path: str, rows: list[dict]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def _pg_copy_sqlite(df: pd.DataFrame, schema: str, table: str, engine) -> None:
    """SQLite substitute for _pg_copy_df — uses pandas to_sql append."""
    df.to_sql(table, con=engine, if_exists="append", index=False)


def _ensure_table(df: pd.DataFrame, engine, table: str) -> None:
    col_types = {c: "TEXT" for c in df.columns}
    cols_sql  = ", ".join(f'"{c}" TEXT' for c in df.columns)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols_sql})'))
        # add any new columns
        existing = {row[1] for row in conn.execute(text(f'PRAGMA table_info("{table}")'))}
        for col in df.columns:
            if col not in existing:
                conn.execute(text(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT'))


def _table_exists(engine, table: str) -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:t"),
            {"t": table},
        ).fetchone()
    return row is not None


class TestEndToEnd:
    """Simulate the full three-stage hybrid pipeline using in-memory SQLite."""

    RAW_ROWS = [
        {"customer_id": "1", "name": " Alice ", "province": "kigali"},
        {"customer_id": "2", "name": " Bob ",   "province": "east"},
        {"customer_id": "3", "name": " Carol",  "province": "west"},
    ]

    def _run_ingest(self, helpers, engine, tmp_path, run_id="run-001",
                     rows=None, file_name="customers.csv", checksum=None,
                     inserted_at=None):
        """Simulate load_to_raw: CSV → raw table, appended (never truncated) —
        raw is a permanent landing zone every run adds to, not a transit
        buffer that gets wiped before each load.

        Mirrors the template's row-level dedup: a row is only skipped if
        BOTH its _row_checksum AND _file_checksum already exist in raw —
        the same data from a genuinely different file is a distinct ingest
        event and gets kept, not silently collapsed.

        `checksum` can be overridden explicitly to simulate two files that
        are byte-different (different real MD5) but happen to parse to
        identical row data — computing MD5 from actual file content would
        give the same hash for identical content regardless of filename,
        which isn't the scenario being tested.

        `inserted_at` pins the batch's _pipeline_inserted_at to an explicit
        value so tests can control watermark ordering deterministically
        instead of racing real wall-clock timestamps. It is applied AFTER
        _add_meta, not before: ingest never passes preserve_inserted_at, so
        anything set on the incoming frame is deliberately discarded and
        re-stamped with "now" (that is the guard against a source column of
        the same name hijacking the watermark). Overwriting afterwards is
        what faithfully simulates "this batch landed at time T".
        """
        csv_path = str(tmp_path / file_name)
        _write_csv(csv_path, rows if rows is not None else self.RAW_ROWS)
        add_meta = helpers["_add_meta"]
        read_fn  = helpers["_read_file"]
        src_cfg  = {
            "file_format": "csv", "delimiter": ",", "encoding": "auto",
            "has_header": True, "skip_rows": 0,
            "null_values": ["", "NULL", "null", "None"],
            "date_columns": [], "dtype_overrides": {}, "dtype_override_formats": {},
        }
        checksum = checksum or helpers["_compute_md5"](csv_path)
        df = read_fn(src_cfg, csv_path)
        df = add_meta(df, run_id=run_id, file_name=file_name, checksum=checksum)
        if inserted_at is not None:
            df["_pipeline_inserted_at"] = inserted_at
        _ensure_table(df, engine, "customers_raw")

        with engine.connect() as conn:
            existing = pd.read_sql(
                text('SELECT "_row_checksum", "_file_checksum" FROM "customers_raw"'), conn
            )
        existing_pairs = set(zip(existing["_row_checksum"], existing["_file_checksum"]))
        new_pairs = list(zip(df["_row_checksum"], df["_file_checksum"]))
        keep_mask = [pair not in existing_pairs for pair in new_pairs]
        df_new = df[keep_mask]

        if len(df_new):
            _pg_copy_sqlite(df_new, "raw", "customers_raw", engine)
        return df

    def _run_refine(self, engine, run_id="run-001", sql_queries=None, reload=False):
        """Simulate refine_data: raw's unprocessed backlog → refined (with
        optional SQL cleaning).

        Normal runs scope to `_pipeline_inserted_at > watermark`, where
        watermark is MAX(_pipeline_inserted_at) already present in
        dw_customers (None — unscoped — if dw_customers doesn't exist yet,
        i.e. the first-ever run). This is deliberately NOT scoped to this
        run's own _pipeline_run_id: it's self-healing against a backlog
        stranded by an earlier run whose refine/dw_load failed after its
        ingest already succeeded — the next successful run sweeps it up
        regardless of which run_id originally ingested it.

        `run_id` no longer participates in scoping (kept as a parameter
        only so existing call sites don't need updating); reload pulls
        everything ever ingested, unscoped."""
        with engine.connect() as conn:
            if reload:
                df = pd.read_sql(text('SELECT * FROM "customers_raw"'), conn)
            else:
                watermark = None
                if _table_exists(engine, "dw_customers"):
                    watermark = conn.execute(
                        text('SELECT MAX("_pipeline_inserted_at") FROM "dw_customers"')
                    ).scalar()
                if watermark is None:
                    df = pd.read_sql(text('SELECT * FROM "customers_raw"'), conn)
                else:
                    df = pd.read_sql(
                        text('SELECT * FROM "customers_raw" WHERE "_pipeline_inserted_at" > :watermark'),
                        conn, params={"watermark": watermark},
                    )
        _ensure_table(df, engine, "customers_refined")
        with engine.begin() as conn:
            conn.execute(text('DELETE FROM "customers_refined"'))
        _pg_copy_sqlite(df, "refined", "customers_refined", engine)
        if sql_queries:
            for q in sql_queries:
                with engine.begin() as conn:
                    conn.execute(text(q))
            with engine.connect() as conn:
                df = pd.read_sql(text('SELECT * FROM "customers_refined"'), conn)
        return df

    def _run_dw_load(self, helpers, engine, run_id="run-001"):
        """Simulate load_to_dw: refined → DW (dedup by _row_checksum).

        Mirrors load_to_dw's own empty-refine guard: if this run's refine
        pass found nothing new, refined can be empty (or hold nothing for
        this scope) — don't call _add_meta on an empty frame, matching
        production, which skips entirely in that case.

        Mirrors load_to_dw's meta-column drop too: everything EXCEPT
        _pipeline_inserted_at is stripped and restamped — that one column
        must survive from refined into target untouched so it keeps
        reflecting the original raw ingest time, which is exactly what the
        next run's refine watermark is computed from.
        """
        add_meta = helpers["_add_meta"]
        meta_cols = helpers["_PIPELINE_META"]
        with engine.connect() as conn:
            df = pd.read_sql(text('SELECT * FROM "customers_refined"'), conn)
        if df.empty:
            with engine.connect() as conn:
                return pd.read_sql(text('SELECT * FROM "dw_customers"'), conn) \
                    if _table_exists(engine, "dw_customers") else df
        df = df.drop(columns=[c for c in meta_cols if c != "_pipeline_inserted_at" and c in df.columns])
        df = add_meta(df, run_id=run_id, preserve_inserted_at=True)
        _ensure_table(df, engine, "dw_customers")
        cols = ", ".join(f'"{c}"' for c in df.columns)
        tmp = "_tmp_dw"
        _ensure_table(df, engine, tmp)
        with engine.begin() as conn:
            conn.execute(text(f'DELETE FROM "{tmp}"'))
        _pg_copy_sqlite(df, "etl_stg", tmp, engine)
        with engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO "dw_customers" ({cols})
                SELECT {cols} FROM "{tmp}" t
                WHERE NOT EXISTS (
                    SELECT 1 FROM "dw_customers" r
                    WHERE r._row_checksum = t._row_checksum
                )
            """))
            conn.execute(text(f'DELETE FROM "{tmp}"'))
        with engine.connect() as conn:
            return pd.read_sql(text('SELECT * FROM "dw_customers"'), conn)

    # ── Tests ──────────────────────────────────────────────────────────

    def test_ingest_creates_raw_rows(self, helpers, engine, tmp_path):
        self._run_ingest(helpers, engine, tmp_path)
        with engine.connect() as conn:
            count = conn.execute(text('SELECT COUNT(*) FROM "customers_raw"')).scalar()
        assert count == 3

    def test_ingest_adds_pipeline_meta(self, helpers, engine, tmp_path):
        df = self._run_ingest(helpers, engine, tmp_path)
        for col in ["_pipeline_run_id", "_source_file", "_row_checksum", "_loaded_at"]:
            assert col in df.columns

    def test_refine_no_cleaning_copies_all_rows(self, helpers, engine, tmp_path):
        self._run_ingest(helpers, engine, tmp_path)
        self._run_refine(engine)
        with engine.connect() as conn:
            count = conn.execute(text('SELECT COUNT(*) FROM "customers_refined"')).scalar()
        assert count == 3

    def test_refine_with_sql_cleaning_modifies_data(self, helpers, engine, tmp_path):
        self._run_ingest(helpers, engine, tmp_path)
        # SQL cleaning now runs against refined, never raw — raw must stay
        # an untouched, permanent copy of exactly what was ingested.
        sql_queries = ["UPDATE customers_refined SET name = TRIM(name)"]
        self._run_refine(engine, sql_queries=sql_queries)
        with engine.connect() as conn:
            names = [r[0] for r in conn.execute(text('SELECT name FROM "customers_refined"'))]
        assert "Alice" in names   # whitespace stripped
        assert " Alice " not in names

    def test_dw_load_inserts_from_refined(self, helpers, engine, tmp_path):
        self._run_ingest(helpers, engine, tmp_path)
        self._run_refine(engine)
        dw = self._run_dw_load(helpers, engine)
        assert len(dw) == 3

    def test_dw_load_dedup_on_rerun(self, helpers, engine, tmp_path):
        """Running the same data twice should not double-insert rows."""
        self._run_ingest(helpers, engine, tmp_path)
        self._run_refine(engine)
        self._run_dw_load(helpers, engine, run_id="run-001")
        # Simulate second run with same data
        self._run_ingest(helpers, engine, tmp_path, run_id="run-002")
        self._run_refine(engine, run_id="run-002")
        dw = self._run_dw_load(helpers, engine, run_id="run-002")
        assert len(dw) == 3, f"Expected 3 rows after dedup rerun, got {len(dw)}"

    def test_raw_accumulates_across_runs(self, helpers, engine, tmp_path):
        """Raw is never truncated — a second run with genuinely different
        data appends on top of the first, it doesn't replace it."""
        self._run_ingest(helpers, engine, tmp_path, run_id="run-001")
        other_rows = [
            {"customer_id": "4", "name": "Dan",  "province": "north"},
            {"customer_id": "5", "name": "Erin", "province": "south"},
        ]
        self._run_ingest(helpers, engine, tmp_path, run_id="run-002",
                          rows=other_rows, file_name="customers_batch2.csv")
        with engine.connect() as conn:
            count = conn.execute(text('SELECT COUNT(*) FROM "customers_raw"')).scalar()
            run_count = conn.execute(
                text('SELECT COUNT(DISTINCT "_pipeline_run_id") FROM "customers_raw"')
            ).scalar()
        assert count == 5, "expected 3 rows from run-001 + 2 from run-002, not replaced"
        assert run_count == 2

    def test_raw_dedups_identical_file_reingest(self, helpers, engine, tmp_path):
        """Re-ingesting the exact same file's content doesn't grow raw —
        same data, same file_checksum, correctly treated as a duplicate."""
        self._run_ingest(helpers, engine, tmp_path, run_id="run-001")
        self._run_ingest(helpers, engine, tmp_path, run_id="run-002")  # identical CSV
        with engine.connect() as conn:
            count = conn.execute(text('SELECT COUNT(*) FROM "customers_raw"')).scalar()
        assert count == 3, "identical file content should not be appended twice"

    def test_raw_keeps_same_data_from_different_file(self, helpers, engine, tmp_path):
        """The same customer row appearing in a genuinely different file is
        a distinct ingest event and must be kept — dedup is on data AND
        file provenance together, not data alone. Losing this would mean
        raw silently forgets that a row was also present in a later file.
        """
        self._run_ingest(helpers, engine, tmp_path, run_id="run-001",
                          rows=self.RAW_ROWS, file_name="jan.csv", checksum="checksum-jan")
        self._run_ingest(helpers, engine, tmp_path, run_id="run-002",
                          rows=self.RAW_ROWS, file_name="feb.csv", checksum="checksum-feb")
        with engine.connect() as conn:
            count = conn.execute(text('SELECT COUNT(*) FROM "customers_raw"')).scalar()
            files = [r[0] for r in conn.execute(
                text('SELECT DISTINCT "_source_file" FROM "customers_raw"')
            )]
        assert count == 6, "same data from a different file must still be kept, not deduped away"
        assert sorted(files) == ["feb.csv", "jan.csv"]

    def test_truncate_refined_after_success(self, helpers, engine, tmp_path):
        self._run_ingest(helpers, engine, tmp_path)
        self._run_refine(engine)
        with engine.begin() as conn:
            conn.execute(text('DELETE FROM "customers_refined"'))
        with engine.connect() as conn:
            count = conn.execute(text('SELECT COUNT(*) FROM "customers_refined"')).scalar()
        assert count == 0

    def test_full_pipeline_no_cleaning(self, helpers, engine, tmp_path):
        """Complete run without cleaning: 3 rows reach DW."""
        self._run_ingest(helpers, engine, tmp_path)
        self._run_refine(engine)
        dw = self._run_dw_load(helpers, engine)
        assert len(dw) == 3
        assert "_pipeline_run_id" in dw.columns

    def test_full_pipeline_with_cleaning(self, helpers, engine, tmp_path):
        """Complete run with SQL cleaning: cleaned data reaches DW."""
        self._run_ingest(helpers, engine, tmp_path)
        # Cleaning runs against refined, never raw — raw stays an untouched,
        # permanent copy of exactly what was ingested.
        sql_queries = [
            "UPDATE customers_refined SET name = TRIM(name)",
            "UPDATE customers_refined SET province = UPPER(province)",
        ]
        self._run_refine(engine, sql_queries=sql_queries)
        dw = self._run_dw_load(helpers, engine)
        assert len(dw) == 3

    def test_reload_reprocesses_all_accumulated_raw(self, helpers, engine, tmp_path):
        """A reload run scopes refine to ALL of raw, unscoped — this is how
        a target gets rebuilt from history without re-requesting the
        original source files, regardless of what's already in target."""
        other_rows = [
            {"customer_id": "4", "name": "Dan",  "province": "north"},
        ]
        self._run_ingest(helpers, engine, tmp_path, run_id="run-001",
                          inserted_at="2024-01-01 00:00:00")
        self._run_refine(engine, run_id="run-001")
        self._run_dw_load(helpers, engine, run_id="run-001")

        self._run_ingest(helpers, engine, tmp_path, run_id="run-002",
                          rows=other_rows, file_name="batch2.csv",
                          inserted_at="2024-01-02 00:00:00")

        # Normal run only sees what's newer than target's watermark (run-002's batch)
        normal = self._run_refine(engine, run_id="run-002", reload=False)
        assert len(normal) == 1

        # Reload sees everything ever ingested, ignoring the watermark entirely
        reloaded = self._run_refine(engine, reload=True)
        assert len(reloaded) == 4

    def test_refine_self_heals_stranded_backlog_from_failed_run(self, helpers, engine, tmp_path):
        """If an earlier run's ingest succeeds but refine/dw_load then fails,
        that batch is stranded in raw under an "old" run_id — never reaching
        target. Watermark-based scoping (vs. the old _pipeline_run_id-based
        scoping) means the very next successful run picks up that backlog
        automatically, with no manual backfill, since it only compares
        against what's already in target — not which run ingested it."""
        stranded_rows = [
            {"customer_id": "9", "name": "Zoe", "province": "kigali"},
        ]
        # run-001: ingest succeeds, but refine/dw_load never run (simulating
        # a mid-pipeline failure) — these rows are now stranded in raw.
        self._run_ingest(helpers, engine, tmp_path, run_id="run-001",
                          rows=stranded_rows, file_name="stranded.csv",
                          inserted_at="2024-01-01 00:00:00")

        # run-002: a later, unrelated run ingests its own new rows.
        self._run_ingest(helpers, engine, tmp_path, run_id="run-002",
                          file_name="customers.csv",
                          inserted_at="2024-01-02 00:00:00")

        # Target has never been loaded, so watermark is None — run-002's
        # refine is unscoped and must sweep up BOTH the stranded run-001
        # backlog AND its own batch, not just its own.
        refined = self._run_refine(engine, run_id="run-002")
        assert len(refined) == 4, "expected run-001's stranded row + run-002's 3 rows"
        names = set(refined["name"].str.strip())
        assert "Zoe" in names, "stranded backlog from the failed run must be recovered"

        dw = self._run_dw_load(helpers, engine, run_id="run-002")
        assert len(dw) == 4

        # A subsequent run-003 with fresh data must NOT reprocess what's
        # already safely in target — only its own new row is in scope.
        self._run_ingest(helpers, engine, tmp_path, run_id="run-003",
                          rows=[{"customer_id": "10", "name": "Amina", "province": "east"}],
                          file_name="batch3.csv", inserted_at="2024-01-03 00:00:00")
        refined_again = self._run_refine(engine, run_id="run-003")
        assert len(refined_again) == 1
        assert refined_again.iloc[0]["name"].strip() == "Amina"


# ─────────────────────────────────────────────────────────────────────────────
# 7. Config schema validation
# ─────────────────────────────────────────────────────────────────────────────

class TestConfigSchema:
    """Verify the config JSON structure the hybrid template expects."""

    MINIMAL_CONFIG = {
        "pipeline_type": "hybrid",
        "source_type": "file",
        "dag_id": "hybrid_test__customers",
        "schedule_interval": "@daily",
        "start_date": "2025-01-01",
        "tags": ["hybrid"],
        "source": {
            "dev": {
                "watch_path": "/opt/airflow/data_dump/incoming",
                "file_name": "customers.csv",
                "file_format": "csv",
                "delimiter": ",",
                "encoding": "auto",
                "has_header": True,
                "skip_rows": 0,
                "null_values": ["NULL", "null"],
                "archive_path": "/opt/airflow/data_dump/archive",
                "archive_retention_days": 30,
            }
        },
        # No "raw"/"refined" keys — their location is fixed by the template,
        # not configurable (see TestRawRefinedSchemaResolution below).
        "cleaning": None,
        "target": {
            "dev": {
                "target_db_conn_id": "local_dw_con",
                "db_type": "postgresql",
                "target_schema": "dw_core",
                "target_table": "dimCustomers",
            }
        },
    }

    def test_cleaning_null_means_no_cleaning(self):
        cfg = dict(self.MINIMAL_CONFIG)
        assert cfg["cleaning"] is None

    def test_cleaning_present_means_cleaning_runs(self):
        cfg = dict(self.MINIMAL_CONFIG)
        cfg["cleaning"] = {
            # Cleaning SQL targets refined (schema "pipeline"), not raw —
            # raw must stay an untouched, permanent copy of exactly what
            # was ingested.
            "sql_queries": ["UPDATE pipeline.dw_core_customers_refined SET name = TRIM(name)"],
            "script_path": "customer_clean.py",
            "functions": ["run_sql_queries"],
        }
        assert cfg["cleaning"] is not None
        assert "sql_queries" in cfg["cleaning"]
        assert "functions" in cfg["cleaning"]

    def test_source_type_field_present(self):
        cfg = dict(self.MINIMAL_CONFIG)
        assert cfg["source_type"] in ("file", "db")


# ─────────────────────────────────────────────────────────────────────────────
# 8. load_config() — the real function, not a hand-authored stand-in
# ─────────────────────────────────────────────────────────────────────────────
#
# Exercises the actual load_config() extracted from the template (with
# Variable/glob/read_json_file mocked out — no Airflow, no real files
# needed) to prove raw/refined resolve to the fixed convention even when
# the config JSON still carries the old, now-dead raw_schema/refined_schema
# keys — i.e. that they're correctly ignored, not just absent from new
# configs.

def _load_config_fn(fake_config: dict):
    """Extract and exec the real load_config() from the template, with its
    Airflow/filesystem dependencies mocked, and return the resolved CONFIG
    dict load_config(dag_name) would produce for fake_config."""
    source = TPL_PATH.read_text()
    snippet = _extract_fn(source, "load_config")

    class _FakeVariable:
        @staticmethod
        def get(key, default_var=None):
            return default_var

    class _FakeGlob:
        @staticmethod
        def glob(pattern, recursive=False):
            return ["/fake/config.json"]

    def _fake_read_json_file(path):
        return fake_config

    ns = {
        "os": __import__("os"),
        "logging": __import__("logging"),
        "Variable": _FakeVariable,
        "glob": _FakeGlob,
        "read_json_file": _fake_read_json_file,
        "dag_name": fake_config.get("dag_id", "test_dag"),
    }
    exec(compile(snippet, str(TPL_PATH), "exec"), ns)  # noqa: S102
    return ns["load_config"](ns["dag_name"])


class TestRawRefinedSchemaResolution:
    """load_config() must resolve raw/refined to the fixed convention
    regardless of what (if anything) a config's dead raw_schema/
    refined_schema keys say."""

    BASE_CONFIG = {
        "dag_id": "hybrid_test__customers",
        "pipeline_type": "hybrid",
        "source_type": "file",
        "schedule_interval": "@daily",
        "start_date": "2025-01-01",
        "source": {
            "watch_path": "/opt/airflow/data_dump/incoming",
            "file_name": "customers.csv",
        },
        "target": {
            "target_db_conn_id": "local_dw_con",
            "target_schema": "dw_core",
            "target_table": "customers",
        },
    }

    def test_raw_always_resolves_to_raw_schema(self):
        cfg = _load_config_fn(dict(self.BASE_CONFIG))
        assert cfg["raw"]["schema"] == "raw"
        assert cfg["raw"]["table"] == "dw_core_customers_raw"

    def test_refined_resolves_to_shared_pipeline_schema(self):
        """Refined lives in the shared "pipeline" schema, not alongside the
        target — so its table name must carry the target_schema prefix,
        otherwise two different targets' refined tables would collide."""
        cfg = _load_config_fn(dict(self.BASE_CONFIG))
        assert cfg["refined"]["schema"] == "pipeline"
        assert cfg["refined"]["table"] == "dw_core_customers_refined"

    def test_legacy_raw_refined_schema_keys_are_ignored(self):
        """A config that still has the old dead keys (from before this
        change, or hand-edited) must resolve identically — proving they're
        genuinely ignored, not just absent from freshly-generated configs."""
        cfg_with_legacy_keys = dict(self.BASE_CONFIG)
        cfg_with_legacy_keys["raw_schema"] = "some_old_value"
        cfg_with_legacy_keys["refined_schema"] = "another_old_value"

        cfg = _load_config_fn(cfg_with_legacy_keys)
        assert cfg["raw"]["schema"] == "raw"
        assert cfg["refined"]["schema"] == "pipeline"

    def test_raw_and_refined_use_target_db_connection(self):
        cfg = _load_config_fn(dict(self.BASE_CONFIG))
        assert cfg["raw"]["conn_id"] == "local_dw_con"
        assert cfg["refined"]["conn_id"] == "local_dw_con"
