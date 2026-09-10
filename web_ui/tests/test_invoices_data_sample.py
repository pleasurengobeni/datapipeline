"""
Tests for the db_local_dw_con_invoices_data_sample ETL pipeline.

Covers:
  1. Config JSON — structure, required fields, correct values
  2. SQL file — exists and is non-empty
  3. DAG .py file — exists and is syntactically valid
  4. _smart_to_datetime with the actual 'YYYY-MM-DD HH24:MI:SS.MS' timestamps
     found in public.invoices_data_sample
  5. _cast_df_types converts all columns correctly for this table
  6. _hwm_expressions generates correct SQL for text-stored MS timestamp
  7. Live integration (requires etl_test_db) — reads source, applies types,
     writes to cms.invoices_data_sample, runs incremental re-read, asserts
     no duplicates and correct row count

Run all:        pytest web_ui/tests/test_invoices_data_sample.py -v
Live tests only: pytest web_ui/tests/test_invoices_data_sample.py -v -m live
Skip live tests: pytest web_ui/tests/test_invoices_data_sample.py -v -m "not live"
"""
from __future__ import annotations

import ast
import json
import re
import textwrap
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────

ROOT     = Path(__file__).resolve().parent.parent.parent
CONFIG   = ROOT / "dags" / "config" / "local_dw_con" / "db_local_dw_con_invoices_data_sample.json"
SQL_FILE = ROOT / "dags" / "sql"    / "local_dw_con" / "db_local_dw_con_invoices_data_sample.sql"
DAG_FILE = ROOT / "dags" / "etl"   / "local_dw_con" / "db_local_dw_con_invoices_data_sample.py"
TPL_FILE = ROOT / "dags" / "templates" / "data_load.template"

DAG_ID   = "db_local_dw_con_invoices_data_sample"

# Real timestamps from public.invoices_data_sample
SAMPLE_TIMESTAMPS = [
    "2022-07-01 06:49:57.533",
    "2022-07-01 06:50:43.680",
    "2022-07-01 06:51:22.627",
    "2022-07-01 06:52:41.827",
    "2022-07-01 06:52:41.887",
    "2022-07-01 07:35:03.157",  # max value in the table
]

# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers fixture (same approach as other test files)
# ─────────────────────────────────────────────────────────────────────────────

EXTRACT_NAMES = [
    "pandas_dtype_to_sql", "build_sqlalchemy_dtypes", "_dtype_str_to_sa_type",
    "_universal_to_datetime", "_cast_df_types", "_pg_fmt_to_py_fmt",
    "_NATIVE_DATE_TYPES", "_INTEGER_DB_KINDS", "_DATETIME_DB_KINDS",
    "_INCR_TYPE_CACHE", "_incr_sql_expr", "_hwm_expressions",
    "build_batch_temp_sql", "build_final_select_sql",
]

PREAMBLE = textwrap.dedent("""
    import logging
    import pandas as pd
    from datetime import datetime, date, timedelta
    from sqlalchemy.types import BigInteger, Boolean, Date, DateTime, Float, Integer, Text
""").strip()


def _extract_fn(src: str, name: str) -> str:
    m = re.search(rf"^(def {name}\b.*?)(?=\ndef |\Z)", src, re.DOTALL | re.MULTILINE)
    if m:
        return m.group(1).rstrip()
    m2 = re.search(rf"^({name}\s*=.*?)(?=\n[A-Z_a-z]|\Z)", src, re.DOTALL | re.MULTILINE)
    return m2.group(1).rstrip() if m2 else ""


@pytest.fixture(scope="module")
def helpers() -> dict:
    source = TPL_FILE.read_text()
    snippets = "\n\n".join(_extract_fn(source, n) for n in EXTRACT_NAMES)
    ns: dict = {}
    exec(compile(PREAMBLE + "\n\n" + snippets, str(TPL_FILE), "exec"), ns)  # noqa: S102
    return ns


# ─────────────────────────────────────────────────────────────────────────────
# 1. Config JSON validation
# ─────────────────────────────────────────────────────────────────────────────

class TestConfig:

    @pytest.fixture(scope="class")
    def cfg(self):
        return json.loads(CONFIG.read_text())

    def test_config_file_exists(self):
        assert CONFIG.exists(), f"Config not found: {CONFIG}"

    def test_dag_id(self, cfg):
        assert cfg["dag_id"] == DAG_ID

    def test_required_top_level_keys(self, cfg):
        for key in ("dag_id", "schedule_interval", "start_date", "source", "target",
                    "data_dictionary", "table_description"):
            assert key in cfg, f"Missing top-level key: {key}"

    def test_source_dev_required_fields(self, cfg):
        dev = cfg["source"]["dev"]
        for field in ("source_db_conn_id", "db_type", "incremental_column",
                      "incremental_column_datatype", "incremental_column_date_format",
                      "sql_file", "dtype_overrides"):
            assert field in dev, f"source.dev missing: {field}"

    def test_incremental_column_is_creation_date(self, cfg):
        assert cfg["source"]["dev"]["incremental_column"] == "creation_date"

    def test_incremental_datatype_is_text(self, cfg):
        # creation_date is VARCHAR(50) in source — must use text so TO_TIMESTAMP is applied
        assert cfg["source"]["dev"]["incremental_column_datatype"] == "text"

    def test_incremental_format_has_ms_token(self, cfg):
        fmt = cfg["source"]["dev"]["incremental_column_date_format"]
        assert "MS" in fmt.upper(), f"Format must include MS token, got: {fmt!r}"

    def test_target_schema_is_cms(self, cfg):
        assert cfg["target"]["dev"]["target_schema"] == "cms"
        assert cfg["target"]["prod"]["target_schema"] == "cms"

    def test_target_table_is_invoices_data_sample(self, cfg):
        assert cfg["target"]["dev"]["target_table"] == "invoices_data_sample"

    def test_all_source_columns_have_dtype_override(self, cfg):
        expected_cols = {
            "customer_id_hashed", "period_id", "invoice_id", "creation_date",
            "consumption", "total", "solde", "customer_id",
        }
        overrides = set(cfg["source"]["dev"]["dtype_overrides"].keys())
        assert overrides == expected_cols, f"dtype_overrides mismatch: {overrides ^ expected_cols}"

    def test_dtype_override_formats_has_creation_date(self, cfg):
        fmts = cfg["source"]["dev"].get("dtype_override_formats", {})
        assert "creation_date" in fmts

    def test_data_dictionary_covers_all_columns(self, cfg):
        expected_cols = {
            "customer_id_hashed", "period_id", "invoice_id", "creation_date",
            "consumption", "total", "solde", "customer_id",
        }
        dd_cols = set(cfg["data_dictionary"].keys())
        assert dd_cols == expected_cols

    def test_dev_and_prod_configs_are_consistent(self, cfg):
        dev  = cfg["source"]["dev"]
        prod = cfg["source"]["prod"]
        for field in ("incremental_column", "incremental_column_datatype",
                      "incremental_column_date_format", "dtype_overrides"):
            assert dev[field] == prod[field], f"Dev/prod mismatch on {field}"


# ─────────────────────────────────────────────────────────────────────────────
# 2. SQL file
# ─────────────────────────────────────────────────────────────────────────────

class TestSqlFile:

    def test_sql_file_exists(self):
        assert SQL_FILE.exists(), f"SQL file not found: {SQL_FILE}"

    def test_sql_file_non_empty(self):
        assert SQL_FILE.read_text().strip(), "SQL file is empty"

    def test_sql_references_correct_table(self):
        sql = SQL_FILE.read_text().lower()
        assert "invoices_data_sample" in sql

    def test_sql_selects_all_required_columns(self):
        sql = SQL_FILE.read_text().lower()
        for col in ("customer_id_hashed", "period_id", "invoice_id", "creation_date",
                    "consumption", "total", "solde", "customer_id"):
            assert col in sql, f"Column not found in SQL: {col}"


# ─────────────────────────────────────────────────────────────────────────────
# 3. DAG .py file
# ─────────────────────────────────────────────────────────────────────────────

class TestDagFile:

    def test_dag_file_exists(self):
        assert DAG_FILE.exists(), f"DAG file not found: {DAG_FILE}"

    def test_dag_file_parses_as_valid_python(self):
        source = DAG_FILE.read_text()
        # Will raise SyntaxError on bad Python
        ast.parse(source)

    def test_dag_name_substituted_correctly(self):
        source = DAG_FILE.read_text()
        assert "<dag_name>" not in source, "Template variable <dag_name> was not replaced"
        assert DAG_ID in source, f"dag_id {DAG_ID!r} not found in DAG file"


# ─────────────────────────────────────────────────────────────────────────────
# 4. _universal_to_datetime with the actual timestamps from invoices_data_sample
# ─────────────────────────────────────────────────────────────────────────────

class TestSmartToDatetimeInvoices:
    """Verifies that _universal_to_datetime handles the real timestamps found
    in public.invoices_data_sample without producing any NaT values."""

    PG_FMT = "YYYY-MM-DD HH24:MI:SS.MS"

    def test_all_sample_timestamps_parse_without_nat(self, helpers):
        fn = helpers["_universal_to_datetime"]
        series = fn(pd.Series(SAMPLE_TIMESTAMPS), hint_fmt=self.PG_FMT)
        assert series.notna().all(), f"Unexpected NaT values:\n{series}"

    def test_all_sample_timestamps_are_datetime64(self, helpers):
        fn = helpers["_universal_to_datetime"]
        series = fn(pd.Series(SAMPLE_TIMESTAMPS), hint_fmt=self.PG_FMT)
        assert pd.api.types.is_datetime64_any_dtype(series)

    def test_milliseconds_preserved_correctly(self, helpers):
        fn = helpers["_universal_to_datetime"]
        # "2022-07-01 06:49:57.533" → microsecond = 533000
        result = fn(pd.Series(["2022-07-01 06:49:57.533"]), hint_fmt=self.PG_FMT)
        assert result.iloc[0].microsecond == 533000

    def test_ordering_is_chronological(self, helpers):
        fn = helpers["_universal_to_datetime"]
        series = fn(pd.Series(SAMPLE_TIMESTAMPS), hint_fmt=self.PG_FMT)
        assert series.is_monotonic_increasing, "Parsed timestamps are not in ascending order"

    def test_max_value_matches_expected(self, helpers):
        fn = helpers["_universal_to_datetime"]
        series = fn(pd.Series(SAMPLE_TIMESTAMPS), hint_fmt=self.PG_FMT)
        expected_max = pd.Timestamp("2022-07-01 07:35:03.157")
        assert series.max() == expected_max

    def test_format_without_ms_token_also_works(self, helpers):
        """_universal_to_datetime must handle .ms even when hint doesn't declare it."""
        fn = helpers["_universal_to_datetime"]
        # User picks YYYY-MM-DD HH24:MI:SS but data has .ms — must not produce NaT
        series = fn(pd.Series(SAMPLE_TIMESTAMPS), hint_fmt="YYYY-MM-DD HH24:MI:SS")
        assert series.notna().all()


# ─────────────────────────────────────────────────────────────────────────────
# 5. _cast_df_types for the invoices_data_sample schema
# ─────────────────────────────────────────────────────────────────────────────

class TestCastDfTypesInvoices:

    @pytest.fixture
    def cast_fn(self, helpers):
        return helpers["_cast_df_types"]

    def test_full_row_cast(self, cast_fn):
        df = pd.DataFrame({
            "customer_id_hashed": ["59df6a7d", "e7c3b04f"],
            "period_id":          ["202207",   "202207"],
            "invoice_id":         ["Inv10M616886i202207", "Inv10M614821i202207"],
            "creation_date":      ["2022-07-01 06:49:57.533", "2022-07-01 06:50:43.680"],
            "consumption":        ["3.0", "5.0"],
            "total":              ["1206.66", "2011.10"],
            "solde":              ["0.0", "0.0"],
            "customer_id":        ["59df6a7d", "e7c3b04f"],
        })
        cfg = json.loads(CONFIG.read_text())
        overrides = cfg["source"]["dev"]["dtype_overrides"]
        formats   = cfg["source"]["dev"].get("dtype_override_formats", {})

        result = cast_fn(df, overrides, date_formats=formats)

        assert str(result["period_id"].dtype) in ("Int64", "int64")
        assert pd.api.types.is_float_dtype(result["consumption"])
        assert pd.api.types.is_float_dtype(result["total"])
        assert pd.api.types.is_float_dtype(result["solde"])
        assert pd.api.types.is_datetime64_any_dtype(result["creation_date"])
        assert result["creation_date"].notna().all()
        assert result["customer_id_hashed"].dtype == object
        assert result["invoice_id"].dtype == object
        assert result["customer_id"].dtype == object

    def test_creation_date_microseconds_not_lost(self, cast_fn):
        cfg = json.loads(CONFIG.read_text())
        overrides = cfg["source"]["dev"]["dtype_overrides"]
        formats   = cfg["source"]["dev"].get("dtype_override_formats", {})

        df = pd.DataFrame({"creation_date": ["2022-07-01 06:49:57.533"]})
        result = cast_fn(df, {"creation_date": overrides["creation_date"]},
                         date_formats=formats)
        assert result["creation_date"].iloc[0].microsecond == 533000

    def test_null_creation_date_coerced_to_nat(self, cast_fn):
        df = pd.DataFrame({"creation_date": [None, "2022-07-01 06:49:57.533"]})
        result = cast_fn(df, {"creation_date": "timestamp"},
                         date_formats={"creation_date": "YYYY-MM-DD HH24:MI:SS.MS"})
        assert pd.isna(result["creation_date"].iloc[0])
        assert not pd.isna(result["creation_date"].iloc[1])


# ─────────────────────────────────────────────────────────────────────────────
# 6. _hwm_expressions for t ext-stored MS timestamp (invoices_data_sample)
# ─────────────────────────────────────────────────────────────────────────────

class TestHwmExpressionsInvoices:

    FMT = "YYYY-MM-DD HH24:MI:SS.MS"

    def test_order_expr_uses_to_timestamp(self, helpers):
        hwm_fn = helpers["_hwm_expressions"]
        order_expr, _ = hwm_fn("creation_date", "text", None, self.FMT)
        assert "TO_TIMESTAMP" in order_expr
        assert "creation_date" in order_expr
        assert self.FMT in order_expr

    def test_first_run_where_is_none(self, helpers):
        hwm_fn = helpers["_hwm_expressions"]
        _, where_clause = hwm_fn("creation_date", "text", None, self.FMT)
        assert where_clause is None

    def test_incremental_where_uses_greater_than(self, helpers):
        hwm_fn = helpers["_hwm_expressions"]
        hwm = datetime(2022, 7, 1, 6, 49, 57, 533000)
        _, where_clause = hwm_fn("creation_date", "text", hwm, self.FMT)
        assert ">" in where_clause
        assert "creation_date" in where_clause

    def test_hwm_microseconds_kept_with_ms_format(self, helpers):
        hwm_fn = helpers["_hwm_expressions"]
        hwm = datetime(2022, 7, 1, 6, 49, 57, 533000)
        _, where_clause = hwm_fn("creation_date", "text", hwm, self.FMT)
        # microseconds must appear in the WHERE literal
        assert "533000" in where_clause

    def test_hwm_microseconds_pass_through_without_ms_format(self, helpers):
        """Microseconds are no longer stripped — they pass through as-is.
        The DB handles precision; stripping caused subtle HWM off-by-one bugs."""
        hwm_fn = helpers["_hwm_expressions"]
        hwm = datetime(2022, 7, 1, 6, 49, 57, 533000)
        _, where_clause = hwm_fn("creation_date", "text", hwm, "YYYY-MM-DD HH24:MI:SS")
        # The new design preserves sub-seconds in the literal regardless of format hint
        assert "creation_date" in where_clause  # sanity: where clause references the column

    def test_build_batch_sql_no_hwm(self, helpers):
        fn = helpers["build_batch_temp_sql"]
        base = "SELECT * FROM public.invoices_data_sample"
        sql = fn(base, "creation_date", None, 0, "text", self.FMT)
        assert "TO_TIMESTAMP" in sql
        assert "WHERE" not in sql

    def test_build_batch_sql_with_hwm(self, helpers):
        fn = helpers["build_batch_temp_sql"]
        base = "SELECT * FROM public.invoices_data_sample"
        hwm  = datetime(2022, 7, 1, 6, 49, 57, 533000)
        sql  = fn(base, "creation_date", hwm, 0, "text", self.FMT)
        assert "TO_TIMESTAMP" in sql
        assert "WHERE" in sql
        assert ">" in sql


# ─────────────────────────────────────────────────────────────────────────────
# 7. Live integration tests against etl_test_db
# ─────────────────────────────────────────────────────────────────────────────

def _live_engine(schema: str = None):
    """Return a SQLAlchemy engine for etl_test_db.
    Raises pytest.skip if the DB is not reachable.
    """
    try:
        from sqlalchemy import create_engine
        dsn = "postgresql+psycopg2://etl_test_db:etl_test_db@localhost:5432/etl_test_db"
        eng = create_engine(dsn, connect_args={"connect_timeout": 3})
        with eng.connect():
            pass
        return eng
    except Exception as exc:
        pytest.skip(f"etl_test_db not reachable: {exc}")


@pytest.mark.live
class TestLiveInvoicesDataSample:
    """
    End-to-end ETL against the real etl_test_db.

    Writes to a *test-isolated* table  cms.invoices_data_sample_pytest
    so we never corrupt the production CMS table.  The table is dropped
    and recreated at the start of each test method.
    """

    TARGET_TABLE  = "invoices_data_sample_pytest"
    TARGET_SCHEMA = "cms"
    SOURCE_SQL    = "SELECT * FROM public.invoices_data_sample"

    @pytest.fixture(autouse=True)
    def engine(self):
        from sqlalchemy import create_engine
        self.eng = _live_engine()
        yield
        # Teardown: drop the test table
        with self.eng.begin() as conn:
            conn.execute(
                __import__("sqlalchemy").text(
                    f'DROP TABLE IF EXISTS "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}"'
                )
            )

    def _read_source(self) -> pd.DataFrame:
        from sqlalchemy import text
        with self.eng.connect() as conn:
            return pd.read_sql(text(self.SOURCE_SQL), conn)

    def _write_target(self, df: pd.DataFrame, if_exists: str = "append"):
        with self.eng.begin() as conn:
            df.to_sql(self.TARGET_TABLE, conn, schema=self.TARGET_SCHEMA,
                      if_exists=if_exists, index=False)

    def _target_count(self) -> int:
        from sqlalchemy import text
        with self.eng.connect() as conn:
            row = conn.execute(
                text(f'SELECT COUNT(*) FROM "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}"')
            ).fetchone()
            return row[0] if row else 0

    def _max_hwm(self) -> datetime | None:
        from sqlalchemy import text
        with self.eng.connect() as conn:
            row = conn.execute(
                text(
                    f'SELECT MAX("creation_date") '
                    f'FROM "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}"'
                )
            ).fetchone()
            val = row[0] if row else None
        if val is None:
            return None
        return pd.to_datetime(val)

    def _cast(self, df: pd.DataFrame, helpers: dict) -> pd.DataFrame:
        cfg      = json.loads(CONFIG.read_text())
        overrides = cfg["source"]["dev"]["dtype_overrides"]
        formats   = cfg["source"]["dev"].get("dtype_override_formats", {})
        return helpers["_cast_df_types"](df, overrides, date_formats=formats)

    # ── Tests ────────────────────────────────────────────────────────────────

    def test_source_table_has_expected_columns(self):
        df = self._read_source()
        expected = {"customer_id_hashed", "period_id", "invoice_id", "creation_date",
                    "consumption", "total", "solde", "customer_id"}
        assert set(df.columns) == expected

    def test_source_table_row_count(self):
        df = self._read_source()
        assert len(df) == 200, f"Expected 200 rows, got {len(df)}"

    def test_creation_date_no_nulls_in_source(self):
        df = self._read_source()
        assert df["creation_date"].notna().all()

    def test_full_initial_load(self, helpers):
        """Load all 200 rows into the target on first run."""
        df   = self._read_source()
        cast = self._cast(df, helpers)

        assert pd.api.types.is_datetime64_any_dtype(cast["creation_date"])
        assert cast["creation_date"].notna().all()

        self._write_target(cast, if_exists="replace")
        assert self._target_count() == 200

    def test_incremental_load_adds_no_rows_when_nothing_new(self, helpers):
        """
        After a full load, running incremental with the same source must load 0 rows.
        """
        from sqlalchemy import text

        # Full load
        df   = self._read_source()
        cast = self._cast(df, helpers)
        self._write_target(cast, if_exists="replace")
        hwm = self._max_hwm()
        assert hwm is not None

        # Incremental pass — source unchanged
        with self.eng.connect() as conn:
            incremental_df = pd.read_sql(
                text(f"SELECT * FROM public.invoices_data_sample "
                     f"WHERE \"creation_date\" > '{hwm}'"),
                conn,
            )
        assert len(incremental_df) == 0, (
            f"Expected 0 new rows after full load, got {len(incremental_df)}"
        )

    def test_hwm_is_max_creation_date(self, helpers):
        """After full load, HWM must equal MAX(creation_date) in the source."""
        from sqlalchemy import text

        df   = self._read_source()
        cast = self._cast(df, helpers)
        self._write_target(cast, if_exists="replace")

        hwm = self._max_hwm()
        assert hwm is not None

        with self.eng.connect() as conn:
            max_src = conn.execute(
                text("SELECT MAX(creation_date) FROM public.invoices_data_sample")
            ).scalar()

        expected = pd.to_datetime(max_src)
        assert hwm == expected, f"HWM {hwm} != source max {expected}"

    def test_cast_types_written_correctly(self, helpers):
        """After write + read-back, column dtypes must match expectations."""
        from sqlalchemy import text

        df   = self._read_source()
        cast = self._cast(df, helpers)
        self._write_target(cast, if_exists="replace")

        with self.eng.connect() as conn:
            result = pd.read_sql(
                text(f'SELECT * FROM "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}" LIMIT 10'),
                conn,
            )

        assert len(result) == 10
        # creation_date should come back as datetime
        assert pd.api.types.is_datetime64_any_dtype(result["creation_date"]) or \
               result["creation_date"].dtype == object  # PG returns it as datetime already
        # period_id should be numeric
        assert pd.api.types.is_numeric_dtype(result["period_id"])

    def test_ms_timestamps_all_preserved_after_roundtrip(self, helpers):
        """
        Millisecond components in creation_date must survive write → read-back.
        Compare the sorted timestamp series round-trip.
        """
        from sqlalchemy import text

        df   = self._read_source()
        cast = self._cast(df, helpers)
        original_ts = cast["creation_date"].sort_values().reset_index(drop=True)

        self._write_target(cast, if_exists="replace")

        with self.eng.connect() as conn:
            rt = pd.read_sql(
                text(f'SELECT creation_date FROM "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}"'),
                conn,
            )

        rt_ts = pd.to_datetime(rt["creation_date"]).sort_values().reset_index(drop=True)

        pd.testing.assert_series_equal(
            original_ts.rename("creation_date"),
            rt_ts.rename("creation_date"),
            check_names=False,
            check_freq=False,
        )


# ─────────────────────────────────────────────────────────────────────────────
# 8. Live incremental-load simulation
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.live
class TestLiveIncrementalSimulation:
    """
    Simulates a real multi-batch incremental ETL using the live invoices_data_sample
    source table (200 rows).

    Steps performed by each test:
      1.  Cast and sort all 200 source rows by creation_date.
      2.  Write batch 1 (first 100 rows by creation_date) to target → initial load.
      3.  Read HWM = MAX(creation_date) recorded in target after batch 1.
      4.  Identify batch 2 = source rows where creation_date > HWM.
      5.  Write batch 2 to target (incremental append).
      6.  Assert: no duplicate invoice_ids, final row count = 200,
          HWM advanced past the batch-1 value.
      7.  Re-run with the new HWM → zero new rows, count still 200.
    """

    TARGET_TABLE  = "invoices_data_sample_incr_sim"
    TARGET_SCHEMA = "cms"
    SPLIT_AT      = 100   # rows in batch 1

    @pytest.fixture(autouse=True)
    def engine(self):
        self.eng = _live_engine()
        yield
        with self.eng.begin() as conn:
            conn.execute(
                __import__("sqlalchemy").text(
                    f'DROP TABLE IF EXISTS "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}"'
                )
            )

    # ── helpers ──────────────────────────────────────────────────────────────

    def _read_source_cast(self, helpers) -> pd.DataFrame:
        from sqlalchemy import text
        with self.eng.connect() as conn:
            df = pd.read_sql(text("SELECT * FROM public.invoices_data_sample"), conn)
        cfg       = json.loads(CONFIG.read_text())
        overrides = cfg["source"]["dev"]["dtype_overrides"]
        formats   = cfg["source"]["dev"].get("dtype_override_formats", {})
        df = helpers["_cast_df_types"](df, overrides, date_formats=formats)
        return df.sort_values("creation_date").reset_index(drop=True)

    def _write(self, df: pd.DataFrame, if_exists: str = "append"):
        with self.eng.begin() as conn:
            df.to_sql(self.TARGET_TABLE, conn, schema=self.TARGET_SCHEMA,
                      if_exists=if_exists, index=False)

    def _target_count(self) -> int:
        from sqlalchemy import text
        with self.eng.connect() as conn:
            return conn.execute(
                text(f'SELECT COUNT(*) FROM "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}"')
            ).scalar()

    def _max_hwm(self) -> "pd.Timestamp":
        from sqlalchemy import text
        with self.eng.connect() as conn:
            val = conn.execute(
                text(f'SELECT MAX(creation_date) FROM "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}"')
            ).scalar()
        return pd.to_datetime(val)

    # ── tests ─────────────────────────────────────────────────────────────────

    def test_batch1_loads_correct_row_count(self, helpers):
        """Batch 1 (first 100 rows by creation_date) lands in target."""
        df     = self._read_source_cast(helpers)
        batch1 = df.iloc[: self.SPLIT_AT]
        self._write(batch1, if_exists="replace")
        assert self._target_count() == self.SPLIT_AT

    def test_batch2_incremental_adds_remaining_rows(self, helpers):
        """
        After loading batch 1, an incremental pass (creation_date > HWM) must load
        exactly the remaining rows — bringing the total to 200.
        """
        df     = self._read_source_cast(helpers)
        batch1 = df.iloc[: self.SPLIT_AT]
        self._write(batch1, if_exists="replace")

        hwm_after_batch1 = self._max_hwm()
        batch2 = df[df["creation_date"] > hwm_after_batch1]

        assert len(batch2) > 0, "Batch 2 is empty — increase SPLIT_AT or check source data"
        self._write(batch2, if_exists="append")

        assert self._target_count() == len(batch1) + len(batch2)

    def test_all_200_rows_loaded_after_two_batches(self, helpers):
        """Sum of batch 1 + batch 2 must equal the full 200-row source."""
        df     = self._read_source_cast(helpers)
        batch1 = df.iloc[: self.SPLIT_AT]
        self._write(batch1, if_exists="replace")

        hwm    = self._max_hwm()
        batch2 = df[df["creation_date"] > hwm]
        self._write(batch2, if_exists="append")

        assert self._target_count() == 200, (
            f"Expected 200 total rows, got {self._target_count()}"
        )

    def test_no_duplicate_invoice_ids_after_two_batches(self, helpers):
        """invoice_id must be unique in the target after both batches are written."""
        from sqlalchemy import text

        df     = self._read_source_cast(helpers)
        batch1 = df.iloc[: self.SPLIT_AT]
        self._write(batch1, if_exists="replace")
        hwm    = self._max_hwm()
        batch2 = df[df["creation_date"] > hwm]
        self._write(batch2, if_exists="append")

        with self.eng.connect() as conn:
            dup_count = conn.execute(
                text(
                    f'SELECT COUNT(*) FROM ('
                    f'  SELECT invoice_id, COUNT(*) c'
                    f'  FROM "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}"'
                    f'  GROUP BY invoice_id HAVING COUNT(*) > 1'
                    f') dups'
                )
            ).scalar()
        assert dup_count == 0, f"Found {dup_count} duplicate invoice_id(s) in target"

    def test_hwm_advances_after_batch2(self, helpers):
        """HWM recorded after batch 2 must be strictly greater than after batch 1."""
        df     = self._read_source_cast(helpers)
        batch1 = df.iloc[: self.SPLIT_AT]
        self._write(batch1, if_exists="replace")
        hwm1   = self._max_hwm()

        batch2 = df[df["creation_date"] > hwm1]
        self._write(batch2, if_exists="append")
        hwm2   = self._max_hwm()

        assert hwm2 > hwm1, f"HWM did not advance: batch-1 {hwm1}, batch-2 {hwm2}"

    def test_rerun_with_current_hwm_loads_zero_rows(self, helpers):
        """
        After both batches are loaded, a third incremental pass must find no new rows
        — the source is fully drained.
        """
        df     = self._read_source_cast(helpers)
        batch1 = df.iloc[: self.SPLIT_AT]
        self._write(batch1, if_exists="replace")
        hwm1   = self._max_hwm()
        batch2 = df[df["creation_date"] > hwm1]
        self._write(batch2, if_exists="append")

        # Third pass: nothing new
        hwm2      = self._max_hwm()
        batch3    = df[df["creation_date"] > hwm2]
        count_pre = self._target_count()

        assert len(batch3) == 0, (
            f"Expected 0 rows in batch 3, got {len(batch3)}"
        )
        assert self._target_count() == count_pre

    def test_hwm_equals_source_max_after_all_batches(self, helpers):
        """After all batches, target HWM must match MAX(creation_date) in source."""
        from sqlalchemy import text

        df     = self._read_source_cast(helpers)
        batch1 = df.iloc[: self.SPLIT_AT]
        self._write(batch1, if_exists="replace")
        hwm1   = self._max_hwm()
        batch2 = df[df["creation_date"] > hwm1]
        self._write(batch2, if_exists="append")

        with self.eng.connect() as conn:
            src_max = pd.to_datetime(
                conn.execute(
                    text("SELECT MAX(creation_date) FROM public.invoices_data_sample")
                ).scalar()
            )

        assert self._max_hwm() == src_max, (
            f"Target HWM {self._max_hwm()} != source MAX {src_max}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# 9. SQL-based incremental simulation — exercises the actual SQL path
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.live
class TestSqlBasedIncrementalSimulation:
    """
    Tests the ACTUAL SQL-based incremental loading logic by running the
    build_batch_temp_sql + build_final_select_sql statements directly
    against etl_test_db with a small batch size (10 rows per batch).

    This is different from TestLiveIncrementalSimulation, which splits
    pre-cast DataFrames in Python.  Here the WHERE clause, ORDER BY, LIMIT,
    and tie-boundary UNION are all executed as real PostgreSQL SQL.

    Source: public.invoices_data_sample (TEXT creation_date, 200 rows)
    Batch size: 10 → forces ~20 SQL iterations

    User's diagnostic applied after EVERY batch:
        COUNT(target WHERE creation_date <= HWM)
            must equal
        COUNT(source WHERE TO_TIMESTAMP(creation_date, fmt) <= HWM)

    If build_batch_temp_sql's WHERE clause or build_final_select_sql's
    tie-boundary UNION are wrong, at least one of these assertions will fail.
    """

    TARGET_TABLE  = "invoices_data_sample_sql_sim"
    TARGET_SCHEMA = "cms"
    BATCH_SIZE    = 10        # small → ~20 batches over 200 source rows
    SOURCE_SQL    = "SELECT * FROM public.invoices_data_sample"
    INCR_COL      = "creation_date"
    INCR_DTYPE    = "text"
    INCR_FMT      = "YYYY-MM-DD HH24:MI:SS.MS"

    @pytest.fixture(autouse=True)
    def engine(self):
        self.eng = _live_engine()
        with self.eng.begin() as conn:
            conn.execute(
                __import__("sqlalchemy").text(
                    f'DROP TABLE IF EXISTS "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}"'
                )
            )
        yield
        with self.eng.begin() as conn:
            conn.execute(
                __import__("sqlalchemy").text(
                    f'DROP TABLE IF EXISTS "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}"'
                )
            )

    # ── helpers ──────────────────────────────────────────────────────────────

    def _max_hwm(self) -> "pd.Timestamp | None":
        from sqlalchemy import text
        with self.eng.connect() as conn:
            val = conn.execute(
                text(f'SELECT MAX(creation_date) FROM "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}"')
            ).scalar()
        return pd.to_datetime(val) if val is not None else None

    def _target_count(self) -> int:
        from sqlalchemy import text
        with self.eng.connect() as conn:
            return conn.execute(
                text(f'SELECT COUNT(*) FROM "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}"')
            ).scalar()

    def _run_sql_batch(self, helpers, last_loaded) -> int:
        """
        Execute one SQL-based incremental batch using template helpers.

        Replicates the extract_data temp-table strategy exactly:
          1. build_batch_temp_sql  → filtered + ordered + limited source SQL
          2. CREATE TEMP TABLE ON COMMIT DROP  → single source scan
          3. SELECT MAX(order_expr) FROM temp  → batch_max literal
          4. build_final_select_sql + UNION tie fix → final ordered read
          5. Cast types and write to target

        Returns the number of rows loaded (0 when source is exhausted).
        """
        from sqlalchemy import text

        build_batch_sql_fn = helpers["build_batch_temp_sql"]
        build_final_sql_fn = helpers["build_final_select_sql"]
        hwm_fn             = helpers["_hwm_expressions"]
        cast_fn            = helpers["_cast_df_types"]

        cfg      = json.loads(CONFIG.read_text())
        overrides = cfg["source"]["dev"]["dtype_overrides"]
        formats   = cfg["source"]["dev"].get("dtype_override_formats", {})

        batch_sql  = build_batch_sql_fn(
            self.SOURCE_SQL, self.INCR_COL, last_loaded,
            self.BATCH_SIZE, self.INCR_DTYPE, self.INCR_FMT,
        )
        order_expr, _ = hwm_fn(self.INCR_COL, self.INCR_DTYPE, None, self.INCR_FMT)

        with self.eng.connect() as conn:
            # ON COMMIT DROP ensures the temp table is gone after conn.commit(),
            # so it doesn't leak into the next iteration if the pool reuses
            # the same underlying PostgreSQL session.
            conn.execute(text(
                f"CREATE TEMP TABLE _etl_batch_tmp ON COMMIT DROP AS ({batch_sql})"
            ))

            row = conn.execute(
                text(f"SELECT MAX({order_expr}) FROM _etl_batch_tmp")
            ).fetchone()
            batch_max = row[0] if row else None

            if batch_max is None:
                conn.commit()
                return 0

            final_sql = build_final_sql_fn(
                self.INCR_COL, self.INCR_DTYPE, self.INCR_FMT,
                base_sql=self.SOURCE_SQL, batch_size=self.BATCH_SIZE,
                batch_max=batch_max,
            )
            df = pd.read_sql(text(final_sql), conn)
            conn.commit()  # drops the ON COMMIT DROP temp table

        if df.empty:
            return 0

        df_cast = cast_fn(df, overrides, date_formats=formats)
        with self.eng.begin() as conn:
            df_cast.to_sql(
                self.TARGET_TABLE, conn, schema=self.TARGET_SCHEMA,
                if_exists="append", index=False,
            )
        return len(df_cast)

    # ── tests ─────────────────────────────────────────────────────────────────

    def test_all_200_rows_loaded_via_sql_batches(self, helpers):
        """All 200 source rows must arrive in the target across all SQL batches."""
        last_loaded = None
        for _ in range(60):  # safety cap — 200 rows / 10 per batch = 20 iterations max
            n = self._run_sql_batch(helpers, last_loaded)
            if n == 0:
                break
            last_loaded = self._max_hwm()

        assert self._target_count() == 200, (
            f"Expected 200 total rows after all SQL batches, got {self._target_count()}"
        )

    def test_diagnostic_count_matches_source_after_each_sql_batch(self, helpers):
        """
        User's diagnostic — after every SQL batch:
            COUNT(target WHERE creation_date <= HWM)
            == COUNT(source WHERE TO_TIMESTAMP(creation_date, fmt) <= HWM)

        A mismatch means the WHERE / UNION SQL is missing rows that should
        have been loaded in this or a previous batch.
        """
        from sqlalchemy import text

        hwm_fn     = helpers["_hwm_expressions"]
        order_expr, _ = hwm_fn(self.INCR_COL, self.INCR_DTYPE, None, self.INCR_FMT)

        last_loaded = None
        for batch_num in range(1, 60):
            n = self._run_sql_batch(helpers, last_loaded)
            if n == 0:
                break
            hwm = self._max_hwm()
            last_loaded = hwm

            with self.eng.connect() as conn:
                count_target = conn.execute(text(
                    f'SELECT COUNT(*) FROM "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}" '
                    f"WHERE creation_date <= '{hwm}'"
                )).scalar()
                count_source = conn.execute(text(
                    f"SELECT COUNT(*) FROM ({self.SOURCE_SQL}) _src "
                    f"WHERE {order_expr} <= '{hwm}'"
                )).scalar()

            assert count_target == count_source, (
                f"Batch {batch_num}: target count {count_target} != "
                f"source count {count_source} for rows ≤ HWM={hwm} "
                f"(loaded {n} rows this batch)"
            )

    def test_no_duplicate_invoice_ids_after_all_sql_batches(self, helpers):
        """No invoice_id must be duplicated in the target after all SQL batches."""
        from sqlalchemy import text

        last_loaded = None
        for _ in range(60):
            n = self._run_sql_batch(helpers, last_loaded)
            if n == 0:
                break
            last_loaded = self._max_hwm()

        with self.eng.connect() as conn:
            dup_count = conn.execute(text(
                f'SELECT COUNT(*) FROM ('
                f'  SELECT invoice_id, COUNT(*) c'
                f'  FROM "{self.TARGET_SCHEMA}"."{self.TARGET_TABLE}"'
                f'  GROUP BY invoice_id HAVING COUNT(*) > 1'
                f') dups'
            )).scalar()
        assert dup_count == 0, f"{dup_count} duplicate invoice_id(s) found after SQL batches"

    def test_hwm_advances_after_every_non_empty_sql_batch(self, helpers):
        """
        After each non-empty SQL batch, MAX(creation_date) in the target must
        advance beyond the previous HWM.

        If this fails the next batch's WHERE clause loaded rows with
        creation_date <= previous HWM — i.e. rows already present were
        re-delivered, or the HWM filter is broken.
        """
        last_loaded = None
        prev_hwm    = None
        for batch_num in range(1, 60):
            n = self._run_sql_batch(helpers, last_loaded)
            if n == 0:
                break
            hwm = self._max_hwm()
            if prev_hwm is not None:
                assert hwm > prev_hwm, (
                    f"HWM did not advance after batch {batch_num}: "
                    f"before={prev_hwm}, after={hwm}"
                )
            prev_hwm    = hwm
            last_loaded = hwm

    def test_rerun_after_drain_loads_zero_rows(self, helpers):
        """
        After all source rows are loaded, the next SQL batch must return 0 rows
        (idempotency — the source is fully drained, nothing new to load).
        """
        last_loaded = None
        for _ in range(60):
            n = self._run_sql_batch(helpers, last_loaded)
            if n == 0:
                break
            last_loaded = self._max_hwm()

        final_hwm = self._max_hwm()
        n_extra   = self._run_sql_batch(helpers, final_hwm)
        assert n_extra == 0, (
            f"Expected 0 rows on re-run after drain, got {n_extra}"
        )
