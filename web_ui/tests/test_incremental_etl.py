"""
Integration tests: timestamp / date formats and incremental ETL correctness.

Covers:
  1. _universal_to_datetime — all common timestamp/date string variants,
     including sub-second precision, day-first, month-first, epoch integers,
     named months, native datetime objects, and fail-loud for bad values.
  2. _cast_df_types — every supported dtype with realistic values
  3. _hwm_expressions — SQL WHERE clause generation for numeric,
     text-stored date, and native-timestamp columns
  4. End-to-end mock ETL — writes batches to a real SQLite DB and asserts that
     incremental runs load new rows only (no duplicates, no missed rows)

Runs with:  pytest web_ui/tests/test_incremental_etl.py -v
No Airflow, no PostgreSQL — everything uses SQLite and pandas in-process.
"""
from __future__ import annotations

import re
import textwrap
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text


# ─────────────────────────────────────────────────────────────────────────────
# Shared fixture: extract helpers from data_load.template
# ─────────────────────────────────────────────────────────────────────────────

TPL_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "dags" / "templates" / "data_load.template"
)

EXTRACT_NAMES = [
    "pandas_dtype_to_sql",
    "build_sqlalchemy_dtypes",
    "_dtype_str_to_sa_type",
    "_universal_to_datetime",
    "_cast_df_types",
    "_pg_fmt_to_py_fmt",
    "_NATIVE_DATE_TYPES",
    "_INTEGER_DB_KINDS",
    "_DATETIME_DB_KINDS",
    "_INCR_TYPE_CACHE",
    "_incr_sql_expr",
    "_hwm_expressions",
    "build_batch_temp_sql",
    "build_final_select_sql",
]

PREAMBLE = textwrap.dedent("""
    import logging
    import pandas as pd
    from datetime import datetime, date, timedelta
    from sqlalchemy.types import BigInteger, Boolean, Date, DateTime, Float, Integer, Text
""").strip()


def _extract_fn(src: str, name: str) -> str:
    """Extract a top-level 'def name' or 'name = ...' block from template source."""
    # Try function definition
    pattern = rf"^(def {name}\b.*?)(?=\ndef |\Z)"
    m = re.search(pattern, src, re.DOTALL | re.MULTILINE)
    if m:
        return m.group(1).rstrip()
    # Try module-level assignment (frozenset, dict, etc.)
    pattern2 = rf"^({name}\s*=.*?)(?=\n[A-Z_a-z]|\Z)"
    m2 = re.search(pattern2, src, re.DOTALL | re.MULTILINE)
    return m2.group(1).rstrip() if m2 else ""


@pytest.fixture(scope="module")
def helpers() -> dict:
    """Compile and exec all required helpers from data_load.template once per module."""
    source = TPL_PATH.read_text()
    snippets = "\n\n".join(_extract_fn(source, n) for n in EXTRACT_NAMES)
    ns: dict = {}
    exec(compile(PREAMBLE + "\n\n" + snippets, str(TPL_PATH), "exec"), ns)  # noqa: S102
    return ns


# ─────────────────────────────────────────────────────────────────────────────
# 1. _universal_to_datetime
# ─────────────────────────────────────────────────────────────────────────────

class TestUniversalToDatetime:
    """All timestamp / date formats that the ETL must handle without NULLs."""

    # (input value, hint_fmt, expect_nat, expect_raise)
    # expect_nat=True: None/empty — non-null bad values now raise ValueError
    CASES = [
        # ISO timestamps — various sub-second precisions
        ("2024-01-15 08:30:00",          "YYYY-MM-DD HH24:MI:SS",       False, False),
        ("2024-01-15 08:30:00.123",      "YYYY-MM-DD HH24:MI:SS",       False, False),
        ("2024-01-15 08:30:00.123456",   "YYYY-MM-DD HH24:MI:SS",       False, False),
        ("2024-01-15 08:30:00.123",      "YYYY-MM-DD HH24:MI:SS.MS",    False, False),
        ("2024-01-15 08:30:00.123456",   "YYYY-MM-DD HH24:MI:SS.US",    False, False),
        # ISO dates
        ("2024-01-15",                   "YYYY-MM-DD",                  False, False),
        ("2024/01/15",                   "YYYY/MM/DD",                  False, False),
        # Day-first formats
        ("15/01/2024",                   "DD/MM/YYYY",                  False, False),
        ("15-01-2024",                   "DD-MM-YYYY",                  False, False),
        ("15/01/2024 08:30:00",          "DD/MM/YYYY HH24:MI:SS",       False, False),
        ("15/01/2024 08:30:00.500",      "DD/MM/YYYY HH24:MI:SS",       False, False),
        # Month-first formats
        ("01/15/2024",                   "MM/DD/YYYY",                  False, False),
        ("01-15-2024",                   "MM-DD-YYYY",                  False, False),
        # No format hint — pandas auto-parse
        ("2024-01-15T08:30:00.123456Z",  None,                          False, False),
        ("2024-01-15",                   None,                          False, False),
        # Native datetime object — must not become NaT
        (datetime(2024, 1, 15, 8, 30, 0, 123456), "YYYY-MM-DD HH24:MI:SS", False, False),
        (datetime(2024, 1, 15, 8, 30, 0),          None,                    False, False),
        # Empty string / None — still NaT (not raises)
        ("",                             None,                           True,  False),
        (None,                           None,                           True,  False),
        # Non-null garbage — raises ValueError (fail-loud)
        ("not-a-date",                   "YYYY-MM-DD",                  False, True),
    ]

    @pytest.mark.parametrize("value,hint_fmt,expect_nat,expect_raise", CASES)
    def test_parse(self, helpers, value, hint_fmt, expect_nat, expect_raise):
        fn = helpers["_universal_to_datetime"]
        series = pd.Series([value])
        if expect_raise:
            with pytest.raises(ValueError, match="Cannot parse"):
                fn(series, hint_fmt=hint_fmt)
            return
        result = fn(series, hint_fmt=hint_fmt)
        if expect_nat:
            assert pd.isna(result.iloc[0]), (
                f"Expected NaT for value={value!r} fmt={hint_fmt!r}, got {result.iloc[0]!r}"
            )
        else:
            assert not pd.isna(result.iloc[0]), (
                f"Got unexpected NaT for value={value!r} fmt={hint_fmt!r}"
            )
            assert pd.api.types.is_datetime64_any_dtype(result), (
                f"Expected datetime64 dtype, got {result.dtype}"
            )

    def test_batch_with_subsecond_no_nulls(self, helpers):
        """A realistic batch of timestamps with varying sub-second precision."""
        fn = helpers["_universal_to_datetime"]
        values = [
            "2022-07-01 06:49:57",
            "2022-07-01 06:49:57.533",
            "2022-07-01 06:49:57.533000",
            "2022-07-01 06:49:57.000001",
            "2022-07-01 06:49:58",
        ]
        series = fn(pd.Series(values), hint_fmt="YYYY-MM-DD HH24:MI:SS")
        assert series.notna().all(), f"Unexpected NaTs:\n{series}"

    def test_dayfirst_ordering_preserved(self, helpers):
        """31/01/2024 must parse as Jan 31, not fail or parse as 01-31."""
        fn = helpers["_universal_to_datetime"]
        result = fn(pd.Series(["31/01/2024"]), hint_fmt="DD/MM/YYYY")
        assert not pd.isna(result.iloc[0])
        assert result.iloc[0].month == 1
        assert result.iloc[0].day == 31

    def test_epoch_seconds(self, helpers):
        """Unix epoch integers (seconds) should be parsed correctly."""
        fn = helpers["_universal_to_datetime"]
        result = fn(pd.Series([1704067200]))
        assert not pd.isna(result.iloc[0])
        assert result.iloc[0].year == 2024

    def test_epoch_milliseconds(self, helpers):
        """Unix epoch integers (milliseconds) should be parsed correctly."""
        fn = helpers["_universal_to_datetime"]
        result = fn(pd.Series([1704067200000]))
        assert not pd.isna(result.iloc[0])
        assert result.iloc[0].year == 2024


# ─────────────────────────────────────────────────────────────────────────────
# 2. _cast_df_types — dtype coverage with realistic ETL data
# ─────────────────────────────────────────────────────────────────────────────

class TestCastDfTypesExtended:
    """Additional coverage beyond the existing TestCastDfTypes."""

    @pytest.fixture
    def cast_fn(self, helpers):
        return helpers["_cast_df_types"]

    def test_timestamp_with_ms(self, cast_fn):
        df = pd.DataFrame({"ts": ["2024-01-01 10:00:00.500", "2024-06-15 23:59:59.999"]})
        result = cast_fn(df, {"ts": "timestamp"}, date_formats={"ts": "YYYY-MM-DD HH24:MI:SS.MS"})
        assert pd.api.types.is_datetime64_any_dtype(result["ts"])
        assert result["ts"].notna().all()

    def test_timestamp_with_us(self, cast_fn):
        df = pd.DataFrame({"ts": ["2024-01-01 10:00:00.123456"]})
        result = cast_fn(df, {"ts": "timestamp"}, date_formats={"ts": "YYYY-MM-DD HH24:MI:SS.US"})
        assert pd.api.types.is_datetime64_any_dtype(result["ts"])
        assert result["ts"].notna().all()

    def test_dayfirst_date(self, cast_fn):
        df = pd.DataFrame({"invoice_date": ["31/12/2023", "01/01/2024"]})
        result = cast_fn(df, {"invoice_date": "date"}, date_formats={"invoice_date": "DD/MM/YYYY"})
        assert pd.api.types.is_datetime64_any_dtype(result["invoice_date"])
        assert result["invoice_date"].iloc[0].day == 31

    def test_native_datetime_object_not_nat(self, cast_fn):
        """Native datetime objects from SQLAlchemy must survive _cast_df_types."""
        df = pd.DataFrame({"created_at": [datetime(2024, 5, 1, 12, 0, 0, 500000)]})
        result = cast_fn(df, {"created_at": "timestamp"})
        assert pd.api.types.is_datetime64_any_dtype(result["created_at"])
        assert result["created_at"].notna().all()

    def test_mixed_valid_invalid_raises(self, cast_fn):
        """A mix of valid and garbage date values must raise ValueError (fail-loud)."""
        import pytest
        df = pd.DataFrame({"ts": ["2024-01-01", "bad-value", "2024-06-15"]})
        with pytest.raises(ValueError, match="Cannot parse"):
            cast_fn(df, {"ts": "timestamp"})

    def test_all_dtypes_together(self, cast_fn):
        df = pd.DataFrame({
            "id":         ["1", "2", "3"],
            "amount":     ["10.50", "20.00", "99.99"],
            "active":     ["true", "false", "true"],
            "created_at": ["2024-01-01 08:00:00", "2024-03-15 12:30:00", "2024-06-01 00:00:00"],
            "score":      ["1.1", "2.2", "3.3"],
            "label":      [42, 43, 44],
        })
        result = cast_fn(df, {
            "id":         "integer",
            "amount":     "decimal",
            "active":     "boolean",
            "created_at": "timestamp",
            "score":      "float",
            "label":      "text",
        })
        assert str(result["id"].dtype) in ("Int64", "int64")
        assert pd.api.types.is_float_dtype(result["amount"])
        assert result["active"].tolist() == [True, False, True]
        assert pd.api.types.is_datetime64_any_dtype(result["created_at"])
        assert pd.api.types.is_float_dtype(result["score"])
        assert result["label"].dtype == object


# ─────────────────────────────────────────────────────────────────────────────
# 3. _hwm_expressions and _align_hwm_to_format
# ─────────────────────────────────────────────────────────────────────────────

class TestHwmExpressions:

    @pytest.fixture
    def hwm_fn(self, helpers):
        return helpers["_hwm_expressions"]

    # ── _hwm_expressions ────────────────────────────────────────────────────

    def test_numeric_hwm(self, hwm_fn):
        order_expr, where_clause = hwm_fn("id", "bigint", 100)
        assert 'BIGINT' in order_expr
        assert "> 100" in where_clause

    def test_numeric_hwm_none(self, hwm_fn):
        _, where_clause = hwm_fn("id", "bigint", None)
        assert where_clause is None

    def test_text_date_with_format(self, hwm_fn):
        hwm = datetime(2024, 1, 15, 8, 30, 0)
        order_expr, where_clause = hwm_fn("created_at", "text", hwm, "YYYY-MM-DD HH24:MI:SS")
        assert "TO_TIMESTAMP" in order_expr
        assert "YYYY-MM-DD HH24:MI:SS" in order_expr
        assert ">" in where_clause

    def test_text_date_with_ms_format_hwm_preserved(self, hwm_fn):
        """Microseconds in the HWM literal are preserved (no truncation)."""
        hwm = datetime(2024, 1, 15, 8, 30, 46, 500000)
        _, where_clause = hwm_fn("ts", "text", hwm, "YYYY-MM-DD HH24:MI:SS.MS")
        assert "500000" in where_clause or ".5" in where_clause

    def test_text_date_without_format_uses_cast(self, hwm_fn):
        """Text column with no explicit format uses CAST(… AS TIMESTAMP)."""
        hwm = datetime(2024, 1, 15, 8, 30, 0)
        order_expr, where_clause = hwm_fn("ts", "text", hwm)
        assert "CAST" in order_expr
        assert "TIMESTAMP" in order_expr
        assert ">" in where_clause

    def test_native_timestamp_is_column_ref(self, hwm_fn):
        """Native datetime column is referenced directly (no CAST needed)."""
        hwm = datetime(2024, 1, 15, 8, 30, 0)
        order_expr, where_clause = hwm_fn("created_at", "timestamp", hwm)
        assert order_expr == '"created_at"'
        assert ">" in where_clause

    def test_no_hwm_returns_none_where(self, hwm_fn):
        _, where_clause = hwm_fn("created_at", "timestamp", None)
        assert where_clause is None


# ─────────────────────────────────────────────────────────────────────────────
# 4. End-to-end mock ETL — SQLite source + target
# ─────────────────────────────────────────────────────────────────────────────

class TestMockEtlIncrementalLoad:
    """
    Simulates the core ETL loop using SQLite databases as source and target.

    Each scenario:
      1. Populates a source table with batch-1 rows.
      2. Runs 'load 1' — full initial load, writes to target.
      3. Appends batch-2 rows to source.
      4. Reads HWM from target.
      5. Runs 'load 2' — incremental load, only new rows.
      6. Asserts final target row count = batch-1 + batch-2, no duplicates.
    """

    # ── helper: create engine / table ───────────────────────────────────────

    @staticmethod
    def _make_engine(path: str):
        return create_engine(f"sqlite:///{path}", future=True)

    @staticmethod
    def _get_hwm(engine, table: str, incremental_col: str, datatype: str):
        """Read MAX(incremental_col) from target — mimics get_last_loaded_value logic."""
        with engine.connect() as conn:
            try:
                row = conn.execute(
                    text(f'SELECT MAX("{incremental_col}") FROM "{table}"')
                ).fetchone()
                val = row[0] if row else None
            except Exception:
                return None
        if val is None:
            return None
        if datatype in ("timestamp", "datetime"):
            if isinstance(val, str):
                return pd.to_datetime(val)
            return val
        if datatype in ("int", "bigint", "integer"):
            return int(val)
        return val

    @staticmethod
    def _run_load(src_engine, tgt_engine, source_sql: str, target_table: str,
                  incremental_col: str, datatype: str, last_loaded,
                  helpers: dict, incr_fmt: str = ""):
        """Minimal ETL: read source with HWM filter → append to target."""
        hwm_fn = helpers["_hwm_expressions"]

        # Build WHERE clause
        _, where_clause = hwm_fn(incremental_col, datatype, last_loaded, incr_fmt)

        if where_clause and last_loaded is not None:
            # SQLite-compatible WHERE: just use a simple comparison on the raw column
            if datatype in ("int", "bigint", "integer"):
                filtered_sql = f"SELECT * FROM ({source_sql}) _s WHERE \"{incremental_col}\" > {int(last_loaded)}"
            else:
                filtered_sql = f"SELECT * FROM ({source_sql}) _s WHERE \"{incremental_col}\" > '{last_loaded}'"
        else:
            filtered_sql = source_sql

        with src_engine.connect() as conn:
            df = pd.read_sql(text(filtered_sql), conn)

        if df.empty:
            return 0

        with tgt_engine.begin() as conn:
            df.to_sql(target_table, conn, if_exists="append", index=False)

        return len(df)

    # ── Scenario helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _batch_len(batch: dict) -> int:
        """Number of rows in a dict-of-lists batch."""
        return len(next(iter(batch.values())))

    def _run_scenario(self, tmp_path, helpers, batch1, batch2, table,
                      incremental_col, datatype, col_type_sql, incr_fmt=""):
        tmp_path.mkdir(parents=True, exist_ok=True)
        src_path = str(tmp_path / "source.db")
        tgt_path = str(tmp_path / "target.db")
        src_eng = self._make_engine(src_path)
        tgt_eng = self._make_engine(tgt_path)
        n1 = self._batch_len(batch1)
        n2 = self._batch_len(batch2)

        # Create & populate source
        df1 = pd.DataFrame(batch1)
        with src_eng.begin() as conn:
            df1.to_sql(table, conn, if_exists="replace", index=False)

        # Load 1 (initial)
        loaded1 = self._run_load(
            src_eng, tgt_eng,
            f'SELECT * FROM "{table}"', table,
            incremental_col, datatype, None, helpers, incr_fmt,
        )
        assert loaded1 == n1, f"First load: expected {n1}, got {loaded1}"

        # Read HWM
        hwm = self._get_hwm(tgt_eng, table, incremental_col, datatype)
        assert hwm is not None, "HWM must not be None after first load"

        # Append batch2 to source
        df2 = pd.DataFrame(batch2)
        with src_eng.begin() as conn:
            df2.to_sql(table, conn, if_exists="append", index=False)

        # Load 2 (incremental)
        loaded2 = self._run_load(
            src_eng, tgt_eng,
            f'SELECT * FROM "{table}"', table,
            incremental_col, datatype, hwm, helpers, incr_fmt,
        )
        assert loaded2 == n2, (
            f"Second load: expected {n2} new rows, got {loaded2}"
        )

        # Final assertion: total rows in target = batch1 + batch2, no duplicates
        with tgt_eng.connect() as conn:
            total = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar()
        assert total == n1 + n2, (
            f"Target row count {total} != expected {n1 + n2}"
        )

    # ── Test cases ───────────────────────────────────────────────────────────

    def test_integer_hwm(self, tmp_path, helpers):
        """Incremental load keyed on an integer auto-increment id."""
        batch1 = {"id": [1, 2, 3], "value": ["a", "b", "c"]}
        batch2 = {"id": [4, 5],    "value": ["d", "e"]}
        self._run_scenario(tmp_path, helpers, batch1, batch2,
                           "events", "id", "bigint", "INTEGER")

    def test_iso_timestamp_hwm(self, tmp_path, helpers):
        """Incremental load with ISO timestamp HWM (no sub-seconds)."""
        batch1 = {
            "created_at": ["2024-01-01 10:00:00", "2024-01-02 10:00:00"],
            "value": [1, 2],
        }
        batch2 = {
            "created_at": ["2024-01-03 10:00:00", "2024-01-04 10:00:00"],
            "value": [3, 4],
        }
        subdir = tmp_path / "iso_ts"
        subdir.mkdir()
        self._run_scenario(subdir, helpers, batch1, batch2,
                           "records", "created_at", "timestamp", "TEXT")

    def test_timestamp_with_milliseconds_hwm(self, tmp_path, helpers):
        """Rows with identical second but different milliseconds must all load."""
        batch1 = {
            "ts": ["2024-01-01 10:00:00.000", "2024-01-01 10:00:00.500"],
            "val": [1, 2],
        }
        batch2 = {
            "ts": ["2024-01-01 10:00:01.000", "2024-01-01 10:00:01.500"],
            "val": [3, 4],
        }
        subdir = tmp_path / "ms_ts"
        subdir.mkdir()
        self._run_scenario(subdir, helpers, batch1, batch2,
                           "events", "ts", "timestamp", "TEXT")

    def test_date_only_hwm(self, tmp_path, helpers):
        """Incremental load with date-only HWM (YYYY-MM-DD)."""
        batch1 = {
            "event_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "val": [10, 20, 30],
        }
        batch2 = {
            "event_date": ["2024-01-04", "2024-01-05"],
            "val": [40, 50],
        }
        subdir = tmp_path / "date_only"
        subdir.mkdir()
        self._run_scenario(subdir, helpers, batch1, batch2,
                           "daily", "event_date", "timestamp", "TEXT")

    def test_no_duplicate_rows_on_repeated_load(self, tmp_path, helpers):
        """
        Running the same incremental load twice must not insert duplicate rows
        (second run finds nothing newer than HWM).
        """
        src_eng = self._make_engine(str(tmp_path / "src.db"))
        tgt_eng = self._make_engine(str(tmp_path / "tgt.db"))
        table = "items"
        data = {
            "created_at": ["2024-01-01 12:00:00", "2024-01-02 12:00:00"],
            "item": ["a", "b"],
        }
        pd.DataFrame(data).to_sql(table, src_eng, if_exists="replace", index=False)

        # Load 1
        self._run_load(src_eng, tgt_eng, f'SELECT * FROM "{table}"', table,
                       "created_at", "timestamp", None, helpers)
        hwm = self._get_hwm(tgt_eng, table, "created_at", "timestamp")

        # Load 2 — same source, nothing new
        loaded2 = self._run_load(src_eng, tgt_eng, f'SELECT * FROM "{table}"', table,
                                 "created_at", "timestamp", hwm, helpers)
        assert loaded2 == 0, "Re-running with same HWM must load 0 rows"

        with tgt_eng.connect() as conn:
            total = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar()
        assert total == 2

    def test_cast_df_types_applied_before_write(self, helpers):
        """
        _cast_df_types converts string columns to the correct pandas types so
        the right SQLAlchemy types are used when writing.  Verify a full-cycle
        cast + write + read with datetime, int, float, bool columns.
        """
        cast_fn = helpers["_cast_df_types"]
        engine  = create_engine("sqlite:///:memory:", future=True)

        raw = pd.DataFrame({
            "id":         ["1", "2", "3"],
            "created_at": ["2024-01-01 10:00:00", "2024-06-15 08:30:00.500", "2024-12-31 23:59:59.999"],
            "amount":     ["10.5", "20.0", "99.99"],
            "active":     ["true", "false", "true"],
        })

        cast = cast_fn(raw, {
            "id":         "integer",
            "created_at": "timestamp",
            "amount":     "float",
            "active":     "boolean",
        })

        # Types must be correct before write
        assert str(cast["id"].dtype) in ("Int64", "int64")
        assert pd.api.types.is_datetime64_any_dtype(cast["created_at"])
        assert pd.api.types.is_float_dtype(cast["amount"])

        # Write & read back
        with engine.begin() as conn:
            cast.to_sql("output", conn, if_exists="replace", index=False)
        with engine.connect() as conn:
            result = pd.read_sql(text("SELECT * FROM output"), conn)

        assert len(result) == 3
        assert result["amount"].tolist() == pytest.approx([10.5, 20.0, 99.99])

    def test_dayfirst_timestamps_load_correctly(self, tmp_path, helpers):
        """
        Source stores dates as DD/MM/YYYY.  After _cast_df_types with the DD
        hint, incremental load must advance HWM correctly and pick up new rows.
        """
        cast_fn = helpers["_cast_df_types"]

        # Simulate what the ETL does after reading the source DataFrame
        raw = pd.DataFrame({
            "connection_date": ["01/01/2024", "15/01/2024", "31/01/2024"],
            "customer_id": [1, 2, 3],
        })
        cast = cast_fn(
            raw,
            {"connection_date": "timestamp"},
            date_formats={"connection_date": "DD/MM/YYYY"},
        )

        assert pd.api.types.is_datetime64_any_dtype(cast["connection_date"])
        assert cast["connection_date"].notna().all()
        # 31/01/2024 must be January, not some invalid date
        assert cast["connection_date"].iloc[2].month == 1
        assert cast["connection_date"].iloc[2].day == 31

        # Write to SQLite and read back HWM
        engine = create_engine("sqlite:///:memory:", future=True)
        with engine.begin() as conn:
            cast.to_sql("customers", conn, if_exists="replace", index=False)
        hwm = self._get_hwm(engine, "customers", "connection_date", "timestamp")
        assert hwm is not None

        # New batch
        raw2 = pd.DataFrame({
            "connection_date": ["28/02/2024", "01/03/2024"],
            "customer_id": [4, 5],
        })
        cast2 = cast_fn(
            raw2,
            {"connection_date": "timestamp"},
            date_formats={"connection_date": "DD/MM/YYYY"},
        )
        assert cast2["connection_date"].notna().all()
        # Both new rows must be after HWM
        assert (cast2["connection_date"] > hwm).all()
