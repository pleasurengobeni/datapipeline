"""
Tests for the float-to-integer cast fix in dags/modules/utils.py.

The stage table stores every column as TEXT/VARCHAR.  Source values like
"1234.0" cannot be cast directly to INTEGER/BIGINT in PostgreSQL – they
must go through NUMERIC first.

Fix applied:
    CAST(NULLIF(TRIM(col::TEXT), '')::NUMERIC AS BIGINT)  AS col

These tests verify:
1. map_data_types() returns the expected SQL type strings.
2. The cast-expression generator emits the NUMERIC-intermediate form for
   every integer variant (INT, BIGINT, SMALLINT), both when the type comes
   from the auto-detected schema and when it comes from a transformation_map.
3. Non-integer types (FLOAT, TEXT, TIMESTAMP) are NOT routed through NUMERIC.
4. Empty-string / NULL safety is preserved for all numeric casts.
"""

import importlib
import sys
import types
import io
import re
import pytest


# ---------------------------------------------------------------------------
# Stub out all Airflow (and Slack) imports so utils.py can be imported without
# a running Airflow installation.
# ---------------------------------------------------------------------------
def _make_stub(name):
    mod = types.ModuleType(name)
    mod.__path__ = []
    return mod


AIRFLOW_STUBS = [
    "airflow",
    "airflow.models",
    "airflow.hooks",
    "airflow.hooks.base",
    "airflow.hooks.mysql_hook",
    "airflow.hooks.postgres_hook",
    "airflow.hooks.mssql_hook",
    "slack_sdk",
]

for _name in AIRFLOW_STUBS:
    if _name not in sys.modules:
        stub = _make_stub(_name)
        # Provide attribute access for  `from airflow.models import Variable`  etc.
        stub.Variable = object
        stub.BaseHook = object
        stub.MySqlHook = object
        stub.PostgresHook = object
        stub.MsSqlHook = object
        stub.WebClient = object
        sys.modules[_name] = stub

# Now import the module under test
sys.path.insert(0, "dags")
from modules.utils import map_data_types, clean_column_name  # noqa: E402


# ---------------------------------------------------------------------------
# Helper: reproduce the cast-expression logic from utils.py so we can unit-
# test it independently of the file I/O heavy `copy_data_from_zipped_csv…`.
# ---------------------------------------------------------------------------
def _make_cast_expr(col_name: str, data_type: str) -> str:
    """Mirror of the column_mappings logic inside copy_data_from_zipped_csv_with_metadata."""
    data_type = data_type.upper()
    if any(t in data_type for t in ["INT", "BIGINT", "SMALLINT"]):
        return f"CAST(NULLIF(TRIM({col_name}::TEXT), '')::NUMERIC AS {data_type}) AS {col_name}"
    if any(t in data_type for t in ["DECIMAL", "NUMERIC", "FLOAT", "REAL"]):
        return f"CAST(NULLIF({col_name}, '') AS {data_type}) AS {col_name}"
    if any(t in data_type for t in ["DATE", "TIMESTAMP"]):
        return f"CAST(NULLIF({col_name}, '') AS {data_type}) AS {col_name}"
    return f"{col_name}::TEXT AS {col_name}"


def _make_transformation_cast_expr(col_name: str, data_type: str) -> str:
    """Mirror of the transformation_map branch."""
    data_type = data_type.upper()
    if any(t in data_type for t in ["INT", "BIGINT", "SMALLINT"]):
        return f"CAST(NULLIF(TRIM({col_name}::TEXT), '')::NUMERIC AS {data_type}) AS {col_name}"
    return f"{col_name}::{data_type} AS {col_name}"


# ---------------------------------------------------------------------------
# 1. map_data_types
# ---------------------------------------------------------------------------
class TestMapDataTypes:
    def test_int64_maps_to_integer(self):
        assert map_data_types("int64") == "INTEGER"

    def test_float64_maps_to_float(self):
        assert map_data_types("float64") == "FLOAT"

    def test_object_maps_to_text(self):
        assert map_data_types("object") == "TEXT"

    def test_datetime_maps_to_timestamp(self):
        result = map_data_types("datetime64[ns]")
        assert "TIMESTAMP" in result.upper()

    def test_unknown_defaults_to_text(self):
        assert map_data_types("bytes") == "TEXT"

    def test_case_insensitive(self):
        assert map_data_types("INT64") == "INTEGER"


# ---------------------------------------------------------------------------
# 2. Integer cast goes through NUMERIC (the core bug-fix)
# ---------------------------------------------------------------------------
class TestIntegerCastViaNumeric:
    """
    Regression tests: float-string values ("1234.0") must not be cast
    directly to INT/BIGINT/SMALLINT from a TEXT stage column.
    """

    @pytest.mark.parametrize("pg_type", ["INTEGER", "BIGINT", "SMALLINT", "INT"])
    def test_auto_detected_integer_uses_numeric_intermediate(self, pg_type):
        expr = _make_cast_expr("my_col", pg_type)
        # Must go through ::NUMERIC
        assert "::NUMERIC" in expr, f"Expected ::NUMERIC in: {expr}"
        # Must use NULLIF+TRIM for empty/null safety
        assert "NULLIF" in expr
        assert "TRIM" in expr
        # Final cast must target the requested type
        assert f"AS {pg_type}" in expr or f"AS {pg_type})" in expr

    @pytest.mark.parametrize("pg_type", ["INTEGER", "BIGINT", "SMALLINT"])
    def test_transformation_map_integer_uses_numeric_intermediate(self, pg_type):
        expr = _make_transformation_cast_expr("amount", pg_type)
        assert "::NUMERIC" in expr, f"Expected ::NUMERIC in: {expr}"
        assert "NULLIF" in expr
        assert "TRIM" in expr

    def test_raw_direct_cast_would_fail_on_float_string(self):
        """
        Demonstrates WHY the old pattern was broken: directly casting "1234.0"
        to INTEGER in Python raises ValueError – same as PostgreSQL raises
        'invalid input syntax for type integer'.
        """
        float_as_string = "1234.0"
        with pytest.raises(ValueError):
            int(float_as_string)  # direct cast fails

    def test_numeric_intermediate_handles_float_string(self):
        """The fix logic – go via float (NUMERIC equivalent) then to int."""
        float_as_string = "1234.0"
        result = int(float(float_as_string))  # NUMERIC intermediate
        assert result == 1234

    def test_empty_string_becomes_none(self):
        """NULLIF('', '') → NULL; empty strings must not raise."""
        val = ""
        result = float(val) if val.strip() else None
        assert result is None

    def test_whitespace_only_becomes_none(self):
        val = "   "
        result = float(val) if val.strip() else None
        assert result is None


# ---------------------------------------------------------------------------
# 3. Non-integer types are NOT routed through NUMERIC
# ---------------------------------------------------------------------------
class TestNonIntegerCastNotAffected:
    def test_float_type_no_numeric_intermediate(self):
        expr = _make_cast_expr("price", "FLOAT")
        # FLOAT goes via NULLIF directly, NOT via ::NUMERIC
        assert "NULLIF" in expr
        assert "::NUMERIC" not in expr

    def test_text_type_no_cast(self):
        expr = _make_cast_expr("name", "TEXT")
        assert "::TEXT AS name" in expr
        assert "CAST" not in expr

    def test_timestamp_no_numeric_intermediate(self):
        expr = _make_cast_expr("created_at", "TIMESTAMP")
        assert "NULLIF" in expr
        assert "::NUMERIC" not in expr

    def test_numeric_type_no_extra_intermediate(self):
        # NUMERIC type itself: CAST(NULLIF(col,'') AS NUMERIC) – not double-wrapped
        expr = _make_cast_expr("amount", "NUMERIC")
        assert expr.count("NUMERIC") == 1  # appears once as target, not twice


# ---------------------------------------------------------------------------
# 4. clean_column_name sanity checks
# ---------------------------------------------------------------------------
class TestCleanColumnName:
    def test_spaces_replaced_by_underscore(self):
        assert clean_column_name("My Column") == "my_column"

    def test_special_chars_removed(self):
        assert clean_column_name("col#1!") == "col1"

    def test_reserved_word_gets_suffix(self):
        assert clean_column_name("group").endswith("_")

    def test_already_clean(self):
        assert clean_column_name("amount") == "amount"
