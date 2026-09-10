"""
Shared column-name/dtype inference over a sample of real rows.

This is an extract-method refactor: the per-column inference loop below was
previously inline inside `api_preview_columns` (the file-upload column
detector). It's factored out here, unchanged in behavior, so a second caller
— the API job's column detector — can reuse the exact same dtype-guessing
rules (and so both stay in lockstep instead of drifting into two copies).

Deliberately NOT used by `api_columns_from_query` (the DB job's column
detector): that endpoint gets ground-truth types straight from the
database driver via a zero-row `information_schema`-backed query — there's
no sample data and no ambiguity to resolve there. Forcing it through this
sampling-based inference would be a regression (discarding real type
information to re-guess it), not a simplification. File-upload and API
detection both start from actual sample rows and belong together; DB
detection starts from schema metadata and is a fundamentally different
operation — keep them separate.
"""

import re
import unicodedata


def _clean_column_name(name: str) -> str:
    """Normalise a column name the same way the DAG templates do at load time."""
    clean = unicodedata.normalize("NFD", str(name))
    clean = "".join(c for c in clean if unicodedata.category(c) != "Mn")
    clean = re.sub(r"[^a-z0-9]+", "_", clean.lower()).strip("_") or "col"
    return clean


def infer_columns_from_dataframe(df) -> list:
    """
    Given a pandas DataFrame of sample rows, return a list of
    {original, clean, dtype, sample} dicts — one per column — with `dtype`
    one of: integer, decimal, boolean, timestamp, text.
    """
    import pandas as pd

    columns = []
    for col in df.columns:
        series = df[col]
        if pd.api.types.is_integer_dtype(series):
            dtype = "integer"
        elif pd.api.types.is_float_dtype(series):
            # Check if it looks like it should be integer (all values whole numbers)
            non_null = series.dropna()
            if len(non_null) > 0 and (non_null % 1 == 0).all():
                dtype = "integer"
            else:
                dtype = "decimal"
        elif pd.api.types.is_bool_dtype(series):
            dtype = "boolean"
        elif pd.api.types.is_datetime64_any_dtype(series):
            dtype = "timestamp"
        else:
            # Try to parse as number — pre-filter with a plain regex first.
            # pandas' native float parser can segfault on pathological strings
            # (e.g. 64-char hex hashes containing an 'e' + digits run that
            # looks like scientific notation), so never hand it a value that
            # doesn't already look like a plausible number.
            non_null = series.dropna()
            str_vals = non_null.astype(str).str.strip()
            numeric_like = str_vals.str.match(r'^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d{1,3})?$')
            if len(non_null) > 0 and numeric_like.mean() >= 0.8:
                parsed = pd.to_numeric(str_vals[numeric_like], errors="coerce").dropna()
            else:
                parsed = pd.Series([], dtype=float)
            if len(non_null) > 0 and len(parsed) / len(non_null) >= 0.8:
                # Looks numeric — integer or decimal?
                if (parsed % 1 == 0).all():
                    dtype = "integer"
                else:
                    dtype = "decimal"
            else:
                # Try date
                try:
                    parsed_dt = pd.to_datetime(series, errors="coerce")
                    if len(non_null) > 0 and parsed_dt.notna().sum() / len(non_null) >= 0.8:
                        dtype = "timestamp"
                    else:
                        dtype = "text"
                except Exception:
                    dtype = "text"

        # Sample values (first 3 non-null)
        sample = [str(v) for v in series.dropna().head(3).tolist()]

        columns.append({
            "original": str(col),
            "clean":    _clean_column_name(col),
            "dtype":    dtype,
            "sample":   sample,
        })

    return columns


def infer_columns_from_records(records: list) -> list:
    """
    Given a list of dict records (e.g. parsed from an API JSON response),
    flatten one level of nesting (address.city, etc.) via pd.json_normalize
    and delegate to infer_columns_from_dataframe for the actual dtype guess.
    """
    import pandas as pd

    if not records:
        return []
    df = pd.json_normalize(records)
    return infer_columns_from_dataframe(df)
