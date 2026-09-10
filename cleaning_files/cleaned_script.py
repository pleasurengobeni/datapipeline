"""
Location data cleaning — province, sector, district, cell.

Two modes:
  Pipeline (called by hybrid DAG):
      clean_data(engine, schema, table)
      Runs against the raw table on the target DB; reads reference_cells from
      the pipeline schema on the same connection.

  Standalone (direct execution):
      python cleaned_script.py
      Runs against customer_dumps on DB_CONFIG host (172.16.1.228).
"""
import os
import re
import logging
import pandas as pd
import psycopg2
from rapidfuzz import process
from rapidfuzz.distance import JaroWinkler
import warnings
warnings.filterwarnings("ignore")

# ── Standalone DB (172.16.1.228) ─────────────────────────────────────────────
# Credentials come from the environment only — never hardcode them here.
# Read lazily (inside __main__, not at import time) so importing this module
# for pipeline mode (clean_data) never requires these env vars to be set.
def _standalone_db_config():
    return {
        'dbname':   os.environ.get('STANDALONE_DB_NAME', 'cenfri'),
        'user':     os.environ.get('STANDALONE_DB_USER', 'cenfri'),
        'password': os.environ['STANDALONE_DB_PASSWORD'],
        'host':     os.environ.get('STANDALONE_DB_HOST', '172.16.1.228'),
        'port':     int(os.environ.get('STANDALONE_DB_PORT', 5432)),
    }

STANDALONE_TABLE = "customer_dumps"

# ── Reference ─────────────────────────────────────────────────────────────────
PROVINCE_KEYS = ["Northern", "Southern", "Eastern", "Western", "City of Kigali"]

# ── Normalise for Jaro-Winkler comparison ─────────────────────────────────────
_NUM_TRANS = str.maketrans("013456789", "oieasggtb")
_NOISE     = re.compile(r'\b(province|district|sector|cell|village|city|town|ville|of|the|and)\b')

def _norm(text):
    t = str(text).strip().lower().translate(_NUM_TRANS)
    t = re.sub(r'\[[a-z]{2}-[a-z]{2}\]', '', t)   # [rw-RW] locale tags
    t = re.sub(r'[^a-z\s]', ' ', t)
    t = _NOISE.sub('', t)                           # strip geo suffixes + stop words
    t = re.sub(r'\s+', ' ', t).strip()
    return ' '.join(sorted(t.split()))              # token-sort for order-invariance


def _best_match(value, choices, threshold=0.85):
    if not choices:
        return None

    v = str(value).strip()
    # Try the value as-is, plus fused-suffix variants (a trailing roman
    # numeral / letter suffix typed with no separating space, e.g.
    # "Rangob" for "Rango B", "Kabugaii" for "Kabuga II") — take whichever
    # variant scores highest against the same candidate list and threshold.
    variants = [v]
    if len(v) > 2:
        variants.append(v[:-1] + ' ' + v[-1:])
    if len(v) > 3:
        variants.append(v[:-2] + ' ' + v[-2:])

    norm_to_orig = {_norm(c): c for c in choices}
    best_score, best_orig = -1.0, None
    for variant in variants:
        val_norm = _norm(variant)
        if not val_norm:
            continue
        result = process.extractOne(val_norm, list(norm_to_orig),
                                    scorer=JaroWinkler.normalized_similarity)
        if result and result[1] > best_score:
            best_score, best_orig = result[1], norm_to_orig[result[0]]

    if best_orig is not None and best_score >= threshold:
        return best_orig
    return None


# ── SQL cleaning statements ───────────────────────────────────────────────────
def _sql_steps(target):
    """Return (label, sql) pairs for the target table expression."""
    return [
        ("Title Case + trim", f"""
            UPDATE {target} SET
                province = INITCAP(LOWER(TRIM(province))),
                district = INITCAP(LOWER(TRIM(district))),
                sector   = INITCAP(LOWER(TRIM(sector))),
                cell     = INITCAP(LOWER(TRIM(cell))),
                village  = INITCAP(LOWER(TRIM(village)))
        """),
        ("Remove [rw-RW] suffix", f"""
            UPDATE {target} SET
                province = TRIM(REGEXP_REPLACE(province,'\\[[Rr][Ww]-[Rr][Ww]\\]','','g')),
                district = TRIM(REGEXP_REPLACE(district,'\\[[Rr][Ww]-[Rr][Ww]\\]','','g')),
                sector   = TRIM(REGEXP_REPLACE(sector,  '\\[[Rr][Ww]-[Rr][Ww]\\]','','g')),
                cell     = TRIM(REGEXP_REPLACE(cell,    '\\[[Rr][Ww]-[Rr][Ww]\\]','','g')),
                village  = TRIM(REGEXP_REPLACE(village, '\\[[Rr][Ww]-[Rr][Ww]\\]','','g'))
        """),
        ("Strip leaked customer-ID suffixes (e.g. 'Tin: 12345')", f"""
            UPDATE {target} SET
                cell     = TRIM(REGEXP_REPLACE(cell,    '\\s*tin[:\\s]*\\d+\\s*$', '', 'gi')),
                village  = TRIM(REGEXP_REPLACE(village, '\\s*tin[:\\s]*\\d+\\s*$', '', 'gi'))
        """),
        ("Null invalid (-1, numeric, <3 chars, non-alpha start)", f"""
            UPDATE {target} SET
                province = CASE WHEN province IN('-1','') OR province~'^[0-9]+$'
                                  OR LENGTH(province)<3  OR province~'^[^a-zA-Z]'
                                THEN NULL ELSE province END,
                district = CASE WHEN district IN('-1','') OR district~'^[0-9]+$'
                                  OR LENGTH(district)<3  OR district~'^[^a-zA-Z]'
                                THEN NULL ELSE district END,
                sector   = CASE WHEN sector   IN('-1','') OR sector  ~'^[0-9]+$'
                                  OR LENGTH(sector  )<3  OR sector  ~'^[^a-zA-Z]'
                                THEN NULL ELSE sector   END,
                cell     = CASE WHEN cell     IN('-1','') OR cell    ~'^[0-9]+$'
                                  OR LENGTH(cell    )<3  OR cell    ~'^[^a-zA-Z]'
                                THEN NULL ELSE cell     END,
                village  = CASE WHEN village  IN('-1','') OR village ~'^[0-9]+$'
                                  OR LENGTH(village )<3  OR village ~'^[^a-zA-Z]'
                                THEN NULL ELSE village  END
        """),
        ("Strip separators, take first word (district + sector)", f"""
            UPDATE {target} SET
                district = TRIM(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
                    district,'[,|]',' ','g'),'[-_/]',' ','g'),'\\s+',' ','g'),' ',1)),
                sector   = TRIM(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
                    sector,  '[,|]',' ','g'),'[-_/]',' ','g'),'\\s+',' ','g'),' ',1))
        """),
        ("Empty strings → NULL", f"""
            UPDATE {target} SET
                province = NULLIF(province,''),
                district = NULLIF(district,''),
                sector   = NULLIF(sector,  ''),
                cell     = NULLIF(cell,    ''),
                village  = NULLIF(village, '')
        """),
    ]


# ── Cell-based sector inference ───────────────────────────────────────────────
def _infer_sector_from_cell(execute_fn, fetch_fn, target, ref_df):
    """
    For rows whose sector is not in reference_cells, infer the correct sector
    from the cell value.  When a cell maps to multiple candidate sectors,
    Jaro-Winkler against the current (bad) sector value breaks the tie.

    Works in both pipeline and standalone modes — uses the already-loaded
    ref_df so no extra schema-specific SQL is needed.
    """
    from collections import defaultdict

    valid_sectors   = {str(s).strip() for s in ref_df['sector'].dropna()}
    cell_to_sectors = defaultdict(list)
    for _, row in ref_df.iterrows():
        s, c = row.get('sector'), row.get('cell')
        if s and c:
            cell_to_sectors[c.strip().lower()].append(s.strip())

    rows = fetch_fn(f"""
        SELECT DISTINCT sector, cell
        FROM   {target}
        WHERE  sector IS NOT NULL AND TRIM(sector) != ''
          AND  cell   IS NOT NULL AND TRIM(cell)   != ''
    """)

    mapping = {}
    for row in rows:
        bad_sec = str(row[0]).strip() if row[0] else ''
        cell    = str(row[1]).strip() if row[1] else ''
        if not bad_sec or not cell or bad_sec in valid_sectors:
            continue

        candidates = cell_to_sectors.get(cell.lower(), [])
        if not candidates:
            continue

        if len(candidates) == 1:
            correct = candidates[0]
        else:
            # Use JW similarity against the bad sector name as a tiebreaker
            correct = _best_match(bad_sec, candidates, threshold=0.0) or candidates[0]

        if correct != bad_sec:
            mapping[(bad_sec, cell)] = correct

    for (bad_sec, cell), correct in mapping.items():
        execute_fn(
            f"UPDATE {target} SET sector = '{correct.replace(chr(39), chr(39)*2)}' "
            f"WHERE  sector = '{bad_sec.replace(chr(39), chr(39)*2)}' "
            f"AND    cell   = '{cell.replace(chr(39), chr(39)*2)}'"
        )
        print(f"  ✓ {bad_sec!r:28s} + cell {cell!r:20s} → {correct!r}")

    print(f"  ✓ sector_infer : {len(mapping):>4} (sector, cell) combinations corrected")
    return mapping


# ── Village-based cell inference ──────────────────────────────────────────────
def _infer_cell_from_village(execute_fn, fetch_fn, target, ref_table):
    """
    For rows whose (sector, cell) still isn't a valid reference_cells combo
    after fuzzy cleaning, check other rows in the *same batch* sharing the
    same (sector, village) that already resolved to a single, unambiguous
    valid cell — apply that correction.

    Two tiers, both requiring unanimous agreement (no guessing):
      1. Direct: a row's own (sector, village) has exactly one valid cell
         among other rows sharing it — use that.
      2. Propagated: even rows with no village at all get fixed if every
         *other* occurrence of that exact (sector, bad_cell) string that DID
         have a village unanimously resolved to the same correct cell — this
         is confirmed batch evidence, not a similarity guess, so it's safe
         to apply regardless of whether this particular row has a village.

    Generalises to any future file: as long as at least one row for a given
    village independently lands on the right cell, every unmatched sibling
    sharing that sector+village (or that exact bad cell string) gets fixed
    too — no reference table of villages required.
    """
    cte = f"""
        WITH trusted_village AS (
            SELECT sector, village, cell
            FROM   {target} t
            WHERE  village IS NOT NULL
              AND  EXISTS (
                    SELECT 1 FROM {ref_table} r
                    WHERE r.sector = t.sector AND r.cell = t.cell
              )
            GROUP BY sector, village, cell
        ),
        unique_trusted_village AS (
            SELECT sector, village, MAX(cell) AS cell
            FROM   trusted_village
            GROUP BY sector, village
            HAVING COUNT(DISTINCT cell) = 1
        ),
        bad_cell_votes AS (
            SELECT t.sector, t.cell AS bad_cell, uv.cell AS suggested_cell
            FROM   {target} t
            JOIN   unique_trusted_village uv
              ON   t.sector = uv.sector AND t.village = uv.village
            WHERE  t.cell IS NOT NULL
              AND  NOT EXISTS (
                    SELECT 1 FROM {ref_table} r
                    WHERE r.sector = t.sector AND r.cell = t.cell
              )
            GROUP BY t.sector, t.cell, uv.cell
        ),
        unique_bad_cell_correction AS (
            SELECT sector, bad_cell, MAX(suggested_cell) AS correct_cell
            FROM   bad_cell_votes
            GROUP BY sector, bad_cell
            HAVING COUNT(DISTINCT suggested_cell) = 1
        )
    """

    n = fetch_fn(f"""
        {cte}
        SELECT COUNT(*) FROM {target} t
        JOIN   unique_bad_cell_correction ubc
          ON   t.sector = ubc.sector AND t.cell = ubc.bad_cell
    """)[0][0]

    execute_fn(f"""
        {cte}
        UPDATE {target} t
        SET    cell = ubc.correct_cell
        FROM   unique_bad_cell_correction ubc
        WHERE  t.sector = ubc.sector AND t.cell = ubc.bad_cell
    """)

    print(f"  ✓ village_infer :  {n:>4} rows corrected via village cross-reference")
    return n


# ── Generic column fuzzy cleaner ──────────────────────────────────────────────
def _fuzzy_clean_col(execute_fn, fetch_fn, target, col, ref_keys,
                     scope_col=None, scope_map=None, threshold=0.85):
    """
    Fetch DISTINCT values of col from target, fuzzy-match each unrecognised
    value using Jaro-Winkler, then UPDATE.

    execute_fn(sql)      — executes a DML statement
    fetch_fn(sql) → rows — returns list of tuples for a SELECT
    """
    scope_sel = f", {scope_col}" if scope_col else ""
    rows = fetch_fn(f"""
        SELECT DISTINCT {col}{scope_sel}
        FROM   {target}
        WHERE  {col} IS NOT NULL AND TRIM({col}) != ''
    """)

    valid   = set(ref_keys)
    mapping = {}   # key: (scope_val_orig_or_None, val) -> corrected

    for row in rows:
        val = str(row[0]).strip() if row[0] else ''
        if not val:
            continue

        scope_val_orig  = None
        scope_valid     = None
        if scope_col and scope_map:
            scope_val_orig = str(row[1]).strip() if len(row) > 1 and row[1] else ''
            scope_valid    = scope_map.get(scope_val_orig.lower(), [])

        # Values are only "already correct" within their own scope — a value
        # reused as a *different* scope's valid entry (e.g. the same cell name
        # existing under several sectors) must not short-circuit the check.
        # Scoped columns never fall back to the global list either: a value
        # unrecognised within its own scope must stay unmatched rather than
        # being corrected against an unrelated scope's entry (e.g. a cell
        # name that happens to belong to a different sector).
        if scope_valid is not None:
            if val in scope_valid:
                continue
            best = _best_match(val, scope_valid, threshold)
        else:
            if val in valid:
                continue
            best = _best_match(val, ref_keys, threshold)

        if best and best != val:
            mapping[(scope_val_orig, val)] = best

    updated = 0
    for (scope_val_orig, original), corrected in mapping.items():
        where_scope = f" AND {scope_col} = '{scope_val_orig.replace(chr(39), chr(39)*2)}'" if scope_val_orig is not None else ""
        execute_fn(
            f"UPDATE {target} SET {col} = '{corrected.replace(chr(39), chr(39)*2)}' "
            f"WHERE {col} = '{original.replace(chr(39), chr(39)*2)}'{where_scope}"
        )
        updated += 1

    logging.info("[location_clean] %s: %d values remapped → ~%d rows",
                 col, len(mapping), updated)
    print(f"  ✓ {col:12s}: {len(mapping):>4} values remapped")
    return mapping


# ── Pipeline entry point (called by hybrid DAG) ───────────────────────────────
def clean_data(engine, schema, table):
    """
    Entry point for the hybrid ETL refine step.
    engine  — SQLAlchemy engine connected to target DB
    schema  — raw schema name (e.g. 'raw')
    table   — raw table name (e.g. 'july_new_connections_raw')
    """
    from sqlalchemy import text

    target = f'"{schema}"."{table}"'

    def _exec(sql):
        with engine.begin() as con:
            con.execute(text(sql))

    def _fetch(sql):
        with engine.connect() as con:
            return con.execute(text(sql)).fetchall()

    print("=" * 60)
    print(f"STEP 1: SQL CLEANING → {target}")
    print("=" * 60)
    for label, sql in _sql_steps(target):
        _exec(sql)
        print(f"  ✓  {label}")

    # Load reference data from pipeline schema (same DB)
    ref_rows = _fetch("SELECT sector, cell FROM pipeline.reference_cells")
    df_ref   = pd.DataFrame(ref_rows, columns=['sector', 'cell'])

    distinct_sectors = sorted(df_ref['sector'].dropna().unique().tolist())
    distinct_cells   = sorted(df_ref['cell'].dropna().unique().tolist())
    scope_map = {}
    for _, row in df_ref.iterrows():
        scope_map.setdefault(row['sector'].strip().lower(), []).append(row['cell'].strip())

    # reference_districts is not on the RDS — derive from sector list for district
    # (best-effort: districts can't be validated without a reference table)
    distinct_districts = []
    try:
        dist_rows = _fetch("SELECT DISTINCT district FROM pipeline.reference_districts")
        distinct_districts = sorted(r[0] for r in dist_rows if r[0])
    except Exception:
        logging.warning("[location_clean] pipeline.reference_districts not found — skipping district fuzzy clean")

    print()
    print("=" * 60)
    print("STEP 2: CELL-BASED SECTOR INFERENCE")
    print("=" * 60)
    _infer_sector_from_cell(_exec, _fetch, target, df_ref)

    # Rebuild scope_map after sector corrections so cell fuzzy clean uses updated sectors
    scope_map = {}
    for _, row in df_ref.iterrows():
        scope_map.setdefault(row['sector'].strip().lower(), []).append(row['cell'].strip())

    print()
    print("=" * 60)
    print("STEP 3: FUZZY CLEANING (Jaro-Winkler)")
    print("=" * 60)

    _fuzzy_clean_col(_exec, _fetch, target, 'province', PROVINCE_KEYS)
    _fuzzy_clean_col(_exec, _fetch, target, 'sector',   distinct_sectors)
    if distinct_districts:
        _fuzzy_clean_col(_exec, _fetch, target, 'district', distinct_districts)
    _fuzzy_clean_col(_exec, _fetch, target, 'cell', distinct_cells,
                     scope_col='sector', scope_map=scope_map)

    print()
    print("=" * 60)
    print("STEP 4: VILLAGE-BASED CELL INFERENCE")
    print("=" * 60)
    _infer_cell_from_village(_exec, _fetch, target, ref_table='pipeline.reference_cells')

    print("\n✓  Location cleaning completed.")


# ── Standalone entry point (direct execution on source DB) ────────────────────
if __name__ == "__main__":
    conn = psycopg2.connect(**_standalone_db_config())
    target = STANDALONE_TABLE

    def _exec(sql):
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()

    def _fetch(sql):
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows

    print("=" * 60)
    print(f"STEP 1: SQL CLEANING → {target}")
    print("=" * 60)
    for label, sql in _sql_steps(target):
        _exec(sql)
        print(f"  ✓  {label}")

    ref_rows = _fetch("SELECT sector, cell FROM reference_cells")
    df_ref   = pd.DataFrame(ref_rows, columns=['sector', 'cell'])
    dist_rows = _fetch("SELECT DISTINCT district FROM reference_districts")

    distinct_sectors   = sorted(df_ref['sector'].dropna().unique().tolist())
    distinct_cells     = sorted(df_ref['cell'].dropna().unique().tolist())
    distinct_districts = sorted(r[0] for r in dist_rows if r[0])
    scope_map = {}
    for _, row in df_ref.iterrows():
        scope_map.setdefault(row['sector'].strip().lower(), []).append(row['cell'].strip())

    print()
    print("=" * 60)
    print("STEP 2: CELL-BASED SECTOR INFERENCE")
    print("=" * 60)
    _infer_sector_from_cell(_exec, _fetch, target, df_ref)

    scope_map = {}
    for _, row in df_ref.iterrows():
        scope_map.setdefault(row['sector'].strip().lower(), []).append(row['cell'].strip())

    print()
    print("=" * 60)
    print("STEP 3: FUZZY CLEANING (Jaro-Winkler)")
    print("=" * 60)

    _fuzzy_clean_col(_exec, _fetch, target, 'province', PROVINCE_KEYS)
    _fuzzy_clean_col(_exec, _fetch, target, 'sector',   distinct_sectors)
    _fuzzy_clean_col(_exec, _fetch, target, 'district', distinct_districts)
    _fuzzy_clean_col(_exec, _fetch, target, 'cell',     distinct_cells,
                     scope_col='sector', scope_map=scope_map)

    print()
    print("=" * 60)
    print("STEP 4: VILLAGE-BASED CELL INFERENCE")
    print("=" * 60)
    _infer_cell_from_village(_exec, _fetch, target, ref_table='reference_cells')

    conn.close()
    print("\n✓  Data cleaning completed.")
