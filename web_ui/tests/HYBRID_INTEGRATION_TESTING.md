# Hybrid Pipeline — Docker/Airflow Integration Testing Playbook

`test_hybrid_pipeline.py` is a fast SQLite-based regression suite — it catches
logic bugs but never touches real Postgres, Airflow scheduling, or the actual
generated DAG file. This playbook is the integration test that follows it:
runs the real DAG against real Postgres via the Docker Compose stack, and is
required before calling any change to `hybrid_load.template` (or
`file_load.template`) done.

Replace `<dag_id>`, `<conn_id>`, `<target_schema>`, `<target_table>`,
`<csv_path>` below with the job under test. Examples throughout use
`hybrid_cenfri_dev_con_july_new_connections` / `cenfri_dev_con` /
`public.july_new_connections`.

## 0. Prerequisites

```bash
docker ps --format '{{.Names}}\t{{.Status}}' | grep airflow   # all healthy
```

Get fresh DB credentials from Airflow rather than reusing an old hardcoded
password — connections rotate:

```bash
docker exec <worker> python3 -c "
from airflow.hooks.base import BaseHook
c = BaseHook.get_connection('<conn_id>')
print(c.host, c.port, c.schema, c.login, c.password)
"
```

If a template file changed, **regenerate the DAG before testing** — editing
`dags/templates/*.template` alone does not update the already-generated
`dags/etl/*.py` file Airflow actually runs:

```bash
python3 scripts/recreate_etl_dags.py --dry-run   # review first
python3 scripts/recreate_etl_dags.py
docker exec <scheduler> airflow dags list-import-errors   # must be empty
grep -c "<marker unique to your change>" dags/etl/<conn_id>/<dag_id>.py   # confirm it landed
```

## 1. Clean slate

```sql
-- adjust table names to the job's actual raw/refined/target locations
TRUNCATE TABLE raw."<target_schema>_<target_table>_raw";
TRUNCATE TABLE pipeline."<target_schema>_<target_table>_refined";  -- may not exist yet, that's fine
TRUNCATE TABLE "<target_schema>"."<target_table>";

DELETE FROM pipeline.file_load_registry WHERE target_table LIKE '%<target_table>%';
DELETE FROM pipeline.etl_metrics WHERE dag_id = '<dag_id>';
```

Run each `TRUNCATE`/`DELETE` as its **own committed statement or its own
script**. Don't mix a step that might fail (e.g. truncating a table that
doesn't exist yet) into the same uncommitted transaction as one that must
succeed — `conn.rollback()` after a failure undoes *everything* still
uncommitted earlier in that same connection, silently. (This bit us once:
an `etl_metrics` clear got silently undone by a later failed `TRUNCATE` in
the same script.)

## 2. Clear the runway — competing runs

The DAG's `max_active_runs=1` plus (often) a tight `schedule_interval` means
a scheduled run is very likely already occupying the one active-run slot,
or will grab your test file before your manual trigger does. Before every
triggered test:

```bash
docker exec <scheduler> airflow dags list-runs -d <dag_id> --state running
docker exec <scheduler> airflow dags list-runs -d <dag_id> --state queued
```

Clear anything found via the REST API (works regardless of pause state):

```bash
curl -s -X PATCH "http://localhost:<port>/api/v1/dags/<dag_id>/dagRuns/<run_id>" \
  -H "Content-Type: application/json" -u "<user>:<pass>" -d '{"state":"failed"}'
```

**Pausing gotcha:** pausing the DAG stops scheduling of task instances
entirely — including subsequent tasks of an *already-running* DagRun, not
just new triggers. If you pause and then see a run stuck with zero task
progress, that's why — unpause to let it continue. Net effect: pausing is
mostly only useful for the brief window between triggering a manual run and
unpausing to let it start; don't rely on it to freeze an in-flight run.

## 3. Normal run

```bash
cp <csv_path> /path/to/data_dump/incoming/<file_name>
curl -s -X POST http://localhost:<port>/api/v1/dags/<dag_id>/dagRuns \
  -H "Content-Type: application/json" -u "<user>:<pass>" -d '{"conf":{}}'
docker exec <scheduler> airflow dags unpause <dag_id>   # only if you paused above
```

Poll (re-copy the file if it vanishes before your run's own ingest reaches
it — a race with a competing scheduled run, not a real failure):

```bash
for i in $(seq 1 24); do
  sleep 10
  OUT=$(docker exec <scheduler> airflow tasks states-for-dag-run <dag_id> "<run_id>" | grep -v "^\[")
  SUCCESS=$(echo "$OUT" | grep -o success | wc -l)
  FAILED=$(echo "$OUT" | grep -o failed | wc -l)
  echo "$(date '+%H:%M:%S') success=$SUCCESS failed=$FAILED"
  [ "$FAILED" -gt 0 ] && { echo "$OUT"; break; }
  [ "$SUCCESS" -ge <expected_task_count> ] && { echo DONE; break; }
done
```

**Verify:**
```sql
SELECT COUNT(*) FROM raw."<...>_raw";           -- == CSV row count
SELECT COUNT(*) FROM "<target_schema>"."<target_table>";  -- same
```
```bash
# refine.refine_data log should show every cleaning stage your config expects,
# and confirm it targeted the REFINED table, never raw:
docker exec <worker> sh -c "cat '/opt/airflow/logs/dag_id=<dag_id>/run_id=<run_id>/task_id=refine.refine_data/attempt=1.log'" | grep -E "STEP|remapped|rows.*reload"
```

## 4. Duplicate-file skip

Re-trigger with the **identical, unmodified** file. Expect:
- `ingest.load_to_raw` log: `"Checksum already loaded — skipping."`
- Raw/target row counts **unchanged**.

This is the file-level dedup gate (`pipeline.file_load_registry`, keyed on
the raw table's checksum) — it runs before any raw write at all.

## 5. Accumulation (the core permanent-raw behavior)

Craft a **genuinely distinct** second batch — don't just reuse the same
rows, MD5 is content-based so an identical row set produces an identical
checksum regardless of filename:

```python
import csv
rows = list(csv.reader(open("<csv_path>")))
header, data = rows[0], rows[1:]
batch2 = data[:5]
for r in batch2:
    r[<id_col_index>] += "-B2"   # force distinct content
with open("<incoming_dir>/<file_name>", "w", newline="") as f:
    w = csv.writer(f); w.writerow(header); w.writerows(batch2)
```

Clear competing runs (step 2), trigger, poll, then verify:

```sql
SELECT COUNT(*) FROM raw."<...>_raw";  -- batch1 + batch2, NOT replaced
SELECT COUNT(DISTINCT _pipeline_run_id) FROM raw."<...>_raw";  -- == number of runs so far
SELECT COUNT(*) FROM "<target_schema>"."<target_table>";  -- grew by batch2's size only
```

## 6. Reload

```bash
curl -s -X POST http://localhost:<port>/api/v1/dags/<dag_id>/dagRuns \
  -H "Content-Type: application/json" -u "<user>:<pass>" -d '{"conf":{"reload": true}}'
```

No CSV file needed — reload bypasses ingest entirely. Expect in
`states-for-dag-run`: `ingest.sense_file`, `ingest.compute_checksum`,
`ingest.load_to_raw` all `skipped`; everything else `success`.

```sql
-- target should NOT grow by the full raw count — only by rows whose
-- checksum genuinely changed since their original load (see note below)
SELECT COUNT(*) FROM "<target_schema>"."<target_table>";
```

**Expected non-zero delta, not a bug:** if cleaning logic references sibling
rows in the same batch (e.g. this pipeline's village-based cross-referencing
step), reprocessing everything together on reload can improve a row's
cleaned value versus when it was originally processed in isolation. That
changes its `_row_checksum`, and since target reconciliation is
merge-by-checksum (not truncate-and-rebuild — a deliberate, agreed tradeoff
for this pipeline), the corrected row lands as an *additional* row rather
than replacing the old one. A handful of new rows on reload is expected;
target duplicating in full is not.

## 7. Cleanup

```bash
docker exec <scheduler> airflow dags list -d <dag_id>   # confirm paused=False
docker exec <worker> ls <incoming_dir>   # confirm no leftover test files
```

```sql
-- if metrics accumulated noise across several test iterations
DELETE FROM pipeline.etl_metrics WHERE dag_id = '<dag_id>' AND run_id NOT LIKE '%<today's date>%';
```

## Reference: common failure signatures

| Symptom | Cause |
|---|---|
| `refine.refine_data` logs "No rows in scope" but target still has data | Another run (usually scheduled) already loaded/cleaned it — check `states-for-dag-run` for a *different* `run_id` that actually did the work; you were watching the wrong run. |
| Manual trigger sits `queued` forever, task states all `None` | `max_active_runs=1` and something else is `running` — see step 2. |
| A DagRun makes zero task progress after you paused | Pausing blocks scheduling of that run's next tasks too — unpause. |
| `dw_load.load_to_dw` inserted 0 rows, `run_status=success` | Checksum-registry skip (step 4) or genuinely nothing new in scope — check `ingest.load_to_raw`'s log for "already loaded". |
| A cleanup/prep script's earlier `DELETE`/`UPDATE` didn't actually persist | A later statement in the *same* unclosed transaction failed and got `rollback()`'d — undoes everything before it too. Commit each logically-independent step separately. |
