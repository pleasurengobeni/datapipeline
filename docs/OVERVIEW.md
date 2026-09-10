# WASAC Analytics — What It Is and How It Works
### A Plain-English Guide for Everyone

---

## What Is This System?

WASAC Analytics is a **data pipeline platform**. Its job is simple:

> *Take data sitting in one database, clean it up, and copy it into another database — automatically, on a schedule, every minute if needed.*

Think of it like a postal service for data. Data is collected at many different places (point-of-sale terminals, billing systems, other databases), and this system picks it up, sorts it, checks it's correct, then delivers it to a central warehouse where it can be analysed and reported on.

Once data lands in the warehouse, dashboards update automatically so managers and analysts always see the latest numbers — without anyone having to manually export spreadsheets or run queries.

---

## The Big Picture

```
  Source Systems                   This Platform                    Analysts
  (where data lives)               (moves the data)                 (use the data)

  ┌─────────────────┐              ┌───────────────────────────┐    ┌─────────────────┐
  │  Billing DB     │──────────┐   │                           │    │  Dashboards     │
  │  (payments,     │          │   │   ┌───────────────────┐   │    │  (Metabase,     │
  │   invoices)     │          ├──►│   │   Data Pipelines  │   │───►│   Streamlit,    │
  ├─────────────────┤          │   │   │   (ETL Jobs)      │   │    │   Analytics)    │
  │  CMS Database   │──────────┤   │   └────────┬──────────┘   │    └─────────────────┘
  │  (customer mgmt)│          │   │            │               │
  ├─────────────────┤          │   │   ┌────────▼──────────┐   │    ┌─────────────────┐
  │  CSV Files      │──────────┘   │   │  Data Warehouse   │   │    │ Pipeline Monitor│
  │  (uploads,      │              │   │  (PostgreSQL)     │   │───►│ (is everything  │
  │   flat files)   │              │   └───────────────────┘   │    │  running OK?)   │
  └─────────────────┘              │                           │    └─────────────────┘
                                   └───────────────────────────┘
```

**ETL** stands for **Extract, Transform, Load** — the three steps every pipeline follows:
- **Extract** — read data from the source
- **Transform** — clean it, fix types, apply any rules
- **Load** — write it to the warehouse

---

## The Services — What Each One Does

The whole system runs inside **Docker containers** on a Linux server. Think of each container as a small computer inside the server, each with its own job.

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                        One Linux Server                         │
  │                                                                 │
  │  ┌─────────────┐  ┌─────────────┐  ┌────────────────────────┐  │
  │  │  Airflow    │  │  Web UI     │  │  Jenkins               │  │
  │  │  :8090      │  │  :5001      │  │  :9090                 │  │
  │  │             │  │             │  │                        │  │
  │  │  The        │  │  Pipeline   │  │  Auto-updates the      │  │
  │  │  Scheduler  │  │  Builder    │  │  platform when code    │  │
  │  │             │  │             │  │  changes on GitHub     │  │
  │  └──────┬──────┘  └─────────────┘  └────────────────────────┘  │
  │         │                                                        │
  │  ┌──────▼──────────────────────────────────────────────────┐    │
  │  │  PostgreSQL :55432                                       │    │
  │  │  Two databases inside:                                   │    │
  │  │    • airflow  — Airflow's own bookkeeping               │    │
  │  │    • (your DW) — the actual data warehouse              │    │
  │  └─────────────────────────────────────────────────────────┘    │
  │                                                                 │
  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
  │  │  Analytics  │  │  Pipeline   │  │  PgAdmin :5050          │ │
  │  │  :8501      │  │  Monitor    │  │                         │ │
  │  │             │  │  :8050      │  │  Web browser for        │ │
  │  │  Streamlit  │  │             │  │  querying Postgres      │ │
  │  │  dashboards │  │  Live run   │  │  directly               │ │
  │  │             │  │  health     │  └─────────────────────────┘ │
  │  └─────────────┘  └─────────────┘                              │
  └─────────────────────────────────────────────────────────────────┘
```

| Service | Web Address | What it does |
|---|---|---|
| **Airflow** | `:8090` | Runs and schedules all ETL jobs |
| **ETL Manager (Web UI)** | `:5001` | Point-and-click tool to create new pipelines |
| **Analytics** | `:8501` | Interactive charts on your warehouse data |
| **Pipeline Monitor** | `:8050` | Live dashboard — is every job succeeding? |
| **Jenkins** | `:9090` | Automatically deploys code updates from GitHub |
| **PgAdmin** | `:5050` | Browser-based Postgres query tool |
| **PostgreSQL** | `:55432` | The database engine powering everything |

---

## How a Data Pipeline is Created

You don't write code to create a pipeline. You use the **ETL Manager** web interface.

```
  You (in a browser)           ETL Manager            The System
       │                           │                      │
       │  1. Fill in a form:       │                      │
       │     - Source database     │                      │
       │     - Which table/SQL     │                      │
       │     - Target schema &     │                      │
       │       table name          │                      │
       │     - How often to run    │                      │
       │─────────────────────────► │                      │
       │                           │  2. Saves a JSON     │
       │                           │     config file      │
       │                           │─────────────────────►│
       │                           │                      │ 3. Generates the
       │                           │                      │    Airflow job
       │                           │                      │    (.py file)
       │                           │                      │
       │  4. New pipeline appears  │                      │
       │     in Airflow, ready     │◄─────────────────────│
       │     to run                │                      │
```

Every pipeline's settings live in a simple **JSON file** (like a form saved as text). Here is what one looks like:

```json
{
  "schedule_interval": "* * * * *",      ← run every minute
  "batch_size": 1000000,                 ← load 1 million rows at a time
  "source": {
    "source_db_conn_id": "billing_db",   ← which database to read from
    "incremental_column": "payment_date",← only load NEW data after this date
    "sql_file": "get_payments.sql"       ← the SQL query to run
  },
  "target": {
    "target_schema": "warehouse",        ← where to write the data
    "target_table": "payments"
  }
}
```

---

## How a Pipeline Runs — Step by Step

Every time a pipeline runs, it follows the same 8 steps. Airflow shows each step as a coloured box — green means success.

```
  ┌─────────┐
  │  START  │
  └────┬────┘
       │
       ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  STEP 1: extract_data                                            │
  │  "How much new data is there?"                                   │
  │  - Reads the source database                                     │
  │  - Finds all rows newer than the last time we ran               │
  │  - If batch_size is set, takes only the first N rows            │
  │  - Saves results to a temporary file (CSV)                      │
  │  - Counts exactly how many rows were extracted                  │
  └────┬─────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  STEP 2: branch_on_rows                                          │
  │  "Did we find any new data?"                                     │
  │  - If YES → continue to process_data                            │
  │  - If NO  → skip everything, mark job as done                   │
  └────┬─────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  STEP 3: process_data                                            │
  │  "Clean and prepare the data"                                    │
  │  - Applies column type conversions (e.g. text→date)             │
  │  - Applies any transformations defined in the config            │
  │  - Ensures columns match what the warehouse table expects       │
  └────┬─────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  STEP 4: load_data                                               │
  │  "Write the data to the warehouse"                               │
  │  - Uses PostgreSQL COPY (fastest bulk insert method)            │
  │  - Streams data in chunks of 50,000 rows at a time              │
  │  - Appends to the target table                                  │
  └────┬─────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  STEP 5: post_load_validate                                      │
  │  "Quick sanity check"                                            │
  │  - Checks no expected columns are missing                       │
  │  - Checks for unexpected nulls                                  │
  │  - Checks row count is greater than zero                        │
  └────┬─────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  STEP 6: source_target_count_check                               │
  │  "Did every row make it across?"                                 │
  │  - Counts rows in source  (what we said we'd copy)              │
  │  - Counts rows in target  (what actually arrived)               │
  │  - If they don't match → FAIL, pause the DAG, alert            │
  │  - If they match       → PASS                                   │
  └────┬─────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  STEP 7: collect_metrics                                         │
  │  "Record what happened"                                          │
  │  - Saves a row to pipeline.etl_metrics with:                    │
  │      how many rows loaded, how long it took,                    │
  │      what the watermark (last loaded date) is now               │
  └────┬─────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  STEP 8: cleanup                                                 │
  │  "Tidy up"                                                       │
  │  - Deletes the temporary CSV file                               │
  │  - Releases database resources                                  │
  └────┬─────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌─────────┐
  │   END   │
  └─────────┘
```

---

## The Three Types of Pipeline

### 1. Database → Database (most common)
Reads rows directly from another database using a SQL query, then writes them to the warehouse.

```
  Billing DB        This System                 Warehouse
  ┌──────────┐      ┌──────────────────────┐    ┌──────────────┐
  │ payments │──────► SQL query            │───►│ payments     │
  │ invoices │      │ (only NEW rows       │    │ (warehouse   │
  │ customers│      │  since last run)     │    │  copy)       │
  └──────────┘      └──────────────────────┘    └──────────────┘
```

Uses an **incremental column** (usually a date or ID) as a bookmark: "last time I loaded up to 2024-01-15, so today only load rows after that date."

### 2. File (CSV) → Database
Watches a folder for new CSV files, reads each file, and loads it into the warehouse. After loading, the file is moved to an archive folder.

```
  Folder: /data_dumps/incoming/
  ┌──────────────────────┐      ┌──────────────────────┐    ┌──────────┐
  │ report_2024_01.csv   │──────► Read CSV             │───►│ Warehouse│
  │ report_2024_02.csv   │      │ Clean columns        │    └──────────┘
  └──────────────────────┘      └──────────────────────┘
              │
              ▼ (after loading)
  Folder: /data_dumps/archive/

```

### 3. Spark → Database (large data)
Uses Apache Spark for very large datasets that are too big for the standard approach. Spark distributes the work across memory efficiently.

---

## The Watermark — How "Only New Data" Works

Every pipeline keeps a **watermark** — a bookmark of how far it has read. After each run, the bookmark advances.

```
  Timeline of data in source:

  Jan──Feb──Mar──Apr──May──Jun──Jul──Aug──...

  Run 1:  loaded up to ─────────────────► Mar 31
          Watermark saved: "2024-03-31"

  Run 2:  starts from ──────────────► Apr 1
          loads Apr + May
          Watermark saved: "2024-05-31"

  Run 3:  starts from ────────────────────────► Jun 1
          etc.
```

This means:
- Each run only loads **new** data — no duplicates, no re-loading old data
- If a run fails, the watermark doesn't advance — so next retry picks up from the same point
- Large tables (millions of rows) can be loaded gradually over many small runs using `batch_size`

---

## Automatic Updates with Jenkins

When a developer pushes code changes to GitHub, Jenkins notices within 5 minutes and automatically:

```
  Developer                   GitHub               Jenkins           Server
     │                           │                    │                │
     │  git push                 │                    │                │
     │──────────────────────────►│                    │                │
     │                           │                    │                │
     │                           │  (every 5 min)     │                │
     │                           │◄───────────────────│                │
     │                           │   any new commits? │                │
     │                           │────────────────────►                │
     │                           │       YES          │                │
     │                           │                    │  git pull      │
     │                           │                    │───────────────►│
     │                           │                    │  restart       │
     │                           │                    │  services      │
     │                           │                    │───────────────►│
     │                           │                    │                │ ✓ Updated
```

This means no one needs to SSH into the server to deploy updates. Push to GitHub → it happens automatically.

---

## The Pipeline Monitor Dashboard

The Pipeline Monitor (`:8050`) gives a live health view of all ETL jobs:

```
  ┌────────────────────────────────────────────────────────────────┐
  │  WASAC ETL Pipeline Monitor                      [auto-refresh]│
  ├────────────────┬───────────┬──────────────┬────────────────────┤
  │  Pipeline      │  Status   │  Last Run    │  Rows Loaded Today │
  ├────────────────┼───────────┼──────────────┼────────────────────┤
  │  cms.payment   │  ✅ OK    │  2 min ago   │  15,041            │
  │  billing.inv   │  ✅ OK    │  1 min ago   │  3,204             │
  │  meter.reads   │  ⚠️  WARN │  35 min ago  │  0                 │
  │  survey.data   │  ❌ FAIL  │  2h ago      │  —                 │
  └────────────────┴───────────┴──────────────┴────────────────────┘
```

It reads from the `pipeline.etl_metrics` table which every pipeline writes to after each run.

---

## Summary

| You want to... | You use... |
|---|---|
| Create a new data pipeline | ETL Manager at `:5001` |
| See all running/failed jobs | Airflow at `:8090` |
| Check if data is flowing | Pipeline Monitor at `:8050` |
| Explore the data | Analytics at `:8501` or PgAdmin at `:5050` |
| Deploy a code change | Just push to GitHub — Jenkins handles the rest |
| Start/stop all services | `make up` / `make down` on the server |
