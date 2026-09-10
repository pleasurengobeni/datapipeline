# Data Pipeline Platform — Demo Guide

---

## Introduction

This platform is a self-hosted, containerised data pipeline solution. It moves data from source databases and flat files into a target database, documents the structure of every table it loads, and makes that data immediately available for analytics — all managed through a set of web interfaces with no manual scripting required.

The platform was built using **WASAC** as the first client. The product itself is client-agnostic: names, connections, schemas, and branding are all configured through environment variables and config files, so the same codebase can be deployed for any organisation.

### What problem does it solve?

**Previous approach:**
- A person had to follow a long set of line-by-line manual instructions to install each component separately
- Pipeline config files had to be written by hand in a text editor — column names, data types, schedules, connection strings — all manually
- No central place to see what tables exist, what columns they contain, or whether pipelines are running correctly
- Every new data source required developer involvement

**This platform:**
- One command (`bash deploy.sh`) installs and starts the full stack
- Pipelines are created through a web form — no manual config file editing
- Table structure is defined in the ETL Manager and stored automatically in the pipeline config; no DDL scripts to write
- A data catalog page documents every table and column in one place
- Any team member can trigger, monitor, and explore data without touching the command line

---

## Components

```
┌──────────────────────────────────────────────────────────────────────┐
│                            Server                                    │
│                                                                      │
│  ┌───────────────┐  ┌──────────────┐  ┌──────────────┐  ┌────────┐  │
│  │  ETL Manager  │  │   Airflow    │  │  Pipeline    │  │Streamlit│ │
│  │  (Flask)      │  │ (Scheduler)  │  │  Monitor     │  │Analytics│ │
│  │  :5001        │  │  :8090       │  │  (Dash)      │  │ :8501  │  │
│  │               │  │              │  │  :8050       │  │        │  │
│  └──────┬────────┘  └──────┬───────┘  └──────────────┘  └────────┘  │
│         │ writes config    │ runs DAGs                               │
│         ▼                  ▼                                         │
│  ┌────────────────────────────────────┐   ┌──────────────────────┐  │
│  │        dags/  (shared volume)      │   │    Jenkins CI/CD     │  │
│  │  config/  etl/  templates/  sql/   │   │    :9090             │  │
│  └────────────────────────────────────┘   └──────────────────────┘  │
│                                                                      │
│  ┌──────────┐  ┌──────────┐  ┌────────────────────────────────────┐ │
│  │PostgreSQL│  │  Redis   │  │          data_dumps/               │ │
│  │ :55432   │  │ (broker) │  │    incoming/        archive/       │ │
│  └──────────┘  └──────────┘  └────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
```

| Component | Port | Role |
|---|---|---|
| **ETL Manager** | 5001 | Web UI — create pipelines, define table structure, document columns |
| **Apache Airflow** | 8090 | Schedules and runs all pipeline DAGs |
| **Pipeline Monitor** | 8050 | Live dashboard — rows loaded, data quality, failure tracking |
| **Streamlit Analytics** | 8501 | Explore loaded tables, AI-assisted chart generation, custom SQL |
| **Jenkins** | 9090 | CI/CD — auto-deploys on every `git push` |
| **PostgreSQL** | 55432 | Airflow metadata DB + target data warehouse |
| **Redis** | internal | Celery task broker for Airflow workers |

### Two pipeline types

| Type | How it works |
|---|---|
| **File → DB** | Drop a CSV or Parquet file into `data_dumps/incoming/`. The pipeline detects it, loads it, and archives the file. |
| **DB → DB** | Write a SQL query in the ETL Manager. The pipeline pulls the result from the source database and loads it into the target — incrementally or as a full reload. |

### How table structure is defined

Every pipeline is described by a JSON config file that is written automatically by the ETL Manager when you save a job. The config stores:
- Target schema and table name
- The column list discovered from the file or SQL query
- Per-column data type overrides
- The data dictionary — a description for each column used by the catalog and analytics views

The target table is **created automatically on the first run**. There are no DDL scripts to write or maintain. Five pipeline metadata columns are added to every table: `_pipeline_inserted_at`, `_pipeline_run_id`, `_source_system`, `_loaded_at`, `_row_checksum`.

---

## Installation (Reference)

<details>
<summary>Expand for full server setup steps</summary>

### Prerequisites

- Ubuntu 22.04 LTS or 20.04
- 4 GB RAM minimum, 8 GB recommended
- 20 GB disk space
- Ports open: `22`, `5001`, `8090`, `8050`, `9090`, `55432`

### Step 1 — Clone and deploy

```bash
git clone https://github.com/pleasurengobeni/datapipeline.git ~/datapipeline/airflow
cd ~/datapipeline/airflow
bash deploy.sh
```

`deploy.sh` installs Docker, creates required directories, sets file ownership, and starts all containers in one step.

### Step 2 — Configure `~/.airflow`

All credentials and paths live in `~/.airflow` on the server — never committed to git.

```bash
export AIRFLOW_UID=1000
export POSTGRES_USER="airflow"
export POSTGRES_PASSWORD="change_me"
export _AIRFLOW_WWW_USER_USERNAME="admin"
export _AIRFLOW_WWW_USER_PASSWORD="change_me"
export WEBUI_ADMIN_USER="admin"
export WEBUI_ADMIN_PASS="change_me"
export AIRFLOW__CORE__FERNET_KEY="<generate with Fernet.generate_key()>"
export DATA_DUMP_PATH_HOST="/path/to/data_dumps"
```

### Step 3 — Register Airflow Variables (one-time)

In Airflow → **Admin → Variables**, create:

| Key | Example Value |
|---|---|
| `environment` | `dev` |
| `modules_path` | `/opt/airflow/dags/modules` |
| `config_path` | `/opt/airflow/dags/config` |
| `data_dump` | `/opt/airflow/data_dump` |
| `metrics_db_conn_id` | `datawh_con` |

### Step 4 — Register database connections

In Airflow → **Admin → Connections**, add a connection for each source and target database.

</details>

---

## Demo Walkthrough

---

### Part 0 — Before We Start

What needs to be in place before creating pipelines:

**1. Airflow connection for the target database**

Log into Airflow → **Admin → Connections** → **+** and register the connection that pipelines will load data into.

| Field | Value |
|---|---|
| Connection ID | `datawh_con` (or any name — you'll enter this in the ETL Manager) |
| Connection Type | `Postgres` |
| Host | your database host |
| Schema | your target database name |
| Login / Password | credentials |
| Port | `5432` |

**2. File drop folder is ready**

For file-based pipelines the data_dumps folder must be accessible to the Airflow worker. Check:

```bash
ls /path/to/data_dumps/incoming/
```

Files placed here are picked up automatically when a pipeline is triggered.

**3. File naming — important rules**
- The filename set in the pipeline config must match the file placed in `incoming/` **exactly**
- Do not rename or change the file after the pipeline is configured — the pipeline looks for that specific name
- Once loaded, the file is moved to `archive/` automatically; drop a new file with the same name for the next load

**4. Source database connection (DB-to-DB pipelines only)**

If demoing a DB-to-DB pipeline, also register a connection for the source database in Airflow → **Admin → Connections**.

---

### Part 1 — ETL Manager

**URL:** `http://<server>:5001` · **Login:** `admin` / `admin123`

---

#### 1.1 — File-Based Pipeline

> Goal: configure a pipeline that loads a flat file into the database.

1. Open the ETL Manager → click **File Jobs** → **New File Job**
2. In the **File Source** section:
   - Type the filename into the search box — the form reads the file from the server and detects columns automatically
   - Confirm the file format (CSV or Parquet), delimiter, and whether it has a header row
   - Review the column list and data type suggestions; adjust any types that need to change
3. In the **Target** section: set Connection ID, Target Schema, Target Table
4. Set a **Schedule** (e.g. `@daily`, or a cron expression)
5. In the **Data Dictionary** section:
   - Each column appears as a row — add a plain-language description for each
   - Use **Suggest with AI** to auto-fill descriptions (only column names are sent to the LLM — never data values)
   - Use **AI Suggest Types** to infer data types from column names
   - Add a table-level description in the text field at the top
6. Click **Save**

> The ETL Manager writes a JSON config file. Within 5 minutes the `dag_builder` Airflow DAG reads that config and generates the pipeline DAG automatically.

**What gets saved in the config:**
- Column names, data type overrides, and the full data dictionary
- Source path, file format, delimiter, archive settings
- Target connection, schema, and table name
- Schedule interval

---

#### 1.2 — DB-to-DB Pipeline

> Goal: configure a pipeline that pulls data from a source database using a SQL query.

1. Open the ETL Manager → click **Jobs** → **New Job**
2. In the **Source** section:
   - Select the **Connection ID** (source database in Airflow)
   - Select the **DB Type** (`postgresql`, `mysql`, or `mssql`)
   - Write the **SQL Query** — any `SELECT` statement; the column list comes directly from what the query returns
   - Optionally set an **HWM Column** (incremental timestamp or integer column) for incremental loads, or leave blank for a full reload each run
3. In the **Target** section: set Connection ID, Target Schema, Target Table
4. Set a **Schedule**
5. Fill in the **Data Dictionary** section as in the file pipeline demo
6. Click **Save**

> The target table is created automatically on the first run — no DDL needed.

---

#### Data Catalog

After saving pipelines, open the **Catalog** page (link in the top navigation).

- Every pipeline is listed with its schema, table name, connection, and column count
- Click any table card to expand its full data dictionary — column names, types, and descriptions
- Use the search bar to filter across table names, column names, and descriptions
- **Join suggestions** — columns that appear in more than one table are surfaced automatically as potential join keys

> This page is the living documentation for the data warehouse. GitHub Copilot can read the data dictionary from this page to understand what tables and columns are available before writing analytics queries or generating dashboards.

---

### Part 2 — Airflow

**URL:** `http://<server>:8090` · **Login:** `admin`

---

#### 2.1 — Confirm the DAG Appeared

After saving a pipeline in the ETL Manager, go to Airflow:

1. Open the **DAGs** list
2. Find the new DAG — it should appear within 5 minutes (the `dag_builder` DAG runs every 5 minutes)
3. If it is not there yet, click the `dag_builder` DAG → **Trigger** to force an immediate regeneration
4. Un-pause the new DAG using the toggle switch on the left

> Every pipeline DAG is a generated `.py` file. You never write or edit these files manually. The source of truth is always the JSON config in `dags/config/`.

---

#### 2.2 — Trigger the Pipeline and Monitor Progress

1. Click the **▶ (Trigger)** button on the DAG row to start a manual run
2. Click the DAG name to open the detail view
3. Switch to **Grid view** to see the run history — each task shows as a coloured box:
   - Green = success, Red = failed, Yellow = running, Grey = skipped
4. Click any coloured box → **Log** tab to see the full task output including rows processed

**What the task graph looks like for a file pipeline:**
```
start → wait_for_file → create_staging → load_to_staging
      → branch_on_rows
            ├─ promote_to_target → archive_file → collect_metrics → end
            └─ no_new_rows ─────────────────────────────────────→ end
```

**For a DB pipeline:**
```
start → create_target → extract_data → branch_on_rows
              ├─ process_data → load_data → collect_metrics → end
              └─ no_new_rows ──────────────────────────────→ end
```

---

### Part 3 — Pipeline Monitor

**URL:** `http://<server>:8050`

The monitor reads from `pipeline.etl_metrics` — a table that is auto-created and updated at the end of every pipeline run. It does not write to any pipeline. Auto-refreshes every 30 seconds.

**Four tabs:**

| Tab | What it shows |
|---|---|
| **Summary View** | KPI cards (tables monitored, total runs, failures today, overall % loaded); per-table summary with row counts, last run status, average load interval, average run duration |
| **Detailed Analytics** | Volume breakdown by engine type (pandas vs Spark) and source type (file vs DB); run trends over time |
| **Data Quality** | Duplicate check — rows are grouped by `_row_checksum`; any checksum appearing more than once is flagged as a potential duplicate; shown as a bar chart and detail table |
| **Failures & Errors** | Failed runs with table name, timestamp, and error detail; failure trend chart; top-10 slowest runs |

Walk through each tab and point out:
- Which table was just loaded and how many rows
- The % loaded metric (source rows vs target rows)
- The Data Quality tab — confirm no duplicates from the load just triggered

---

### Part 4 — Streamlit Analytics + GitHub Copilot

**URL:** `http://<server>:8501`

---

#### 4.1 — Explore a Loaded Table

1. In the sidebar, select the pipeline that was just loaded
2. **Overview tab** — review the data dictionary (column names, types, descriptions pulled from the config), the pipeline config summary, and the live data preview
3. **AI Charts tab** — click **Suggest Charts with AI**:
   - The app sends column names, types, and descriptions to the LLM
   - No data values are ever sent
   - Charts are rendered in a two-column grid
4. **Explore tab** — write a custom SQL query against the loaded table, run it, and use Quick Chart to visualise the result

---

#### 4.2 — GitHub Copilot: Security Review

Before using Copilot to build anything on top of the data, ask it to review the codebase for security concerns. Suggested prompt:

> "Review this ETL platform codebase for OWASP Top 10 security issues. Focus on the Flask web UI, SQL query handling, file upload paths, and any places where user input reaches the database."

Points Copilot should identify:
- SQL injection risk if raw user SQL is passed to the database without parameterisation
- Path traversal risk if file paths from config are not validated
- Authentication and session management on the Flask app
- Secrets management — are credentials exposed in logs or error messages?

---

#### 4.3 — GitHub Copilot: Build a Dashboard from the Data

1. Open the ETL Manager → **Catalog** and show Copilot the data dictionary for the table that was just loaded
2. Ask Copilot to read the table structure and suggest what analytics are possible
3. Then ask:

> "Using the data dictionary from the catalog page, write a Streamlit dashboard that connects to the `<schema>.<table>` table and builds meaningful charts. Use the column descriptions to decide what to visualise."

Copilot will use the column names and descriptions from the data dictionary to produce context-aware dashboard code rather than generic charts.

---

### Part 5 — Load Additional Data + Predictive Analytics with Copilot

1. Drop additional data files into `data_dumps/incoming/` and trigger the pipeline again — or create a second pipeline for a different table
2. Once multiple tables are loaded, show the **Catalog** join suggestions — columns shared across tables that can be used to combine data
3. Ask GitHub Copilot:

> "Given the tables and column descriptions in the data catalog, what predictive or ML analysis would be meaningful here? Write a Python script using scikit-learn or statsmodels that runs against this data."

Copilot should:
- Identify numeric columns suitable for regression or clustering
- Identify timestamp columns suitable for time-series forecasting
- Suggest and generate starter code for a relevant model
- Flag any data quality issues (missing values, class imbalance) it notices from the column descriptions

---

## Day-to-Day Operations

```bash
# Start everything
make up

# Stop everything
make down

# Restart one service
make restart-service s=web-ui

# Follow all logs
make logs

# Show container status
make ps
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| DAG not appearing in Airflow | Wait up to 5 min or manually trigger `dag_builder`; check its logs |
| `Permission denied` on `data_dumps/` | `sudo chown -R $(id -u):$(id -g) data_dumps/` |
| File pipeline not detecting file | Confirm filename in config matches the file in `incoming/` exactly |
| Target table has wrong columns | Drop the table and re-trigger — `create_target` recreates it from config |
| Pipeline Monitor shows no data | Run at least one pipeline first so `pipeline.etl_metrics` is populated |
| Streamlit shows "No pipelines found" | Confirm `dags/config/` is mounted and contains at least one `.json` file |
| `No module named ...` in DAG | Restart airflow-worker: `make restart-service s=airflow-worker` |
