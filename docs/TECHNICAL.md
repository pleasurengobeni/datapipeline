# WASAC Analytics — Technical Reference
### Installation, Architecture, Configuration & Troubleshooting

---

## Table of Contents

1. [Architecture Deep Dive](#1-architecture-deep-dive)
2. [Prerequisites](#2-prerequisites)
3. [Installation — Fresh Linux Server](#3-installation--fresh-linux-server)
4. [Environment Variables Reference](#4-environment-variables-reference)
5. [The Template & Code-Generation System](#5-the-template--code-generation-system)
6. [Pipeline Config JSON Reference](#6-pipeline-config-json-reference)
7. [The ETL Task Graph — Internals](#7-the-etl-task-graph--internals)
8. [Airflow Connections & Variables](#8-airflow-connections--variables)
9. [Makefile Commands Reference](#9-makefile-commands-reference)
10. [Jenkins CI/CD Pipeline](#10-jenkins-cicd-pipeline)
11. [Troubleshooting](#11-troubleshooting)
12. [Tests](#12-tests)

---

## 1. Architecture Deep Dive

### Services and Ports

| Container | Port | Technology | Role |
|---|---|---|---|
| `airflow-webserver` | `8090` | Apache Airflow 2.6 | DAG UI, API |
| `airflow-scheduler` | — | Airflow CeleryExecutor | Schedules task runs |
| `airflow-worker` | — | Airflow Celery Worker | Executes tasks |
| `airflow-triggerer` | — | Airflow Triggerer | Async sensor operators |
| `postgres` | `55432` | PostgreSQL 13 | Airflow metadata + ETL metrics |
| `redis` | `6379` | Redis | Celery message broker |
| `web-ui` | `5001` | Flask | No-code ETL pipeline builder |
| `analytics` | `8501` | Streamlit | AI-powered data dashboards |
| `pipeline-monitor` | `8050` | Streamlit | Live ETL run health dashboard |
| `jenkins` | `9090` | Jenkins | CI/CD, auto-deploy on git push |
| `pgadmin` | `5050` | PgAdmin 4 | Web Postgres query tool |

### Data Flow

```
  Source DB / CSV File
         │
         │ SQL query / file read
         ▼
  ┌──────────────────────────────────────────────────────┐
  │  Airflow Worker (inside Docker)                      │
  │                                                      │
  │  1. extract_data                                     │
  │     └─ SELECT * FROM source WHERE HWM > last_run    │
  │        LIMIT batch_size                              │
  │        + UNION ALL tie-boundary rows at batch_max    │
  │        → writes to /tmp/<dag_id>_<run_id>.csv        │
  │        → XCom: {row_count, batch_max, source_count_  │
  │                 snapshot, last_loaded_value}         │
  │                                                      │
  │  2. process_data                                     │
  │     └─ reads CSV → applies dtype_overrides           │
  │        → applies transformations                     │
  │        → adds _pipeline_* metadata columns          │
  │        → writes back to same CSV                    │
  │                                                      │
  │  3. load_data                                        │
  │     └─ PostgreSQL COPY (streaming, 50k chunks)       │
  │        → INSERT INTO target.schema.table            │
  │                                                      │
  │  4. post_load_validate                               │
  │     └─ column presence check                        │
  │        null check on critical columns               │
  │        target row count > 0                         │
  │                                                      │
  │  5. source_target_count_check                        │
  │     └─ source COUNT(*) WHERE HWM <= batch_max        │
  │        vs target COUNT(*) WHERE HWM <= batch_max     │
  │        uses source_count_snapshot (race-safe)        │
  │        raises ValueError + pauses DAG on mismatch   │
  │                                                      │
  │  6. collect_metrics                                  │
  │     └─ INSERT INTO pipeline.etl_metrics             │
  │                                                      │
  │  7. cleanup                                          │
  │     └─ DELETE /tmp/<dag_id>_<run_id>.csv            │
  └──────────────────────────────────────────────────────┘
         │
         ▼
  Target DB (Data Warehouse)
```

### Incremental Loading — How the Watermark Works

Each run stores a High Watermark (HWM) — the MAX value of the incremental column from the last successful batch.

```
  Airflow metadata DB: dag_run.conf  +  XCom
  ─────────────────────────────────────────────
  Key: "last_loaded_value"
  Value: "2024-03-31" (date) or 12345678 (integer ID)

  Next run WHERE clause:
    CAST("transaction_date" AS TIMESTAMP) > '2024-03-31'
    ORDER BY CAST("transaction_date" AS TIMESTAMP)
    LIMIT 1000000
```

**Boundary handling**: at `batch_size` cut-off, rows with `HWM = batch_max` may be split across batches. The query:
```sql
SELECT * FROM _etl_batch_tmp WHERE HWM < batch_max
UNION ALL
SELECT * FROM source WHERE HWM = batch_max
```
ensures all tied boundary rows are always included exactly once.

---

## 2. Prerequisites

| Requirement | Minimum | Recommended |
|---|---|---|
| OS | Ubuntu 22.04 LTS | Ubuntu 24.04 LTS |
| RAM | 4 GB | 8 GB |
| CPU | 2 cores | 4 cores |
| Disk | 20 GB | 50 GB |
| Docker | 24+ | latest |
| Docker Compose | v2 | v2 |
| Python | 3.8 | 3.10 |
| Internet | Required | Required |

---

## 3. Installation — Fresh Linux Server

### Step 1 — Create project directory

```bash
mkdir -p ~/datapipeline/airflow
cd ~/datapipeline/airflow
```

### Step 2 — Install Docker

```bash
sudo apt-get update -y && sudo apt-get upgrade -y
sudo apt-get install -y ca-certificates curl gnupg lsb-release

sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io \
  docker-buildx-plugin docker-compose-plugin

sudo systemctl enable docker && sudo systemctl start docker
sudo usermod -aG docker $USER
newgrp docker
docker run hello-world   # verify
```

### Step 3 — SSH key for GitHub

```bash
ssh-keygen -t ed25519 -C "wasac-server" -f ~/.ssh/github_wasac -N ""
cat ~/.ssh/github_wasac.pub   # copy this to GitHub → Settings → SSH keys

cat >> ~/.ssh/config << 'EOF'
Host github.com
  IdentityFile ~/.ssh/github_wasac
  StrictHostKeyChecking no
EOF

ssh -T git@github.com   # should say: "Hi pleasurengobeni! You've successfully authenticated..."
```

### Step 4 — Clone the repo

```bash
cd ~/datapipeline/airflow
git clone git@github.com:pleasurengobeni/datapipeline.git .
```

### Step 5 — Create `~/.airflow` (secrets file)

Generate required keys first:

```bash
# Fernet key (Airflow encryption)
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Secret keys (run twice — one for Airflow webserver, one for Web UI)
python3 -c "import secrets; print(secrets.token_hex(32))"
python3 -c "import secrets; print(secrets.token_hex(32))"
```

Create the file (see Section 4 for all variables):

```bash
nano ~/.airflow
# paste the template from Section 4 and fill in real values
```

### Step 6 — Run the installer

```bash
bash install.sh
```

The installer:
1. Validates `~/.airflow` is present and all required vars are set
2. Creates `dags/`, `logs/`, `plugins/`, `data_dumps/` directories
3. Generates `.env` from `~/.airflow` values
4. Runs `docker compose up -d`
5. Waits for Airflow to be healthy
6. Creates the Airflow admin user
7. Sets all `AIRFLOW_VAR_*` Airflow Variables via the Airflow API

### Step 7 — Create Airflow DB connections

In the Airflow UI at `http://<server-ip>:8090`:

1. Admin → Connections → `+`
2. Create a connection for each source/target database:

| Field | Value |
|---|---|
| Connection Id | e.g. `wasac_dw_cenfri_db` |
| Connection Type | `Postgres` |
| Host | your DB host |
| Schema | your database name |
| Login | your DB user |
| Password | your DB password |
| Port | `5432` |

### Step 8 — Verify

```bash
make check   # shows git sync status, credential check, container health
make ps      # shows all running containers
```

---

## 4. Environment Variables Reference

All variables live in `~/.airflow` on the server. **Never committed to git.**

```bash
# ── Project identity ───────────────────────────────────────────
export PROJECT_NAME="datapipeline"
export PROJECT_SERVER_IP="<your-server-ip>"
export SERVER_SSH_USER="ubuntu"
export SERVER_SSH_KEY_PATH="$HOME/.ssh/id_rsa"
export PROJECT_GITHUB_REPO_SSH="git@github.com:pleasurengobeni/datapipeline.git"

# ── Docker paths ───────────────────────────────────────────────
export AIRFLOW_PROJ_DIR="$HOME/datapipeline/datapipeline"
export AIRFLOW_UID=$(id -u)
export AIRFLOW_GID=$(id -g)

# ── PostgreSQL — Airflow metadata DB ──────────────────────────
export POSTGRES_USER="airflow"
export POSTGRES_PASSWORD="<password>"

# ── PostgreSQL — Data warehouse ────────────────────────────────
export POSTGRES_DATA_USER="<dw_user>"
export POSTGRES_DATA_PWD="<dw_password>"
export POSTGRES_DATA_HOST="<dw_host>"
export POSTGRES_DATA_PORT=5432
export POSTGRES_DATA_DB="<dw_database>"

# ── Pipeline Monitor dashboard DB ─────────────────────────────
export METRICS_DB_USER="<user>"
export METRICS_DB_PASS="<password>"
export METRICS_DB_HOST="<host>"
export METRICS_DB_PORT=5432
export METRICS_DB_NAME="<database>"

# ── PgAdmin ───────────────────────────────────────────────────
export PGADMIN_DEFAULT_EMAIL="admin@example.com"
export PGADMIN_DEFAULT_PASSWORD="<password>"

# ── Airflow core ──────────────────────────────────────────────
export AIRFLOW__CORE__FERNET_KEY="<generated-fernet-key>"
export AIRFLOW__WEBSERVER__SECRET_KEY="<generated-hex-32>"
export _AIRFLOW_WWW_USER_USERNAME="admin"
export _AIRFLOW_WWW_USER_PASSWORD="<airflow-ui-password>"

# ── ETL Manager (Web UI) ──────────────────────────────────────
export WEBUI_SECRET_KEY="<generated-hex-32>"
export WEBUI_ADMIN_USER="admin"
export WEBUI_ADMIN_PASS="<webui-password>"

# ── Airflow Variables (paths inside container) ────────────────
export AIRFLOW_VAR_ENVIRONMENT="prod"
export AIRFLOW_VAR_DAG_HOME="/opt/airflow/dags"
export AIRFLOW_VAR_MODULES_PATH="/opt/airflow/dags"
export AIRFLOW_VAR_CONFIG_PATH="/opt/airflow/dags/config"
export AIRFLOW_VAR_SQL_PATH="/opt/airflow/dags/sql"
export AIRFLOW_VAR_ETL_PATH="/opt/airflow/dags/etl"
export AIRFLOW_VAR_TEMPLATE_PATH="/opt/airflow/dags/templates"
export AIRFLOW_VAR_DATA_DUMP="/opt/airflow/data_dump"

# ── AI API keys (optional) ────────────────────────────────────
export GOOGLE_AI_API_KEY=""      # https://aistudio.google.com/app/apikey
export GROQ_API_KEY=""           # https://console.groq.com/keys
export MISTRAL_API_KEY=""        # https://console.mistral.ai/api-keys
```

---

## 5. The Template & Code-Generation System

### How it works

Pipeline `.py` files (Airflow DAGs) are **generated**, not hand-written. The source of truth is:

```
dags/templates/data_load.template   ← master template (tracked in git)
dags/config/<conn_id>/<dag_id>.json ← per-pipeline config  (gitignored)
dags/sql/<conn_id>/<dag_id>.sql     ← source SQL query     (gitignored)
                    │
                    ▼
scripts/recreate_etl_dags.py        ← generator script
                    │
                    ▼
dags/etl/<conn_id>/<dag_id>.py      ← generated DAG        (gitignored)
```

### Running the generator

```bash
# Inside the project root (on the server, from the Airflow container, or locally)
python scripts/recreate_etl_dags.py

# Output:
#   [OK]  db_wasac_dw_cenfri_db_cms_payment  →  dags/etl/cenfri_db/...py
#   [OK]  db_wasac_dw_cenfri_db_invoice      →  dags/etl/cenfri_db/...py
#   Done: 47 DAG(s) generated, 0 error(s)
```

Or from within the Airflow container:

```bash
docker exec -it $(docker ps --filter name=airflow-worker --format '{{.Names}}' | head -1) \
  python /opt/airflow/dags/../scripts/recreate_etl_dags.py
```

### Template structure

The template is a valid Python file with `<dag_name>` as a placeholder. The generator:
1. Reads each JSON config from `dags/config/`
2. Replaces `<dag_name>` with the actual DAG ID
3. Writes the result to `dags/etl/<conn_id>/<dag_id>.py`

---

## 6. Pipeline Config JSON Reference

Full annotated example:

```json
{
  "dag_id": "db_wasac_dw_cenfri_db_cms_payment",
  "schedule_interval": "* * * * *",     // cron: every minute
  "start_date": "2023-01-01",
  "batch_size": 1000000,                 // rows per run (0 = no limit)
  "read_chunk_size": 50000,              // pandas read chunk size
  "write_mode": "append",                // append | replace

  "source": {
    "dev": {                             // environment key: dev | prod
      "source_db_conn_id": "cenfri_db", // Airflow connection ID
      "db_type": "postgresql",           // postgresql | mssql | mysql
      "incremental_column": "transaction_date",
      "incremental_column_datatype": "date",   // date | timestamp | integer | text
      "incremental_column_date_format": "YYYY-MM-DD",   // PostgreSQL format string
      "batch_size": 1000000,
      "full_load": false,                // true = ignore watermark, reload all
      "sql_file": "cenfri_db/cms_payment.sql",

      "dtype_overrides": {              // force column types after extraction
        "transaction_date": "date",
        "amount": "decimal",
        "customer_id": "text"
      },
      "dtype_override_formats": {       // format hints for text date columns
        "transaction_date": "YYYY-MM-DD"
      }
    },
    "prod": { ... }                     // same keys, different conn_id / values
  },

  "target": {
    "dev": {
      "target_db_conn_id": "wasac_dw",  // Airflow connection ID for target DB
      "db_type": "postgresql",
      "target_schema": "cms",           // PostgreSQL schema
      "target_table": "payment",        // table name (auto-created if missing)
      "hwm_column": "transaction_date",
      "load_strategy": "append"         // append | replace
    },
    "prod": { ... }
  },

  "data_dictionary": {                  // optional — used by Web UI & Analytics
    "transaction_date": {
      "description": "Date of the payment transaction",
      "dtype": "date",
      "date_format": "YYYY-MM-DD"
    }
  },

  "table_description": "Customer payment records from CMS.",
  "tags": ["WASAC", "ETL", "CMS"],
  "created_at": "2026-01-15 10:00:00",
  "last_modified": "2026-05-27 15:33:24"
}
```

### Supported `db_type` values

| Value | Notes |
|---|---|
| `postgresql` | Native COPY support, fastest |
| `mssql` | SQL Server — requires JDBC driver |
| `mysql` | MySQL/MariaDB |

### Supported `incremental_column_datatype` values

| Value | Behaviour |
|---|---|
| `date` | `CAST("col" AS TIMESTAMP) > 'YYYY-MM-DD'` |
| `timestamp` | `"col" > 'YYYY-MM-DD HH:MI:SS'` |
| `integer` | `CAST("col" AS BIGINT) > 12345` |
| `text` | `CAST("col" AS TIMESTAMP) > 'value'` — auto-detects format |
| *(omitted)* | Auto-detected from `information_schema.columns` |

---

## 7. The ETL Task Graph — Internals

### XCom payload from `extract_data`

```python
{
  "row_count": 1007313,              # rows written to CSV
  "batch_max": "2022-10-14",         # MAX HWM value in this batch
  "last_loaded_value": "2022-10-14", # new watermark to save
  "source_count_snapshot": 1007313,  # COUNT(*) at extract time (race-safe)
}
```

### Boundary-inclusive SQL (why it matters)

Without the boundary fix, rows at exactly `batch_max` are split:
- Run 1: WHERE HWM > '2022-10-13' LIMIT 1000000 → gets rows up to 2022-10-14, but some rows on 2022-10-14 are cut off by LIMIT
- Next run: WHERE HWM > '2022-10-14' → **skips remaining 2022-10-14 rows entirely**

The fix:

```sql
SELECT * FROM _etl_batch_tmp WHERE HWM < '2022-10-14'
UNION ALL
SELECT * FROM source WHERE HWM = '2022-10-14'
```

This replaces the temp table's boundary rows with ALL source rows at that value.

### `source_target_count_check`

```
source_count_snapshot  (taken at extract time, inside same DB connection)
        ↓
   compared against
        ↓
target COUNT(*) WHERE HWM <= batch_max

  PASS: counts match → advance watermark
  FAIL: counts differ → raise ValueError, pause DAG, log gap
```

Race-safety: `source_count_snapshot` is captured inside `extract_data`'s open connection before it closes — it sees the same snapshot isolation as the extract query. A live re-query could include rows inserted after extraction started, causing false alarms.

### `pipeline.etl_metrics` columns

| Column | Type | Description |
|---|---|---|
| `dag_id` | text | Pipeline name |
| `schema` | text | Source schema/system |
| `table_name` | text | Target table |
| `run_id` | text | Airflow run_id |
| `run_start` | timestamptz | Task start time |
| `run_end` | timestamptz | Task end time |
| `run_duration_secs` | integer | Duration |
| `run_status` | text | `success` \| `failed` |
| `error_msg` | text | Exception message on failure |
| `batch_size` | integer | Configured batch_size |
| `rows_loaded_now` | integer | Rows loaded in this run |
| `target_total_records` | bigint | Total rows in target |
| `target_today_records` | integer | Rows with today's load date |
| `source_total_records` | bigint | Total rows in source |
| `target_max_incremental` | text | MAX HWM in target |
| `source_max_incremental` | text | MAX HWM in source |
| `source_loaded_to_max_target` | bigint | Source count up to target HWM |
| `perc_loaded` | numeric | % of source copied to target |
| `engine_type` | text | `pandas` \| `spark` |
| `source_type` | text | `db` \| `file` |
| `created_at` | timestamptz | Row insert time |

---

## 8. Airflow Connections & Variables

### Required Airflow Variables

Set via UI (Admin → Variables) or auto-set by `install.sh`:

| Key | Example Value |
|---|---|
| `environment` | `prod` |
| `dag_home` | `/opt/airflow/dags` |
| `modules_path` | `/opt/airflow/dags` |
| `config_path` | `/opt/airflow/dags/config` |
| `sql_path` | `/opt/airflow/dags/sql` |
| `etl_path` | `/opt/airflow/dags/etl` |
| `template_path` | `/opt/airflow/dags/templates` |
| `data_dump` | `/opt/airflow/data_dump` |

### Naming convention for connection IDs

Connections in Airflow follow this pattern which matches the DAG naming convention:

```
<project>_<environment>_<system>
  e.g.  wasac_dw_cenfri_db
        wasac_dw_cms_prd
        local_dw_con
```

---

## 9. Makefile Commands Reference

```bash
make              # start all services (detached)
make up           # same as above
make down         # stop and remove containers
make restart      # down + up
make build        # rebuild Docker images without cache
make fresh        # nuclear: prune cache + full rebuild + up
make pull         # git pull + restart web-ui & analytics only (fastest deploy)
make logs         # follow all container logs
make ps           # show container status
make check        # verify git sync, credentials, container health, login
make demo         # print all service URLs

# Single-service operations:
make restart-service s=web-ui
make logs-service s=airflow-scheduler
make logs-service s=airflow-worker

# Rebuild & restart the pipeline monitor only:
make restart-pipeline-monitor
```

---

## 10. Jenkins CI/CD Pipeline

Jenkins polls GitHub every **5 minutes**. When it detects new commits on `main`:

```
  Jenkinsfile stages:
  ─────────────────────────────────────────────────────────
  1. Checkout
     └─ git pull on the server

  2. Build (conditional)
     └─ only if Dockerfile or requirements.txt changed
        docker compose build --no-cache

  3. Restart services (conditional)
     └─ if .env or ~/.airflow changed:
          docker compose down && docker compose up -d
        else:
          docker compose restart web-ui analytics \
            pipeline-monitor airflow-webserver \
            airflow-scheduler airflow-worker
  ─────────────────────────────────────────────────────────
```

Jenkins admin password is set via `JENKINS_ADMIN_PASSWORD` in `~/.airflow`. The initial Groovy seed script (`jenkins/init.groovy`) configures it on first start.

---

## 11. Troubleshooting

### Container won't start

```bash
make logs                          # see all logs
make logs-service s=airflow-worker # specific service
docker compose ps                  # check health status
```

Common causes:
- `~/.airflow` missing or has empty required variables → run `make check`
- Port already in use → `sudo lsof -i :8090` then kill the process
- Insufficient RAM → Airflow needs at least 4 GB; check `free -h`

---

### DAG not appearing in Airflow UI

```bash
# Check if the .py file was generated
ls dags/etl/<conn_id>/

# Regenerate from template
python scripts/recreate_etl_dags.py

# Check for syntax errors in the generated file
docker exec -it $(docker ps --filter name=airflow-scheduler --format '{{.Names}}' | head -1) \
  python /opt/airflow/dags/etl/<conn_id>/<dag_id>.py

# Check Airflow DAG import errors
docker exec -it $(docker ps --filter name=airflow-scheduler --format '{{.Names}}' | head -1) \
  airflow dags list-import-errors
```

---

### `source_target_count_check` failing (count mismatch)

Check the task log first:

```bash
# In the project root:
cat logs/dag_id=<dag_id>/run_id=<run_id>/task_id=source_target_count_check/attempt=1.log \
  | grep -E "snapshot|source_count|target_count|Gap|mismatch"
```

| Symptom | Cause | Fix |
|---|---|---|
| Gap = small positive (e.g. 12) | Source has exact-duplicate rows — shouldn't happen | Investigate source data; duplicates are preserved by UNION ALL |
| Gap = large positive | Rows not loaded (load_data failed partway) | Re-run after fixing load error |
| Gap = negative | More rows in target than source | Target has duplicates from a double-load; truncate and re-run |
| Gap increases each run | New rows added to source in same HWM window mid-run | Review source write timing vs pipeline schedule |

To manually unpause a DAG after resolving:

```bash
docker exec -it $(docker ps --filter name=airflow-webserver --format '{{.Names}}' | head -1) \
  airflow dags unpause <dag_id>
```

---

### Watermark stuck / pipeline re-loading same data

```bash
# Check current XCom values for a DAG
PGPASSWORD=airflow psql -h localhost -p 55432 -U airflow -d airflow \
  -c "SELECT key, value FROM xcom WHERE dag_id='<dag_id>' ORDER BY timestamp DESC LIMIT 5;"
```

To force a full reload (reset watermark):

1. In Airflow UI: Admin → Variables → find `<dag_id>_last_loaded` → delete it
2. Or: truncate the target table and re-trigger the DAG

---

### Airflow showing "Scheduler not running"

```bash
make restart-service s=airflow-scheduler
make logs-service s=airflow-scheduler
```

If it keeps crashing, increase `AIRFLOW__SCHEDULER__PARSING_PROCESSES` from 1 to 2 in `~/.airflow` then `make restart`.

---

### Database connection errors in tasks

```bash
# Test the connection from inside the worker
docker exec -it $(docker ps --filter name=airflow-worker --format '{{.Names}}' | head -1) \
  airflow connections test <conn_id>
```

Check the connection host — inside Docker, `localhost` refers to the container itself. Use `host.docker.internal` (Mac) or the actual server IP to reach databases running outside Docker.

---

### Jenkins not picking up changes

```bash
make logs-service s=jenkins
```

Common causes:
- Jenkins can't reach GitHub (SSH key issue) → check `jenkins/init.groovy` and test `ssh -T git@github.com` inside the Jenkins container
- Webhook firewall blocked → Jenkins uses polling (every 5 min) as fallback; no action needed

---

### Out of disk space

```bash
df -h                        # check disk usage
docker system df             # see Docker's usage
docker system prune -f       # remove unused images/containers/cache
docker volume ls             # check volumes
```

Log files under `logs/` are the usual culprit — they are gitignored but accumulate. Clean old DAG run logs:

```bash
find logs/ -name "*.log" -mtime +30 -delete   # delete logs older than 30 days
```

---

### Pipeline Monitor shows no data

Check the monitor's DB environment variables:

```bash
docker exec pipeline-monitor env | grep -E "DB_HOST|DB_NAME|DB_USER"
```

The monitor reads from `pipeline.etl_metrics` on the DB pointed to by `METRICS_DB_*` variables. If the schema or table doesn't exist:

```sql
CREATE SCHEMA IF NOT EXISTS pipeline;
-- The etl_metrics table is auto-created by the first successful pipeline run.
```

---

## 12. Tests

The test suite uses **pytest** and runs against a live local Postgres instance.

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run a specific test file
pytest web_ui/tests/test_incremental_etl.py -v

# Run tests matching a keyword
pytest -k "count_check" -v
```

Test configuration is in `pytest.ini`. Tests require the local Postgres at `localhost:5432` with credentials from the environment.

```bash
# Check how many tests pass
pytest --tb=no -q    # should show: 212 passed
```

Key test files:

| File | What it tests |
|---|---|
| `web_ui/tests/test_incremental_etl.py` | Watermark logic, batch boundary, count-check |
| `web_ui/tests/test_app.py` | Web UI routes and config creation |
| `web_ui/tests/test_utils_cast.py` | Type conversion and date parsing |
| `web_ui/tests/test_metadata_columns.py` | `_pipeline_*` column injection |
| `web_ui/tests/test_invoices_data_sample.py` | End-to-end sample pipeline |







