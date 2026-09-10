# Linux Installation Guide — datapipeline

Step-by-step guide to install and run the full ETL stack on a fresh **Ubuntu 24.04 LTS** server.

> **Quickstart flow:**
> 1. Add SSH key to GitHub
> 2. Create `~/.airflow` with all real values
> 3. `mkdir -p ~/datapipeline/airflow && cd ~/datapipeline/airflow`
> 4. `git clone git@github.com:pleasurengobeni/datapipeline.git .`
> 5. `bash install.sh`  ← does everything else automatically

---

## What you'll get

| Service | URL | Description |
|---|---|---|
| Airflow Webserver | http://\<server-ip\>:8090 | ETL pipeline orchestration |
| Web UI (ETL Manager) | http://\<server-ip\>:5001 | No-code pipeline config editor |
| Analytics (Streamlit) | http://\<server-ip\>:8501 | AI-powered data dashboards |
| Pipeline Monitor | http://\<server-ip\>:8050 | Real-time ETL metrics dashboard |
| Jenkins | http://\<server-ip\>:9090 | Auto-deploy on every git push |
| PgAdmin | http://\<server-ip\>:5050 | Web-based Postgres query UI |
| PostgreSQL | \<server-ip\>:55432 | Airflow metadata database |

---

## Requirements

- Ubuntu 22.04 or 24.04 LTS (64-bit)
- 4 GB RAM minimum (8 GB recommended)
- 2 CPU cores minimum
- 20 GB disk space
- Internet access (to pull Docker images and JDBC drivers)
- GitHub account with access to `pleasurengobeni/datapipeline`

---

## Step 1 — Create the project directory

```bash
cd ~/
mkdir -p datapipeline/airflow
cd datapipeline/airflow
```

---

## Step 2 — Install Docker

```bash
# Remove any old Docker installations
sudo apt-get remove -y docker docker-engine docker.io containerd runc 2>/dev/null || true
sudo apt-get purge -y docker docker-engine docker.io containerd runc 2>/dev/null || true
sudo rm -rf /var/lib/docker /var/lib/containerd 2>/dev/null || true

# Update system
sudo apt-get update -y && sudo apt-get upgrade -y

# Install prerequisites
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# Add Docker's official GPG key
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Add Docker repository
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker
sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Enable Docker service
sudo systemctl enable docker
sudo systemctl start docker

# Add your user to the docker group (avoids needing sudo for docker commands)
sudo usermod -aG docker $USER
newgrp docker

# Verify installation
docker run hello-world
docker compose version
```

---

## Step 3 — Set up SSH access to GitHub

```bash
# Generate a dedicated deploy key for this server
ssh-keygen -t ed25519 -C "wasac-server" -f ~/.ssh/github_wasac -N ""
```

```bash
# Print the public key — copy ALL of this output
cat ~/.ssh/github_wasac.pub
```

Go to **GitHub → Settings → SSH and GPG keys → New SSH key**, paste the key, title it `wasac-server`, save.

```bash
# Tell SSH to use this key for github.com
cat >> ~/.ssh/config << 'EOF'
Host github.com
  IdentityFile ~/.ssh/github_wasac
  StrictHostKeyChecking no
EOF
```

```bash
# Test the connection — expect: "Hi pleasurengobeni! You've successfully authenticated..."
ssh -T git@github.com
```

---

## Step 4 — Clone the repository

```bash
# Make sure you are inside ~/datapipeline/airflow
cd ~/datapipeline/airflow
git clone git@github.com:pleasurengobeni/datapipeline.git .
```

---

## Step 5 — Create `~/.airflow` (all secrets in one place)

All secrets live in `~/.airflow` on the server — **never committed to git**.  
`install.sh` sources this file first; every other config file (`.env`, `docker-compose.yaml`) references these values.

### 5.1 — Generate the security keys you'll need

```bash
# Fernet key (Airflow encryption — generates a new one each time)
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# → run once, copy output

# Secret keys (Airflow webserver + Web UI — run twice, one key each)
python3 -c "import secrets; print(secrets.token_hex(32))"
```

### 5.2 — Create the file

```bash
nano ~/.airflow
```

Paste the template below and replace every `your_*` placeholder with real values:

```bash
# =============================================================================
# ~/.airflow — server environment secrets
# DO NOT COMMIT — listed in .gitignore
# install.sh and make source this file before every docker compose command.
# =============================================================================

# ── Project / server identity ──────────────────────────────────────────────
export PROJECT_NAME="datapipeline"
export PROJECT_NAME_LOWER="datapipeline"
export PROJECT_SERVER_IP="your_server_ip"
export SERVER_SSH_PORT=22
export SERVER_SSH_USER="ubuntu"
export SERVER_SSH_KEY_PATH="$HOME/.ssh/id_rsa"
export PROJECT_GITHUB_REPO_SSH="git@github.com:pleasurengobeni/datapipeline.git"

# ── Docker Compose host settings ───────────────────────────────────────────
export AIRFLOW_PROJ_DIR="$HOME/datapipeline/datapipeline"
export AIRFLOW_UID=$(id -u)
export AIRFLOW_GID=$(id -g)
export DOCKER_AIRFLOW_HOME="/opt/airflow"

# ── PostgreSQL — Airflow metadata DB ──────────────────────────────────────
export POSTGRES_USER="airflow"
export POSTGRES_PASSWORD="your_airflow_db_password"

# ── PostgreSQL — Data Lake / target DB ────────────────────────────────────
export POSTGRES_DATA_USER="your_db_user"
export POSTGRES_DATA_PWD="your_db_password"
export POSTGRES_DATA_HOST="your_db_host_or_ip"
export POSTGRES_DATA_PORT=5432
export POSTGRES_DATA_DB="your_db_name"

# ── Pipeline Monitor dashboard DB ─────────────────────────────────────────
# Where pipeline.etl_metrics is written.
# Use the same external DB host if metrics go there, or "postgres" for Airflow's DB.
export METRICS_DB_USER="your_db_user"
export METRICS_DB_PASS="your_db_password"
export METRICS_DB_HOST="your_db_host_or_ip"
export METRICS_DB_PORT=5432
export METRICS_DB_NAME="your_db_name"

# ── PgAdmin ───────────────────────────────────────────────────────────────
export PGADMIN_DEFAULT_EMAIL="you@example.com"
export PGADMIN_DEFAULT_PASSWORD="your_pgadmin_password"
export PGADMIN_PORT=5050

# ── Airflow core ──────────────────────────────────────────────────────────
export AIRFLOW__CORE__FERNET_KEY="your_fernet_key"          # from step 5.1
export AIRFLOW__CORE__DEFAULT_TIMEZONE="UTC"
export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=1000
export AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT=1000
export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:your_airflow_db_password@postgres/airflow"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:your_airflow_db_password@postgres/airflow"
export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://airflow:your_airflow_db_password@postgres/airflow"
export AIRFLOW__CELERY__BROKER_URL="redis://:@redis:6379/0"
export AIRFLOW__SCHEDULER__PARSING_PROCESSES=4
export AIRFLOW__WEBSERVER__SECRET_KEY="your_airflow_webserver_secret"   # from step 5.1
export AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT=1000
export SQLALCHEMY_SILENCE_UBER_WARNING=1

# ── Airflow admin UI login ─────────────────────────────────────────────────
export _AIRFLOW_WWW_USER_USERNAME="admin"
export _AIRFLOW_WWW_USER_PASSWORD="your_airflow_admin_password"
export AIRFLOW_CONN_AIRFLOW="postgresql+psycopg2://airflow:your_airflow_db_password@postgres/airflow"

# ── ETL Manager (Web UI) ───────────────────────────────────────────────────
export WEBUI_SECRET_KEY="your_webui_secret_key"             # from step 5.1
export WEBUI_ADMIN_USER="admin"
export WEBUI_ADMIN_PASS="your_webui_password"
export WEBUI_SESSION_COOKIE_NAME="datapipeline_webui_session"

# ── Airflow Variables — ETL paths (as seen inside the container) ──────────
export AIRFLOW_VAR_ENVIRONMENT="prod"
export AIRFLOW_VAR_DAG_HOME="/opt/airflow/dags"
export AIRFLOW_VAR_MODULES_PATH="/opt/airflow/dags"
export AIRFLOW_VAR_CONFIG_PATH="/opt/airflow/dags/config"
export AIRFLOW_VAR_SQL_PATH="/opt/airflow/dags/sql"
export AIRFLOW_VAR_ETL_PATH="/opt/airflow/dags/etl"
export AIRFLOW_VAR_TEMPLATE_PATH="/opt/airflow/dags/templates"
export AIRFLOW_VAR_DATA_DUMP="/opt/airflow/data_dump"
export AIRFLOW_VAR_LOGS_PATH="/opt/airflow/logs"
export AIRFLOW_VAR_SLACK_TOKEN=""

# ── Airflow Variables — Data Warehouse paths ──────────────────────────────
export AIRFLOW_VAR_DW_CONFIG_PATH="/opt/airflow/dags/dw_config"
export AIRFLOW_VAR_DW_TEMPLATE_PATH="/opt/airflow/dags/templates"
export AIRFLOW_VAR_DW_ETL_PATH="/opt/airflow/dags/dw_etl"
export AIRFLOW_VAR_DW_SQL_PATH="/opt/airflow/dags/dw_sql"
export AIRFLOW_VAR_DW_DEFAULT_SCHEMA="edw_core"
export AIRFLOW_VAR_DW_LOG_RETENTION_DAYS="30"

# ── AI API keys (optional — get free keys at the URLs below) ──────────────
export GOOGLE_AI_API_KEY=""      # https://aistudio.google.com/app/apikey
export GROQ_API_KEY=""           # https://console.groq.com/keys
export MISTRAL_API_KEY=""        # https://console.mistral.ai/api-keys
export DEEPSEEK_API_KEY=""       # https://platform.deepseek.com/api_keys
export OPENROUTER_API_KEY=""     # https://openrouter.ai/keys
export CEREBRAS_API_KEY=""       # https://cloud.cerebras.ai
export SAMBANOVA_API_KEY=""      # https://cloud.sambanova.ai
```

Save and close (`Ctrl+O`, `Enter`, `Ctrl+X`), then protect and auto-load it:

```bash
chmod 600 ~/.airflow
echo 'source ~/.airflow' >> ~/.bashrc
source ~/.bashrc
```

---

## Step 6 — Open firewall ports

```bash
sudo iptables -A INPUT -p tcp --dport 8090 -j ACCEPT   # Airflow webserver
sudo iptables -A INPUT -p tcp --dport 5001 -j ACCEPT   # Web UI (ETL Manager)
sudo iptables -A INPUT -p tcp --dport 8501 -j ACCEPT   # Analytics (Streamlit)
sudo iptables -A INPUT -p tcp --dport 8050 -j ACCEPT   # Pipeline Monitor
sudo iptables -A INPUT -p tcp --dport 9090 -j ACCEPT   # Jenkins
sudo iptables -A INPUT -p tcp --dport 5050 -j ACCEPT   # PgAdmin
sudo iptables -A INPUT -p tcp --dport 55432 -j ACCEPT  # PostgreSQL (external)
```

> On cloud providers (AWS, Azure, GCP) also open these ports in your Security Group / Firewall rules.

---

## Step 7 — Run `install.sh`

```bash
cd ~/datapipeline/airflow
bash install.sh
```

`install.sh` automatically:
1. Sources `~/.airflow` → aborts with clear instructions if the file is missing or still has placeholder values
2. Installs Docker Engine + docker compose plugin (skips if already installed)
3. Creates required host directories (`data_dumps/`, `logs/`, `dags/config/`, etc.) with correct permissions
4. Writes `.env` by expanding all vars from `~/.airflow` — no hardcoded values anywhere
5. Starts the full stack with `docker compose up -d`
6. Waits for containers to start and prints a health-check summary

First start takes **5–10 minutes** to pull images and build the custom Airflow image.

Check that all containers are running:

```bash
docker compose ps
```

You should see these services with status `running` or `healthy`:

```
airflow-webserver
airflow-scheduler
airflow-worker
airflow-triggerer
airflow-init        (exits with code 0 after first run — normal)
postgres
redis
web-ui
analytics
pipeline-monitor
jenkins
pgadmin
```

---

## Step 8 — Verify Airflow is up

```bash
# Check the init log — should end with "exited with code 0"
docker compose logs airflow-init --tail=20
```

Open **http://\<server-ip\>:8090** and log in with `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` from `~/.airflow`.

---

## Step 9 — Set up Airflow Connections

Go to **Airflow UI → Admin → Connections → +** and add your source and target database connections.

| Field | Value |
|---|---|
| Connection Id | e.g. `cenfri_prd_con` |
| Connection Type | `Postgres` (or MySQL / MSSQL) |
| Host | your database hostname |
| Schema | your database name |
| Login | your database username |
| Password | your database password |
| Port | 5432 (Postgres) / 3306 (MySQL) / 1433 (MSSQL) |

Repeat for every source and target database your ETL pipelines need.

---

## Step 10 — Set up Jenkins CI/CD (auto-deploy on push)

### 10.1 Get the initial Jenkins admin password

```bash
docker exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword
```

Open **http://\<server-ip\>:9090**, paste the password → **Install suggested plugins** → create your admin user.

### 10.2 Trust GitHub host keys inside Jenkins

```bash
docker exec -u root jenkins bash -c "
  mkdir -p /var/jenkins_home/.ssh &&
  ssh-keyscan -t rsa,ecdsa,ed25519 github.com >> /var/jenkins_home/.ssh/known_hosts &&
  chmod 600 /var/jenkins_home/.ssh/known_hosts &&
  chown -R jenkins:jenkins /var/jenkins_home/.ssh
"
```

### 10.3 Generate a dedicated deploy key

```bash
ssh-keygen -t ed25519 -C "jenkins-deploy@wasac" -f ~/.ssh/jenkins_deploy -N ""
cat ~/.ssh/jenkins_deploy.pub   # copy this — add to GitHub Deploy Keys
```

Add SSH config entry:

```bash
cat >> ~/.ssh/config << 'EOF'

Host github.com
    HostName github.com
    User git
    IdentityFile ~/.ssh/jenkins_deploy
    IdentitiesOnly yes
EOF
chmod 600 ~/.ssh/config
```

### 10.4 Add deploy key to GitHub

GitHub → **pleasurengobeni/datapipeline → Settings → Deploy keys → Add deploy key**
- Title: `wasac-server-deploy`
- Key: paste output of `cat ~/.ssh/jenkins_deploy.pub`
- ✅ Allow write access → **Add key**

### 10.5 Add private key to Jenkins credentials

Jenkins → **Manage Jenkins → Credentials → System → Global → Add Credentials**
- Kind: `SSH Username with private key`
- ID: `github-deploy-key`
- Username: `git`
- Private key → **Enter directly** → paste output of `cat ~/.ssh/jenkins_deploy`
- Save

### 10.6 Create the deploy job

Jenkins → **New Item** → name: `wasac-deploy` → **Freestyle project** → OK

**Source Code Management → Git:**
- Repository URL: `git@github.com:pleasurengobeni/datapipeline.git`
- Credentials: `github-deploy-key`
- Branch specifier: `*/main`

**Build Triggers:** ✅ **GitHub hook trigger for GITScm polling**

**Build Steps → Execute shell:**
```bash
chmod +x $WORKSPACE/deploy.sh
$WORKSPACE/deploy.sh
```

Save → **Build Now** to test.

### 11.7 Add GitHub webhook

GitHub → **pleasurengobeni/datapipeline → Settings → Webhooks → Add webhook**
- Payload URL: `http://<server-ip>:9090/github-webhook/`
- Content type: `application/json`
- Trigger: **Just the push event**
- Save

> Every `git push` to `main` now auto-deploys to the server. ✅

---

## Daily operations

```bash
make              # start all services
make down         # stop all services
make restart      # stop + start
make logs         # tail all logs

# Single service
make restart-service s=pipeline-monitor
make logs-service s=airflow-scheduler
```

### Rebuild a single service after code changes

```bash
docker compose build pipeline-monitor
docker compose up -d pipeline-monitor
```

---

## Common issues

| Symptom | Fix |
|---|---|
| `docker: permission denied` | `sudo usermod -aG docker $USER && newgrp docker` |
| Airflow webserver not starting | Wait for `airflow-init` to exit 0 first: `docker compose logs airflow-init` |
| `source ~/.airflow: No such file or directory` | Create `~/.airflow` — see Step 5 |
| Blank values / connection refused in containers | Shell vars not loaded — run `source ~/.airflow` then `make down && make` |
| Pipeline Monitor shows no data | Check `DB_*` env vars inside container: `docker compose exec pipeline-monitor env \| grep DB_` |
| Port already in use | Change the host port in `docker-compose.yaml` (left side of `"8090:8080"`) |
| `Permission denied` on DAG files | Run Step 6 (fix permissions) then `make restart` |
| `Fernet key invalid` | Generate a new key and update `AIRFLOW__CORE__FERNET_KEY` in `~/.airflow`, then restart |

---

## Updating the stack

```bash
cd ~/datapipeline/airflow
git pull
make restart
```

If `Dockerfile` or `requirements.txt` changed, rebuild images:

```bash
make down
make
```

---

## Day-to-day deploy (after code changes)

After the initial install, pushing new code is a single command:

```bash
# SSH into the server
ssh -i ~/your-key.pem ubuntu@<server-ip>

# Pull latest code and restart web-ui + analytics
cd ~/datapipeline/airflow
make pull
```

To show all service URLs at any time:

```bash
make demo
```

To rebuild only the analytics (Streamlit) image after adding a new dashboard page:

```bash
make analytics
```

To rebuild ALL images after a `requirements.txt` change:

```bash
make build
make up
```

---

## Nuclear reset — wipe and reinstall

```bash
# Stop and remove all containers, images, and volumes for this stack only
docker ps -a | grep wasac | awk '{print $1}' | xargs -r docker stop
docker ps -a | grep wasac | awk '{print $1}' | xargs -r docker rm
docker images | grep wasac | awk '{print $3}' | xargs -r docker rmi
docker volume ls | grep wasac | awk '{print $2}' | xargs -r docker volume rm
docker network ls | grep wasac | awk '{print $1}' | xargs -r docker network rm

# Start fresh
make
```

---

## Support

📧 [datateam@cenfri.org](mailto:datateam@cenfri.org)  
🌐 [cenfri.org](https://cenfri.org)
