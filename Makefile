# =============================================================================
# Makefile — datapipeline stack management
#
# Always sources .airflow (project-local) then ~/.airflow before any docker
# compose command so real env vars are available regardless of how the command is run.
#
# Usage:
#   make              → start stack   (same as: make up)
#   make up           → start all services (detached)
#   make build        → rebuild Docker images without cache
#   make down         → stop and remove containers
#   make restart      → down then up
#   make pull         → git pull + restart web-ui & analytics (quick deploy)
#   make logs         → follow all logs
#   make ps           → show container status
#   make restart-service s=web-ui   → restart (or start) a single service
#   make logs-service   s=web-ui   → tail logs for one service
#   make analytics    → (re)build and start analytics dashboard only
#   make demo         → show all service URLs
#
# Security:
#   make security-scan  → scan pip packages for CVEs + check hardening config
#   make db-audit       → audit all Airflow-registered DBs for unexpected users
#   make falco-up       → start Falco runtime threat detection (Linux only)
#   make falco-down     → stop Falco
# =============================================================================

SHELL       := /bin/bash
PROJECT_ROOT := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
ENV_FILE    := $(PROJECT_ROOT)/.env
AIRFLOW_ENV_LOCAL := $(PROJECT_ROOT)/.airflow
AIRFLOW_ENV_HOME  := $(HOME)/.airflow

# Load .airflow (project-local) then ~/.airflow if they exist, then run docker compose
DC = { [ -f $(AIRFLOW_ENV_LOCAL) ] && source $(AIRFLOW_ENV_LOCAL); [ -f $(AIRFLOW_ENV_HOME) ] && source $(AIRFLOW_ENV_HOME); true; } && docker compose --project-directory "$(PROJECT_ROOT)" --env-file "$(ENV_FILE)"

.PHONY: up build fresh down restart pull logs ps restart-service restart-service-recreate restart-pipeline-monitor logs-service init-dirs analytics demo check wipe-server install-server install-server wipe-server reset-jenkins reset-jenkins-remote sync-server gen-env security-scan db-audit falco-up falco-down falco-logs init-secrets clean-logs

## Regenerate .env from ~/.airflow — single source of truth, called automatically by up/restart/fresh
## Sources the file first so bash evaluates variable references (e.g. METRICS_DB_USER=${POSTGRES_DATA_USER})
## before writing resolved values to .env.
gen-env:
	@if [ ! -f "$(AIRFLOW_ENV_HOME)" ]; then \
	    echo "✗  $(AIRFLOW_ENV_HOME) not found — cannot generate .env"; \
	    exit 1; \
	fi
	@set -a; source "$(AIRFLOW_ENV_HOME)"; set +a; \
	printenv | grep -E '^(_?AIRFLOW|PROJECT_|SERVER_|DATA_DUMP|POSTGRES_|REDIS_|PGADMIN_|WEBUI_|JENKINS_|METRICS_DB_|GOOGLE_AI_|GROQ_|MISTRAL_|DEEPSEEK_|OPENROUTER_|CEREBRAS_|SAMBANOVA_)' \
	| grep -vE '^(AIRFLOW__(DATABASE|CORE)__SQL_ALCHEMY_CONN|AIRFLOW__CELERY__RESULT_BACKEND|AIRFLOW_CONN_AIRFLOW)=' \
	| sort > "$(ENV_FILE)"
	@echo "✓  .env generated from $(AIRFLOW_ENV_HOME)"

up: gen-env init-dirs
	@$(DC) up -d
	@echo ""
	@echo "✓  Stack started"
	@echo "   Airflow           → http://localhost:8090"
	@echo "   Web UI            → http://localhost:5001"
	@echo "   Analytics         → http://localhost:8501"
	@echo "   Pipeline Monitor  → http://localhost:8050"
	@echo "   Jenkins           → http://localhost:9090"
	@echo "   PgAdmin           → http://localhost:5050"
	@echo "   PostgreSQL        → localhost:55432"

build: init-dirs
	@echo "▶  Rebuilding images (no cache, pulling latest base images) ..."
	@$(DC) build --no-cache --pull
	@echo "✓  Build complete"

## Nuclear option: stop stack, clear build cache, pull latest base images, rebuild and restart
fresh: gen-env init-dirs
	@echo "▶  Full cache-busting rebuild — stopping stack ..."
	@$(DC) down --remove-orphans
	@echo "▶  Pruning Docker build cache ..."
	@docker builder prune -f
	@echo "▶  Rebuilding all images (no cache, pulling latest base) ..."
	@$(DC) build --no-cache --pull
	@$(DC) up -d
	@echo ""
	@echo "✓  Stack rebuilt from scratch"
	@echo "   Airflow           → http://localhost:8090"
	@echo "   Web UI            → http://localhost:5001"
	@echo "   Analytics         → http://localhost:8501"
	@echo "   Pipeline Monitor  → http://localhost:8050"
	@echo "   Jenkins           → http://localhost:9090"
	@echo "   PgAdmin           → http://localhost:5050"
	@echo "   PostgreSQL        → localhost:55432"

down:
	@echo "▶  Sourcing $(AIRFLOW_ENV) ..."
	@$(DC) down
	@echo "✓  Stack stopped"

restart: gen-env init-dirs
	@echo "▶  Restarting stack ..."
	@$(DC) down
	@$(DC) up -d
	@echo "✓  Stack restarted"

## Sanity-check that local code, ~/.airflow, containers and login are all in sync
check:
	@echo ""
	@echo "══════════════════════════════════════════════"
	@echo "  SYNC CHECK"
	@echo "══════════════════════════════════════════════"
	@echo ""
	@echo "── 1. Git ──────────────────────────────────"
	@LOCAL=$$(git -C "$(PROJECT_ROOT)" rev-parse HEAD); \
	 REMOTE=$$(git -C "$(PROJECT_ROOT)" ls-remote origin HEAD | cut -f1); \
	 echo "   local  : $$LOCAL"; \
	 echo "   origin : $$REMOTE"; \
	 if [ "$$LOCAL" = "$$REMOTE" ]; then echo "   ✓  in sync"; else echo "   ✗  OUT OF SYNC — run: git push / git pull"; fi
	@echo ""
	@echo "── 2. ~/.airflow credentials ───────────────"
	@source $(AIRFLOW_ENV) 2>/dev/null; \
	 PASS=$${WEBUI_ADMIN_PASS:-}; \
	 USER=$${WEBUI_ADMIN_USER:-admin}; \
	 APPASS=$${_AIRFLOW_WWW_USER_PASSWORD:-}; \
	 APUSER=$${_AIRFLOW_WWW_USER_USERNAME:-admin}; \
	 JPASS=$${JENKINS_ADMIN_PASSWORD:-}; \
	 echo "   ETL Manager  →  $$USER / $$(echo $$PASS | sed 's/./*/g')  (raw check only)"; \
	 echo "   Airflow UI   →  $$APUSER / $$(echo $$APPASS | sed 's/./*/g')"; \
	 echo "   Jenkins      →  admin / $$(echo $$JPASS | sed 's/./*/g')"; \
	 if [ -z "$$PASS" ]; then echo "   ✗  WEBUI_ADMIN_PASS is empty in ~/.airflow"; else echo "   ✓  WEBUI_ADMIN_PASS is set"; fi; \
	 if [ -z "$$APPASS" ]; then echo "   ✗  _AIRFLOW_WWW_USER_PASSWORD is empty in ~/.airflow"; else echo "   ✓  _AIRFLOW_WWW_USER_PASSWORD is set"; fi; \
	 if [ -z "$$JPASS" ]; then echo "   ✗  JENKINS_ADMIN_PASSWORD is empty in ~/.airflow — Jenkins will use 'changeme'"; else echo "   ✓  JENKINS_ADMIN_PASSWORD is set"; fi
	@echo ""
	@echo "── 3. Container health ─────────────────────"
	@docker ps --format "   {{.Names}} → {{.Status}}" | grep -E "web-ui|airflow-webserver|airflow-scheduler|postgres|redis" || echo "   (no matching containers running)"
	@echo ""
	@echo "── 4. ETL Manager login test ───────────────"
	@source $(AIRFLOW_ENV) 2>/dev/null; \
	 PASS=$${WEBUI_ADMIN_PASS:-}; \
	 USER=$${WEBUI_ADMIN_USER:-admin}; \
	 CODE=$$(curl -s -o /dev/null -w "%{http_code}" \
	   -c /tmp/_wui_check.txt -b /tmp/_wui_check.txt \
	   --data "username=$$USER&password=$$PASS" \
	   http://localhost:5001/login); \
	 rm -f /tmp/_wui_check.txt; \
	 if [ "$$CODE" = "302" ] || [ "$$CODE" = "303" ]; then echo "   ✓  Login OK"; \
	 elif [ "$$CODE" = "200" ]; then echo "   ✗  Login FAILED (credentials rejected) — check WEBUI_ADMIN_PASS in ~/.airflow"; \
	 else echo "   ?  Unexpected HTTP $$CODE — container may still be starting"; fi
	@echo ""
	@echo "── 5. Airflow local username ───────────────"
	@USERS=$$(docker exec datapipeline-airflow-webserver-1 python -c \
	  "from airflow.www.app import create_app; app=create_app(); \
	   [print('  ',u.username) for u in (lambda a: a.appbuilder.sm.get_all_users())(app) if True]" \
	  2>/dev/null | grep -v WARNING | grep -v UserWarning || echo "   (container not running)"); \
	 echo "   Airflow users in DB: $$USERS"; \
	 echo "   Login at http://localhost:8090 with one of the above + _AIRFLOW_WWW_USER_PASSWORD from ~/.airflow"
	@echo ""
	@echo "══════════════════════════════════════════════"
	@echo ""

## Quick code deploy — git pull then restart web-ui + analytics (no full rebuild)
pull:
	@echo "▶  Pulling latest code from git ..."
	@git -C "$(PROJECT_ROOT)" pull --ff-only
	@echo "▶  Restarting web-ui and analytics to pick up changes ..."
	@$(DC) up -d --no-deps web-ui analytics
	@echo "✓  Code deployed."
	@echo "   Web UI   → http://localhost:5001"
	@echo "   Analytics → http://localhost:8501"

## Upload a server-adapted ~/.airflow to the remote (fixes missing env vars after git pull/deploy)
## Usage:  make push-env
push-env:
	@set -a; source "$(AIRFLOW_ENV_LOCAL)"; set +a; \
	HOST="$${PROJECT_SERVER_IP:?PROJECT_SERVER_IP not set in .airflow}"; \
	PORT="$${SERVER_SSH_PORT:-22}"; \
	RUSER="$${SERVER_SSH_USER:-ubuntu}"; \
	KEY="$${SERVER_SSH_KEY_PATH:?SERVER_SSH_KEY_PATH not set in .airflow}"; \
	PNAME="$${PROJECT_NAME}"; \
	SSH_OPTS="-i $${KEY} -p $${PORT} -o StrictHostKeyChecking=no -o BatchMode=yes"; \
	SCP_OPTS="-i $${KEY} -P $${PORT} -o StrictHostKeyChecking=no"; \
	echo "▶  Fetching server UID/GID and HOME from $${RUSER}@$${HOST}..."; \
	REMOTE_HOME=$$(ssh $${SSH_OPTS} "$${RUSER}@$${HOST}" 'echo $$HOME'); \
	REMOTE_UID=$$(ssh  $${SSH_OPTS} "$${RUSER}@$${HOST}" 'id -u'); \
	REMOTE_GID=$$(ssh  $${SSH_OPTS} "$${RUSER}@$${HOST}" 'id -g'); \
	REMOTE_PROJ_DIR="$${REMOTE_HOME}/datapipeline/$${PNAME}"; \
	TMPFILE=$$(mktemp); \
	sed \
	    -e "s|^export AIRFLOW_PROJ_DIR=.*|export AIRFLOW_PROJ_DIR=\"$${REMOTE_PROJ_DIR}\"|" \
	    -e "s|^export AIRFLOW_UID=.*|export AIRFLOW_UID=$${REMOTE_UID}|" \
	    -e "s|^export AIRFLOW_GID=.*|export AIRFLOW_GID=$${REMOTE_GID}|" \
	    "$(AIRFLOW_ENV_LOCAL)" > "$${TMPFILE}"; \
	scp $${SCP_OPTS} "$${TMPFILE}" "$${RUSER}@$${HOST}:~/.airflow"; \
	ssh $${SSH_OPTS} "$${RUSER}@$${HOST}" 'chmod 600 ~/.airflow'; \
	rm -f "$${TMPFILE}"; \
	echo "✓  ~/.airflow uploaded to $${RUSER}@$${HOST} (AIRFLOW_PROJ_DIR=$${REMOTE_PROJ_DIR})"

## Build and (re)start the Streamlit analytics service only
analytics: init-dirs
	@echo "▶  Building analytics image ..."
	@$(DC) build analytics
	@$(DC) up -d --no-deps analytics
	@echo "✓  Analytics dashboard started → http://localhost:8501"

## Print all service URLs (demo / onboarding helper)
demo:
	@source $(AIRFLOW_ENV) 2>/dev/null; \
	PNAME=$${PROJECT_NAME:-Analytics Pipeline}; \
	echo ""; \
	echo "╔══════════════════════════════════════════════════════╗"; \
	printf "║  %-52s ║\n" "$$PNAME — Service URLs"; \
	echo "╠══════════════════════════════════════════════════════╣"; \
	echo "║  Airflow (scheduler/UI)  → http://localhost:8090     ║"; \
	echo "║  ETL Manager (Web UI)    → http://localhost:5001     ║"; \
	echo "║  Analytics (Streamlit)   → http://localhost:8501     ║"; \
	echo "║  Pipeline Monitor        → http://localhost:8050     ║"; \
	echo "║  Jenkins (CI/CD)         → http://localhost:9090     ║"; \
	echo "║  PgAdmin                 → http://localhost:5050     ║"; \
	echo "║  PostgreSQL              → localhost:55432           ║"; \
	echo "╚══════════════════════════════════════════════════════╝"; \
	echo ""

logs:
	@$(DC) logs -f

ps:
	@$(DC) ps

## Restart (or start) a single service: make restart-service s=web-ui
restart-service:
	@echo "▶  Restarting $(s) ..."
	@$(DC) up -d --no-deps $(s)
	@echo "✓  $(s) (re)started"

## Force-recreate one service so fresh env values are injected every time
## Usage: make restart-service-recreate s=pipeline-monitor
restart-service-recreate:
	@echo "▶  Force-recreating $(s) with refreshed env ..."
	@$(DC) up -d --no-deps --force-recreate $(s)
	@echo "✓  $(s) recreated with latest env"

## Env-safe restart for pipeline-monitor (validates METRICS_DB_* first)
restart-pipeline-monitor:
	@set -a; [ -f "$(AIRFLOW_ENV_LOCAL)" ] && source "$(AIRFLOW_ENV_LOCAL)"; [ -f "$(AIRFLOW_ENV_HOME)" ] && source "$(AIRFLOW_ENV_HOME)"; set +a; \
	  missing=0; \
	  for v in METRICS_DB_USER METRICS_DB_PASS METRICS_DB_HOST METRICS_DB_NAME; do \
	    if [ -z "$${!v}" ]; then echo "✗ Missing $$v (set it in .airflow or ~/.airflow)"; missing=1; fi; \
	  done; \
	  if [ "$${METRICS_DB_NAME}" = "airflow" ] && [ "$${METRICS_DB_HOST}" = "postgres" ] && [ "$${ALLOW_METRICS_ON_AIRFLOW_DB:-0}" != "1" ]; then \
	    echo "✗ METRICS_DB points to Airflow metadata DB (postgres/airflow)."; \
	    echo "  Set METRICS_DB_* to your DW DB, or export ALLOW_METRICS_ON_AIRFLOW_DB=1 to override intentionally."; \
	    exit 1; \
	  fi; \
	  if [ $$missing -ne 0 ]; then exit 1; fi
	@$(MAKE) restart-service-recreate s=pipeline-monitor
	@echo "▶  Runtime DB_* inside pipeline-monitor:"
	@$(DC) exec -T pipeline-monitor /bin/sh -c 'echo "DB_USER=$${DB_USER:+set}"; echo "DB_PASS=$${DB_PASS:+set}"; echo "DB_HOST=$$DB_HOST"; echo "DB_PORT=$$DB_PORT"; echo "DB_NAME=$$DB_NAME"'

## Tail logs for one service: make logs-service s=web-ui
logs-service:
	@$(DC) logs -f $(s)

## Create pipeline.etl_metrics in postgres (idempotent — safe to run repeatedly)
init-db:
	@echo "▶  Initialising pipeline.etl_metrics in postgres..."
	@cat "$(PROJECT_ROOT)/etl_metrics.sql" | $(DC) exec -T postgres psql -U airflow -d airflow
	@echo "✓  pipeline.etl_metrics ready"

## Ensure required host directories exist before any docker compose command
init-dirs:
	@mkdir -p "$(PROJECT_ROOT)/data_dumps/incoming"
	@mkdir -p "$(PROJECT_ROOT)/data_dumps/archive"
	@mkdir -p "$(PROJECT_ROOT)/logs"
	@mkdir -p "$(PROJECT_ROOT)/logs/falco"

# ── Security ──────────────────────────────────────────────────────────────────

## Create ./secrets/ files from ~/.airflow — run once on fresh deploy or after credential rotation
## Never commits to git (secrets/ is gitignored)
init-secrets:
	@bash "$(PROJECT_ROOT)/scripts/init-secrets.sh"

## Scan pip packages for CVEs and check container hardening config
## Requires Docker. pip-audit runs inside a temporary container if not installed locally.
security-scan:
	@echo "▶  Running security scan ..."
	@bash "$(PROJECT_ROOT)/scripts/security-scan.sh"

## Audit all Airflow-registered databases for unexpected/backdoor users
## Runs inside the web-ui container (has Fernet key + DB drivers)
## Usage: make db-audit
##        make db-audit AUDIT_ARGS="--drop-unknown --allowlist etl_ro"
db-audit:
	@echo "▶  Auditing DB users across all Airflow connections ..."
	@$(DC) exec -T web-ui python /app/scripts/db-user-audit.py $(AUDIT_ARGS)

## Start Falco runtime threat detection (Linux server only — no-op on macOS)
## Requires: sudo apt-get install linux-headers-$(uname -r) on the server
falco-up: init-dirs
	@echo "▶  Starting Falco runtime security ..."
	@$(DC) -f docker-compose.yaml -f docker-compose.security.yaml up -d falco
	@echo "✓  Falco started — alerts: ./logs/falco/alerts.log"

## Stop Falco
falco-down:
	@echo "▶  Stopping Falco ..."
	@$(DC) -f docker-compose.yaml -f docker-compose.security.yaml stop falco
	@$(DC) -f docker-compose.yaml -f docker-compose.security.yaml rm -f falco
	@echo "✓  Falco stopped"

## Tail Falco security alerts in real time
falco-logs:
	@tail -f "$(PROJECT_ROOT)/logs/falco/alerts.log" 2>/dev/null \
	  || echo "No Falco alert log yet — is Falco running? (make falco-up)"

## Delete Airflow task logs older than RETAIN_DAYS (default 30) and remove empty directories.
## Safe to run while Airflow is running.
## Usage:
##   make clean-logs               → delete logs older than 30 days
##   RETAIN_DAYS=7 make clean-logs → delete logs older than 7 days
clean-logs:
	@bash "$(PROJECT_ROOT)/scripts/clean-logs.sh"

## Reset Jenkins admin password on the REMOTE server via SSH
## Reads server connection from ~/.airflow (same as push-env / wipe-server)
## Usage: make reset-jenkins-remote
reset-jenkins-remote:
	@set -a; source "$(AIRFLOW_ENV_LOCAL)"; set +a; \
	HOST="$${PROJECT_SERVER_IP:?PROJECT_SERVER_IP not set in .airflow}"; \
	PORT="$${SERVER_SSH_PORT:-22}"; \
	RUSER="$${SERVER_SSH_USER:-ubuntu}"; \
	KEY="$${SERVER_SSH_KEY_PATH:?SERVER_SSH_KEY_PATH not set in .airflow}"; \
	PNAME="$${PROJECT_NAME:?PROJECT_NAME not set in .airflow}"; \
	SSH_OPTS="-i $${KEY} -p $${PORT} -o StrictHostKeyChecking=no -o BatchMode=yes"; \
	echo "▶  Resetting Jenkins password on $${RUSER}@$${HOST} ..."; \
	ssh $${SSH_OPTS} "$${RUSER}@$${HOST}" \
	  "cd ~/datapipeline/airflow && git fetch origin && git reset --hard origin/\$$(git rev-parse --abbrev-ref HEAD) && git clean -fd && make reset-jenkins"; \
	echo "✓  Done — Jenkins on $${HOST}:9090 reset to JENKINS_ADMIN_PASSWORD from server ~/.airflow"

## Force-sync the remote server git repo to origin/main (discards all server drift)
## No Jenkins password needed — git only
## Usage: make sync-server
sync-server:
	@set -a; source "$(AIRFLOW_ENV_LOCAL)"; set +a; \
	HOST="$${PROJECT_SERVER_IP:?PROJECT_SERVER_IP not set in .airflow}"; \
	PORT="$${SERVER_SSH_PORT:-22}"; \
	RUSER="$${SERVER_SSH_USER:-ubuntu}"; \
	KEY="$${SERVER_SSH_KEY_PATH:?SERVER_SSH_KEY_PATH not set in .airflow}"; \
	PNAME="$${PROJECT_NAME:?PROJECT_NAME not set in .airflow}"; \
	SSH_OPTS="-i $${KEY} -p $${PORT} -o StrictHostKeyChecking=no -o BatchMode=yes"; \
	echo "▶  Syncing git on $${RUSER}@$${HOST} ..."; \
	ssh $${SSH_OPTS} "$${RUSER}@$${HOST}" \
	  "cd ~/datapipeline/airflow && git fetch origin && git reset --hard origin/\$$(git rev-parse --abbrev-ref HEAD) && git clean -fd && echo '✓  Git synced'"; \
	echo "✓  Done — $${RUSER}@$${HOST} is in sync with origin"

## Reset Jenkins admin password to JENKINS_ADMIN_PASSWORD from ~/.airflow
## Works locally or on any server with Docker running the jenkins container
## Usage: make reset-jenkins
reset-jenkins:
	@source $(AIRFLOW_ENV) 2>/dev/null; \
	JPASS="$${JENKINS_ADMIN_PASSWORD:-}"; \
	if [ -z "$$JPASS" ]; then \
	  echo "✗  JENKINS_ADMIN_PASSWORD is not set in ~/.airflow — aborting."; exit 1; \
	fi; \
	echo "▶  Generating bcrypt hash for JENKINS_ADMIN_PASSWORD ..."; \
	HASH=$$(python3 -c "import bcrypt; print(bcrypt.hashpw('$$JPASS'.encode(), bcrypt.gensalt(10)).decode())"); \
	ADMIN_DIR=$$(docker exec jenkins ls /var/jenkins_home/users/ 2>/dev/null | grep "^admin"); \
	if [ -z "$$ADMIN_DIR" ]; then echo "✗  No admin user found in jenkins_home"; exit 1; fi; \
	docker exec jenkins sed -i \
	  "s|<passwordHash>.*</passwordHash>|<passwordHash>#jbcrypt:$$HASH</passwordHash>|" \
	  "/var/jenkins_home/users/$$ADMIN_DIR/config.xml"; \
	docker restart jenkins; \
	echo ""; \
	echo "✓  Jenkins admin password reset."; \
	echo "   URL      → http://localhost:9090"; \
	echo "   Username → admin"; \
	echo "   Password → (your JENKINS_ADMIN_PASSWORD from ~/.airflow)"


	@bash "$(PROJECT_ROOT)/wipe_and_reinstall.sh"

## Alias for wipe-server (rsync + fresh install on remote)
install-server: wipe-server

## Deploy the local project to the remote server (rsync + install)
install-server:
	@bash "$(PROJECT_ROOT)/wipe_and_reinstall.sh"
