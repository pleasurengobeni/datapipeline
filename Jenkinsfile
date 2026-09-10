pipeline {
    agent any

    // Poll GitHub every 5 minutes (no webhook needed — we don't have repo admin rights)
    triggers {
        pollSCM('H/5 * * * *')
    }

    environment {
        // /opt/deploy is the deployment directory bind-mounted into this Jenkins container
        // by docker-compose.yaml:  ${AIRFLOW_PROJ_DIR:-.}:/opt/deploy
        // Jenkins already receives all env vars from .env via env_file in its service definition.
        DEPLOY_DIR      = '/opt/deploy'
        COMPOSE_PROJECT = 'airflow'
    }

    stages {
        stage('Sync to deployment directory') {
            steps {
                sh '''
                    git config --global --add safe.directory ${DEPLOY_DIR}
                    # Jenkins already checked out the latest code into $WORKSPACE.
                    # Sync it to the deployment directory, preserving server-generated files.
                    rsync -rl --no-owner --no-group --omit-dir-times \
                        --exclude='.git' \
                        --exclude='dags/config/' \
                        --exclude='dags/etl/' \
                        --exclude='dags/sql/' \
                        --exclude='web_ui/webui.db' \
                        --exclude='__pycache__/' \
                        --exclude='*.pyc' \
                        "${WORKSPACE}/" ${DEPLOY_DIR}/
                    echo "Synced workspace to ${DEPLOY_DIR}"
                '''
            }
        }

        stage('Full Stack Redeploy') {
            // Always runs — stops and removes all services except Jenkins (which is running
            // this very build), then brings the full stack back up.
            steps {
                sh '''
                    HOST_DEPLOY=$(docker inspect jenkins --format '{{range .Mounts}}{{if eq .Destination "/opt/deploy"}}{{.Source}}{{end}}{{end}}')
                    cd ${DEPLOY_DIR}

                    # Warn if Airflow tasks are running so the operator is aware
                    RUNNING_TASKS=$(docker exec datapipeline-airflow-worker-1 \
                        bash -c "celery -A airflow.executors.celery_executor inspect active 2>/dev/null | grep -c task_id || echo 0" \
                        2>/dev/null || echo "0")
                    echo "Running Airflow tasks at deploy time: ${RUNNING_TASKS}"
                    if [ "${RUNNING_TASKS}" -gt "0" ] 2>/dev/null; then
                        echo "WARNING: ${RUNNING_TASKS} task(s) in flight — they will be interrupted by the restart"
                    fi

                    # ── Decide whether images need rebuilding ────────────────────────────
                    # Only rebuild when a Dockerfile or requirements file actually changed.
                    # Python/template changes are picked up via volume mounts on restart.
                    REBUILD_TRIGGERS="Dockerfile requirements.txt analytics/Dockerfile analytics/requirements.txt web_ui/Dockerfile web_ui/requirements.txt data_pipeline_monitor/Dockerfile"
                    NEEDS_BUILD=0
                    for f in ${REBUILD_TRIGGERS}; do
                        if git -C "${DEPLOY_DIR}" diff --name-only HEAD~1 HEAD 2>/dev/null | grep -qF "$f"; then
                            echo "Image rebuild triggered by: $f"
                            NEEDS_BUILD=1
                            break
                        fi
                    done

                    if [ "${NEEDS_BUILD}" -eq "1" ]; then
                        echo "▶  Building images in parallel ..."
                        AIRFLOW_PROJ_DIR="${HOST_DEPLOY}" docker compose -p ${COMPOSE_PROJECT} build --parallel
                        echo "✓  Build complete"
                    else
                        echo "▶  No Dockerfile/requirements changes — skipping image rebuild (using cached images)"
                    fi

                    # ── Regenerate .env from Jenkins container env (loaded from .env at startup) ──
                    # Keeps .env in sync even if it was wiped by a git clean or manual error.
                    env | grep -E '^(PROJECT_|SERVER_|DATA_DUMP|POSTGRES_|PGADMIN_|AIRFLOW|WEBUI_|JENKINS_|METRICS_DB_|GOOGLE_AI|GROQ_|MISTRAL_|DEEPSEEK_|OPENROUTER_|CEREBRAS_|SAMBANOVA_)' \
                        | sort > ${DEPLOY_DIR}/.env
                    echo "✓ .env regenerated from container environment"

                    # ── make down (excluding Jenkins so this build survives) ──────────────
                    SERVICES=$(AIRFLOW_PROJ_DIR="${HOST_DEPLOY}" docker compose -p ${COMPOSE_PROJECT} \
                        config --services 2>/dev/null | grep -v '^jenkins$' | tr '\\n' ' ')
                    echo "Stopping: ${SERVICES}"
                    AIRFLOW_PROJ_DIR="${HOST_DEPLOY}" docker compose -p ${COMPOSE_PROJECT} stop ${SERVICES}
                    AIRFLOW_PROJ_DIR="${HOST_DEPLOY}" docker compose -p ${COMPOSE_PROJECT} rm -f ${SERVICES}

                    # ── make up ──────────────────────────────────────────────────────────
                    echo "Starting full stack..."
                    AIRFLOW_PROJ_DIR="${HOST_DEPLOY}" docker compose -p ${COMPOSE_PROJECT} up -d
                    echo "Stack redeployed"
                '''
            }
        }
    }

    post {
        success {
            echo "Deployed commit ${env.GIT_COMMIT} successfully"
        }
        failure {
            echo "Deployment FAILED — review console output above"
        }
    }
}
