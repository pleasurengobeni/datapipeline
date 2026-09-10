# ETL Config UI

Lightweight web UI to create and manage ETL job configs without editing JSON by hand.

## Features
- Create DB-to-DB ETL configs
- List, edit, and delete jobs
- Writes Airflow DAG files directly from the template
- Pulls Airflow connection IDs for dropdowns (if available)

## Run
```bash
export FLASK_APP=web_ui/app.py
export FLASK_ENV=development
export WEBUI_SECRET_KEY=change_me
export WEBUI_ADMIN_USER=admin
export WEBUI_ADMIN_PASS=admin123
python -m flask run --host 0.0.0.0 --port 5001
```

## Tests
```bash
pytest -q web_ui/tests/test_app.py
```

## Environment Variables
- `CONFIG_DIR`: config folder (default `dags/config`)
- `ETL_DIR`: DAG output folder (default `dags/etl`)
- `TEMPLATE_PATH`: template file (default `dags/templates/data_load.template`)
- `SQL_DIR`: SQL folder (default `dags/sql`)
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`: Airflow DB for connections list
- `WEBUI_DB`: UI SQLite DB (default `web_ui/webui.db`)
- `WEBUI_SECRET_KEY`: Flask session secret
- `WEBUI_ADMIN_USER`: bootstrap admin user
- `WEBUI_ADMIN_PASS`: bootstrap admin password

## Notes
- Only simple `select_expr` like `col` or `col as alias` is supported in the UI path.
- After saving a job, Airflow should pick up the new DAG file automatically.
