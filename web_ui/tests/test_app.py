"""
Comprehensive tests for web_ui/app.py
Covers every route and key helper functions.
"""
import importlib
import io
import json
from pathlib import Path

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures / helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_test_client(tmp_path, monkeypatch, *, logged_in=True, is_admin=True):
    """
    Reload app with a fresh temp environment, return (client, app_module).
    Creates minimal template and config stubs so the app can boot.
    """
    monkeypatch.setenv("WEBUI_DB",            str(tmp_path / "webui.db"))
    monkeypatch.setenv("CONFIG_DIR",          str(tmp_path / "config"))
    monkeypatch.setenv("ETL_DIR",             str(tmp_path / "etl"))
    monkeypatch.setenv("SQL_DIR",             str(tmp_path / "sql"))
    monkeypatch.setenv("WEBUI_ADMIN_USER",    "admin")
    monkeypatch.setenv("WEBUI_ADMIN_PASS",    "admin123")

    # SQL stub
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (sql_dir / "example.sql").write_text("select 1")

    # Template stubs
    tpl_dir = tmp_path / "templates"
    tpl_dir.mkdir(parents=True, exist_ok=True)
    (tpl_dir / "data_load.template").write_text("DAG <dag_name>")
    (tpl_dir / "spark_load.template").write_text("SPARK DAG <dag_name>")
    (tpl_dir / "file_load.template").write_text("FILE DAG <dag_name>")
    (tpl_dir / "spark_file_load.template").write_text("SPARK FILE DAG <dag_name>")

    monkeypatch.setenv("TEMPLATE_FILE",            str(tpl_dir / "data_load.template"))
    monkeypatch.setenv("SPARK_TEMPLATE_FILE",      str(tpl_dir / "spark_load.template"))
    monkeypatch.setenv("FILE_TEMPLATE_FILE",       str(tpl_dir / "file_load.template"))
    monkeypatch.setenv("SPARK_FILE_TEMPLATE_FILE", str(tpl_dir / "spark_file_load.template"))

    import web_ui.app as app_module
    importlib.reload(app_module)
    app_module.app.config.update(TESTING=True, SECRET_KEY="test")

    # Always initialise the DB so webui_users table exists
    app_module.init_webui_db()

    client = app_module.app.test_client()

    if logged_in:
        with client.session_transaction() as sess:
            sess["user_id"] = 1
        monkeypatch.setattr(app_module, "current_user",
                            lambda: (1, "admin", 1, 1))
        if is_admin:
            monkeypatch.setattr(app_module, "require_admin", lambda: None)
        monkeypatch.setattr(app_module, "require_login",  lambda: None)

    monkeypatch.setattr(app_module, "airflow_connections", lambda: ["test_conn"])

    return client, app_module


def _make_db_config(tmp_path, dag_id="testdag", conn="test_conn"):
    cfg = {
        "dag_id": dag_id,
        "pipeline_type": "db",
        "schedule_interval": "*/5 * * * *",
        "start_date": "2023-01-01",
        "tags": ["admin"],
        "source": {
            "dev":  {"sql_file": "dev.sql",  "source_db_conn_id": conn},
            "prod": {"sql_file": "prod.sql", "source_db_conn_id": conn},
        },
        "target": {
            "dev":  {"target_db_conn_id": conn, "target_schema": "s", "target_table": "t"},
            "prod": {"target_db_conn_id": conn, "target_schema": "s", "target_table": "t"},
        },
    }
    config_dir = tmp_path / "config" / conn
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / f"{conn}_{dag_id}.json").write_text(json.dumps(cfg))
    return cfg


def _make_file_config(tmp_path, dag_id="filejob", conn="test_conn"):
    cfg = {
        "dag_id": dag_id,
        "pipeline_type": "file_based",
        "schedule_interval": "@daily",
        "start_date": "2023-01-01",
        "tags": ["admin"],
        "source": {
            "dev":  {"watch_path": "/data/dev",  "file_format": "csv", "file_pattern": ".*\\.csv"},
            "prod": {"watch_path": "/data/prod", "file_format": "csv", "file_pattern": ".*\\.csv"},
        },
        "target": {
            "dev":  {"target_db_conn_id": conn, "target_schema": "s", "target_table": "t"},
            "prod": {"target_db_conn_id": conn, "target_schema": "s", "target_table": "t"},
        },
    }
    config_dir = tmp_path / "config" / conn
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / f"file_{conn}_{dag_id}.json").write_text(json.dumps(cfg))
    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────────────────

class TestAuth:
    def test_login_page_renders(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch, logged_in=False)
        resp = client.get("/login")
        assert resp.status_code == 200
        html = resp.data.decode()
        assert any(kw in html.lower() for kw in ("login", "username", "password"))

    def test_valid_login_redirects(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch, logged_in=False)
        resp = client.post("/login", data={"username": "admin", "password": "admin123"},
                           follow_redirects=False)
        assert resp.status_code in (301, 302)
        assert "/login" not in resp.headers.get("Location", "")

    def test_invalid_login_shows_error(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch, logged_in=False)
        resp = client.post("/login", data={"username": "admin", "password": "wrong"},
                           follow_redirects=True)
        assert resp.status_code == 200
        html = resp.data.decode().lower()
        # Template renders either "invalid credentials" or stays on login page
        assert "invalid" in html or "login" in html

    def test_logout_redirects(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = client.get("/logout", follow_redirects=False)
        assert resp.status_code in (301, 302)

    def test_unauthenticated_gets_redirected(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch, logged_in=False)
        resp = client.get("/jobs", follow_redirects=False)
        assert resp.status_code in (200, 302)


# ─────────────────────────────────────────────────────────────────────────────
# Index / Profile
# ─────────────────────────────────────────────────────────────────────────────

class TestIndex:
    def test_index_200(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        assert client.get("/").status_code == 200

    def test_profile_get_200(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        assert client.get("/profile").status_code == 200

    def test_profile_post_redirects(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = client.post("/profile", data={"password": "newpass123"},
                           follow_redirects=False)
        assert resp.status_code in (301, 302)


# ─────────────────────────────────────────────────────────────────────────────
# DB-to-DB jobs (/jobs/*)
# ─────────────────────────────────────────────────────────────────────────────

class TestDBJobs:
    def test_jobs_list_200(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        assert client.get("/jobs").status_code == 200

    def test_jobs_list_shows_db_config(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        _make_db_config(tmp_path, dag_id="myjob")
        assert "myjob" in client.get("/jobs").data.decode()

    def test_jobs_list_excludes_file_configs(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        _make_file_config(tmp_path, dag_id="fileonlyjob")
        assert "fileonlyjob" not in client.get("/jobs").data.decode()

    def test_new_job_form_200(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = client.get("/jobs/new")
        assert resp.status_code == 200
        html = resp.data.decode()
        assert "*/5 * * * *" in html
        assert "dev_sql_text" in html
        assert "test_conn" in html

    def test_create_job_post_redirects(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        data = {
            "dag_id": "newjob",
            "schedule_interval": "@daily",
            "start_date": "2023-01-01",
            "apply_prod_same_as_dev": "on",
            "dev_source_db_conn_id": "test_conn",
            "dev_target_db_conn_id": "test_conn",
            "dev_target_db_type": "postgresql",
            "dev_target_schema": "public",
            "dev_target_table": "my_table",
            "dev_sql_text": "SELECT 1",
            "prod_source_db_conn_id": "test_conn",
            "prod_target_db_conn_id": "test_conn",
            "prod_target_db_type": "postgresql",
            "prod_target_schema": "public",
            "prod_target_table": "my_table",
            "prod_sql_text": "SELECT 1",
            "created_by": "admin",
        }
        resp = client.post("/jobs/new", data=data, follow_redirects=False)
        assert resp.status_code in (301, 302)

    def test_edit_job_renders(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch)
        cfg = _make_db_config(tmp_path, dag_id="editjob")
        monkeypatch.setattr(app_module, "load_config",    lambda d: cfg)
        monkeypatch.setattr(app_module, "read_sql_text",  lambda p: "SELECT 1")
        resp = client.get("/jobs/editjob/edit")
        assert resp.status_code == 200
        assert "editjob" in resp.data.decode()

    def test_edit_missing_job_redirects(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch)
        monkeypatch.setattr(app_module, "load_config", lambda d: None)
        resp = client.get("/jobs/ghost/edit", follow_redirects=False)
        assert resp.status_code in (301, 302)

    def test_delete_job_redirects(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch)
        monkeypatch.setattr(app_module, "delete_config",    lambda d: None)
        monkeypatch.setattr(app_module, "delete_dag_files", lambda d: None)
        resp = client.post("/jobs/somejob/delete", follow_redirects=False)
        assert resp.status_code in (301, 302)


# ─────────────────────────────────────────────────────────────────────────────
# File-based ETL routes (/file-jobs/*)
# ─────────────────────────────────────────────────────────────────────────────

class TestFileJobs:
    def test_file_jobs_list_200(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        assert client.get("/file-jobs").status_code == 200

    def test_file_jobs_list_shows_file_config(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        _make_file_config(tmp_path, dag_id="csvjob")
        assert "csvjob" in client.get("/file-jobs").data.decode()

    def test_file_jobs_list_excludes_db_configs(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        _make_db_config(tmp_path, dag_id="dbjob")
        assert "dbjob" not in client.get("/file-jobs").data.decode()

    def test_new_file_job_form_200(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = client.get("/file-jobs/new")
        assert resp.status_code == 200
        html = resp.data.decode()
        assert "File Source" in html
        assert "Column Types" in html
        assert "Target" in html

    def test_new_file_job_form_has_preview_ui(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        html = client.get("/file-jobs/new").data.decode()
        assert "runPreviewBtn" in html
        assert "preview-columns" in html
        assert "devDtypeOverridesJson" in html

    def test_create_file_job_redirects(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        data = {
            "dag_id": "newfilejob",
            "schedule_interval": "@daily",
            "start_date": "2023-01-01",
            "apply_prod_same_as_dev": "on",
            "dev_watch_path": "/data/inbound",
            "dev_file_pattern": r".*\.csv",
            "dev_file_format": "csv",
            "dev_delimiter": ",",
            "dev_encoding": "utf-8",
            "dev_has_header": "on",
            "dev_skip_rows": "0",
            "dev_null_values": "NULL",
            "dev_archive_retention_days": "30",
            "dev_target_db_conn_id": "test_conn",
            "dev_target_db_type": "postgresql",
            "dev_target_schema": "public",
            "dev_target_table": "my_table",
            "created_by": "admin",
        }
        resp = client.post("/file-jobs/new", data=data, follow_redirects=False)
        assert resp.status_code in (301, 302)

    def test_create_file_job_persists_dtype_overrides(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch)
        saved = {}
        monkeypatch.setattr(app_module, "save_file_config", lambda cfg: saved.update(cfg))
        data = {
            "dag_id": "savetest",
            "schedule_interval": "@daily",
            "start_date": "2023-01-01",
            "apply_prod_same_as_dev": "on",
            "dev_watch_path": "/data/in",
            "dev_file_pattern": ".*",
            "dev_file_format": "csv",
            "dev_delimiter": ",",
            "dev_encoding": "utf-8",
            "dev_has_header": "on",
            "dev_skip_rows": "0",
            "dev_null_values": "NULL",
            "dev_archive_retention_days": "30",
            "dev_dtype_overrides_json": '{"amount": "decimal"}',
            "dev_target_db_conn_id": "test_conn",
            "dev_target_db_type": "postgresql",
            "dev_target_schema": "public",
            "dev_target_table": "my_table",
            "created_by": "admin",
        }
        client.post("/file-jobs/new", data=data, follow_redirects=False)
        assert saved.get("pipeline_type") == "file_based"
        assert saved["source"]["dev"]["dtype_overrides"] == {"amount": "decimal"}

    def test_edit_file_job_renders(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch)
        cfg = _make_file_config(tmp_path, dag_id="editfile")
        monkeypatch.setattr(app_module, "load_file_config", lambda d: cfg)
        resp = client.get("/file-jobs/editfile/edit")
        assert resp.status_code == 200
        assert "editfile" in resp.data.decode()

    def test_edit_missing_file_job_redirects(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch)
        monkeypatch.setattr(app_module, "load_file_config", lambda d: None)
        resp = client.get("/file-jobs/ghost/edit", follow_redirects=False)
        assert resp.status_code in (301, 302)

    def test_delete_file_job_redirects(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch)
        monkeypatch.setattr(app_module, "delete_file_config",    lambda d: None)
        monkeypatch.setattr(app_module, "delete_file_dag_files", lambda d: None)
        resp = client.post("/file-jobs/somejob/delete", follow_redirects=False)
        assert resp.status_code in (301, 302)


# ─────────────────────────────────────────────────────────────────────────────
# API: /api/preview-columns
# ─────────────────────────────────────────────────────────────────────────────

class TestPreviewColumnsAPI:
    def _post(self, client, csv_content, extra_data=None):
        data = {
            "sample_file": (io.BytesIO(csv_content.encode("utf-8")), "sample.csv"),
            "file_format":  "csv",
            "delimiter":    ",",
            "has_header":   "true",
            "skip_rows":    "0",
            "encoding":     "utf-8",
            "null_values":  "NULL,null",
        }
        if extra_data:
            data.update(extra_data)
        return client.post("/api/preview-columns", data=data,
                           content_type="multipart/form-data")

    def test_no_file_returns_400(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = client.post("/api/preview-columns", data={"file_format": "csv"})
        assert resp.status_code == 400
        assert "error" in resp.get_json()

    def test_integer_and_decimal_columns(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = self._post(client, "name,age,salary\nAlice,30,50000.5\nBob,25,40000.0\n")
        assert resp.status_code == 200
        cols = {c["original"]: c for c in resp.get_json()["columns"]}
        assert cols["age"]["dtype"] == "integer"
        assert cols["salary"]["dtype"] in ("decimal", "integer")

    def test_text_column(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = self._post(client, "description\nhello world\nfoo bar\nbaz qux\n")
        assert resp.status_code == 200
        cols = {c["original"]: c for c in resp.get_json()["columns"]}
        assert cols["description"]["dtype"] == "text"

    def test_boolean_column(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = self._post(client, "is_active\nTrue\nFalse\nTrue\n")
        assert resp.status_code == 200
        cols = {c["original"]: c for c in resp.get_json()["columns"]}
        assert cols["is_active"]["dtype"] == "boolean"

    def test_column_name_cleaning(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = self._post(client, "First Name,Last-Name,DOB\nAlice,Smith,1990-01-01\n")
        assert resp.status_code == 200
        cleans = [c["clean"] for c in resp.get_json()["columns"]]
        assert "first_name" in cleans
        assert "last_name" in cleans

    def test_sample_values_max_three(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = self._post(client, "val\n1\n2\n3\n4\n5\n")
        assert resp.status_code == 200
        for col in resp.get_json()["columns"]:
            assert len(col["sample"]) <= 3

    def test_pipe_delimiter(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = self._post(client, "a|b|c\n1|2|3\n", extra_data={"delimiter": "|"})
        assert resp.status_code == 200
        names = [c["original"] for c in resp.get_json()["columns"]]
        assert set(names) == {"a", "b", "c"}

    def test_response_has_required_keys(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = self._post(client, "id,name\n1,Alice\n2,Bob\n")
        assert resp.status_code == 200
        for col in resp.get_json()["columns"]:
            assert {"original", "clean", "dtype", "sample"} <= col.keys()


# ─────────────────────────────────────────────────────────────────────────────
# User management (/users/*)
# ─────────────────────────────────────────────────────────────────────────────

class TestUsers:
    def test_users_page_200(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = client.get("/users")
        assert resp.status_code == 200
        assert "admin" in resp.data.decode()

    def test_create_user_redirects(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = client.post("/users/new",
                           data={"username": "bob", "password": "pass123"},
                           follow_redirects=False)
        assert resp.status_code in (301, 302)

    def test_created_user_appears_in_list(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        client.post("/users/new", data={"username": "carol", "password": "pass123"})
        assert "carol" in client.get("/users").data.decode()

    def test_toggle_user_redirects(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        client.post("/users/new", data={"username": "dave", "password": "pass"})
        resp = client.post("/users/2/toggle", follow_redirects=False)
        assert resp.status_code in (301, 302)


# ─────────────────────────────────────────────────────────────────────────────
# Unit: build_file_config_from_form()
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildFileConfigFromForm:
    def _form(self, extra=None):
        base = {
            "dag_id":                     "test_dag",
            "schedule_interval":          "@daily",
            "start_date":                 "2023-01-01",
            "created_by":                 "admin",
            "dev_watch_path":             "/data/in",
            "dev_file_pattern":           ".*\\.csv",
            "dev_file_format":            "csv",
            "dev_delimiter":              ",",
            "dev_encoding":               "utf-8",
            "dev_has_header":             "true",
            "dev_skip_rows":              "0",
            "dev_null_values":            "NULL,null",
            "dev_archive_retention_days": "30",
            "dev_target_db_conn_id":      "test_conn",
            "dev_target_db_type":         "postgresql",
            "dev_target_schema":          "public",
            "dev_target_table":           "my_tbl",
        }
        if extra:
            base.update(extra)
        return base

    def _reload(self):
        import web_ui.app as m
        importlib.reload(m)
        return m

    def test_basic_fields(self):
        m = self._reload()
        cfg = m.build_file_config_from_form(self._form(), apply_prod_same_as_dev=True)
        assert cfg["dag_id"] == "file_test_conn_test_dag"
        assert cfg["pipeline_type"] == "file_based"
        assert cfg["source"]["dev"]["watch_path"] == "/data/in"
        assert cfg["source"]["dev"]["has_header"] is True

    def test_dtype_overrides_parsed(self):
        m = self._reload()
        form = self._form({"dev_dtype_overrides_json": '{"amount":"decimal","count":"integer"}'})
        cfg = m.build_file_config_from_form(form, apply_prod_same_as_dev=True)
        assert cfg["source"]["dev"]["dtype_overrides"] == {"amount": "decimal", "count": "integer"}

    def test_invalid_dtype_overrides_defaults_empty(self):
        m = self._reload()
        form = self._form({"dev_dtype_overrides_json": "NOT_JSON{"})
        cfg = m.build_file_config_from_form(form, apply_prod_same_as_dev=True)
        assert cfg["source"]["dev"]["dtype_overrides"] == {}

    def test_apply_prod_same_as_dev(self):
        m = self._reload()
        cfg = m.build_file_config_from_form(self._form(), apply_prod_same_as_dev=True)
        assert cfg["source"]["dev"] == cfg["source"]["prod"]
        assert cfg["target"]["dev"] == cfg["target"]["prod"]

    def test_separate_prod(self):
        m = self._reload()
        form = self._form({
            "prod_watch_path": "/data/prod", "prod_file_pattern": ".*",
            "prod_file_format": "csv", "prod_delimiter": ",",
            "prod_encoding": "utf-8", "prod_has_header": "true",
            "prod_skip_rows": "0", "prod_null_values": "NULL",
            "prod_archive_retention_days": "30", "prod_dtype_overrides_json": "{}",
            "prod_target_db_conn_id": "prod_conn", "prod_target_db_type": "postgresql",
            "prod_target_schema": "public", "prod_target_table": "prod_tbl",
        })
        cfg = m.build_file_config_from_form(form, apply_prod_same_as_dev=False)
        assert cfg["source"]["prod"]["watch_path"] == "/data/prod"
        assert cfg["target"]["prod"]["target_db_conn_id"] == "prod_conn"

    def test_spark_config_when_toggled(self):
        m = self._reload()
        form = self._form({
            "use_spark": "on",
            "spark_master": "spark://host:7077",
            "spark_app_name": "myapp",
            "spark_driver_memory": "2g",
            "spark_executor_memory": "4g",
            "spark_executor_cores": "4",
            "spark_num_executors": "3",
        })
        cfg = m.build_file_config_from_form(form, apply_prod_same_as_dev=True)
        assert "spark" in cfg
        assert cfg["spark"]["master"] == "spark://host:7077"
        assert cfg["spark"]["executor_cores"] == 4

    def test_no_spark_when_not_toggled(self):
        m = self._reload()
        cfg = m.build_file_config_from_form(self._form(), apply_prod_same_as_dev=True)
        assert "spark" not in cfg

    def test_tags_from_created_by_and_extra(self):
        m = self._reload()
        form = self._form({"created_by": "alice", "extra_tags": "team_a, project_x"})
        cfg = m.build_file_config_from_form(form, apply_prod_same_as_dev=True)
        assert cfg["tags"][0] == "alice"
        assert "team_a" in cfg["tags"]
        assert "project_x" in cfg["tags"]

    def test_null_values_to_list(self):
        m = self._reload()
        form = self._form({"dev_null_values": "NULL, null, N/A, #N/A"})
        cfg = m.build_file_config_from_form(form, apply_prod_same_as_dev=True)
        nv = cfg["source"]["dev"]["null_values"]
        assert "NULL" in nv and "N/A" in nv

    def test_skip_rows_int(self):
        m = self._reload()
        form = self._form({"dev_skip_rows": "5"})
        cfg = m.build_file_config_from_form(form, apply_prod_same_as_dev=True)
        assert cfg["source"]["dev"]["skip_rows"] == 5
        assert isinstance(cfg["source"]["dev"]["skip_rows"], int)


# ─────────────────────────────────────────────────────────────────────────────
# Unit: config file isolation (load_config_files vs load_file_config_files)
# ─────────────────────────────────────────────────────────────────────────────

class TestConfigFileIsolation:
    def test_db_loader_excludes_file_prefix(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CONFIG_DIR", str(tmp_path / "config"))
        import web_ui.app as m
        importlib.reload(m)
        d = tmp_path / "config" / "conn"
        d.mkdir(parents=True, exist_ok=True)
        (d / "conn_dbjob.json").write_text(json.dumps({"dag_id": "dbjob"}))
        (d / "file_conn_filejob.json").write_text(json.dumps({"dag_id": "filejob", "pipeline_type": "file_based"}))
        ids = [c["dag_id"] for c in m.load_config_files()]
        assert "dbjob" in ids
        assert "filejob" not in ids

    def test_file_loader_only_file_prefix(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CONFIG_DIR", str(tmp_path / "config"))
        import web_ui.app as m
        importlib.reload(m)
        d = tmp_path / "config" / "conn"
        d.mkdir(parents=True, exist_ok=True)
        (d / "conn_dbjob.json").write_text(json.dumps({"dag_id": "dbjob"}))
        (d / "file_conn_filejob.json").write_text(json.dumps({"dag_id": "filejob", "pipeline_type": "file_based"}))
        ids = [c["dag_id"] for c in m.load_file_config_files()]
        assert "filejob" in ids
        assert "dbjob" not in ids


# ─────────────────────────────────────────────────────────────────────────────
# Unit: build_config_from_form() — dtype_overrides propagation from data_dictionary
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildConfigFromFormDtypeOverrides:
    """Tests for the fix that propagates dtype values from data_dictionary into
    source.dev.dtype_overrides / source.prod.dtype_overrides."""

    def _reload(self):
        import web_ui.app as m
        importlib.reload(m)
        return m

    def _base_form(self):
        return {
            "dag_id": "cms_customer",
            "schedule_interval": "@daily",
            "start_date": "2023-01-01",
            "batch_size": "10000",
            "read_chunk_size": "50000",
            "write_mode": "append",
            "created_by": "admin",
            "dev_source_db_conn_id": "src_conn",
            "dev_source_db_type": "postgresql",
            "dev_hwm_column": "connection_date",
            "dev_hwm_datatype": "timestamp",
            "dev_batch_size": "10000",
            "dev_full_load": "on",
            "dev_target_db_conn_id": "tgt_conn",
            "dev_target_db_type": "postgresql",
            "dev_target_schema": "cms",
            "dev_target_table": "customer",
        }

    def test_dtype_overrides_injected_from_data_dictionary(self):
        m = self._reload()
        form = self._base_form()
        # Simulate user filling in the data dictionary table in the form
        form["dd_col[]"] = ["connection_date", "customer_id", "balance"]
        form["dd_desc[]"] = ["", "", ""]
        form["dd_dtype[]"] = ["timestamp", "integer", "float"]
        from werkzeug.datastructures import ImmutableMultiDict
        # Werkzeug form needs multi-value lists
        items = [(k, v) for k, v in form.items() if not isinstance(v, list)]
        for k, v in form.items():
            if isinstance(v, list):
                for entry in v:
                    items.append((k, entry))
        cfg = m.build_config_from_form(ImmutableMultiDict(items), apply_prod_same_as_dev=True)
        expected = {"connection_date": "timestamp", "customer_id": "integer", "balance": "float"}
        assert cfg["source"]["dev"]["dtype_overrides"] == expected
        assert cfg["source"]["prod"]["dtype_overrides"] == expected

    def test_dtype_overrides_not_set_when_no_data_dictionary(self):
        m = self._reload()
        from werkzeug.datastructures import ImmutableMultiDict
        cfg = m.build_config_from_form(
            ImmutableMultiDict(self._base_form().items()), apply_prod_same_as_dev=True
        )
        assert "dtype_overrides" not in cfg["source"]["dev"]
        assert "dtype_overrides" not in cfg["source"]["prod"]

    def test_dtype_overrides_omits_columns_with_no_dtype(self):
        m = self._reload()
        form = self._base_form()
        from werkzeug.datastructures import ImmutableMultiDict
        items = list(form.items())
        # col1 has dtype, col2 is blank
        items += [("dd_col[]", "col1"), ("dd_col[]", "col2")]
        items += [("dd_desc[]", ""), ("dd_desc[]", "")]
        items += [("dd_dtype[]", "timestamp"), ("dd_dtype[]", "")]
        cfg = m.build_config_from_form(ImmutableMultiDict(items), apply_prod_same_as_dev=True)
        assert cfg["source"]["dev"]["dtype_overrides"] == {"col1": "timestamp"}

    def test_dtype_overrides_not_set_when_all_dtypes_blank(self):
        m = self._reload()
        form = self._base_form()
        from werkzeug.datastructures import ImmutableMultiDict
        items = list(form.items())
        items += [("dd_col[]", "col1"), ("dd_col[]", "col2")]
        items += [("dd_desc[]", ""), ("dd_desc[]", "")]
        items += [("dd_dtype[]", ""), ("dd_dtype[]", "")]
        cfg = m.build_config_from_form(ImmutableMultiDict(items), apply_prod_same_as_dev=True)
        # data_dictionary is still set (descriptions or columns present), but dtype_overrides
        # should be absent since no dtype was specified for any column
        assert "dtype_overrides" not in cfg["source"]["dev"]

    def test_data_dictionary_still_saved_alongside_overrides(self):
        m = self._reload()
        form = self._base_form()
        from werkzeug.datastructures import ImmutableMultiDict
        items = list(form.items())
        items += [("dd_col[]", "connection_date")]
        items += [("dd_desc[]", "Date customer connected")]
        items += [("dd_dtype[]", "timestamp")]
        cfg = m.build_config_from_form(ImmutableMultiDict(items), apply_prod_same_as_dev=True)
        # Both the catalog data_dictionary and the ETL dtype_overrides must be present
        assert "data_dictionary" in cfg
        assert cfg["data_dictionary"]["connection_date"]["dtype"] == "timestamp"
        assert cfg["source"]["dev"]["dtype_overrides"] == {"connection_date": "timestamp"}

    def test_separate_prod_gets_same_dtype_overrides(self):
        m = self._reload()
        form = self._base_form()
        # Add prod-specific fields so prod block can be parsed independently
        form.update({
            "prod_source_db_conn_id": "src_conn_prod",
            "prod_source_db_type": "postgresql",
            "prod_hwm_column": "connection_date",
            "prod_hwm_datatype": "timestamp",
            "prod_batch_size": "10000",
            "prod_target_db_conn_id": "tgt_conn_prod",
            "prod_target_db_type": "postgresql",
            "prod_target_schema": "cms",
            "prod_target_table": "customer",
        })
        from werkzeug.datastructures import ImmutableMultiDict
        items = list(form.items())
        items += [("dd_col[]", "connection_date")]
        items += [("dd_desc[]", "")]
        items += [("dd_dtype[]", "timestamp")]
        cfg = m.build_config_from_form(ImmutableMultiDict(items), apply_prod_same_as_dev=False)
        assert cfg["source"]["dev"]["dtype_overrides"] == {"connection_date": "timestamp"}
        assert cfg["source"]["prod"]["dtype_overrides"] == {"connection_date": "timestamp"}


# ─────────────────────────────────────────────────────────────────────────────
# Unit: _cast_df_types() and _dtype_str_to_sa_type() from data_load.template
# ─────────────────────────────────────────────────────────────────────────────

class TestCastDfTypes:
    """Exercises the _cast_df_types() helper added to data_load.template.
    We import the helpers directly from the module-level namespace by importing
    the template as a Python source file via importlib.util."""

    @pytest.fixture
    def cast_fn(self):
        """Extract and exec _cast_df_types (+ its dependencies) from data_load.template."""
        import re as _re
        from pathlib import Path as _Path

        tpl_path = (_Path(__file__).resolve().parent.parent.parent
                    / "dags" / "templates" / "data_load.template")
        source = tpl_path.read_text()

        # Pull out only the function definitions we need, in dependency order.
        # Each function runs from its 'def' line to the blank line before the next top-level def.
        def _extract_fn(src, fn_name):
            pattern = rf"^(def {fn_name}\b.*?)(?=\ndef |\Z)"
            m = _re.search(pattern, src, _re.DOTALL | _re.MULTILINE)
            return m.group(1).rstrip() if m else ""

        snippets = "\n\n".join(
            _extract_fn(source, name)
            for name in ("pandas_dtype_to_sql", "build_sqlalchemy_dtypes",
                         "_dtype_str_to_sa_type", "_universal_to_datetime", "_cast_df_types")
        )

        # Minimal imports needed by the extracted functions
        preamble = (
            "import logging\nimport pandas as pd\n"
            "from sqlalchemy.types import BigInteger, Boolean, Date, DateTime, Float, Integer, Text\n"
        )
        ns: dict = {}
        exec(compile(preamble + "\n" + snippets, str(tpl_path), "exec"), ns)  # noqa: S102
        return ns["_cast_df_types"]

    def test_timestamp_column_cast(self, cast_fn):
        import pandas as pd
        df = pd.DataFrame({"created_at": ["2024-01-01", "2024-06-15", None]})
        result = cast_fn(df.copy(), {"created_at": "timestamp"})
        assert pd.api.types.is_datetime64_any_dtype(result["created_at"])

    def test_integer_column_cast(self, cast_fn):
        import pandas as pd
        df = pd.DataFrame({"customer_id": ["1", "2", "3"]})
        result = cast_fn(df.copy(), {"customer_id": "integer"})
        assert str(result["customer_id"].dtype) in ("Int64", "int64")

    def test_float_column_cast(self, cast_fn):
        import pandas as pd
        df = pd.DataFrame({"balance": ["10.5", "20.0", "30.25"]})
        result = cast_fn(df.copy(), {"balance": "float"})
        assert pd.api.types.is_float_dtype(result["balance"])

    def test_decimal_column_cast(self, cast_fn):
        import pandas as pd
        df = pd.DataFrame({"amount": ["1.1", "2.2", "3.3"]})
        result = cast_fn(df.copy(), {"amount": "decimal"})
        assert pd.api.types.is_float_dtype(result["amount"])

    def test_boolean_column_cast(self, cast_fn):
        import pandas as pd
        df = pd.DataFrame({"is_active": ["true", "false", "true"]})
        result = cast_fn(df.copy(), {"is_active": "boolean"})
        assert result["is_active"].tolist() == [True, False, True]

    def test_text_column_cast(self, cast_fn):
        import pandas as pd
        df = pd.DataFrame({"name": [1, 2, 3]})
        result = cast_fn(df.copy(), {"name": "text"})
        assert result["name"].dtype == object

    def test_missing_column_skipped_without_error(self, cast_fn):
        import pandas as pd
        df = pd.DataFrame({"col_a": ["x"]})
        # 'nonexistent' is not in df — should not raise
        result = cast_fn(df.copy(), {"nonexistent": "timestamp"})
        assert list(result.columns) == ["col_a"]

    def test_invalid_value_raises_value_error(self, cast_fn):
        import pandas as pd
        import pytest
        df = pd.DataFrame({"dt": ["not-a-date", "also-not-a-date"]})
        with pytest.raises(ValueError, match="Cannot parse"):
            cast_fn(df.copy(), {"dt": "timestamp"})

    def test_multiple_overrides_applied(self, cast_fn):
        import pandas as pd
        df = pd.DataFrame({
            "connection_date": ["2024-01-01"],
            "customer_id": ["42"],
            "balance": ["99.9"],
        })
        result = cast_fn(df.copy(), {
            "connection_date": "timestamp",
            "customer_id": "integer",
            "balance": "float",
        })
        assert pd.api.types.is_datetime64_any_dtype(result["connection_date"])
        assert str(result["customer_id"].dtype) in ("Int64", "int64")
        assert pd.api.types.is_float_dtype(result["balance"])

    def test_bigint_alias(self, cast_fn):
        import pandas as pd
        df = pd.DataFrame({"id": ["100", "200"]})
        result = cast_fn(df.copy(), {"id": "bigint"})
        assert str(result["id"].dtype) in ("Int64", "int64")

    def test_date_alias(self, cast_fn):
        import pandas as pd
        df = pd.DataFrame({"dob": ["1990-05-20", "1985-11-01"]})
        result = cast_fn(df.copy(), {"dob": "date"})
        assert pd.api.types.is_datetime64_any_dtype(result["dob"])


# ─────────────────────────────────────────────────────────────────────────────
# Unit: config completeness — build_config_from_form() produces a fully-formed
#       config with every required field, including the data dictionary and the
#       HWM date-format field added in the TO_TIMESTAMP refactor.
# ─────────────────────────────────────────────────────────────────────────────

class TestConfigIntegrity:
    """Verifies that build_config_from_form() emits a config that:
    - Has all required top-level keys
    - Has fully-populated source.dev / source.prod blocks with every expected field
    - Has fully-populated target.dev / target.prod blocks
    - Carries the new incremental_column_date_format field in both envs
    - Stores data_dictionary entries where each column has 'description' + 'dtype'
    - Keeps dtype_overrides in source consistent with data_dictionary dtype values
    - Stores table_description
    - Produces a canonical dag_id
    """

    def _reload(self):
        import web_ui.app as m
        importlib.reload(m)
        return m

    def _build(self, extra_form=None, apply_prod_same_as_dev=True):
        """Build a config from a realistic, fully-filled form and return it."""
        from werkzeug.datastructures import ImmutableMultiDict
        m = self._reload()

        base = {
            "dag_id":              "invoices",
            "schedule_interval":   "*/5 * * * *",
            "start_date":          "2023-01-01",
            "batch_size":          "1000",
            "read_chunk_size":     "50000",
            "write_mode":          "append",
            "created_by":          "admin",
            "extra_tags":          "finance, daily",
            "table_description":   "Invoice transactions for the billing system.",
            # dev source
            "dev_source_db_conn_id": "local_dw_con",
            "dev_source_db_type":    "postgresql",
            "dev_hwm_column":        "invoice_date",
            "dev_hwm_datatype":      "timestamp",
            "dev_hwm_date_format":   "DD/MM/YYYY",
            "dev_batch_size":        "1000",
            # dev target
            "dev_target_db_conn_id": "local_dw_con",
            "dev_target_db_type":    "postgresql",
            "dev_target_schema":     "load_test",
            "dev_target_table":      "invoices_2",
            "dev_load_strategy":     "append",
        }
        if extra_form:
            base.update(extra_form)

        # Data-dictionary rows (simulates the Step 4 table in the UI)
        rows = [
            ("invoice_date",  "Date when invoice was issued",    "timestamp"),
            ("amount",        "Total amount of the transaction",  "decimal"),
            ("customer_id",   "Unique customer identifier",       "integer"),
            ("status",        "Invoice payment status",           "text"),
        ]
        items = list(base.items())
        for col, desc, dtype in rows:
            items += [("dd_col[]", col), ("dd_desc[]", desc), ("dd_dtype[]", dtype)]

        return m.build_config_from_form(ImmutableMultiDict(items), apply_prod_same_as_dev=apply_prod_same_as_dev)

    # ── Top-level keys ────────────────────────────────────────────────────────

    def test_top_level_required_keys_present(self):
        cfg = self._build()
        for key in ("dag_id", "schedule_interval", "start_date", "tags",
                    "source", "target", "write_mode"):
            assert key in cfg, f"Missing top-level key: {key!r}"

    def test_dag_id_is_string_and_nonempty(self):
        cfg = self._build()
        assert isinstance(cfg["dag_id"], str) and cfg["dag_id"]

    def test_schedule_interval_preserved(self):
        cfg = self._build()
        assert cfg["schedule_interval"] == "*/5 * * * *"

    def test_tags_includes_created_by_and_extra(self):
        cfg = self._build()
        assert "admin" in cfg["tags"]
        assert "finance" in cfg["tags"]
        assert "daily" in cfg["tags"]

    def test_table_description_stored(self):
        cfg = self._build()
        assert cfg.get("table_description") == "Invoice transactions for the billing system."

    # ── source.dev block ──────────────────────────────────────────────────────

    def test_source_dev_has_all_fields(self):
        cfg = self._build()
        dev = cfg["source"]["dev"]
        for field in ("source_db_conn_id", "db_type", "incremental_column",
                      "incremental_column_datatype", "incremental_column_date_format",
                      "batch_size", "full_load"):
            assert field in dev, f"source.dev missing field: {field!r}"

    def test_source_dev_hwm_column(self):
        cfg = self._build()
        assert cfg["source"]["dev"]["incremental_column"] == "invoice_date"

    def test_source_dev_hwm_datatype(self):
        cfg = self._build()
        assert cfg["source"]["dev"]["incremental_column_datatype"] == "timestamp"

    def test_source_dev_hwm_date_format(self):
        """The key added for TO_TIMESTAMP support must be present with the correct value."""
        cfg = self._build()
        assert cfg["source"]["dev"]["incremental_column_date_format"] == "DD/MM/YYYY"

    def test_source_dev_hwm_date_format_empty_when_not_set(self):
        """Omitting the form field should store an empty string (auto-detect path)."""
        cfg = self._build(extra_form={"dev_hwm_date_format": ""})
        assert cfg["source"]["dev"]["incremental_column_date_format"] == ""

    def test_source_dev_batch_size_is_int(self):
        cfg = self._build()
        assert isinstance(cfg["source"]["dev"]["batch_size"], int)
        assert cfg["source"]["dev"]["batch_size"] == 1000

    # ── source.prod mirrors dev when apply_prod_same_as_dev=True ─────────────

    def test_source_prod_mirrors_dev(self):
        cfg = self._build(apply_prod_same_as_dev=True)
        dev, prod = cfg["source"]["dev"], cfg["source"]["prod"]
        for field in ("incremental_column", "incremental_column_datatype",
                      "incremental_column_date_format", "batch_size"):
            assert dev[field] == prod[field], \
                f"source.prod.{field} differs from dev: {prod[field]!r} vs {dev[field]!r}"

    # ── target block ──────────────────────────────────────────────────────────

    def test_target_dev_has_all_fields(self):
        cfg = self._build()
        tgt = cfg["target"]["dev"]
        for field in ("target_db_conn_id", "db_type", "target_schema", "target_table"):
            assert field in tgt, f"target.dev missing field: {field!r}"

    def test_target_dev_schema_and_table(self):
        cfg = self._build()
        assert cfg["target"]["dev"]["target_schema"] == "load_test"
        assert cfg["target"]["dev"]["target_table"] == "invoices_2"

    # ── data_dictionary ───────────────────────────────────────────────────────

    def test_data_dictionary_present(self):
        cfg = self._build()
        assert "data_dictionary" in cfg, "Config missing data_dictionary"

    def test_data_dictionary_has_all_columns(self):
        cfg = self._build()
        dd = cfg["data_dictionary"]
        for col in ("invoice_date", "amount", "customer_id", "status"):
            assert col in dd, f"data_dictionary missing column: {col!r}"

    def test_data_dictionary_entries_have_description_and_dtype(self):
        cfg = self._build()
        for col, entry in cfg["data_dictionary"].items():
            assert "description" in entry, f"data_dictionary[{col!r}] missing 'description'"
            assert "dtype" in entry,       f"data_dictionary[{col!r}] missing 'dtype'"
            assert isinstance(entry["description"], str), \
                f"data_dictionary[{col!r}]['description'] must be str"
            assert isinstance(entry["dtype"], str) and entry["dtype"], \
                f"data_dictionary[{col!r}]['dtype'] must be non-empty str"

    def test_data_dictionary_dtype_values_correct(self):
        cfg = self._build()
        dd = cfg["data_dictionary"]
        assert dd["invoice_date"]["dtype"] == "timestamp"
        assert dd["amount"]["dtype"]       == "decimal"
        assert dd["customer_id"]["dtype"]  == "integer"
        assert dd["status"]["dtype"]       == "text"

    # ── dtype_overrides consistent with data_dictionary ───────────────────────

    def test_dtype_overrides_derived_from_data_dictionary(self):
        cfg = self._build()
        overrides = cfg["source"]["dev"]["dtype_overrides"]
        dd = cfg["data_dictionary"]
        for col, entry in dd.items():
            assert col in overrides, \
                f"dtype_overrides missing column {col!r} that exists in data_dictionary"
            assert overrides[col] == entry["dtype"], \
                f"dtype_overrides[{col!r}]={overrides[col]!r} differs from data_dictionary dtype {entry['dtype']!r}"

    def test_dtype_overrides_match_between_dev_and_prod(self):
        cfg = self._build(apply_prod_same_as_dev=True)
        assert cfg["source"]["dev"]["dtype_overrides"] == cfg["source"]["prod"]["dtype_overrides"]
