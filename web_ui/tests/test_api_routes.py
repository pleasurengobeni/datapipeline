"""
Tests for the API source_type's web UI layer. API is one of the three
source_type choices ("file" | "db" | "api") in the single Hybrid Job
wizard/routes — not a separate wizard — so these tests exercise:
  - build_hybrid_config_from_form(source_type="api") (secret encryption,
    mask-on-edit convention, filters/HWM/pagination round-trip)
  - POST /api/api-test-connection (response-format auto-detection, auth
    header building, never echoing a secret back)
  - POST /api/api-detect-columns (column/dtype inference over API records)
  - POST /api/suggest-descriptions (AI response-format suggestion + the
    multi-provider fallback resilience it shares with column/table
    suggestions used by all three source types)

All outbound HTTP is mocked (unittest.mock.patch on urllib.request.urlopen)
— no real network access, no Airflow, no PostgreSQL required.

Run with:  pytest web_ui/tests/test_api_routes.py -v
"""
import json
from unittest.mock import MagicMock, patch

import pytest

from cryptography.fernet import Fernet

from web_ui.tests.test_app import build_test_client

# Fixed for the whole test session: dags.modules.secrets_crypto caches its
# Fernet instance after the first encrypt/decrypt call, so re-setting a
# DIFFERENT key per test wouldn't actually take effect on the second test
# anyway. One fixed key, set before any test in this module runs, is
# correct and sufficient.
_TEST_FERNET_KEY = Fernet.generate_key().decode()


def _client(tmp_path, monkeypatch):
    monkeypatch.setenv("WEBUI_FERNET_KEY", _TEST_FERNET_KEY)
    return build_test_client(tmp_path, monkeypatch)


def _mock_response(body_bytes, status=200):
    resp = MagicMock()
    resp.read.return_value = body_bytes
    resp.status = status
    cm = MagicMock()
    cm.__enter__.return_value = resp
    cm.__exit__.return_value = False
    return cm


# ─────────────────────────────────────────────────────────────────────────────
# build_hybrid_config_from_form(source_type="api") — API is a source_type
# choice inside the one Hybrid Job wizard/builder, not a separate function.
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildApiConfigFromForm:
    def _base_form(self, **overrides):
        form = {
            "dag_id": "weather", "schedule_interval": "@daily", "source_type": "api",
            # Data Endpoint was folded into Base URL — the full URL lives
            # here now, no separate dev_data_endpoint field.
            "dev_base_url": "https://api.example.com/v1/weather",
            "dev_http_method": "GET", "dev_auth_type": "none",
            "dev_target_db_conn_id": "test_conn", "dev_target_schema": "raw", "dev_target_table": "weather",
            "apply_prod_same_as_dev": "on",
        }
        form.update(overrides)
        return form

    def test_minimal_none_auth(self, tmp_path, monkeypatch):
        _, app_module = _client(tmp_path, monkeypatch)
        config = app_module.build_hybrid_config_from_form(self._base_form(), True)
        # API is a standalone job type from the user's perspective (own
        # "api_" dag_id prefix), but is built by this same function/wizard
        # as source_type "api", to reuse hybrid_load.template's
        # raw -> refined -> target code.
        assert config["pipeline_type"] == "hybrid"
        assert config["source_type"] == "api"
        assert config["source"]["dev"]["base_url"] == "https://api.example.com/v1/weather"
        assert config["source"]["dev"]["auth"] == {"type": "none"}
        assert config["source"]["prod"]["base_url"] == "https://api.example.com/v1/weather"
        assert config["dag_id"].startswith("api_test_conn_")

    def test_api_key_secret_is_encrypted_not_plaintext(self, tmp_path, monkeypatch):
        _, app_module = _client(tmp_path, monkeypatch)
        form = self._base_form(
            dev_auth_type="api_key_header",
            dev_auth_header_name="X-API-Key",
            dev_auth_api_key="super-secret-value",
        )
        config = app_module.build_hybrid_config_from_form(form, True)
        enc = config["source"]["dev"]["auth"]["api_key_enc"]
        assert enc != "super-secret-value"
        assert app_module.secrets_crypto.decrypt(enc) == "super-secret-value"

    def test_api_key_query_param_secret_is_encrypted(self, tmp_path, monkeypatch):
        # A common REST auth pattern for map/geospatial services and similar
        # APIs — a static token sent as a query param, not a header.
        _, app_module = _client(tmp_path, monkeypatch)
        form = self._base_form(
            dev_auth_type="api_key_query_param",
            dev_auth_param_name="token",
            dev_auth_api_key_param="super-secret-token-value",
        )
        config = app_module.build_hybrid_config_from_form(form, True)
        auth = config["source"]["dev"]["auth"]
        assert auth["type"] == "api_key_query_param"
        assert auth["param_name"] == "token"
        enc = auth["api_key_enc"]
        assert enc != "super-secret-token-value"
        assert app_module.secrets_crypto.decrypt(enc) == "super-secret-token-value"

    def test_api_key_query_param_defaults_param_name_to_token(self, tmp_path, monkeypatch):
        _, app_module = _client(tmp_path, monkeypatch)
        form = self._base_form(dev_auth_type="api_key_query_param", dev_auth_api_key_param="abc")
        config = app_module.build_hybrid_config_from_form(form, True)
        assert config["source"]["dev"]["auth"]["param_name"] == "token"

    def test_api_key_query_param_and_header_fields_dont_collide(self, tmp_path, monkeypatch):
        # Both auth-type panels are always present in the submitted form
        # (only one shown/active in the UI at a time) — regression test for
        # the bug where sharing one field name between them meant
        # form.get() silently read whichever panel came first in the DOM.
        _, app_module = _client(tmp_path, monkeypatch)
        form = self._base_form(
            dev_auth_type="api_key_query_param",
            dev_auth_api_key="unused-header-panel-value",
            dev_auth_api_key_param="the-real-query-param-value",
        )
        config = app_module.build_hybrid_config_from_form(form, True)
        decrypted = app_module.secrets_crypto.decrypt(config["source"]["dev"]["auth"]["api_key_enc"])
        assert decrypted == "the-real-query-param-value"

    def test_masked_value_keeps_existing_ciphertext_on_edit(self, tmp_path, monkeypatch):
        _, app_module = _client(tmp_path, monkeypatch)
        existing_ct = app_module.secrets_crypto.encrypt("original-secret")
        existing_config = {
            "source": {
                "dev":  {"auth": {"type": "api_key_header", "api_key_enc": existing_ct}},
                "prod": {"auth": {"type": "api_key_header", "api_key_enc": existing_ct}},
            }
        }
        form = self._base_form(
            dev_auth_type="api_key_header",
            dev_auth_header_name="X-API-Key",
            dev_auth_api_key=app_module.secrets_crypto.MASK,  # user didn't change it
        )
        config = app_module.build_hybrid_config_from_form(form, True, existing_config=existing_config)
        assert config["source"]["dev"]["auth"]["api_key_enc"] == existing_ct
        assert app_module.secrets_crypto.decrypt(config["source"]["dev"]["auth"]["api_key_enc"]) == "original-secret"

    def test_blank_value_keeps_existing_ciphertext_on_edit(self, tmp_path, monkeypatch):
        _, app_module = _client(tmp_path, monkeypatch)
        existing_ct = app_module.secrets_crypto.encrypt("original-secret")
        existing_config = {"source": {"dev": {"auth": {"type": "api_key_header", "api_key_enc": existing_ct}}}}
        form = self._base_form(dev_auth_type="api_key_header", dev_auth_api_key="")
        config = app_module.build_hybrid_config_from_form(form, True, existing_config=existing_config)
        assert config["source"]["dev"]["auth"]["api_key_enc"] == existing_ct

    def test_new_value_overwrites_existing_ciphertext(self, tmp_path, monkeypatch):
        _, app_module = _client(tmp_path, monkeypatch)
        existing_ct = app_module.secrets_crypto.encrypt("old-secret")
        existing_config = {"source": {"dev": {"auth": {"type": "api_key_header", "api_key_enc": existing_ct}}}}
        form = self._base_form(dev_auth_type="api_key_header", dev_auth_api_key="brand-new-secret")
        config = app_module.build_hybrid_config_from_form(form, True, existing_config=existing_config)
        new_ct = config["source"]["dev"]["auth"]["api_key_enc"]
        assert new_ct != existing_ct
        assert app_module.secrets_crypto.decrypt(new_ct) == "brand-new-secret"

    def test_login_token_fields(self, tmp_path, monkeypatch):
        _, app_module = _client(tmp_path, monkeypatch)
        form = self._base_form(
            dev_auth_type="login_token",
            dev_auth_token_endpoint="https://api.example.com/auth/login",
            dev_auth_token_username="svc",
            dev_auth_token_password="pw123",
            dev_auth_token_response_field="data.access_token",
        )
        config = app_module.build_hybrid_config_from_form(form, True)
        auth = config["source"]["dev"]["auth"]
        assert auth["token_endpoint"] == "https://api.example.com/auth/login"
        assert auth["token_username"] == "svc"
        assert app_module.secrets_crypto.decrypt(auth["token_password_enc"]) == "pw123"
        assert auth["token_response_field"] == "data.access_token"

    def test_filters_and_hwm_and_pagination_round_trip(self, tmp_path, monkeypatch):
        _, app_module = _client(tmp_path, monkeypatch)
        form = self._base_form(
            dev_pagination_strategy="offset_limit", dev_pagination_page_size="50",
            dev_hwm_column="updated_at", dev_hwm_column_datatype="timestamp",
            dev_hwm_request_param="updated_after",
            filters_json=json.dumps([{"column": "amount", "operator": ">", "value": "10", "api_param": "min_amount"}]),
        )
        config = app_module.build_hybrid_config_from_form(form, True)
        src = config["source"]["dev"]
        assert src["pagination"]["strategy"] == "offset_limit"
        assert src["pagination"]["page_size"] == 50
        assert src["hwm"]["column"] == "updated_at"
        assert src["hwm"]["request_param"] == "updated_after"
        assert src["filters"] == [{"column": "amount", "operator": ">", "value": "10", "api_param": "min_amount"}]
        # "apply prod same as dev" carries filters/hwm/pagination through too
        assert config["source"]["prod"]["filters"] == src["filters"]

    def test_api_connection_always_mirrors_dev_to_prod_even_if_unchecked(self, tmp_path, monkeypatch):
        # Unlike Target, the wizard never renders a prod-specific Connection
        # section for API — apply_prod_same_as_dev only ever governs Target
        # for an api-sourced job.
        _, app_module = _client(tmp_path, monkeypatch)
        form = self._base_form(apply_prod_same_as_dev="")
        config = app_module.build_hybrid_config_from_form(form, False)
        assert config["source"]["prod"]["base_url"] == config["source"]["dev"]["base_url"]

    def test_dag_id_uses_api_prefix(self, tmp_path, monkeypatch):
        _, app_module = _client(tmp_path, monkeypatch)
        config = app_module.build_hybrid_config_from_form(self._base_form(), True)
        assert config["dag_id"] == "api_test_conn_weather"

    def test_file_source_type_still_works_unaffected(self, tmp_path, monkeypatch):
        # Regression: adding the api_src branch must not disturb file/db.
        _, app_module = _client(tmp_path, monkeypatch)
        form = {
            "dag_id": "customers", "schedule_interval": "@daily", "source_type": "file",
            "dev_file_name": "customers.csv", "dev_file_format": "csv",
            "dev_target_db_conn_id": "test_conn", "dev_target_schema": "raw", "dev_target_table": "customers",
            "apply_prod_same_as_dev": "on",
        }
        config = app_module.build_hybrid_config_from_form(form, True)
        assert config["source_type"] == "file"
        assert config["source"]["dev"]["file_name"] == "customers.csv"
        assert "auth" not in config["source"]["dev"]
        assert config["dag_id"] == "hybrid_test_conn_customers"


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/api-test-connection
# ─────────────────────────────────────────────────────────────────────────────

class TestApiTestConnectionEndpoint:
    @patch("urllib.request.urlopen")
    def test_success_detects_json_array(self, mock_urlopen, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        mock_urlopen.return_value = _mock_response(b'[{"id": 1}, {"id": 2}]')
        resp = client.post("/api/api-test-connection", json={
            "base_url": "https://api.example.com", "data_endpoint": "/v1/items",
            "auth": {"type": "none"},
        })
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["detected_format"] == "json_array"
        assert data["record_count_sample"] == 2

    @patch("urllib.request.urlopen")
    def test_success_detects_json_object_path(self, mock_urlopen, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        mock_urlopen.return_value = _mock_response(b'{"data": {"results": [{"id": 1}]}}')
        resp = client.post("/api/api-test-connection", json={
            "base_url": "https://api.example.com", "data_endpoint": "/v1/items",
            "auth": {"type": "none"},
        })
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["detected_format"] == "json_object_path"
        assert data["detected_records_path"] == "data.results"

    def test_missing_base_url_returns_400(self, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        resp = client.post("/api/api-test-connection", json={"auth": {"type": "none"}})
        assert resp.status_code == 400

    @patch("urllib.request.urlopen")
    def test_http_error_reported_as_status_error(self, mock_urlopen, tmp_path, monkeypatch):
        import urllib.error
        client, _ = _client(tmp_path, monkeypatch)
        mock_urlopen.side_effect = urllib.error.HTTPError("https://x", 401, "Unauthorized", {}, None)
        resp = client.post("/api/api-test-connection", json={
            "base_url": "https://api.example.com", "data_endpoint": "/v1/items",
            "auth": {"type": "none"},
        })
        data = resp.get_json()
        assert data["status"] == "error"
        assert "401" in data["error"]

    @patch("urllib.request.urlopen")
    def test_api_key_auth_sends_header(self, mock_urlopen, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        mock_urlopen.return_value = _mock_response(b'[]')
        resp = client.post("/api/api-test-connection", json={
            "base_url": "https://api.example.com", "data_endpoint": "/v1/items",
            "auth": {"type": "api_key_header", "header_name": "X-API-Key", "api_key": "secret123"},
        })
        assert resp.get_json()["status"] == "ok"
        sent_req = mock_urlopen.call_args[0][0]
        sent_headers = {k.lower(): v for k, v in sent_req.headers.items()}
        assert sent_headers.get("x-api-key") == "secret123"

    @patch("urllib.request.urlopen")
    def test_api_key_query_param_auth_sends_query_param_not_header(self, mock_urlopen, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        mock_urlopen.return_value = _mock_response(b'[]')
        resp = client.post("/api/api-test-connection", json={
            "base_url": "https://api.example.com", "data_endpoint": "/v1/items",
            "auth": {"type": "api_key_query_param", "param_name": "token", "api_key": "a-secret-token"},
        })
        assert resp.get_json()["status"] == "ok"
        sent_req = mock_urlopen.call_args[0][0]
        assert "token=a-secret-token" in sent_req.full_url
        sent_headers = {k.lower(): v for k, v in sent_req.headers.items()}
        assert "a-secret-token" not in sent_headers.values()

    @patch("urllib.request.urlopen")
    def test_response_never_echoes_secret(self, mock_urlopen, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        mock_urlopen.return_value = _mock_response(b'[]')
        resp = client.post("/api/api-test-connection", json={
            "base_url": "https://api.example.com", "data_endpoint": "/v1/items",
            "auth": {"type": "api_key_header", "header_name": "X-API-Key", "api_key": "super-secret-999"},
        })
        assert "super-secret-999" not in resp.get_data(as_text=True)

    @patch("urllib.request.urlopen")
    def test_missing_api_key_reported_as_error_not_exception(self, mock_urlopen, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        resp = client.post("/api/api-test-connection", json={
            "base_url": "https://api.example.com", "data_endpoint": "/v1/items",
            "auth": {"type": "api_key_header", "header_name": "X-API-Key", "api_key": ""},
        })
        data = resp.get_json()
        assert data["status"] == "error"
        mock_urlopen.assert_not_called()

    @patch("urllib.request.urlopen")
    def test_error_envelope_reported_despite_http_200(self, mock_urlopen, tmp_path, monkeypatch):
        # Some APIs (ArcGIS/Esri Server among them) return HTTP 200 with the
        # real failure only visible in the JSON body — a status-code check
        # alone would report "ok".
        client, _ = _client(tmp_path, monkeypatch)
        mock_urlopen.return_value = _mock_response(
            b'{"error": {"code": 498, "message": "Invalid Token", "details": []}}'
        )
        resp = client.post("/api/api-test-connection", json={
            "base_url": "https://api.example.com", "data_endpoint": "/v1/items",
            "auth": {"type": "api_key_query_param", "param_name": "token", "api_key": "expired-token"},
        })
        data = resp.get_json()
        assert data["status"] == "error"
        assert "Invalid Token" in data["error"]

    # ── url_mode="full" — the whole URL (token embedded) is one field ──

    @patch("urllib.request.urlopen")
    def test_full_url_mode_sends_embedded_token_untouched(self, mock_urlopen, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        mock_urlopen.return_value = _mock_response(b'[{"id": 1}]')
        resp = client.post("/api/api-test-connection", json={
            "url_mode": "full",
            "full_url": "https://api.example.com/v1/items?where=1%3D1&token=secret-abc.",
        })
        data = resp.get_json()
        assert data["status"] == "ok"
        sent_req = mock_urlopen.call_args[0][0]
        assert "token=secret-abc." in sent_req.full_url
        assert "secret-abc." not in resp.get_data(as_text=True).replace(sent_req.full_url, "")

    def test_full_url_mode_missing_url_returns_400(self, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        resp = client.post("/api/api-test-connection", json={"url_mode": "full", "full_url": ""})
        assert resp.status_code == 400

    @patch("urllib.request.urlopen")
    def test_full_url_mode_masked_value_resolves_against_saved_config(self, mock_urlopen, tmp_path, monkeypatch):
        client, app_module = _client(tmp_path, monkeypatch)
        mock_urlopen.return_value = _mock_response(b'[]')
        # Save a job with url_mode="full" first.
        form = {
            "dag_id": "masked_full_url_test", "schedule_interval": "@daily", "source_type": "api",
            "url_mode": "full", "full_url": "https://api.example.com/v1/items?token=original-secret",
            "dev_target_db_conn_id": "test_conn", "dev_target_schema": "raw", "dev_target_table": "t1",
            "apply_prod_same_as_dev": "on", "created_by": "tester",
        }
        # build_hybrid_config_from_form reads {prefix}_url_mode / {prefix}_full_url
        form["dev_url_mode"] = "full"
        form["dev_full_url"] = "https://api.example.com/v1/items?token=original-secret"
        config = app_module.build_hybrid_config_from_form(form, True)
        app_module.save_config(config)

        resp = client.post("/api/api-test-connection", json={
            "url_mode": "full", "full_url": app_module.secrets_crypto.MASK,
            "dag_id": config["dag_id"], "env": "dev",
        })
        assert resp.get_json()["status"] == "ok"
        sent_req = mock_urlopen.call_args[0][0]
        assert "token=original-secret" in sent_req.full_url


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/api-detect-columns
# ─────────────────────────────────────────────────────────────────────────────

class TestApiDetectColumnsEndpoint:
    @patch("urllib.request.urlopen")
    def test_detects_columns_from_json_array(self, mock_urlopen, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        mock_urlopen.return_value = _mock_response(
            b'[{"id": 1, "amount": 10.5, "name": "a"}, {"id": 2, "amount": 20.0, "name": "b"}]'
        )
        resp = client.post("/api/api-detect-columns", json={
            "base_url": "https://api.example.com", "data_endpoint": "/v1/items",
            "auth": {"type": "none"}, "response_format": {"type": "json_array"},
        })
        data = resp.get_json()
        names = {c["original"] for c in data["columns"]}
        assert {"id", "amount", "name"} <= names
        assert data["records_fetched"] == 2

    @patch("urllib.request.urlopen")
    def test_detects_columns_from_json_object_path(self, mock_urlopen, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        mock_urlopen.return_value = _mock_response(b'{"data": {"results": [{"id": 1, "active": true}]}}')
        resp = client.post("/api/api-detect-columns", json={
            "base_url": "https://api.example.com", "data_endpoint": "/v1/items",
            "auth": {"type": "none"},
            "response_format": {"type": "json_object_path", "records_path": "data.results"},
        })
        data = resp.get_json()
        names = {c["original"] for c in data["columns"]}
        assert {"id", "active"} <= names

    @patch("urllib.request.urlopen")
    def test_no_records_returns_helpful_error(self, mock_urlopen, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        mock_urlopen.return_value = _mock_response(b'{"data": {"results": []}}')
        resp = client.post("/api/api-detect-columns", json={
            "base_url": "https://api.example.com", "data_endpoint": "/v1/items",
            "auth": {"type": "none"},
            "response_format": {"type": "json_object_path", "records_path": "data.results"},
        })
        data = resp.get_json()
        assert data["columns"] == []
        assert "error" in data

    @patch("urllib.request.urlopen")
    def test_error_envelope_not_treated_as_columns(self, mock_urlopen, tmp_path, monkeypatch):
        # Without the error-envelope check, this would infer bogus columns
        # like "error_code"/"error_message" from the error object itself.
        client, _ = _client(tmp_path, monkeypatch)
        mock_urlopen.return_value = _mock_response(b'{"error": {"code": 499, "message": "Token Required"}}')
        resp = client.post("/api/api-detect-columns", json={
            "base_url": "https://api.example.com", "data_endpoint": "/v1/items",
            "auth": {"type": "none"}, "response_format": {"type": "json_array"},
        })
        data = resp.get_json()
        assert data["columns"] == []
        assert "Token Required" in data["error"]

    @patch("urllib.request.urlopen")
    def test_filters_applied_to_sample_request(self, mock_urlopen, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        mock_urlopen.return_value = _mock_response(b'[{"id": 1}]')
        resp = client.post("/api/api-detect-columns", json={
            "base_url": "https://api.example.com", "data_endpoint": "/v1/items",
            "auth": {"type": "none"}, "response_format": {"type": "json_array"},
            "filters": [{"column": "amount", "operator": ">", "value": "100", "api_param": "min_amount"}],
        })
        assert resp.get_json()["records_fetched"] == 1
        sent_url = mock_urlopen.call_args[0][0].full_url
        assert "min_amount=100" in sent_url


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/suggest-descriptions — response_format suggestion for the API
# job's Response Format card (sample_response field, additive to the
# existing columns/describe_table modes — no existing caller sends it, so
# this must never change behavior when it's absent).
# ─────────────────────────────────────────────────────────────────────────────

def _mock_gemini_response(ai_text):
    """Wrap `ai_text` (the AI's own raw text output) in Gemini's response
    envelope, JSON-encoded, as urlopen().read() would return it."""
    body = json.dumps({"candidates": [{"content": {"parts": [{"text": ai_text}]}}]}).encode()
    return _mock_response(body)


class TestSuggestDescriptionsResponseFormat:
    @patch("urllib.request.urlopen")
    def test_sample_response_suggests_format(self, mock_urlopen, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        monkeypatch.setenv("GOOGLE_AI_API_KEY", "test-key")
        mock_urlopen.return_value = _mock_gemini_response(
            '{"type": "json_object_path", "records_path": "data.results"}'
        )
        resp = client.post("/api/suggest-descriptions", json={
            "sample_response": '{"data": {"results": [{"id": 1}]}}',
        })
        data = resp.get_json()
        assert data["response_format"] == {"type": "json_object_path", "records_path": "data.results"}
        # No columns/describe_table were requested — no unrelated keys.
        assert "suggestions" not in data
        assert "table_description" not in data

    @patch("urllib.request.urlopen")
    def test_invalid_type_falls_back_to_json_array(self, mock_urlopen, tmp_path, monkeypatch):
        client, _ = _client(tmp_path, monkeypatch)
        monkeypatch.setenv("GOOGLE_AI_API_KEY", "test-key")
        mock_urlopen.return_value = _mock_gemini_response('{"type": "made_up_type", "records_path": ""}')
        resp = client.post("/api/suggest-descriptions", json={"sample_response": "[1,2,3]"})
        data = resp.get_json()
        assert data["response_format"]["type"] == "json_array"

    def test_no_sample_response_no_columns_no_describe_returns_empty(self, tmp_path, monkeypatch):
        # Regression: the pre-existing short-circuit for "nothing to do" must
        # still fire when sample_response is also absent.
        client, _ = _client(tmp_path, monkeypatch)
        resp = client.post("/api/suggest-descriptions", json={})
        assert resp.get_json() == {"suggestions": {}}

    @patch("urllib.request.urlopen")
    def test_columns_request_unaffected_by_new_field(self, mock_urlopen, tmp_path, monkeypatch):
        # Regression: existing columns-only callers (hybrid/db/file wizards)
        # never send sample_response — response_format must not appear.
        client, _ = _client(tmp_path, monkeypatch)
        monkeypatch.setenv("GOOGLE_AI_API_KEY", "test-key")
        mock_urlopen.return_value = _mock_gemini_response(
            '{"amount": {"description": "The order total", "dtype": "decimal"}}'
        )
        resp = client.post("/api/suggest-descriptions", json={"columns": ["amount"]})
        data = resp.get_json()
        assert data["suggestions"]["amount"]["dtype"] == "decimal"
        assert "response_format" not in data

    @patch("urllib.request.urlopen")
    def test_first_provider_error_falls_through_to_second(self, mock_urlopen, tmp_path, monkeypatch):
        # The core "try the others" guarantee: a broken/expired first
        # provider must not fail the request outright.
        client, _ = _client(tmp_path, monkeypatch)
        monkeypatch.setenv("GOOGLE_AI_API_KEY", "bad-key")
        monkeypatch.setenv("GROQ_API_KEY", "good-key")

        def _groq_chat(ai_text):
            body = json.dumps({"choices": [{"message": {"content": ai_text}}]}).encode()
            return _mock_response(body)

        import urllib.error
        mock_urlopen.side_effect = [
            urllib.error.HTTPError("https://generativelanguage.googleapis.com", 403, "Forbidden", {}, None),
            _groq_chat('{"type": "ndjson", "records_path": ""}'),
        ]
        resp = client.post("/api/suggest-descriptions", json={"sample_response": '{"a":1}\n{"a":2}'})
        data = resp.get_json()
        assert data["response_format"]["type"] == "ndjson"

    @patch("urllib.request.urlopen")
    def test_partial_failure_keeps_the_piece_that_succeeded(self, mock_urlopen, tmp_path, monkeypatch):
        # A single provider that returns a good column-suggestions payload
        # but an unparseable format payload must still hand back the
        # suggestions — not discard them because format parsing blew up.
        client, _ = _client(tmp_path, monkeypatch)
        monkeypatch.setenv("GOOGLE_AI_API_KEY", "test-key")
        mock_urlopen.side_effect = [
            _mock_gemini_response('{"amount": {"description": "Order total", "dtype": "decimal"}}'),
            _mock_gemini_response('not valid json at all'),
        ]
        resp = client.post("/api/suggest-descriptions", json={
            "columns": ["amount"], "sample_response": "[1,2,3]",
        })
        data = resp.get_json()
        assert data["suggestions"]["amount"]["dtype"] == "decimal"
        assert "response_format" not in data

    @patch("urllib.request.urlopen")
    def test_all_providers_failing_reports_error_not_a_crash(self, mock_urlopen, tmp_path, monkeypatch):
        import urllib.error
        client, _ = _client(tmp_path, monkeypatch)
        monkeypatch.setenv("GOOGLE_AI_API_KEY", "bad-key")
        mock_urlopen.side_effect = urllib.error.HTTPError("https://x", 500, "Server Error", {}, None)
        resp = client.post("/api/suggest-descriptions", json={"sample_response": "[1,2,3]"})
        data = resp.get_json()
        assert resp.status_code == 502
        assert "error" in data


# ─────────────────────────────────────────────────────────────────────────────
# business_date_column — the optional "how current is the data itself" column
# the monitoring dashboard renders as "Business Date"
# ─────────────────────────────────────────────────────────────────────────────

class TestBusinessDateColumn:
    """One job-level answer written into both env blocks. It has to reach
    every source_type: only DB-style sources have an incremental_column for
    the dashboard to fall back on, so without this a file or API job could
    never show a business date at all."""

    def _form(self, source_type, **extra):
        form = {
            "dag_id": "bd", "schedule_interval": "@daily", "source_type": source_type,
            "dev_target_db_conn_id": "test_conn", "dev_target_schema": "raw",
            "dev_target_table": "t", "apply_prod_same_as_dev": "on",
            "business_date_column": "report_date",
        }
        if source_type == "api":
            form.update({"dev_base_url": "https://api.example.com/v1/x",
                         "dev_http_method": "GET", "dev_auth_type": "none"})
        elif source_type == "file":
            form.update({"dev_file_name": "x.csv", "dev_file_format": "csv"})
        else:
            form.update({"dev_source_db_conn_id": "src_conn"})
        form.update(extra)
        return form

    @pytest.mark.parametrize("source_type", ["file", "db", "api"])
    def test_captured_for_every_source_type(self, tmp_path, monkeypatch, source_type):
        _, app_module = _client(tmp_path, monkeypatch)
        config = app_module.build_hybrid_config_from_form(self._form(source_type), True)
        for env in ("dev", "prod"):
            assert config["source"][env]["business_date_column"] == "report_date", (
                f"{source_type}/{env} lost the business date column"
            )

    @pytest.mark.parametrize("source_type", ["file", "db", "api"])
    def test_optional_defaults_to_blank(self, tmp_path, monkeypatch, source_type):
        _, app_module = _client(tmp_path, monkeypatch)
        form = self._form(source_type)
        del form["business_date_column"]
        config = app_module.build_hybrid_config_from_form(form, True)
        assert config["source"]["dev"]["business_date_column"] == ""

    def test_business_date_col_reader_prefers_dev_then_prod(self, tmp_path, monkeypatch):
        _, app_module = _client(tmp_path, monkeypatch)
        read = app_module._business_date_col
        assert read({"source": {"dev": {"business_date_column": "d"},
                                "prod": {"business_date_column": "p"}}}) == "d"
        assert read({"source": {"dev": {}, "prod": {"business_date_column": "p"}}}) == "p"
        assert read({"source": {"dev": {}, "prod": {}}}) == ""
        assert read(None) == ""


# ─────────────────────────────────────────────────────────────────────────────
# _bare_dag_id — the edit form's "name" field
# ─────────────────────────────────────────────────────────────────────────────

class TestBareDagId:
    """Regression guard for a confirmed data-corruption bug: the old regex
    stripped the conn_id as `[^_]+`, but every conn_id in use contains
    underscores. Editing and saving a job therefore re-derived a DIFFERENT
    dag_id, silently creating a second job writing to the same target table
    (this is how a duplicate DAG came to share gis.water_production)."""

    def _cfg(self, dag_id, conn_id="local_dw_con"):
        return {"dag_id": dag_id, "target": {"dev": {"target_db_conn_id": conn_id}}}

    @pytest.mark.parametrize("dag_id,conn_id,expected", [
        ("api_local_dw_con_water_production",         "local_dw_con",   "water_production"),
        ("hybrid_local_dw_con_qa_prefill",            "local_dw_con",   "qa_prefill"),
        ("hybrid_cenfri_dev_con_july_new_connections","cenfri_dev_con", "july_new_connections"),
        ("file_local_dw_con__humidity",               "local_dw_con",   "_humidity"),
        ("db_local_dw_con_testing",                   "local_dw_con",   "testing"),
        ("spark_file_local_dw_con_wine",              "local_dw_con",   "wine"),
    ])
    def test_strips_prefix_and_full_conn_id(self, tmp_path, monkeypatch, dag_id, conn_id, expected):
        _, app_module = _client(tmp_path, monkeypatch)
        assert app_module._bare_dag_id(self._cfg(dag_id, conn_id)) == expected

    def test_editing_does_not_rename_the_job(self, tmp_path, monkeypatch):
        """The round trip that used to duplicate jobs: bare name fed back
        through generate_dag_id must reproduce the SAME dag_id."""
        _, app_module = _client(tmp_path, monkeypatch)
        from dags.modules.naming import generate_dag_id
        cfg = {
            "dag_id": "api_local_dw_con_water_production",
            "pipeline_type": "hybrid", "source_type": "api",
            "target": {"dev": {"target_db_conn_id": "local_dw_con"}},
        }
        bare = app_module._bare_dag_id(cfg)
        assert generate_dag_id(cfg, bare) == cfg["dag_id"]

    def test_missing_config_is_safe(self, tmp_path, monkeypatch):
        _, app_module = _client(tmp_path, monkeypatch)
        assert app_module._bare_dag_id(None) == ""
        assert app_module._bare_dag_id({}) == ""
