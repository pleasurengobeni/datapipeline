"""
Tests for the API-source helpers (auth resolution, pagination,
response-format parsing) inside hybrid_load.template.

API ingestion is not a separate template — it's a third `source_type`
("api", alongside "file" and "db") inside hybrid_load.template, reusing the
same raw -> refined -> target stages the file/db sources already go
through. Only the ingest side (fetching + parsing API pages) is
API-specific; that's what this file tests.

Follows the same pattern test_hybrid_pipeline.py uses: extract named
top-level functions straight out of the .template source and exec() them
in an isolated namespace with a small stdlib preamble — no Airflow, no
Postgres, no real network access required.

The downstream refine/dw_load/cleanup stages are shared with the file/db
sources and are already covered by test_hybrid_pipeline.py — not re-tested
here.

Run with:  pytest web_ui/tests/test_api_pipeline.py -v
"""
from __future__ import annotations

import re
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

TPL_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "dags" / "templates" / "hybrid_load.template"
)

EXTRACT_NAMES = [
    "_get_nested",
    "_resolve_auth",
    "_extract_error_envelope",
    "_parse_page",
    "_build_page_request",
    "fetch_api_pages",
]

PREAMBLE = textwrap.dedent("""
    import json
    import logging
    import time
    import urllib.error
    import urllib.request
    from urllib.parse import urlencode, urlsplit, parse_qsl

    # Stand-in for modules.secrets_crypto.decrypt — the real module needs the
    # `cryptography` package and a configured Fernet key, neither of which
    # this hermetic test needs; ciphertext in these tests IS the plaintext.
    def decrypt(ciphertext):
        return ciphertext

    # Module-level constants the template defines near the top (not a `def`,
    # so _extract_fn's regex won't pull them in automatically).
    API_JOB_USER_AGENT = "ETL-Manager/1.0"
    API_PAGE_MAX_RETRIES = 3
    API_PAGE_RETRY_BACKOFF_SECONDS = 5
""").strip()


def _extract_fn(src: str, name: str) -> str:
    """Extract a top-level 'def name' block from template source."""
    pattern = rf"^(def {name}\b.*?)(?=\ndef |\Z)"
    m = re.search(pattern, src, re.DOTALL | re.MULTILINE)
    return m.group(1).rstrip() if m else ""


@pytest.fixture(scope="module")
def helpers() -> dict:
    """Compile and exec the API-source helpers from hybrid_load.template once per module."""
    source = TPL_PATH.read_text()
    snippets = "\n\n".join(_extract_fn(source, n) for n in EXTRACT_NAMES)
    assert all(f"def {n}" in snippets for n in EXTRACT_NAMES), (
        "One or more functions were not found in hybrid_load.template — "
        "check EXTRACT_NAMES against the template's current def names."
    )
    ns: dict = {}
    exec(compile(PREAMBLE + "\n\n" + snippets, str(TPL_PATH), "exec"), ns)  # noqa: S102
    return ns


def _default_pagination(**overrides):
    cfg = {
        "strategy": "none",
        "page_size": 100,
        "offset_param": "offset",
        "limit_param": "limit",
        "page_param": "page",
        "page_size_param": "page_size",
        "start_page": 1,
        "cursor_param": "cursor",
        "cursor_response_field": "next_cursor",
        "next_link_response_field": "next",
        "max_pages": 500,
        "stop_when_empty": True,
    }
    cfg.update(overrides)
    return cfg


def _mock_response(body_bytes):
    """A MagicMock usable as `with urllib.request.urlopen(...) as resp:` that
    returns body_bytes from resp.read()."""
    resp = MagicMock()
    resp.read.return_value = body_bytes
    cm = MagicMock()
    cm.__enter__.return_value = resp
    cm.__exit__.return_value = False
    return cm


# ─────────────────────────────────────────────────────────────────────────────
# _extract_error_envelope — many REST APIs report failures as HTTP 200 with
# an error object in the body instead of a non-2xx status code
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractErrorEnvelope:
    def test_error_dict_with_message_and_code(self, helpers):
        parsed = {"error": {"code": 498, "message": "Invalid Token", "details": []}}
        msg = helpers["_extract_error_envelope"](parsed)
        assert msg == "Invalid Token (code 498)"

    def test_error_dict_without_code(self, helpers):
        parsed = {"error": {"code": 499, "message": "Token Required", "details": []}}
        assert helpers["_extract_error_envelope"](parsed) == "Token Required (code 499)"

    def test_string_error_value(self, helpers):
        assert helpers["_extract_error_envelope"]({"error": "something went wrong"}) == "something went wrong"

    def test_error_dict_without_message_falls_back_to_details(self, helpers):
        parsed = {"error": {"details": ["bad field"]}}
        assert "bad field" in helpers["_extract_error_envelope"](parsed)

    def test_no_error_key_returns_none(self, helpers):
        assert helpers["_extract_error_envelope"]({"data": [{"id": 1}]}) is None

    def test_non_dict_input_returns_none(self, helpers):
        assert helpers["_extract_error_envelope"]([{"id": 1}]) is None
        assert helpers["_extract_error_envelope"](None) is None

    def test_null_error_value_returns_none(self, helpers):
        # e.g. a legitimate record that happens to have a top-level "error"
        # field explicitly set to null/None must not be misdetected.
        assert helpers["_extract_error_envelope"]({"error": None, "data": []}) is None


# ─────────────────────────────────────────────────────────────────────────────
# _get_nested
# ─────────────────────────────────────────────────────────────────────────────

class TestGetNested:
    def test_simple_key(self, helpers):
        assert helpers["_get_nested"]({"a": 1}, "a") == 1

    def test_dotted_path(self, helpers):
        assert helpers["_get_nested"]({"data": {"access_token": "xyz"}}, "data.access_token") == "xyz"

    def test_list_index(self, helpers):
        assert helpers["_get_nested"]({"items": [{"id": 1}, {"id": 2}]}, "items.1.id") == 2

    def test_missing_key_returns_none(self, helpers):
        assert helpers["_get_nested"]({"a": 1}, "b.c") is None

    def test_none_input(self, helpers):
        assert helpers["_get_nested"](None, "a.b") is None

    def test_empty_path_returns_whole_value(self, helpers):
        d = {"a": 1}
        assert helpers["_get_nested"](d, "") is d

    def test_bad_list_index_returns_none(self, helpers):
        assert helpers["_get_nested"]({"items": [1, 2]}, "items.abc") is None
        assert helpers["_get_nested"]({"items": [1, 2]}, "items.9") is None


# ─────────────────────────────────────────────────────────────────────────────
# _parse_page — response format handling
# ─────────────────────────────────────────────────────────────────────────────

class TestParsePage:
    def test_json_array(self, helpers):
        body = b'[{"id": 1}, {"id": 2}]'
        records, parsed = helpers["_parse_page"](body, {"type": "json_array"})
        assert records == [{"id": 1}, {"id": 2}]
        assert parsed == [{"id": 1}, {"id": 2}]

    def test_json_object_path(self, helpers):
        body = b'{"data": {"results": [{"id": 1}], "next_cursor": "abc"}}'
        records, parsed = helpers["_parse_page"](body, {"type": "json_object_path", "records_path": "data.results"})
        assert records == [{"id": 1}]
        assert parsed["data"]["next_cursor"] == "abc"

    def test_ndjson(self, helpers):
        body = b'{"id": 1}\n{"id": 2}\n\n{"id": 3}\n'
        records, parsed = helpers["_parse_page"](body, {"type": "ndjson"})
        assert records == [{"id": 1}, {"id": 2}, {"id": 3}]
        assert parsed is None

    def test_json_object_path_single_object_wrapped_in_list(self, helpers):
        body = b'{"data": {"result": {"id": 1}}}'
        records, _ = helpers["_parse_page"](body, {"type": "json_object_path", "records_path": "data.result"})
        assert records == [{"id": 1}]

    def test_non_dict_items_dropped(self, helpers):
        body = b'[1, 2, {"id": 3}]'
        records, _ = helpers["_parse_page"](body, {"type": "json_array"})
        assert records == [{"id": 3}]

    def test_missing_records_path_returns_empty(self, helpers):
        body = b'{"data": {}}'
        records, _ = helpers["_parse_page"](body, {"type": "json_object_path", "records_path": "data.missing"})
        assert records == []

    def test_empty_body(self, helpers):
        records, parsed = helpers["_parse_page"](b"", {"type": "json_array"})
        assert records == []
        assert parsed is None


# ─────────────────────────────────────────────────────────────────────────────
# _build_page_request — pagination param building per strategy
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildPageRequest:
    def test_none_strategy_returns_base_params_untouched(self, helpers):
        url, params = helpers["_build_page_request"](
            "https://api.example.com", "/v1/items", {"format": "json"},
            _default_pagination(strategy="none"), 0, None, None,
        )
        assert url == "https://api.example.com/v1/items"
        assert params == {"format": "json"}

    def test_offset_limit(self, helpers):
        pag = _default_pagination(strategy="offset_limit", page_size=50)
        _, params = helpers["_build_page_request"]("https://x", "/y", {}, pag, 2, None, None)
        assert params["offset"] == 100
        assert params["limit"] == 50

    def test_page_number(self, helpers):
        pag = _default_pagination(strategy="page_number", page_size=25, start_page=1)
        _, params = helpers["_build_page_request"]("https://x", "/y", {}, pag, 0, None, None)
        assert params["page"] == 1
        assert params["page_size"] == 25
        _, params2 = helpers["_build_page_request"]("https://x", "/y", {}, pag, 3, None, None)
        assert params2["page"] == 4

    def test_cursor_first_page_has_no_cursor_param(self, helpers):
        pag = _default_pagination(strategy="cursor")
        _, params = helpers["_build_page_request"]("https://x", "/y", {}, pag, 0, None, None)
        assert "cursor" not in params

    def test_cursor_subsequent_page_includes_cursor(self, helpers):
        pag = _default_pagination(strategy="cursor")
        _, params = helpers["_build_page_request"]("https://x", "/y", {}, pag, 1, "abc123", None)
        assert params["cursor"] == "abc123"

    def test_next_link_uses_absolute_url_and_no_params(self, helpers):
        pag = _default_pagination(strategy="next_link")
        url, params = helpers["_build_page_request"](
            "https://x", "/y", {"a": "1"}, pag, 1, None, "https://x/y?page=2&token=zzz",
        )
        assert url == "https://x/y?page=2&token=zzz"
        assert params is None

    def test_next_link_first_page_uses_base_url(self, helpers):
        pag = _default_pagination(strategy="next_link")
        url, params = helpers["_build_page_request"]("https://x", "/y", {"a": "1"}, pag, 0, None, None)
        assert url == "https://x/y"
        assert params == {"a": "1"}


# ─────────────────────────────────────────────────────────────────────────────
# fetch_api_pages — end-to-end pagination loop, urlopen mocked
# ─────────────────────────────────────────────────────────────────────────────

class TestFetchApiPages:
    def _cfg(self, **overrides):
        cfg = {
            "base_url": "https://api.example.com",
            "data_endpoint": "/v1/items",
            "http_method": "GET",
            "request_params": {},
            "request_headers": {},
            "timeout_seconds": 30,
            "auth": {"type": "none"},
            "response_format": {"type": "json_array", "records_path": ""},
            "pagination": _default_pagination(),
            "filters": [],
            "hwm": {"column": "", "column_datatype": "", "date_format": "", "request_param": ""},
        }
        cfg.update(overrides)
        return cfg

    @patch("urllib.request.urlopen")
    def test_single_page_no_pagination(self, mock_urlopen, helpers):
        mock_urlopen.return_value = _mock_response(b'[{"id": 1}, {"id": 2}]')
        cfg = self._cfg()
        records = helpers["fetch_api_pages"](cfg, None)
        assert records == [{"id": 1}, {"id": 2}]
        assert mock_urlopen.call_count == 1

    @patch("urllib.request.urlopen")
    def test_offset_limit_pagination_stops_on_short_page(self, mock_urlopen, helpers):
        page1 = _mock_response(b'[{"id": 1}, {"id": 2}]')
        page2 = _mock_response(b'[{"id": 3}]')  # shorter than page_size -> last page
        mock_urlopen.side_effect = [page1, page2]
        cfg = self._cfg(pagination=_default_pagination(strategy="offset_limit", page_size=2))
        records = helpers["fetch_api_pages"](cfg, None)
        assert records == [{"id": 1}, {"id": 2}, {"id": 3}]
        assert mock_urlopen.call_count == 2

    @patch("urllib.request.urlopen")
    def test_offset_limit_stops_on_empty_page(self, mock_urlopen, helpers):
        page1 = _mock_response(b'[{"id": 1}, {"id": 2}]')
        page2 = _mock_response(b'[]')
        mock_urlopen.side_effect = [page1, page2]
        cfg = self._cfg(pagination=_default_pagination(strategy="offset_limit", page_size=2))
        records = helpers["fetch_api_pages"](cfg, None)
        assert records == [{"id": 1}, {"id": 2}]
        assert mock_urlopen.call_count == 2

    # ── url_mode="full" — the whole URL (token embedded and all) is one
    # encrypted secret instead of a separate base_url + auth mechanism ──

    @patch("urllib.request.urlopen")
    def test_full_url_mode_sends_embedded_token_untouched(self, mock_urlopen, helpers):
        mock_urlopen.return_value = _mock_response(b'[{"id": 1}]')
        cfg = self._cfg(
            url_mode="full",
            full_url_enc="https://api.example.com/v1/items?where=1%3D1&token=secret-abc.",
        )
        records = helpers["fetch_api_pages"](cfg, None)
        assert records == [{"id": 1}]
        sent_url = mock_urlopen.call_args[0][0].full_url
        assert "token=secret-abc." in sent_url
        assert "where=1" in sent_url

    @patch("urllib.request.urlopen")
    def test_full_url_mode_missing_secret_raises(self, mock_urlopen, helpers):
        cfg = self._cfg(url_mode="full", full_url_enc="")
        with pytest.raises(ValueError, match="url_mode=full"):
            helpers["fetch_api_pages"](cfg, None)
        mock_urlopen.assert_not_called()

    @patch("urllib.request.urlopen")
    def test_full_url_mode_filters_and_pagination_layer_on_top(self, mock_urlopen, helpers):
        page1 = _mock_response(b'[{"id": 1}, {"id": 2}]')
        page2 = _mock_response(b'[{"id": 3}]')
        mock_urlopen.side_effect = [page1, page2]
        cfg = self._cfg(
            url_mode="full",
            full_url_enc="https://api.example.com/v1/items?token=secret-abc",
            pagination=_default_pagination(strategy="offset_limit", page_size=2),
            filters=[{"column": "amount", "operator": ">", "value": "10", "api_param": "min_amount"}],
        )
        records = helpers["fetch_api_pages"](cfg, None)
        assert records == [{"id": 1}, {"id": 2}, {"id": 3}]
        first_url = mock_urlopen.call_args_list[0][0][0].full_url
        assert "token=secret-abc" in first_url
        assert "min_amount=10" in first_url
        assert "offset=0" in first_url

    @patch("urllib.request.urlopen")
    def test_cursor_pagination_follows_cursor_until_absent(self, mock_urlopen, helpers):
        page1 = _mock_response(b'{"results": [{"id": 1}], "next_cursor": "c2"}')
        page2 = _mock_response(b'{"results": [{"id": 2}], "next_cursor": null}')
        mock_urlopen.side_effect = [page1, page2]
        cfg = self._cfg(
            response_format={"type": "json_object_path", "records_path": "results"},
            pagination=_default_pagination(strategy="cursor", cursor_response_field="next_cursor"),
        )
        records = helpers["fetch_api_pages"](cfg, None)
        assert records == [{"id": 1}, {"id": 2}]
        assert mock_urlopen.call_count == 2

    @patch("urllib.request.urlopen")
    def test_next_link_pagination_follows_link_until_absent(self, mock_urlopen, helpers):
        page1 = _mock_response(b'{"results": [{"id": 1}], "next": "https://api.example.com/v1/items?page=2"}')
        page2 = _mock_response(b'{"results": [{"id": 2}], "next": null}')
        mock_urlopen.side_effect = [page1, page2]
        cfg = self._cfg(
            response_format={"type": "json_object_path", "records_path": "results"},
            pagination=_default_pagination(strategy="next_link", next_link_response_field="next"),
        )
        records = helpers["fetch_api_pages"](cfg, None)
        assert records == [{"id": 1}, {"id": 2}]
        assert mock_urlopen.call_count == 2

    @patch("urllib.request.urlopen")
    def test_max_pages_safety_cap_enforced(self, mock_urlopen, helpers):
        # Every page looks "full" (never triggers the short-page stop), so
        # without the cap this would loop forever.
        mock_urlopen.return_value = _mock_response(b'[{"id": 1}, {"id": 2}]')
        cfg = self._cfg(pagination=_default_pagination(strategy="offset_limit", page_size=2, max_pages=3))
        records = helpers["fetch_api_pages"](cfg, None)
        assert mock_urlopen.call_count == 3
        assert len(records) == 6

    @patch("urllib.request.urlopen")
    def test_filters_and_hwm_merged_into_request_params(self, mock_urlopen, helpers):
        mock_urlopen.return_value = _mock_response(b'[]')
        cfg = self._cfg(
            filters=[{"column": "amount", "operator": ">", "value": "100", "api_param": "min_amount"}],
            hwm={"column": "updated_at", "column_datatype": "timestamp", "date_format": "", "request_param": "updated_after"},
        )
        helpers["fetch_api_pages"](cfg, "2024-01-01T00:00:00")
        sent_url = mock_urlopen.call_args[0][0].full_url
        assert "min_amount=100" in sent_url
        assert "updated_after=2024-01-01" in sent_url

    @patch("urllib.request.urlopen")
    def test_api_key_query_param_auth_appends_token_to_every_page(self, mock_urlopen, helpers):
        # A common REST auth pattern for map/geospatial services and similar
        # APIs: a static token sent as a query param (e.g. "...&token=..."),
        # not a header.
        page1 = _mock_response(b'[{"id": 1}, {"id": 2}]')
        page2 = _mock_response(b'[{"id": 3}]')
        mock_urlopen.side_effect = [page1, page2]
        cfg = self._cfg(
            auth={"type": "api_key_query_param", "param_name": "token", "api_key_enc": "secret-token-value"},
            pagination=_default_pagination(strategy="offset_limit", page_size=2),
        )
        records = helpers["fetch_api_pages"](cfg, None)
        assert records == [{"id": 1}, {"id": 2}, {"id": 3}]
        for call in mock_urlopen.call_args_list:
            sent_url = call[0][0].full_url
            assert "token=secret-token-value" in sent_url

    @patch("urllib.request.urlopen")
    def test_arcgis_style_error_envelope_raises_despite_http_200(self, mock_urlopen, helpers):
        # The failure mode a status-code-only check would miss entirely: some
        # APIs (ArcGIS/Esri Server among them) return HTTP 200 with the real
        # failure only visible in the JSON body. Without _extract_error_envelope
        # this would silently "succeed" and load one bogus record containing
        # the error object instead of failing the Airflow task loudly.
        mock_urlopen.return_value = _mock_response(
            b'{"error": {"code": 498, "message": "Invalid Token", "details": []}}'
        )
        cfg = self._cfg()
        with pytest.raises(RuntimeError, match="Invalid Token"):
            helpers["fetch_api_pages"](cfg, None)

    @patch("urllib.request.urlopen")
    def test_http_error_raises_runtime_error(self, mock_urlopen, helpers):
        import urllib.error
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "https://api.example.com/v1/items", 401, "Unauthorized", {}, None
        )
        cfg = self._cfg()
        with pytest.raises(RuntimeError, match="HTTP 401"):
            helpers["fetch_api_pages"](cfg, None)


# ─────────────────────────────────────────────────────────────────────────────
# _resolve_auth — returns (headers, params); exactly one is populated
# ─────────────────────────────────────────────────────────────────────────────

class TestResolveAuth:
    def test_none_auth_returns_empty(self, helpers):
        assert helpers["_resolve_auth"]({"type": "none"}) == ({}, {})
        assert helpers["_resolve_auth"](None) == ({}, {})
        assert helpers["_resolve_auth"]({}) == ({}, {})

    def test_api_key_header(self, helpers):
        headers, params = helpers["_resolve_auth"]({
            "type": "api_key_header", "header_name": "X-API-Key", "api_key_enc": "my-secret-key",
        })
        assert headers == {"X-API-Key": "my-secret-key"}
        assert params == {}

    def test_api_key_header_defaults_to_authorization(self, helpers):
        headers, params = helpers["_resolve_auth"]({"type": "api_key_header", "api_key_enc": "abc"})
        assert headers == {"Authorization": "abc"}
        assert params == {}

    def test_api_key_header_missing_key_raises(self, helpers):
        with pytest.raises(ValueError, match="api_key_enc"):
            helpers["_resolve_auth"]({"type": "api_key_header", "api_key_enc": ""})

    def test_api_key_query_param(self, helpers):
        headers, params = helpers["_resolve_auth"]({
            "type": "api_key_query_param", "param_name": "token", "api_key_enc": "my-token",
        })
        assert headers == {}
        assert params == {"token": "my-token"}

    def test_api_key_query_param_defaults_to_token(self, helpers):
        headers, params = helpers["_resolve_auth"]({"type": "api_key_query_param", "api_key_enc": "abc"})
        assert params == {"token": "abc"}

    def test_api_key_query_param_missing_key_raises(self, helpers):
        with pytest.raises(ValueError, match="api_key_enc"):
            helpers["_resolve_auth"]({"type": "api_key_query_param", "api_key_enc": ""})

    @patch("urllib.request.urlopen")
    def test_login_token_fetches_and_builds_bearer_header(self, mock_urlopen, helpers):
        mock_urlopen.return_value = _mock_response(b'{"data": {"access_token": "abc123"}}')
        headers, params = helpers["_resolve_auth"]({
            "type": "login_token",
            "token_endpoint": "https://api.example.com/auth/login",
            "token_username": "svc_account",
            "token_password_enc": "svc_password",
            "token_response_field": "data.access_token",
        })
        assert headers == {"Authorization": "Bearer abc123"}
        assert params == {}
        # credentials were POSTed, not appended to a URL
        sent_req = mock_urlopen.call_args[0][0]
        assert sent_req.method == "POST"
        assert sent_req.full_url == "https://api.example.com/auth/login"

    @patch("urllib.request.urlopen")
    def test_login_token_custom_header_name_and_prefix(self, mock_urlopen, helpers):
        mock_urlopen.return_value = _mock_response(b'{"token": "xyz"}')
        headers, params = helpers["_resolve_auth"]({
            "type": "login_token",
            "token_endpoint": "https://api.example.com/auth/login",
            "token_response_field": "token",
            "token_header_name": "X-Auth-Token",
            "token_header_prefix": "Token ",
        })
        assert headers == {"X-Auth-Token": "Token xyz"}

    @patch("urllib.request.urlopen")
    def test_login_token_blank_prefix_defaults_to_bearer(self, mock_urlopen, helpers):
        # A present-but-empty prefix (the wizard always sends this key) must
        # still default to "Bearer " — .get(key, default) only falls back
        # when the key is entirely absent, not when it's "".
        mock_urlopen.return_value = _mock_response(b'{"token": "xyz"}')
        headers, params = helpers["_resolve_auth"]({
            "type": "login_token",
            "token_endpoint": "https://api.example.com/auth/login",
            "token_response_field": "token",
            "token_header_prefix": "",
        })
        assert headers == {"Authorization": "Bearer xyz"}

    def test_login_token_missing_endpoint_raises(self, helpers):
        with pytest.raises(ValueError, match="token_endpoint"):
            helpers["_resolve_auth"]({"type": "login_token"})

    @patch("urllib.request.urlopen")
    def test_login_token_missing_response_field_raises(self, mock_urlopen, helpers):
        mock_urlopen.return_value = _mock_response(b'{"unexpected": "shape"}')
        with pytest.raises(ValueError, match="access_token"):
            helpers["_resolve_auth"]({
                "type": "login_token",
                "token_endpoint": "https://api.example.com/auth/login",
            })

    def test_unknown_auth_type_raises(self, helpers):
        with pytest.raises(ValueError, match="Unknown auth.type"):
            helpers["_resolve_auth"]({"type": "totally_made_up"})
