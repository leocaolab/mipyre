"""Regression for the CORS-404 short-circuit bug.

Before the fix, early-return paths in `handle_request` / `handle_request_subinterp`
(unmatched route, static-file hit) bypassed `apply_cors`. Browsers doing a
CORS preflight (OPTIONS) against an unknown path got a bare 404 with no
Access-Control-* headers, blocking the follow-up real request with an
opaque CORS policy error.
"""

import urllib.request

import pytest

from mipyre.testing import TestClient, TestServer


def _request_with_cors(base: str, path: str, method: str = "GET"):
    req = urllib.request.Request(
        f"{base}{path}",
        headers={"Origin": "https://app.example.com"},
        method=method,
    )
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        return resp.status, dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers)


def test_404_carries_cors_headers():
    # Its own module: a worker serves one app per module.
    from tests.apps.cors_404 import app

    with TestServer(app) as server, TestClient(server) as client:
        status, headers = _request_with_cors(client.base_url, "/nonexistent")
        assert status == 404
        lower = {k.lower(): v for k, v in headers.items()}
        # Must have allow-origin or the browser blocks the real request
        assert "access-control-allow-origin" in lower, (
            f"404 response missing CORS headers: {headers}"
        )


def test_options_preflight_on_unknown_path_not_blocked():
    """Simulates a real CORS preflight for a path the dev forgot to register
    an OPTIONS handler for. The browser will see allow-origin and proceed."""
    # Its own module: a worker serves one app per module.
    from tests.apps.cors_preflight_unknown_path import app

    with TestServer(app) as server, TestClient(server) as client:
        # OPTIONS /api/login — route not registered for OPTIONS, falls to 404
        status, headers = _request_with_cors(
            client.base_url, "/api/login", method="OPTIONS"
        )
        lower = {k.lower(): v for k, v in headers.items()}
        assert "access-control-allow-origin" in lower


def test_static_file_hit_carries_cors():
    """Static-file success path was also short-circuiting. A CSS file served
    over CORS needs the headers too."""
    import os
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        css = os.path.join(tmpdir, "reset.css")
        with open(css, "w") as f:
            f.write("body { margin: 0 }")

        # The app's module reads its static root from here, in main and in every worker.
        os.environ["MIPYRE_TEST_CORS_STATIC_DIR"] = tmpdir
        try:
            from tests.apps.cors_static import app

            with TestServer(app) as server, TestClient(server) as client:
                status, headers = _request_with_cors(client.base_url, "/static/reset.css")
                assert status == 200
                lower = {k.lower(): v for k, v in headers.items()}
                assert "access-control-allow-origin" in lower
        finally:
            del os.environ["MIPYRE_TEST_CORS_STATIC_DIR"]


def _preflight(base: str, path: str):
    req = urllib.request.Request(
        f"{base}{path}",
        headers={
            "Origin": "https://app.example.com",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
        method="OPTIONS",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        return resp.status, {k.lower(): v for k, v in resp.headers.items()}
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in e.headers.items()}


def test_a_preflight_to_a_route_without_options_is_answered_204():
    """A browser drops the real request unless its preflight is answered 2xx (W3C Fetch); a
    preflight to a POST-only route used to get 405."""
    from tests.apps.cors_preflight_unknown_path import app

    with TestServer(app) as server, TestClient(server) as client:
        for path in ("/api/login", "/nowhere"):
            status, headers = _preflight(client.base_url, path)
            assert status == 204, (path, status)
            assert headers["access-control-allow-origin"] == "https://app.example.com"
            assert headers["access-control-allow-methods"] == "GET, POST, OPTIONS"
            assert headers["access-control-allow-headers"] == "content-type"
            assert headers["access-control-max-age"] == "86400"


def test_any_origin_with_credentials_is_refused_by_the_engine_itself():
    from mipyre import Mipyre

    app = Mipyre()
    with pytest.raises(ValueError, match="allow_credentials"):
        app.enable_cors(allow_origins=["https://a.example", "*"], allow_credentials=True)
    with pytest.raises(ValueError, match="allow_credentials"):
        app._engine.set_cors_config(["*"], "GET", "*", None, True)


def _get_from(base: str, origin: str | None):
    headers = {} if origin is None else {"Origin": origin}
    resp = urllib.request.urlopen(urllib.request.Request(f"{base}/", headers=headers), timeout=5)
    return resp.status, {k.lower(): v for k, v in resp.headers.items()}


def test_of_several_origins_only_the_request_s_own_is_named():
    """`Access-Control-Allow-Origin` holds one origin (W3C Fetch): a browser takes a list there
    for no match, so the configured list used to fail for every origin."""
    from tests.apps.cors_several_origins import app

    with TestServer(app) as server, TestClient(server) as client:
        for origin in ("https://a.example", "https://b.example"):
            status, headers = _get_from(client.base_url, origin)
            assert status == 200
            assert headers["access-control-allow-origin"] == origin
            assert headers["access-control-allow-credentials"] == "true"
            assert headers["vary"] == "Origin"
        for origin in ("https://c.example", None):
            status, headers = _get_from(client.base_url, origin)
            assert status == 200
            assert not [k for k in headers if k.startswith("access-control-")], (origin, headers)
            assert headers["vary"] == "Origin"
