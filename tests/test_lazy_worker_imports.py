"""A worker of a plain sync app imports none of importlib.metadata, email.utils, ctypes (~1.9 MiB a
worker, a90a637a) and urllib.parse: `mipyre.__version__` is read on first use, the cookie expires
formatter imports email.utils itself, the bootstrap imports ctypes on its first private clone,
multipart imports urllib.parse for a filename* parameter."""

import http.client

from mipyre.testing import TestClient, TestServer


def _get(conn, path):
    conn.request("GET", path)
    resp = conn.getresponse()
    return resp.status, resp.read().decode()


def test_a_worker_of_a_sync_app_does_not_import_metadata_email_ctypes_or_urllib_parse():
    from tests.apps.lazy_imports_sync import app

    with TestServer(app) as server, TestClient(server) as client:
        # One connection: one TPC thread, one worker.
        conn = http.client.HTTPConnection("127.0.0.1", client.port, timeout=10)
        assert _get(conn, "/deferred-loaded") == (200, "")
        # The probe reads the worker's own modules: one the handler imports shows up, with
        # urllib.parse, which email.utils imports.
        assert _get(conn, "/import-email-utils") == (200, "imported")
        assert _get(conn, "/deferred-loaded") == (200, "email.utils,urllib.parse")
        conn.close()
