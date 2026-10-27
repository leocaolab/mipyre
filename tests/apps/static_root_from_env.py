"""Static mount for tests/test_ws_static_logging_settings.py's static tests. The `static_server` fixture
creates the tree and exports its root as MIPYRE_TEST_STATIC_ROOT before importing
this module; workers inherit the environment, so they mount the same root."""

import os

from mipyre import Mipyre

app = Mipyre()


@app.get("/health")
def health(req):
    return {"ok": True}


app.static("/static", os.environ["MIPYRE_TEST_STATIC_ROOT"])
