"""Static mount at /s/ for tests/test_static_files.py's symlink test. Same root as
tests/apps/static_site.py (MIPYRE_TEST_STATIC_DIR); its own module because a worker
serves one app per module."""

import os

from mipyre import Mipyre

app = Mipyre()


@app.get("/")
def _health(req):
    # TestClient polls `/` to detect server readiness.
    return {"ok": True}


app.static("/s/", os.environ["MIPYRE_TEST_STATIC_DIR"])
