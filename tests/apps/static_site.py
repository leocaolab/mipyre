"""Static-file app for tests/test_static_files.py. The suite's `static_dir` fixture
creates the root and exports it as MIPYRE_TEST_STATIC_DIR before importing this
module; workers inherit the environment, so they mount the same root."""

import os

from mipyre import Mipyre

app = Mipyre()


@app.get("/")
def index(req):
    return {"api": True}


app.static("/static/", os.environ["MIPYRE_TEST_STATIC_DIR"])
