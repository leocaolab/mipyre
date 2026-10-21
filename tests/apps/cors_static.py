"""App for tests/test_cors_404.py::test_static_file_hit_carries_cors: CORS plus a static
mount whose root the test creates and exports as MIPYRE_TEST_CORS_STATIC_DIR before
importing this module (workers inherit the environment)."""

import os

from mipyre import Mipyre

app = Mipyre()
app.enable_cors(allow_origins="https://app.example.com")
app.static("/static", os.environ["MIPYRE_TEST_CORS_STATIC_DIR"])


@app.get("/")
def root(req):
    return "hello"
