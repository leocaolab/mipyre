"""App for tests/test_blas_threads.py: where the BLAS thread variables are set."""

import os

from mipyre import Mipyre

VARS = ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "POLARS_MAX_THREADS")
AT_IMPORT = {v: os.environ.get(v) for v in VARS}  # what the app's own import saw

app = Mipyre()


@app.get("/worker", runtime="process")
def worker(req):
    return {"at_import": AT_IMPORT, "pid": os.getpid()}


@app.get("/server", runtime="main")
def server(req):
    return {"now": {v: os.environ.get(v) for v in VARS}, "pid": os.getpid()}
