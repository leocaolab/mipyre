"""The app every runtime mode is measured on; `BENCH_RUNTIME` names the runtime, unset is the default.

numpy is imported inside its handler, so a sub-interpreter never loads it.
"""

import functools
import os

from mipyre import Mipyre

CPU_ITERATIONS = 60_000

app = Mipyre(runtime=os.environ.get("BENCH_RUNTIME"))


@app.get("/plaintext")
def plaintext(req):
    return "Hello, World!"


@app.get("/json")
def json_small(req):
    return {"message": "Hello, World!"}


@app.get("/cpu")
def cpu(req):
    return {"result": sum(i * i % 7 for i in range(CPU_ITERATIONS))}


@functools.cache
def _samples():
    import numpy as np

    return np.arange(10_000, dtype=np.float64)


@app.get("/numpy")
def numpy_mean(req):
    return {"mean": float(_samples().mean())}
