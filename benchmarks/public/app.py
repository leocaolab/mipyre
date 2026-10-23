"""The app every runtime mode is measured on; the runtime comes from `mipyre run --runtime`.

numpy is imported inside its handler, so a sub-interpreter never loads it.
"""

import functools

from mipyre import Mipyre

CPU_ITERATIONS = 60_000

app = Mipyre()


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
