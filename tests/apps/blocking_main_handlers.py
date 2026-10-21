"""Main-interpreter handlers that block until the test lets them go — for
tests/test_blocking_python_calls.py."""

import threading

from mipyre import Mipyre

app = Mipyre(runtime="main")

# Released once by each `/slow` handler when it is running; `release` lets them all return.
entered = threading.Semaphore(0)
release = threading.Event()


@app.get("/slow")
def slow(req):
    entered.release()
    release.wait()
    return "slow"


@app.get("/fast")
def fast(req):
    return "fast"
