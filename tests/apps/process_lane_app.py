"""App for tests/test_process_lane.py: runtime="process" routes that answer, raise, die, hang.

Settings come from MIPYRE_PROCESS_* (the test sets workers, timeout and queue)."""

import builtins
import os
import threading
import time

from mipyre import Mipyre, Response, Stream, ServerFlags

app = Mipyre()
app.enable_access_log()


@app.get("/hello", runtime="process")
def hello(req):
    return "hello"


@app.get("/pid", runtime="process")
def pid(req):
    return {"pid": os.getpid(), "ppid": os.getppid()}


@app.get("/mipyre-env", runtime="process")
def mipyre_env(req):
    return sorted(name for name in os.environ if name.startswith("MIPYRE_"))


@app.post("/echo/{name}", runtime="process")
def echo(req, name):
    return Response(
        body=req.body,
        status_code=201,
        content_type="application/x-echo",
        headers={"x-name": name, "x-q": req.query_params.get("q", ""), "x-rid": req.request_id},
    )


@app.get("/raise", runtime="process")
def boom(req):
    raise ValueError("process lane test exception 7f3a")


@app.get("/too-many-headers", runtime="process")
def too_many_headers(req):
    return Response("hi", headers={f"x-{i}": "v" for i in range(24_577)})


@app.get("/exit", runtime="process")
def die(req):
    os._exit(3)


@app.get("/sleep", runtime="process")
def sleep(req):
    time.sleep(float(req.query_params.get("s", "0.5")))
    return {"pid": os.getpid()}


@app.get("/block", runtime="process")
def block(req):
    """Writes this worker's pid to the file `?mark=` names, then never returns: the request
    ends only by what is done to its worker (a kill, the lane's timeout)."""
    mark = req.query_params["mark"]
    with open(mark + ".part", "w") as f:
        f.write(str(os.getpid()))
    os.replace(mark + ".part", mark)  # whole, never half-written
    threading.Event().wait()


@app.get("/mark", runtime="process")
def mark(req):
    builtins._pl_mark = os.getpid()
    return {"pid": os.getpid()}


@app.get("/check", runtime="process")
def check(req):
    time.sleep(0.02)  # overlapping requests spread over both lanes
    return {"pid": os.getpid(), "mark": getattr(builtins, "_pl_mark", None)}


@app.get("/check_main", runtime="main")
def check_main(req):
    return {"pid": os.getpid(), "mark": getattr(builtins, "_pl_mark", None)}


@app.get("/state", runtime="process")
def state(req):
    return {"n": len(app.state)}


@app.get("/longdouble", runtime="process")
def longdouble(req):
    import numpy as np

    return {"value": str(np.longdouble(int(req.query_params.get("n", "7")))), "numpy": np.__version__}


@app.get("/stream", runtime="process")
def stream(req):
    return Stream()


@app.get("/hog", runtime="process")
def hog(req):
    """Caps this worker's address space (RLIMIT_AS, enforced on Linux and macOS) 256 MiB above
    what it maps now, then grows a list of 8 MiB blocks until the allocator is refused."""
    import resource
    import subprocess

    vsz_kib = int(subprocess.run(["ps", "-o", "vsz=", "-p", str(os.getpid())], capture_output=True, text=True,
                                 check=True).stdout)
    cap = vsz_kib * 1024 + 256 * 1024 * 1024
    # The hard limit too: macOS refuses a soft limit below an unlimited hard one.
    resource.setrlimit(resource.RLIMIT_AS, (cap, cap))
    blocks = []
    while True:
        blocks.append(bytearray(8 * 1024 * 1024))


@app.get("/lanes", runtime="main")
def lanes(req):
    return app.lane_stats()


@app.get("/server_pid", runtime="main")
def server_pid(req):
    return {"pid": os.getpid()}


@app.get("/lane_stats", runtime="main")
def lane_stats(req):
    stats = app.lane_stats()
    return {"deaths": stats["deaths"], "respawns": stats["respawns"]}


if __name__ == "__main__":
    app.run(ServerFlags(port=0))
