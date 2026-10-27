"""App for tests/test_process_lane_faults.py: a process route, the metrics page, on the main
interpreter the lanes' template's pid (the test kills the template), and what fds a worker holds."""

import errno
import os
import stat

from mipyre import Mipyre

app = Mipyre()
app.enable_access_log()
app.enable_metrics()


@app.get("/hello", runtime="process")
def hello(req):
    return {"pid": os.getpid()}


@app.get("/lanes/template", runtime="main")
def lanes_template(req):
    return {"pid": app._runs.process_lanes.template_pid}


@app.get("/lanes/worker-fds", runtime="process")
def worker_fds(req):
    """The pipes and sockets past the standard streams that a program the worker runs would get. Files are left
    out: libffi keeps the python binary open without close-on-exec once ctypes is used."""
    inheritable = []
    for name in os.listdir("/dev/fd"):
        fd = int(name)
        try:
            mode = os.fstat(fd).st_mode
        except OSError as e:
            if e.errno != errno.EBADF:
                raise
            continue  # the listing's own fd, closed once the listing is done
        if fd > 2 and (stat.S_ISFIFO(mode) or stat.S_ISSOCK(mode)) and os.get_inheritable(fd):
            inheritable.append(fd)
    return {"pid": os.getpid(), "inheritable": inheritable}
