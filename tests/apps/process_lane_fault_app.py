"""App for tests/test_process_lane_faults.py: a process route, and on the main interpreter the
lanes' template's pid (the test kills the template)."""

import os

from mipyre import Mipyre

app = Mipyre()
app.enable_access_log()


@app.get("/hello", runtime="process")
def hello(req):
    return {"pid": os.getpid()}


@app.get("/lanes/template", runtime="main")
def lanes_template(req):
    return {"pid": app._process_lanes.template_pid}
