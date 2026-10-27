"""App for tests/test_server_topology.py: its first server has no MCP tools; a tool is added
before the second, which must serve `/mcp`. Served on the main interpreter (`runtime="main"`)."""

from mipyre import Mipyre

app = Mipyre(runtime="main")


@app.get("/")
def index(req):
    return "ok"
