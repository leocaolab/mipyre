"""App for tests/test_python_package.py::test_rpc_and_mcp_routes_are_listed_in_app_routes:
an RPC route and an MCP tool. Its own module: a worker serves one app per module."""

from mipyre import Mipyre, rpc
from mipyre.mcp import MCPServer

app = Mipyre()


@rpc(app)
def echo(data):
    return data


mcp = MCPServer(app)

@mcp.tool()
def ping() -> str:
    return "pong"


@app.get("/")
def index(req):
    return "ok"
