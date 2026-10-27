"""Tests for MCP (Model Context Protocol) server."""

import json
import pytest
from mipyre import Mipyre
from mipyre.mcp import MCPServer, _extract_schema
from mipyre.testing import TestClient


# -- Schema extraction tests --------------------------------------------------

def test_extract_schema_basic():
    def add(a: int, b: int) -> int:
        return a + b
    schema = _extract_schema(add)
    assert schema["type"] == "object"
    assert schema["properties"]["a"]["type"] == "integer"
    assert schema["properties"]["b"]["type"] == "integer"
    assert set(schema["required"]) == {"a", "b"}


def test_extract_schema_optional():
    def greet(name: str, greeting: str = "hello"):
        pass
    schema = _extract_schema(greet)
    assert schema["required"] == ["name"]
    assert "greeting" not in schema.get("required", [])


def test_extract_schema_various_types():
    def fn(a: int, b: float, c: str, d: bool, e: list, f: dict):
        pass
    schema = _extract_schema(fn)
    assert schema["properties"]["a"]["type"] == "integer"
    assert schema["properties"]["b"]["type"] == "number"
    assert schema["properties"]["c"]["type"] == "string"
    assert schema["properties"]["d"]["type"] == "boolean"
    assert schema["properties"]["e"]["type"] == "array"
    assert schema["properties"]["f"]["type"] == "object"


# -- MCP protocol tests -------------------------------------------------------

@pytest.fixture
def mcp():
    server = MCPServer(Mipyre())

    @server.tool(description="Add two numbers")
    def add(a: int, b: int) -> int:
        return a + b

    @server.resource("config://app", description="App config")
    def get_config():
        return {"version": "1.0"}

    @server.prompt("greeting", description="Greet user")
    def greeting(name: str) -> str:
        return f"Hello {name}!"

    return server


def _rpc(mcp, method, params=None):
    return mcp.answer({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}})


def test_initialize(mcp):
    resp = _rpc(mcp, "initialize")
    assert resp["result"]["protocolVersion"] == "2024-11-05"
    assert "tools" in resp["result"]["capabilities"]


def test_tools_list(mcp):
    resp = _rpc(mcp, "tools/list")
    tools = resp["result"]["tools"]
    assert len(tools) == 1
    assert tools[0]["name"] == "add"
    assert tools[0]["description"] == "Add two numbers"


def test_tools_call(mcp):
    resp = _rpc(mcp, "tools/call", {"name": "add", "arguments": {"a": 3, "b": 5}})
    content = resp["result"]["content"]
    assert content[0]["text"] == "8"
    assert resp["result"]["isError"] is False


def test_tools_call_unknown():
    server = MCPServer(Mipyre())
    resp = _rpc(server, "tools/call", {"name": "nonexistent"})
    assert "error" in resp
    assert resp["error"]["code"] == -32602


def test_resources_list(mcp):
    resp = _rpc(mcp, "resources/list")
    resources = resp["result"]["resources"]
    assert len(resources) == 1
    assert resources[0]["uri"] == "config://app"


def test_resources_read(mcp):
    resp = _rpc(mcp, "resources/read", {"uri": "config://app"})
    contents = resp["result"]["contents"]
    assert json.loads(contents[0]["text"]) == {"version": "1.0"}


def test_resources_read_unknown(mcp):
    resp = _rpc(mcp, "resources/read", {"uri": "config://missing"})
    assert "error" in resp


def test_prompts_list(mcp):
    resp = _rpc(mcp, "prompts/list")
    prompts = resp["result"]["prompts"]
    assert len(prompts) == 1
    assert prompts[0]["name"] == "greeting"


def test_prompts_get(mcp):
    resp = _rpc(mcp, "prompts/get", {"name": "greeting", "arguments": {"name": "Alice"}})
    msg = resp["result"]["messages"][0]
    assert msg["role"] == "user"
    assert "Hello Alice!" in msg["content"]["text"]


def test_method_not_found(mcp):
    resp = _rpc(mcp, "nonexistent/method")
    assert resp["error"]["code"] == -32601


def test_parse_error():
    app = Mipyre(runtime="main")
    MCPServer(app).tool(lambda: "pong", name="ping")
    with TestClient(app) as c:
        r = c.post("/mcp", body="not json", headers={"Content-Type": "application/json"})
    assert r.json()["error"]["code"] == -32700


# -- Inputs a client controls -------------------------------------------------


def _main_app_with_ping() -> Mipyre:
    app = Mipyre(runtime="main")

    @MCPServer(app).tool
    def ping() -> str:
        return "pong"

    return app


def _post_mcp(app: Mipyre, body: str | bytes):
    with TestClient(app) as c:
        return c.post("/mcp", body=body, headers={"Content-Type": "application/json"})


def test_mcp_tool_call_runs_on_the_requests_thread_and_starts_none():
    """A tool runs where the request runs, under the engine's request_timeout: no thread of
    its own for each call (a hung tool used to leave one per call behind)."""
    import threading

    ran_on = []
    server = MCPServer(Mipyre())

    @server.tool
    def where() -> str:
        ran_on.append(threading.current_thread())
        return "here"

    before = threading.active_count()
    for i in range(20):
        reply = server.answer({"jsonrpc": "2.0", "id": i, "method": "tools/call", "params": {"name": "where"}})
        assert reply["result"]["content"][0]["text"] == "here"
    assert set(ran_on) == {threading.current_thread()}
    assert threading.active_count() == before


def test_mcp_over_http_answers_deep_nesting_with_json_rpc_not_500():
    body = "[" * 100_000 + "]" * 100_000
    r = _post_mcp(_main_app_with_ping(), body)
    assert r.status_code == 200, (r.status_code, r.text[:200])
    assert r.json()["error"]["code"] == -32700


@pytest.mark.parametrize("raw_id", ["NaN", "1e999", "1" * 5000])
def test_mcp_answers_any_id_a_client_sends_with_valid_json(raw_id):
    """Python's `json.loads` took `NaN` and `1e999` (inf) as numbers and echoed them into a
    reply that is not JSON; a 5000-digit id raised ValueError (a 500)."""
    body = '{"jsonrpc": "2.0", "id": ' + raw_id + ', "method": "initialize"}'
    r = _post_mcp(_main_app_with_ping(), body)
    assert r.status_code == 200, (r.status_code, r.text[:200])
    reply = json.loads(r.text, parse_constant=lambda c: pytest.fail(f"reply is not JSON: carries {c}"))
    assert reply["error"]["code"] == -32700


@pytest.mark.parametrize("req_id", [2**64, -(2**63) - 1, 2**200])
def test_mcp_echoes_an_id_wider_than_64_bits_in_full(req_id):
    """The reply is encoded by the engine: an id wider than 64 bits comes back as
    the same integer, not a 500."""
    body = json.dumps({"jsonrpc": "2.0", "id": req_id, "method": "tools/call", "params": {"name": "ping"}})
    r = _post_mcp(_main_app_with_ping(), body)
    assert r.status_code == 200, (r.status_code, r.text[:200])
    assert json.loads(r.text) == {"jsonrpc": "2.0", "id": req_id,
                                  "result": {"content": [{"type": "text", "text": "pong"}], "isError": False}}


def test_mcp_notification_over_http_is_202_with_no_body():
    r = _post_mcp(_main_app_with_ping(), '{"jsonrpc": "2.0", "method": "notifications/initialized"}')
    assert (r.status_code, r.text) == (202, "")


def test_mcp_async_tool_over_http_is_driven_to_its_value():
    app = Mipyre(runtime="main")

    @MCPServer(app).tool
    async def double(x: int) -> int:
        return 2 * x

    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                       "params": {"name": "double", "arguments": {"x": 21}}})
    assert _post_mcp(app, body).json()["result"]["content"][0]["text"] == "42"


# -- ToolError: a failure the tool reports to the model ------------------------


def test_mcp_tool_error_is_a_result_with_is_error(caplog):
    from mipyre.mcp import ToolError

    server = MCPServer(Mipyre())

    @server.tool
    def lookup(key: str) -> str:
        raise ToolError(f"no entry {key!r}")

    @server.tool
    async def lookup_later(key: str) -> str:
        raise ToolError(f"no entry {key!r} yet")

    import asyncio

    sync_reply = _rpc(server, "tools/call", {"name": "lookup", "arguments": {"key": "k"}})
    async_reply = asyncio.run(_rpc(server, "tools/call", {"name": "lookup_later", "arguments": {"key": "k"}}))
    assert sync_reply["result"] == {"content": [{"type": "text", "text": "no entry 'k'"}], "isError": True}
    assert async_reply["result"] == {"content": [{"type": "text", "text": "no entry 'k' yet"}], "isError": True}
    assert not [r for r in caplog.records if r.name == "mipyre.mcp"]


# -- Registration -------------------------------------------------------------


def test_mcp_registering_a_name_twice_raises():
    server = MCPServer(Mipyre())
    server.tool(lambda: 1, name="t")
    server.resource("config://a")(lambda: 1)
    server.prompt("p")(lambda: "x")
    with pytest.raises(ValueError, match="tool 't' is already registered"):
        server.tool(lambda: 2, name="t")
    with pytest.raises(ValueError, match="resource 'config://a' is already registered"):
        server.resource("config://a")(lambda: 2)
    with pytest.raises(ValueError, match="prompt 'p' is already registered"):
        server.prompt("p")(lambda: "y")


def _greet(name: str, tone: str = "warm") -> str:
    return f"{tone} hello {name}"


@pytest.mark.parametrize("arguments,error,words", [
    ("name", TypeError, "takes a list of dicts"),
    (["name"], TypeError, "each of arguments= is a dict"),
    ([{"description": "who"}], TypeError, "non-empty str 'name'"),
    ([{"name": "name", "required": True, "kind": "str"}], ValueError, "unknown key"),
    ([{"name": "name", "required": "yes"}], TypeError, "non-bool 'required'"),
    ([{"name": "name", "required": True}, {"name": "name"}], ValueError, "more than once"),
    ([{"name": "name", "required": True}, {"name": "mood"}], ValueError, "does not take"),
    ([{"name": "tone"}], ValueError, r"requires \['name'\]"),
])
def test_mcp_prompt_arguments_a_client_could_not_call_with_are_refused_at_registration(arguments, error, words):
    with pytest.raises(error, match=words):
        MCPServer(Mipyre()).prompt("greet", arguments=arguments)(_greet)


def test_mcp_prompt_arguments_declared_are_announced_and_called_with():
    server = MCPServer(Mipyre())
    server.prompt("greet", arguments=[{"name": "name", "description": "who", "required": True}, {"name": "tone"}])(_greet)
    listed = _rpc(server, "prompts/list")["result"]["prompts"][0]["arguments"]
    assert listed == [{"name": "name", "required": True, "description": "who"}, {"name": "tone", "required": False}]
    reply = _rpc(server, "prompts/get", {"name": "greet", "arguments": {"name": "Ann"}})
    assert reply["result"]["messages"][0]["content"]["text"] == "warm hello Ann"


# -- a tool's input_schema is checked whole --------------------------------------


@pytest.mark.parametrize("arguments,accepted", [({"x": 1}, True), ({"x": 1, "y": 2}, False)])
def test_mcp_tool_schema_without_additional_properties_refuses_an_argument_it_does_not_list(arguments, accepted):
    server = MCPServer(Mipyre())
    schema = {"type": "object", "properties": {"x": {"type": "integer"}}, "required": ["x"], "additionalProperties": False}
    server.tool(lambda x, y=None: "ok", name="t", input_schema=schema)
    reply = _rpc(server, "tools/call", {"name": "t", "arguments": arguments})
    assert ("result" in reply) == accepted, reply
    if not accepted:
        assert reply["error"] == {"code": -32602, "message": "invalid params: arguments: unexpected key 'y'"}


@pytest.mark.parametrize("required,refused", [(["x"], False), ([], True)])
def test_mcp_tool_schema_must_require_what_its_handler_requires(required, refused):
    server = MCPServer(Mipyre())
    schema = {"type": "object", "properties": {"x": {}}, "required": required}
    register = server.tool(name="t", input_schema=schema)
    if refused:
        with pytest.raises(ValueError, match=r"MCP tool 't': its handler requires \['x'\]"):
            register(lambda x: "ok")
    else:
        register(lambda x: "ok")


# -- enum / const compare JSON values, not Python ones -------------------------


@pytest.mark.parametrize("schema,value,accepted", [
    ({"enum": [1]}, True, False),
    ({"enum": [True]}, 1, False),
    ({"const": 0}, False, False),
    ({"const": [1, {"a": True}]}, [1, {"a": 1}], False),
    ({"enum": [1]}, 1.0, True),
    ({"const": [1, {"a": True}]}, [1.0, {"a": True}], True),
    ({"enum": ["a", None]}, None, True),
])
def test_mcp_enum_and_const_tell_true_from_1(schema, value, accepted):
    server = MCPServer(Mipyre())
    server.tool(lambda x: "ok", name="t", input_schema={"type": "object", "properties": {"x": schema}, "required": ["x"]})
    reply = _rpc(server, "tools/call", {"name": "t", "arguments": {"x": value}})
    assert ("result" in reply) == accepted, reply


def test_mcp_async_tool_that_raises_is_an_internal_error_naming_the_request(caplog):
    import asyncio

    server = MCPServer(Mipyre())

    @server.tool
    async def broken() -> str:
        raise RuntimeError("db down")

    reply = asyncio.run(server.answer(
        {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "broken"}}, request_id="rid-1"
    ))
    assert reply["error"] == {"code": -32603, "message": "Internal error", "data": {"request_id": "rid-1"}}
    [record] = [r for r in caplog.records if r.name == "mipyre.mcp"]
    assert "rid-1" in record.getMessage() and record.exc_info[0] is RuntimeError


def test_mcp_hung_tool_is_answered_by_the_engines_request_timeout():
    import threading

    app = Mipyre(runtime="main")
    app.request_timeout = 0.05
    release = threading.Event()

    @MCPServer(app).tool
    def hang() -> str:
        release.wait()
        return "late"

    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "hang"}})
    with TestClient(app) as c:
        try:
            r = c.post("/mcp", body=body, headers={"Content-Type": "application/json"})
        finally:
            release.set()
    assert r.status_code == 504, (r.status_code, r.text)
