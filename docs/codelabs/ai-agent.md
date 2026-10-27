# Build an AI agent server with mipyre

A step-by-step guide to the parts of mipyre an AI application uses: MCP tools an agent can call, a chat route validated with pydantic, the answer streamed as Server-Sent Events, and session memory that every interpreter shares.

## Before you begin

### What you'll build

A server for an AI agent, with a simulated model in place of a real one:

- `POST /mcp`: an MCP server with three tools (a document search, a calculator and a clock) and one resource, the list of sessions.
- `POST /chat`: a chat request validated with pydantic, answered in one piece.
- `GET /stream`: the same answer sent token by token as Server-Sent Events.
- `GET /memory/{session_id}`: a session's conversation history, written by the chat routes and read from any interpreter.

The finished app is [`examples/ai_agent_server.py`](../../examples/ai_agent_server.py) and the [`examples/ai_agent/`](../../examples/ai_agent) package.

### What you'll learn

- How to serve MCP tools and resources with `MCPServer`, and how a tool reports a failure to the model.
- Why the chat routes run on `runtime="main"` and the memory route on the default `subinterp`.
- How to stream a response with `Stream` and stop when the client goes away.
- How `app.state` shares data between the main interpreter and the sub-interpreters.

### What you'll need

- macOS on Apple silicon or Linux x86_64.
- Python 3.14: the regular build, not the free-threaded one.
- `curl`.

Estimated time: 30 minutes.

## 1. Set up the project

**Goal:** a virtual environment with mipyre and pydantic.

```bash
mkdir agent && cd agent
python3.14 -m venv .venv
source .venv/bin/activate
pip install mipyre pydantic
mipyre --version
```

**Checkpoint:**

```text
mipyre 3.0.0
```

## 2. Serve the first route

**Goal:** an app in a package of its own, with one route on the sub-interpreters.

The app lives in a package, `ai_agent`, so that each part can go in its own module; step 4 shows why that matters. Create the package:

```bash
mkdir ai_agent
touch ai_agent/__init__.py
```

Create `ai_agent/main.py`:

```python
from mipyre import Mipyre

app = Mipyre()
app.enable_cors(allow_origins="*")


@app.get("/")
def index(req):
    return {"service": "Mipyre AI Agent Server"}
```

`enable_cors(allow_origins="*")` lets a web front end on any origin call the API; list your front end's origins instead to have the server refuse requests that act (`POST`, WebSocket upgrades) from every other site's page with `403`. Create `ai_agent_server.py`, the script that starts it:

```python
from ai_agent.main import app

if __name__ == "__main__":
    app.run()
```

Start it:

```bash
python ai_agent_server.py
```

**Checkpoint:** the server prints its address. The thread and CPU counts depend on your machine:

```text
  Mipyre v3.0.0
  Listening on http://127.0.0.1:8000
  TPC threads: 6 (CPUs: 18, not pinned: this platform has no CPU pinning)
  Routes: 1 sub-interp + 0 main + 0 process + 0 async
```

In a second terminal:

```bash
curl -s localhost:8000/
```

```text
{"service":"Mipyre AI Agent Server"}
```

`index` runs on the default runtime, `subinterp`: every server thread has its own Python sub-interpreter with its own GIL, so the route answers in parallel on every core. It is pure Python, which is all a sub-interpreter route needs. Each sub-interpreter imports `ai_agent/main.py` too, so that module must not import a library a sub-interpreter can't load.

Stop the server with Ctrl-C.

## 3. Serve MCP tools

**Goal:** tools an AI agent can discover and call over MCP.

Replace `ai_agent/main.py` with:

```python
import ast
import datetime
import operator
from collections.abc import Callable

from mipyre import Mipyre
from mipyre.mcp import MCPServer, ToolError

app = Mipyre()
app.enable_cors(allow_origins="*")

mcp = MCPServer(app)

OPERATORS: dict[type, Callable[..., float]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.USub: operator.neg,
}
# Bounds the parser's and evaluate()'s recursion, and the size of the numbers.
MAX_EXPRESSION_CHARS = 200


@mcp.tool(description="Search the knowledge base for relevant documents")
def search_docs(query: str, top_k: int = 3) -> list:
    return [
        {"title": f"Doc about {query}", "score": 0.95, "snippet": f"Information about {query}..."},
        {"title": f"Related: {query} guide", "score": 0.87, "snippet": f"A guide covering {query}..."},
    ][:top_k]


def evaluate(node: ast.expr) -> float:
    match node:
        case ast.Constant(value=int() | float() as value) if not isinstance(value, bool):
            return value
        case ast.BinOp(left, op, right) if type(op) in OPERATORS:
            return OPERATORS[type(op)](evaluate(left), evaluate(right))
        case ast.UnaryOp(op, operand) if type(op) in OPERATORS:
            return OPERATORS[type(op)](evaluate(operand))
    raise ToolError(f"not arithmetic: {ast.unparse(node)}")


@mcp.tool(description="Evaluate an arithmetic expression with + - * / and parentheses")
def calculate(expression: str) -> float:
    if len(expression) > MAX_EXPRESSION_CHARS:
        raise ToolError(f"the expression is longer than {MAX_EXPRESSION_CHARS} characters")
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as e:
        raise ToolError(f"cannot parse {expression!r}: {e.msg}") from None
    try:
        return evaluate(tree.body)
    except ZeroDivisionError:
        raise ToolError("division by zero") from None


@mcp.tool(description="Get the server's current time")
def get_timestamp() -> str:
    return datetime.datetime.now().isoformat()


@app.get("/")
def index(req):
    return {"service": "Mipyre AI Agent Server"}
```

`MCPServer(app)` registers `POST /mcp`, which speaks MCP's JSON-RPC 2.0. Each `@mcp.tool` becomes a tool; its input schema comes from the type hints, so `query: str, top_k: int = 3` is announced as a required string and an optional integer, and arguments of the wrong type are refused before the tool runs.

The calculator evaluates what a model sends it, so it is written as an allow-list: it parses the expression and walks the tree, accepting numbers and four operators and nothing else. Never `eval()` a model's input, even with `__builtins__` emptied; that is not a sandbox. A tool reports a failure the model should see by raising `ToolError`: the call's result carries the message with `isError: true`, and nothing is logged. Any other exception is the server's failure: the client gets error `-32603` with a request id, and the traceback goes to the log under the same id.

Run `python ai_agent_server.py` again. The server now prints `MCP: 3 tools, 0 resources, 0 prompts → POST /mcp` above its banner. In the second terminal, list the tools:

```bash
curl -s localhost:8000/mcp -H 'content-type: application/json' -d '{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}'
```

**Checkpoint:**

```text
{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"search_docs","description":"Search the knowledge base for relevant documents","inputSchema":{"type":"object","properties":{"query":{"type":"string"},"top_k":{"type":"integer"}},"required":["query"]}},{"name":"calculate","description":"Evaluate an arithmetic expression with + - * / and parentheses","inputSchema":{"type":"object","properties":{"expression":{"type":"string"}},"required":["expression"]}},{"name":"get_timestamp","description":"Get the server's current time","inputSchema":{"type":"object","properties":{}}}]}}
```

Call the calculator three times: with arithmetic, with code, and with a number where a string belongs:

```bash
curl -s localhost:8000/mcp -H 'content-type: application/json' -d '{"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "calculate", "arguments": {"expression": "(2 + 3) * 4"}}}'; echo
curl -s localhost:8000/mcp -H 'content-type: application/json' -d '{"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "calculate", "arguments": {"expression": "open(\"/etc/passwd\").read()"}}}'; echo
curl -s localhost:8000/mcp -H 'content-type: application/json' -d '{"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "calculate", "arguments": {"expression": 42}}}'; echo
```

```text
{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"20"}],"isError":false}}
{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"not arithmetic: open('/etc/passwd').read()"}],"isError":true}}
{"jsonrpc":"2.0","id":4,"error":{"code":-32602,"message":"invalid params: arguments.expression: expected string, got integer"}}
```

`/mcp` acts on what a POST body asks for, so it refuses a request a web page could make a browser send behind the user's back. A `POST` from another site's page gets `403` from the engine before `/mcp` runs, unless that site is in `enable_cors(allow_origins=...)`; this app's `"*"` trusts every site, so list your front end's origins when it has one. Requests without an `Origin`, such as curl's or an agent's, are admitted. A body that isn't `application/json` gets `415`, which a page can't get around by choosing a form's content type:

```bash
curl -s localhost:8000/mcp -H 'content-type: text/plain' -d '{"jsonrpc": "2.0", "id": 5, "method": "tools/list"}'
```

```text
{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Content-Type must be application/json, got 'text/plain'"}}
```

`/mcp` always runs on the main interpreter, whatever the app's runtime: the tools are Python functions registered on one `MCPServer` object, so they run where that object lives. Stop the server.

## 4. Add a chat route on `main`

**Goal:** a chat route validated with pydantic that remembers each exchange, without the sub-interpreters ever importing pydantic.

The chat route needs two things a sub-interpreter route can't have:

- **pydantic**, whose core can't load in a sub-interpreter. A sub-interpreter imports every module that has routes it serves, so pydantic must not be imported in `ai_agent/main.py`.
- **the session history in `app.state`**, which a `runtime="process"` route can't reach, because a worker process has its own copy of the server's state.

That leaves `runtime="main"`, the server's main interpreter. It has one GIL for all its routes, which is enough for chat: each request spends its time waiting on the model, not computing.

Create `ai_agent/memory.py`, which keeps a session's history in `app.state`:

```python
import json

from mipyre import SharedState

SESSION_PREFIX = "session:"
HISTORY_LIMIT = 20


def history(state: SharedState, session_id: str) -> list[dict]:
    return json.loads(state.get(SESSION_PREFIX + session_id) or "[]")


def remember(state: SharedState, session_id: str, prompt: str, answer: str) -> None:
    """Append one exchange and keep the last HISTORY_LIMIT messages. Two requests of the same
    session at once can lose one of the two exchanges: the read and the write are separate
    calls."""
    messages = history(state, session_id)
    messages += [{"role": "user", "content": prompt}, {"role": "assistant", "content": answer}]
    state[SESSION_PREFIX + session_id] = json.dumps(messages[-HISTORY_LIMIT:])
```

`app.state` is a `SharedState`: a string-to-string map that lives in Rust and that the main interpreter and every sub-interpreter see the same. Each call on it is atomic; a read followed by a write is not, as the docstring says. For a counter, use `state.incr(key, 1)`, which is.

Create `ai_agent/chat.py`, with the routes in a `Router` that runs on `main`:

```python
import time
from collections.abc import Iterator

from pydantic import BaseModel

from mipyre import Router, SharedState

from ai_agent.memory import remember

router = Router(runtime="main")

TOKEN_DELAY_SECONDS = 0.03


class ChatRequest(BaseModel):
    prompt: str
    session_id: str = "default"
    max_tokens: int = 50


def fake_llm_generate(prompt: str, max_tokens: int = 50) -> Iterator[str]:
    """Yield the words of a canned answer, one every TOKEN_DELAY_SECONDS. Replace it with
    a real model client."""
    answer = (
        f"I received your question about '{prompt}'. Here is a thoughtful response that "
        "demonstrates the streaming capabilities of the Mipyre framework. Each token is "
        "sent individually with realistic delays."
    )
    for word in answer.split()[:max_tokens]:
        time.sleep(TOKEN_DELAY_SECONDS)
        yield word + " "


def _state() -> SharedState:
    # ai_agent.main imports this module while it builds the app, so the app is looked up
    # when a request comes, not at import.
    from ai_agent.main import app

    return app.state


@router.post("/chat", body=ChatRequest)
def chat(body: ChatRequest):
    tokens = list(fake_llm_generate(body.prompt, body.max_tokens))
    answer = "".join(tokens)
    remember(_state(), body.session_id, body.prompt, answer)
    return {"response": answer, "session_id": body.session_id, "tokens": len(tokens)}
```

With `body=ChatRequest`, the handler receives the validated model instead of the request. A body that fails validation is answered `422` with pydantic's errors, and the handler doesn't run.

In `ai_agent/main.py`, list the router and turn on the access log. Change the two lines after the imports to:

```python
app = Mipyre()
app.include_router("ai_agent.chat:router")
app.enable_cors(allow_origins="*")
app.enable_access_log()
```

The app names the router by import string, not by object, so each runtime imports only the routers it serves: the main interpreter imports `ai_agent.chat`, and the sub-interpreters never do.

Run `python ai_agent_server.py`. The banner now counts the two routes on the main interpreter as `main`:

```text
  Routes: 1 sub-interp + 2 main + 0 process + 0 async
```

In the second terminal, send a chat request and an invalid one:

```bash
curl -s localhost:8000/chat -H 'content-type: application/json' -d '{"prompt": "What is Python?", "session_id": "ada", "max_tokens": 8}'; echo
curl -s localhost:8000/chat -H 'content-type: application/json' -d '{"session_id": "ada", "max_tokens": "many"}'; echo
```

**Checkpoint:**

```text
{"response":"I received your question about 'What is Python?'. ","session_id":"ada","tokens":8}
{"detail":[{"type":"missing","loc":["prompt"],"msg":"Field required"},{"type":"int_parsing","loc":["max_tokens"],"msg":"Input should be a valid integer, unable to parse string as an integer"}],"error_count":2}
```

The access log in the server's terminal shows where each request ran: `"source":"tpc-inline"` for a sub-interpreter route, answered on the thread that took the request, and `"source":"tpc-main-bridge"` for a route handed to the main interpreter:

```text
{"timestamp":"...","level":"INFO","fields":{"message":"Request handled","method":"POST","path":"/chat","status":200,"latency_us":287956,"source":"tpc-main-bridge","request_id":"...","client_ip":"127.0.0.1"},"target":"mipyre::access"}
```

Stop the server.

## 5. Stream the answer with Server-Sent Events

**Goal:** send the answer token by token as it is generated.

Add this import at the top of `ai_agent/chat.py`:

```python
import json
```

and add `EventSourceResponse`, `ServerSentEvent` and `Stream` to the mipyre import:

```python
from mipyre import EventSourceResponse, Router, ServerSentEvent, SharedState, Stream
```

Then add to the end of `ai_agent/chat.py`:

```python
def answer_events(state: SharedState, prompt: str, session_id: str) -> Iterator[ServerSentEvent]:
    """Yield the answer's tokens, then a ``done`` event, then remember the answer. A client
    that leaves closes the generator at a ``yield``, so an answer it didn't read is not
    remembered."""
    tokens = []
    for token in fake_llm_generate(prompt):
        yield ServerSentEvent(token, event="token")
        tokens.append(token)
    yield ServerSentEvent(json.dumps({"done": True, "tokens": len(tokens)}), event="done")
    remember(state, session_id, prompt, "".join(tokens))


@router.get("/stream")
def stream_chat(req) -> Stream:
    prompt = req.query_params.get("prompt", "hello")
    session_id = req.query_params.get("session_id", "default")
    return EventSourceResponse(answer_events(_state(), prompt, session_id))
```

`EventSourceResponse` returns a `Stream`, which sends the response headers at once and the body as it is written, as `text/event-stream`. Each `ServerSentEvent(data, event=...)` the generator yields is one SSE event, and the response ends when the generator does. The handler returns right away and a thread of its own reads the generator, so no worker of the main interpreter is held for the length of the answer.

`Stream` works only on the main interpreter, which is one more reason this router is on `main`. When the client disconnects, the generator is closed at its `yield` and stops generating for nobody. While a slow client lets the buffer fill, the generator is not read until the client takes more.

Run `python ai_agent_server.py` and stream an answer:

```bash
curl -s -N 'localhost:8000/stream?prompt=SSE&session_id=ada'
```

**Checkpoint:** the events arrive one every 30 ms or so, and the last one is `done`:

```text
event: token
data: I 

event: token
data: received 

...

event: done
data: {"done": true, "tokens": 28}
```

`-N` turns off curl's buffering, so you see each event as it arrives. Stop the server.

## 6. Read the memory from any interpreter

**Goal:** a route on the sub-interpreters, and an MCP resource, that read what the chat routes wrote.

Reading the history needs only `app.state` and `json`, so the route goes back on the default `subinterp` runtime, where it runs in parallel on every core. In `ai_agent/main.py`, add the import below the mipyre imports:

```python
from ai_agent.memory import SESSION_PREFIX, history
```

Then add this resource after the `get_timestamp` tool:

```python
@mcp.resource("memory://sessions", description="Sessions that have a conversation history")
def list_sessions():
    sessions = [k.removeprefix(SESSION_PREFIX) for k in app.state.keys() if k.startswith(SESSION_PREFIX)]
    return {"active_sessions": sessions, "count": len(sessions)}
```

and this route at the end of the file:

```python
@app.get("/memory/{session_id}")
def get_memory(req, session_id):
    messages = history(app.state, session_id)
    return {"session_id": session_id, "messages": messages, "count": len(messages)}
```

`ai_agent/memory.py` imports only `json` and mipyre, so the sub-interpreters can import it. A handler takes the request first, then the path parameters by name.

Run `python ai_agent_server.py`. Chat once, stream once, then read the history and the resource:

```bash
curl -s localhost:8000/chat -H 'content-type: application/json' -d '{"prompt": "What is Python?", "session_id": "ada", "max_tokens": 8}' > /dev/null
curl -s -N 'localhost:8000/stream?prompt=Rust&session_id=ada' > /dev/null
curl -s localhost:8000/memory/ada; echo
curl -s localhost:8000/mcp -H 'content-type: application/json' -d '{"jsonrpc": "2.0", "id": 6, "method": "resources/read", "params": {"uri": "memory://sessions"}}'; echo
```

**Checkpoint:** both exchanges, written on the main interpreter and read on a sub-interpreter:

```text
{"session_id":"ada","messages":[{"role":"user","content":"What is Python?"},{"role":"assistant","content":"I received your question about 'What is Python?'. "},{"role":"user","content":"Rust"},{"role":"assistant","content":"I received your question about 'Rust'. Here is a thoughtful response that demonstrates the streaming capabilities of the Mipyre framework. Each token is sent individually with realistic delays. "}],"count":4}
{"jsonrpc":"2.0","id":6,"result":{"contents":[{"uri":"memory://sessions","mimeType":"application/json","text":"{\"active_sessions\": [\"ada\"], \"count\": 1}"}]}}
```

`app.state` lives in the server process. It is not saved anywhere: a restart starts with no sessions.

Stop the server and print the route table, which `mipyre routes` builds without starting a server:

```bash
mipyre routes ai_agent_server:app
```

```text
METHOD  PATH                  HANDLER                                       FLAGS
------  --------------------  --------------------------------------------  ------------------------------
POST    /chat                 chat                                          runtime=main,body=ChatRequest
GET     /stream               stream_chat                                   runtime=main
POST    /mcp                  MCPServer._http_handler.<locals>.mcp_handler  runtime=main
GET     /                     index                                         runtime=subinterp
GET     /memory/{session_id}  get_memory                                    runtime=subinterp

5 route(s)
```

## What you've built

- An MCP server with three tools and a resource, whose tools check their arguments against their type hints, report failures to the model with `ToolError`, and refuse cross-site requests.
- Each route on the runtime it needs:

  | Route | Runtime | Why |
  |---|---|---|
  | `GET /`, `GET /memory/{session_id}` | `subinterp` | pure Python and `app.state`: parallel on every core |
  | `POST /chat` | `main` | pydantic can't load in a sub-interpreter, and the route writes `app.state`, which a worker process can't reach |
  | `GET /stream` | `main` | `Stream` works only on the main interpreter, and the route writes `app.state` |
  | `POST /mcp` | `main` | always: the tools run where the `MCPServer` lives |

- Session memory in `app.state`, written on the main interpreter and read from the sub-interpreters.

To use a real model, replace `fake_llm_generate` with your client's streaming call. The finished app in [`examples/ai_agent_server.py`](../../examples/ai_agent_server.py) is the same code, with an index that lists the endpoints. For WebSocket and more on streaming, see the [realtime codelab](realtime.md); for databases, configuration, tests and production, see the [fortune codelab](fortune.md).
