"""The app, its MCP tools and its sub-interpreter routes. Every interpreter imports this
module, so it imports nothing a sub-interpreter can't load: pydantic lives in
ai_agent.chat."""

import ast
import datetime
import operator
from collections.abc import Callable

from mipyre import Mipyre
from mipyre.mcp import MCPServer, ToolError

from ai_agent.memory import SESSION_PREFIX, history

app = Mipyre(routers=["ai_agent.chat:router"])
app.enable_cors(allow_origins="*")
app.enable_access_log()

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


@mcp.resource("memory://sessions", description="Sessions that have a conversation history")
def list_sessions():
    sessions = [k.removeprefix(SESSION_PREFIX) for k in app.state.keys() if k.startswith(SESSION_PREFIX)]
    return {"active_sessions": sessions, "count": len(sessions)}


@app.get("/")
def index(req):
    return {
        "service": "Mipyre AI Agent Server",
        "endpoints": [
            "POST /chat: chat with the model (JSON response)",
            "GET /stream?prompt=...: the answer as Server-Sent Events",
            "GET /memory/{session_id}: a session's conversation history",
            "POST /mcp: MCP tools and resources (JSON-RPC)",
        ],
    }


@app.get("/memory/{session_id}")
def get_memory(req, session_id):
    messages = history(app.state, session_id)
    return {"session_id": session_id, "messages": messages, "count": len(messages)}
