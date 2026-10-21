"""The app, its MCP tools and its sub-interpreter routes; every interpreter imports this
module, so it imports nothing a sub-interpreter can't load (pydantic lives in
ai_agent.chat)."""

from mipyre import Mipyre

app = Mipyre(routers=["ai_agent.chat:router"])
app.enable_cors()
app.enable_access_log()


# ---------------------------------------------------------------------------
# MCP Tools (discoverable by AI agents)
# ---------------------------------------------------------------------------

@app.mcp.tool(description="Search knowledge base for relevant documents")
def search_docs(query: str, top_k: int = 3) -> list:
    """Simulate RAG vector search."""
    return [
        {"title": f"Doc about {query}", "score": 0.95, "snippet": f"Information about {query}..."},
        {"title": f"Related: {query} guide", "score": 0.87, "snippet": f"A guide covering {query}..."},
    ][:top_k]


@app.mcp.tool(description="Execute Python code in sandbox")
def run_code(code: str) -> dict:
    """Simulate code execution sandbox."""
    try:
        result = eval(code, {"__builtins__": {}}, {})
        return {"output": str(result), "error": None}
    except Exception as e:  # noqa: BLE001 — the tool's answer carries the error to the agent
        return {"output": None, "error": str(e)}


@app.mcp.tool(description="Get current timestamp")
def get_timestamp() -> str:
    import datetime
    return datetime.datetime.now().isoformat()


@app.mcp.resource("memory://sessions", description="Active session list")
def list_sessions():
    keys = app.state.keys()
    sessions = [k.replace("session:", "") for k in keys if k.startswith("session:")]
    return {"active_sessions": sessions, "count": len(sessions)}


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/")
def index(req):
    return {
        "service": "Mipyre AI Agent Server",
        "endpoints": [
            "POST /chat — chat with AI (JSON response)",
            "GET /stream?prompt=... — streaming SSE response",
            "GET /memory/{session_id} — view session memory",
            "POST /mcp — MCP tool discovery (for MCP clients)",
        ],
    }
