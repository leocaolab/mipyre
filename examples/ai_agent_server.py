"""
AI Agent Server — demonstrates Mipyre for agentic AI applications.

Features used:
  - MCP Server (AI tool discovery)
  - SSE streaming (token-by-token LLM output)
  - async handlers (concurrent LLM calls)
  - SharedState (agent session memory)
  - Pydantic validation
  - CORS (frontend integration)

Layout (each interpreter imports only the modules it serves):
  ai_agent/main.py  the app, its MCP tools and its sub-interpreter routes
  ai_agent/chat.py  Router(runtime="main"): /chat (a pydantic body), /stream, /memory

Run:
  python examples/ai_agent_server.py

Test:
  # Chat endpoint (simulated LLM)
  curl -X POST http://127.0.0.1:8000/chat -H 'Content-Type: application/json' \
    -d '{"prompt": "What is Python?", "session_id": "user1"}'

  # Streaming response
  curl -N http://127.0.0.1:8000/stream?prompt=hello

  # MCP tool discovery
  curl -X POST http://127.0.0.1:8000/mcp -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'

  # Check session memory
  curl http://127.0.0.1:8000/memory/user1
"""

from ai_agent.main import app

if __name__ == "__main__":
    app.run()
