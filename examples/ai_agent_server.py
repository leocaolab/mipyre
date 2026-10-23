"""AI agent server: MCP tools, a chat route validated with pydantic, the answer streamed as
Server-Sent Events, and session memory shared by every interpreter.

Layout (each interpreter imports only the modules it serves):
  ai_agent/main.py    the app, its MCP tools and its sub-interpreter routes
  ai_agent/chat.py    Router(runtime="main"): POST /chat, GET /stream
  ai_agent/memory.py  the session history in app.state

Run:
  pip install mipyre pydantic
  python examples/ai_agent_server.py

Try:
  curl -X POST http://127.0.0.1:8000/chat -H 'content-type: application/json' \
    -d '{"prompt": "What is Python?", "session_id": "user1"}'
  curl -N 'http://127.0.0.1:8000/stream?prompt=hello&session_id=user1'
  curl http://127.0.0.1:8000/memory/user1
  curl -X POST http://127.0.0.1:8000/mcp -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
"""

from ai_agent.main import app

if __name__ == "__main__":
    app.run()
