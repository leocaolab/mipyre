"""
Full-stack REST API — demonstrates Mipyre as a FastAPI replacement.

Features used:
  - CRUD routes (GET/POST/PUT/DELETE)
  - Pydantic validation
  - Cookie-based auth
  - File upload
  - Redirect
  - CORS
  - SharedState as in-memory database
  - Structured logging
  - Hot reload ready

Layout (each interpreter imports only the modules it serves):
  fullstack/main.py       the app and its sub-interpreter routes
  fullstack/validated.py  Router(runtime="main"): the routes with pydantic bodies
  fullstack/auth.py       the session-cookie helpers both use

Run:
  python examples/fullstack_api.py
  # Or with hot reload:
  MIPYRE_RELOAD=1 python examples/fullstack_api.py

Test:
  # Register
  curl -X POST http://127.0.0.1:8000/auth/register -H 'Content-Type: application/json' \
    -d '{"username": "alice", "email": "alice@example.com", "password": "secret123"}'

  # Login (sets cookie)
  curl -c cookies.txt -X POST http://127.0.0.1:8000/auth/login \
    -H 'Content-Type: application/json' -d '{"username": "alice", "password": "secret123"}'

  # Create item (with auth cookie)
  curl -b cookies.txt -X POST http://127.0.0.1:8000/items \
    -H 'Content-Type: application/json' -d '{"name": "Widget", "price": 9.99, "tags": ["new"]}'

  # List items
  curl http://127.0.0.1:8000/items

  # Upload avatar
  curl -b cookies.txt -F "avatar=@photo.jpg" http://127.0.0.1:8000/auth/avatar
"""

from fullstack.main import app

if __name__ == "__main__":
    app.run()
