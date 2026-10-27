"""Mipyre example — decorator syntax, an after_request hook, custom responses, a custom 404."""

from mipyre import Mipyre, Response, ServerFlags

app = Mipyre()

# Enable built-in request logging
app.enable_access_log()

# Serve static files from ./public under /static
# app.static("/static", "./public")


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

@app.after_request
def add_cors(req, resp):
    """Add CORS headers to every response."""
    return Response(
        body=resp.body,
        status_code=resp.status_code,
        content_type=resp.content_type,
        headers={**resp.headers, "access-control-allow-origin": "*"},
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/")
def index(req):
    return "Hello from Mipyre!"


@app.get("/hello/{name}")
def greet(req):
    name = req.params.get("name", "world")
    return {"message": f"Hello, {name}!"}


@app.post("/echo")
def echo(req):
    return req.text()


@app.get("/search")
def search(req):
    """Query params + request headers."""
    q = req.query_params.get("q", "")
    user_agent = req.headers.get("user-agent", "unknown")
    return {"query": q, "user_agent": user_agent}


@app.get("/html")
def html_page(req):
    """Custom content-type."""
    return Response(
        body="<h1>Hello from Mipyre</h1>",
        content_type="text/html; charset=utf-8",
    )


@app.fallback
def not_found(req):
    """Custom 404 handler."""
    return Response(
        body={"error": "not found", "path": req.path},
        status_code=404,
        content_type="application/json",
    )


if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=8000))
