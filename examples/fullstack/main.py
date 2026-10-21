"""The app and its sub-interpreter routes; every interpreter imports this module, so it
imports nothing a sub-interpreter can't load (pydantic lives in fullstack.validated)."""

import json

from mipyre import Mipyre, Response
from mipyre.cookies import delete_cookie, get_cookie
from mipyre.uploads import parse_multipart

from fullstack.auth import require_auth

app = Mipyre(routers=["fullstack.validated:router"])
app.enable_cors()
app.enable_access_log()


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------

@app.get("/")
def index(req):
    return {
        "service": "Mipyre Full-stack API",
        "endpoints": {
            "auth": ["POST /auth/register", "POST /auth/login", "POST /auth/logout", "GET /auth/me"],
            "items": ["GET /items", "POST /items", "GET /items/{id}", "PUT /items/{id}", "DELETE /items/{id}"],
            "files": ["POST /auth/avatar"],
        },
    }


@app.get("/auth/me")
def me(req):
    return require_auth(req, app.state)


@app.post("/auth/logout")
def logout(req):
    token = get_cookie(req, "session_token")
    if token:
        app.state.delete(f"session:{token}")
    resp = Response(body=json.dumps({"status": "logged out"}), content_type="application/json")
    return delete_cookie(resp, "session_token")


@app.post("/auth/avatar")
def upload_avatar(req):
    auth = require_auth(req, app.state)
    if isinstance(auth, Response):
        return auth

    form = parse_multipart(req)
    avatar = form.get("avatar")
    if not avatar:
        return Response(body=json.dumps({"error": "no avatar file"}), status_code=400,
                           content_type="application/json")

    # Store avatar size (in real app, save to S3/disk)
    user_json = app.state.get(f"user:{auth['username']}")
    if user_json:
        user = json.loads(user_json)
        user["avatar_size"] = avatar.size
        app.state[f"user:{auth['username']}"] = json.dumps(user)

    return {
        "status": "uploaded",
        "filename": avatar.filename,
        "size": avatar.size,
        "content_type": avatar.content_type,
    }


# ---------------------------------------------------------------------------
# CRUD — Items
# ---------------------------------------------------------------------------

@app.get("/items")
def list_items(req):
    items_json = app.state.get("items_db")
    items = json.loads(items_json) if items_json else []

    # Pagination
    page = int(req.query_params.get("page", "1"))
    per_page = int(req.query_params.get("per_page", "10"))
    start = (page - 1) * per_page
    end = start + per_page

    return {
        "items": items[start:end],
        "total": len(items),
        "page": page,
        "per_page": per_page,
    }


@app.get("/items/{item_id}")
def get_item(req):
    item_id = int(req.params["item_id"])
    items = json.loads(app.state.get("items_db") or "[]")
    for item in items:
        if item["id"] == item_id:
            return item
    return Response(body=json.dumps({"error": "not found"}), status_code=404,
                       content_type="application/json")


@app.delete("/items/{item_id}")
def delete_item(req):
    auth = require_auth(req, app.state)
    if isinstance(auth, Response):
        return auth

    item_id = int(req.params["item_id"])
    items = json.loads(app.state.get("items_db") or "[]")
    items = [i for i in items if i["id"] != item_id]
    app.state["items_db"] = json.dumps(items)
    return {"status": "deleted", "id": item_id}
