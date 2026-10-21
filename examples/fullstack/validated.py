"""Routes whose JSON bodies pydantic validates. pydantic can't load in a sub-interpreter,
so they run on the main interpreter and only the main interpreter imports this module."""

import hashlib
import json
import time

from pydantic import BaseModel, Field, field_validator

from mipyre import Response, Router, SharedState
from mipyre.cookies import set_cookie

from fullstack.auth import hash_password, require_auth

router = Router(runtime="main")


def _state() -> SharedState:
    # fullstack.main imports this module while it builds its app, so the app is looked up
    # when a request comes, not at import.
    from fullstack.main import app

    return app.state


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class UserRegister(BaseModel):
    username: str = Field(min_length=3, max_length=20)
    email: str
    password: str = Field(min_length=6)

    @field_validator("email")
    @classmethod
    def validate_email(cls, v):
        if "@" not in v:
            raise ValueError("invalid email")
        return v


class UserLogin(BaseModel):
    username: str
    password: str


class ItemCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    price: float = Field(gt=0)
    tags: list[str] = []


class ItemUpdate(BaseModel):
    name: str | None = None
    price: float | None = None
    tags: list[str] | None = None


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------

@router.post("/auth/register", model=UserRegister)
def register(req, user: UserRegister):
    state = _state()
    # Check if user exists
    if state.get(f"user:{user.username}"):
        return Response(
            body=json.dumps({"error": "username already taken"}),
            status_code=409,
            content_type="application/json",
        )

    user_data = {
        "username": user.username,
        "email": user.email,
        "password_hash": hash_password(user.password),
        "created_at": time.time(),
        "avatar_size": 0,
    }
    state[f"user:{user.username}"] = json.dumps(user_data)

    return {"status": "registered", "username": user.username}


@router.post("/auth/login", model=UserLogin)
def login(req, creds: UserLogin):
    state = _state()
    user_json = state.get(f"user:{creds.username}")
    if not user_json:
        return Response(
            body=json.dumps({"error": "invalid credentials"}),
            status_code=401,
            content_type="application/json",
        )

    user = json.loads(user_json)
    if user["password_hash"] != hash_password(creds.password):
        return Response(
            body=json.dumps({"error": "invalid credentials"}),
            status_code=401,
            content_type="application/json",
        )

    # Create session token
    token = hashlib.sha256(f"{creds.username}:{time.time()}".encode()).hexdigest()[:32]
    state[f"session:{token}"] = json.dumps({"username": creds.username, "email": user["email"]})

    resp = Response(body=json.dumps({"status": "logged in", "username": creds.username}),
                       content_type="application/json")
    return set_cookie(resp, "session_token", token, httponly=True, max_age=86400)


# ---------------------------------------------------------------------------
# CRUD — Items
# ---------------------------------------------------------------------------

@router.post("/items", model=ItemCreate)
def create_item(req, item: ItemCreate):
    state = _state()
    auth = require_auth(req, state)
    if isinstance(auth, Response):
        return auth

    items = json.loads(state.get("items_db") or "[]")
    new_item = {
        "id": len(items) + 1,
        "name": item.name,
        "price": item.price,
        "tags": item.tags,
        "created_by": auth["username"],
        "created_at": time.time(),
    }
    items.append(new_item)
    state["items_db"] = json.dumps(items)

    return Response(
        body=json.dumps(new_item),
        status_code=201,
        content_type="application/json",
    )


@router.put("/items/{item_id}", model=ItemUpdate)
def update_item(req, updates: ItemUpdate):
    state = _state()
    auth = require_auth(req, state)
    if isinstance(auth, Response):
        return auth

    item_id = int(req.params["item_id"])
    items = json.loads(state.get("items_db") or "[]")
    for item in items:
        if item["id"] == item_id:
            if updates.name is not None:
                item["name"] = updates.name
            if updates.price is not None:
                item["price"] = updates.price
            if updates.tags is not None:
                item["tags"] = updates.tags
            state["items_db"] = json.dumps(items)
            return item

    return Response(body=json.dumps({"error": "not found"}), status_code=404,
                       content_type="application/json")
