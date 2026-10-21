import hashlib
import json

from mipyre import Response, SharedState
from mipyre.cookies import get_cookie


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def get_current_user(req, state: SharedState) -> dict | None:
    token = get_cookie(req, "session_token")
    if not token:
        return None
    user_json = state.get(f"session:{token}")
    if not user_json:
        return None
    return json.loads(user_json)


def require_auth(req, state: SharedState):
    user = get_current_user(req, state)
    if not user:
        return Response(
            body=json.dumps({"error": "unauthorized"}),
            status_code=401,
            content_type="application/json",
        )
    return user
