"""Passwords, session tokens and the session cookie check both modules use."""

import hashlib
import hmac
import json
import secrets

from mipyre import Response, SharedState
from mipyre.cookies import get_cookie

SALT_BYTES = 16
# scrypt's cost: 2**14 rounds of 8-block memory, 16 MiB per hash, as Python's docs suggest.
SCRYPT_N, SCRYPT_R, SCRYPT_P = 2**14, 8, 1


def hash_password(password: str) -> str:
    """Return ``salt$hash`` for ``password``, hex-encoded, with a new random salt."""
    salt = secrets.token_bytes(SALT_BYTES)
    digest = hashlib.scrypt(password.encode(), salt=salt, n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P)
    return f"{salt.hex()}${digest.hex()}"


def password_matches(password: str, stored: str) -> bool:
    salt, digest = stored.split("$")
    given = hashlib.scrypt(password.encode(), salt=bytes.fromhex(salt), n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P)
    return hmac.compare_digest(given.hex(), digest)


def new_session_token() -> str:
    return secrets.token_urlsafe(32)


def get_current_user(req, state: SharedState) -> dict | None:
    token = get_cookie(req, "session_token")
    if not token:
        return None
    user_json = state.get(f"session:{token}")
    if not user_json:
        return None
    return json.loads(user_json)


def require_auth(req, state: SharedState):
    """Return the logged-in user, or the 401 to answer with."""
    user = get_current_user(req, state)
    if not user:
        return Response({"error": "unauthorized"}, status_code=401)
    return user
