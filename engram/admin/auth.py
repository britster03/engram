import os
import secrets

from fastapi import Request
from fastapi.responses import RedirectResponse
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

SESSION_COOKIE_NAME = "engram_session_token"
SESSION_MAX_AGE = 86400

_secret_key = os.environ.get("ENGRAM_SECRET_KEY") or secrets.token_hex(32)
_serializer = URLSafeTimedSerializer(_secret_key)


def init_auth(secret_key=None):
    global _serializer, _secret_key
    key = os.environ.get("ENGRAM_SECRET_KEY") or secret_key or _secret_key
    _secret_key = key
    _serializer = URLSafeTimedSerializer(key)


def create_session_token():
    payload = {"admin": True}
    return _serializer.dumps(payload)


def verify_session_token(token, max_age=SESSION_MAX_AGE):
    if not token:
        return False
    try:
        data = _serializer.loads(token, max_age=max_age)
        return data.get("admin", False) is True
    except (BadSignature, SignatureExpired):
        return False


def verify_api_key(api_key, server_api_key):
    if not api_key or not server_api_key:
        return False
    return secrets.compare_digest(api_key, server_api_key)


async def require_ui_auth(request: Request):
    from engram.config import get_config

    path = request.url.path
    if path == "/admin/login" or path.startswith("/admin/static/"):
        return None

    token = request.cookies.get(SESSION_COOKIE_NAME)
    if token and verify_session_token(token):
        return None

    auth = request.headers.get("Authorization")
    if auth:
        try:
            cfg = get_config()
            admin_key = getattr(cfg.api, "admin_key", None) or os.environ.get("ENGRAM_ADMIN_KEY")
            if (
                admin_key
                and auth.startswith("Bearer ")
                and secrets.compare_digest(auth.split(" ", 1)[1].strip(), admin_key)
            ):
                return None
        except Exception:
            pass

    return RedirectResponse("/admin/login", status_code=307)
