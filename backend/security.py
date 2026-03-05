"""JWT-like auth helpers with HMAC SHA256."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .config import settings

bearer_scheme = HTTPBearer(auto_error=False)


def is_auth_enabled() -> bool:
    return bool(settings.auth_enabled)


def authenticate_user(username: str, password: str) -> bool:
    return hmac.compare_digest((username or "").strip(), (settings.auth_username or "").strip()) and hmac.compare_digest(
        password or "", settings.auth_password or ""
    )


def create_access_token(username: str) -> str:
    payload = {
        "sub": username,
        "exp": int(time.time()) + max(1, int(settings.auth_token_expire_hours or 24)) * 3600,
    }
    return _encode(payload)


def verify_access_token(token: str) -> dict[str, Any] | None:
    try:
        return _decode(token)
    except Exception:
        return None


def require_auth(credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme)) -> str:
    if not is_auth_enabled():
        return "anonymous"
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=401, detail="未登录或登录已过期")
    payload = verify_access_token(credentials.credentials)
    if not payload:
        raise HTTPException(status_code=401, detail="认证令牌无效")
    sub = str(payload.get("sub") or "")
    if not sub:
        raise HTTPException(status_code=401, detail="认证令牌无效")
    return sub


def _secret() -> bytes:
    secret = (settings.auth_secret or "").strip()
    if not secret:
        raise RuntimeError("AUTH_SECRET 未配置")
    return secret.encode("utf-8")


def _encode(payload: dict[str, Any]) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    h = _b64url(json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    p = _b64url(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    signing_input = f"{h}.{p}".encode("utf-8")
    sig = hmac.new(_secret(), signing_input, hashlib.sha256).digest()
    return f"{h}.{p}.{_b64url(sig)}"


def _decode(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("bad token")
    h, p, s = parts
    signing_input = f"{h}.{p}".encode("utf-8")
    expected = hmac.new(_secret(), signing_input, hashlib.sha256).digest()
    got = _unb64url(s)
    if not hmac.compare_digest(expected, got):
        raise ValueError("bad signature")
    payload = json.loads(_unb64url(p).decode("utf-8"))
    if int(payload.get("exp") or 0) <= int(time.time()):
        raise ValueError("expired")
    return payload


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _unb64url(s: str) -> bytes:
    return base64.urlsafe_b64decode((s + "=" * (-len(s) % 4)).encode("utf-8"))
