"""Auth endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ..security import authenticate_user, create_access_token, is_auth_enabled, require_auth

router = APIRouter()


class LoginRequest(BaseModel):
    username: str
    password: str


@router.get("/status")
def auth_status():
    return {"enabled": bool(is_auth_enabled())}


@router.post("/login")
def login(data: LoginRequest):
    if not is_auth_enabled():
        return {"auth_enabled": False, "access_token": ""}
    if not authenticate_user(data.username, data.password):
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    token = create_access_token(data.username)
    return {"auth_enabled": True, "access_token": token, "token_type": "bearer"}


@router.get("/me")
def me(username: str = Depends(require_auth)):
    return {"username": username, "auth_enabled": bool(is_auth_enabled())}
