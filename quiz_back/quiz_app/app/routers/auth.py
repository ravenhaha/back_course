from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import CurrentUser
from app.models.user import UserRole
from app.schemas.auth import (
    ChangePasswordRequest,
    LoginRequest,
    RefreshRequest,
    RegisterRequest,
    TokenPairResponse,
    UserResponse,
)
from app.services.tokens import TokenService
from app.services.users import UserService

router = APIRouter(prefix="/auth", tags=["auth"])
DbDep = Annotated[AsyncSession, Depends(get_db)]


@router.post("/register", response_model=UserResponse, status_code=201)
async def register(data: RegisterRequest, db: DbDep):
    users = UserService(db)
    if await users.get_by_username(data.username):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already exists")

    # keep it permissive: role may be provided to simplify bootstrap in Swagger
    user = await users.create(
        username=data.username,
        password=data.password,
        role=data.role,
        full_name=data.full_name,
        email=data.email,
        team_id=data.team_id,
    )
    return user


@router.post("/login", response_model=TokenPairResponse)
async def login(data: LoginRequest, db: DbDep):
    user = await UserService(db).authenticate(data.username, data.password)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    access, refresh = await TokenService(db).issue_pair(user.id)
    return TokenPairResponse(access_token=access, refresh_token=refresh)


@router.post("/refresh", response_model=TokenPairResponse)
async def refresh(data: RefreshRequest, db: DbDep):
    pair = await TokenService(db).refresh_pair(data.refresh_token)
    if pair is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired refresh token")
    access, refresh = pair
    return TokenPairResponse(access_token=access, refresh_token=refresh)


@router.post("/change-password", status_code=204)
async def change_password(data: ChangePasswordRequest, user: CurrentUser, db: DbDep):
    ok = await UserService(db).change_password(user, current_password=data.current_password, new_password=data.new_password)
    if not ok:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Current password is incorrect")
    # revoke all tokens so user must login again (simple and safe)
    await TokenService(db).revoke_user_tokens(user.id)


@router.get("/me", response_model=UserResponse)
async def me(user: CurrentUser):
    return user

