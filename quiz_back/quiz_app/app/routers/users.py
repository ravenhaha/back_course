from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import AdminUser
from app.schemas.users import UserCreate, UserOut, UserUpdate
from app.services.users import UserService

router = APIRouter(prefix="/users", tags=["users"])
DbDep = Annotated[AsyncSession, Depends(get_db)]


@router.get("", response_model=list[UserOut])
async def list_users(
    db: DbDep,
    _: AdminUser,
    offset: int = 0,
    limit: int = 100,
):
    return await UserService(db).list(offset=offset, limit=limit)


@router.post("", response_model=UserOut, status_code=201)
async def create_user(db: DbDep, _: AdminUser, data: UserCreate):
    users = UserService(db)
    if await users.get_by_username(data.username):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already exists")
    return await users.create(
        username=data.username,
        password=data.password,
        role=data.role,
        full_name=data.full_name,
        email=data.email,
        team_id=data.team_id,
    )


@router.get("/{user_id}", response_model=UserOut)
async def get_user(db: DbDep, _: AdminUser, user_id: int):
    user = await UserService(db).get_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@router.patch("/{user_id}", response_model=UserOut)
async def update_user(db: DbDep, _: AdminUser, user_id: int, data: UserUpdate):
    users = UserService(db)
    user = await users.get_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return await users.update(
        user,
        role=data.role,
        full_name=data.full_name,
        email=data.email,
        team_id=data.team_id,
    )


@router.delete("/{user_id}", status_code=204)
async def delete_user(db: DbDep, _: AdminUser, user_id: int):
    ok = await UserService(db).delete(user_id)
    if not ok:
        raise HTTPException(status_code=404, detail="User not found")

