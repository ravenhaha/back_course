from __future__ import annotations

from pydantic import BaseModel, Field

from app.models.user import UserRole


class UserCreate(BaseModel):
    username: str = Field(min_length=3, max_length=50)
    password: str = Field(min_length=6, max_length=100)
    role: UserRole
    full_name: str | None = Field(default=None, max_length=200)
    email: str | None = Field(default=None, max_length=200)
    team_id: int | None = None


class UserUpdate(BaseModel):
    role: UserRole | None = None
    full_name: str | None = Field(default=None, max_length=200)
    email: str | None = Field(default=None, max_length=200)
    team_id: int | None = None


class UserOut(BaseModel):
    id: int
    username: str
    role: UserRole
    full_name: str | None = None
    email: str | None = None
    team_id: int | None = None

    model_config = {"from_attributes": True}

