from __future__ import annotations

from pydantic import BaseModel, Field

from app.models.user import UserRole


class RegisterRequest(BaseModel):
    username: str = Field(min_length=3, max_length=50)
    password: str = Field(min_length=6, max_length=100)

    # role: admin or player (to keep the project easy to bootstrap/test)
    role: UserRole = UserRole.player

    full_name: str | None = Field(default=None, max_length=200)
    email: str | None = Field(default=None, max_length=200)
    team_id: int | None = None


class LoginRequest(BaseModel):
    username: str
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class ChangePasswordRequest(BaseModel):
    current_password: str = Field(min_length=6, max_length=100)
    new_password: str = Field(min_length=6, max_length=100)


class TokenPairResponse(BaseModel):
    token_type: str = "bearer"
    access_token: str
    refresh_token: str


class UserResponse(BaseModel):
    id: int
    username: str
    role: UserRole
    full_name: str | None = None
    email: str | None = None
    team_id: int | None = None

    model_config = {"from_attributes": True}

