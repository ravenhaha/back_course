from typing import Annotated

from fastapi import APIRouter, Cookie, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field

from students_db import SessionRepository, User, UserRepository

SESSION_COOKIE = "session_id"


# ─── Pydantic-схемы ───────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    username: str = Field(min_length=3, max_length=50)
    password: str = Field(min_length=6, max_length=100)


class LoginRequest(BaseModel):
    username: str
    password: str


class UserResponse(BaseModel):
    id: int
    username: str

    model_config = {"from_attributes": True}


# ─── Зависимости ──────────────────────────────────────────────────────────────

def get_user_repo() -> UserRepository:
    return UserRepository(db_url="sqlite:///students.db")


def get_session_repo() -> SessionRepository:
    return SessionRepository(db_url="sqlite:///students.db")


UserRepoDep = Annotated[UserRepository, Depends(get_user_repo)]
SessionRepoDep = Annotated[SessionRepository, Depends(get_session_repo)]


def get_current_user(
    users: UserRepoDep,
    sessions: SessionRepoDep,
    session_id: Annotated[str | None, Cookie(alias=SESSION_COOKIE)] = None,
) -> User:
    if not session_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Требуется авторизация",
        )
    user_id = sessions.get_user_id(session_id)
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Сессия недействительна",
        )
    user = users.get_by_id(user_id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Пользователь не найден",
        )
    return user


CurrentUser = Annotated[User, Depends(get_current_user)]


# ─── Роутер ───────────────────────────────────────────────────────────────────

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=UserResponse, status_code=201,
             summary="Регистрация пользователя")
def register(data: RegisterRequest, users: UserRepoDep):
    if users.get_by_username(data.username):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Пользователь с таким именем уже существует",
        )
    user = users.create(data.username, data.password)
    return user


@router.post("/login", response_model=UserResponse, summary="Вход (аутентификация)")
def login(
    data: LoginRequest,
    response: Response,
    users: UserRepoDep,
    sessions: SessionRepoDep,
):
    user = users.authenticate(data.username, data.password)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверное имя пользователя или пароль",
        )
    session_id = sessions.create(user.id)
    response.set_cookie(
        key=SESSION_COOKIE,
        value=session_id,
        httponly=True,
        samesite="lax",
    )
    return user


@router.post("/logout", status_code=204, summary="Выход (завершение сессии)")
def logout(
    response: Response,
    sessions: SessionRepoDep,
    session_id: Annotated[str | None, Cookie(alias=SESSION_COOKIE)] = None,
):
    if session_id:
        sessions.delete(session_id)
    response.delete_cookie(SESSION_COOKIE)


@router.get("/me", response_model=UserResponse, summary="Текущий пользователь")
def me(user: CurrentUser):
    return user
