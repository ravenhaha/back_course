from __future__ import annotations

from sqlalchemy import delete
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import hash_password, verify_password
from app.models.user import User, UserRole


class UserService:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def get_by_username(self, username: str) -> User | None:
        res = await self.db.execute(select(User).where(User.username == username))
        return res.scalar_one_or_none()

    async def get_by_id(self, user_id: int) -> User | None:
        return await self.db.get(User, user_id)

    async def list(self, *, offset: int = 0, limit: int = 100) -> list[User]:
        res = await self.db.execute(select(User).offset(offset).limit(limit).order_by(User.id))
        return list(res.scalars().all())

    async def create(
        self,
        *,
        username: str,
        password: str,
        role: UserRole,
        full_name: str | None = None,
        email: str | None = None,
        team_id: int | None = None,
    ) -> User:
        password_hash, salt = hash_password(password)
        user = User(
            username=username,
            password_hash=password_hash,
            password_salt=salt,
            role=role,
            full_name=full_name,
            email=email,
            team_id=team_id,
        )
        self.db.add(user)
        await self.db.commit()
        await self.db.refresh(user)
        return user

    async def update(
        self,
        user: User,
        *,
        role: UserRole | None = None,
        full_name: str | None = None,
        email: str | None = None,
        team_id: int | None = None,
    ) -> User:
        if role is not None:
            user.role = role
        if full_name is not None:
            user.full_name = full_name
        if email is not None:
            user.email = email
        if team_id is not None:
            user.team_id = team_id
        await self.db.commit()
        await self.db.refresh(user)
        return user

    async def delete(self, user_id: int) -> bool:
        res = await self.db.execute(delete(User).where(User.id == user_id))
        await self.db.commit()
        return bool(res.rowcount)

    async def authenticate(self, username: str, password: str) -> User | None:
        user = await self.get_by_username(username)
        if user is None:
            return None
        if not verify_password(password, user.password_hash, user.password_salt):
            return None
        return user

    async def change_password(self, user: User, *, current_password: str, new_password: str) -> bool:
        if not verify_password(current_password, user.password_hash, user.password_salt):
            return False
        password_hash, salt = hash_password(new_password)
        user.password_hash = password_hash
        user.password_salt = salt
        await self.db.commit()
        return True
