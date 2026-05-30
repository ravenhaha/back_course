from __future__ import annotations

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.tokens import access_expiry, hash_token, new_token, refresh_expiry, utcnow
from app.models.token import AuthToken
from app.models.user import User


class TokenService:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def issue_pair(self, user_id: int) -> tuple[str, str]:
        access = new_token()
        refresh = new_token()
        row = AuthToken(
            user_id=user_id,
            access_token_hash=hash_token(access),
            refresh_token_hash=hash_token(refresh),
            access_expires_at=access_expiry(),
            refresh_expires_at=refresh_expiry(),
            revoked=False,
        )
        self.db.add(row)
        await self.db.commit()
        return access, refresh

    async def revoke_user_tokens(self, user_id: int) -> None:
        await self.db.execute(update(AuthToken).where(AuthToken.user_id == user_id).values(revoked=True))
        await self.db.commit()

    async def get_user_by_access_token(self, token: str) -> User | None:
        token_hash = hash_token(token)
        now = utcnow()
        res = await self.db.execute(
            select(User)
            .join(AuthToken, AuthToken.user_id == User.id)
            .where(
                AuthToken.access_token_hash == token_hash,
                AuthToken.revoked.is_(False),
                AuthToken.access_expires_at > now,
            )
        )
        return res.scalar_one_or_none()

    async def refresh_pair(self, refresh_token: str) -> tuple[str, str] | None:
        rt_hash = hash_token(refresh_token)
        now = utcnow()
        res = await self.db.execute(
            select(AuthToken).where(
                AuthToken.refresh_token_hash == rt_hash,
                AuthToken.revoked.is_(False),
                AuthToken.refresh_expires_at > now,
            )
        )
        row = res.scalar_one_or_none()
        if row is None:
            return None
        user_id = row.user_id
        # Revoke old pair and issue a new one
        row.revoked = True
        await self.db.commit()
        return await self.issue_pair(user_id)

    async def revoke_pair_by_refresh(self, refresh_token: str) -> bool:
        rt_hash = hash_token(refresh_token)
        res = await self.db.execute(delete(AuthToken).where(AuthToken.refresh_token_hash == rt_hash))
        await self.db.commit()
        return bool(res.rowcount)

