from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.team import Team


class TeamService:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def list(self, *, offset: int = 0, limit: int = 100) -> list[Team]:
        res = await self.db.execute(select(Team).offset(offset).limit(limit).order_by(Team.id))
        return list(res.scalars().all())

    async def get(self, team_id: int) -> Team | None:
        return await self.db.get(Team, team_id)

    async def get_by_name(self, name: str) -> Team | None:
        res = await self.db.execute(select(Team).where(Team.name == name))
        return res.scalar_one_or_none()

    async def create(self, name: str) -> Team:
        team = Team(name=name)
        self.db.add(team)
        await self.db.commit()
        await self.db.refresh(team)
        return team

    async def update(self, team: Team, *, name: str | None = None) -> Team:
        if name is not None:
            team.name = name
        await self.db.commit()
        await self.db.refresh(team)
        return team

    async def delete(self, team_id: int) -> bool:
        res = await self.db.execute(delete(Team).where(Team.id == team_id))
        await self.db.commit()
        return bool(res.rowcount)

