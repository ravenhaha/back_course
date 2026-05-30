from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import AdminUser
from app.schemas.teams import TeamCreate, TeamOut, TeamUpdate
from app.services.teams import TeamService

router = APIRouter(prefix="/teams", tags=["teams"])
DbDep = Annotated[AsyncSession, Depends(get_db)]


@router.get("", response_model=list[TeamOut])
async def list_teams(db: DbDep, _: AdminUser, offset: int = 0, limit: int = 100):
    return await TeamService(db).list(offset=offset, limit=limit)


@router.post("", response_model=TeamOut, status_code=201)
async def create_team(db: DbDep, _: AdminUser, data: TeamCreate):
    teams = TeamService(db)
    if await teams.get_by_name(data.name):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Team name already exists")
    return await teams.create(data.name)


@router.get("/{team_id}", response_model=TeamOut)
async def get_team(db: DbDep, _: AdminUser, team_id: int):
    team = await TeamService(db).get(team_id)
    if team is None:
        raise HTTPException(status_code=404, detail="Team not found")
    return team


@router.patch("/{team_id}", response_model=TeamOut)
async def update_team(db: DbDep, _: AdminUser, team_id: int, data: TeamUpdate):
    teams = TeamService(db)
    team = await teams.get(team_id)
    if team is None:
        raise HTTPException(status_code=404, detail="Team not found")
    if data.name is not None:
        existing = await teams.get_by_name(data.name)
        if existing and existing.id != team_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Team name already exists")
    return await teams.update(team, name=data.name)


@router.delete("/{team_id}", status_code=204)
async def delete_team(db: DbDep, _: AdminUser, team_id: int):
    ok = await TeamService(db).delete(team_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Team not found")

