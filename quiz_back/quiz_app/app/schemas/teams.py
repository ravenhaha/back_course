from __future__ import annotations

from pydantic import BaseModel, Field


class TeamCreate(BaseModel):
    name: str = Field(min_length=2, max_length=100)


class TeamUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=2, max_length=100)


class TeamOut(BaseModel):
    id: int
    name: str
    total_points: int
    games_played: int
    wins: int
    losses: int

    model_config = {"from_attributes": True}

