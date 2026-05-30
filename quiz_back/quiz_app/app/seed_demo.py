from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.models.game import Game
from app.models.question import Question
from app.models.user import UserRole
from app.services.games import GameService
from app.services.questions import QuestionService
from app.services.teams import TeamService
from app.services.users import UserService


async def _get_or_create_team(db: AsyncSession, name: str) -> int:
    ts = TeamService(db)
    team = await ts.get_by_name(name)
    if team is None:
        team = await ts.create(name)
    return team.id


async def _get_or_create_question(
    db: AsyncSession,
    *,
    text: str,
    options: list[str],
    correct_option_index: int,
) -> int:
    res = await db.execute(select(Question).where(Question.text == text))
    existing = res.scalar_one_or_none()
    if existing is not None:
        return existing.id
    q = await QuestionService(db).create(text=text, options=options, correct_option_index=correct_option_index)
    return q.id


async def seed_demo_data() -> None:
    """
    Seeds a minimal demo dataset so Swagger/UI checks are fast:
    - admin user: admin / admin123
    - two teams
    - two players (assigned to teams)
    - a few questions (with options)
    - one draft game referencing the seeded teams/questions

    The seeding is idempotent-ish: it won't recreate entities if they already exist.
    """
    async with AsyncSessionLocal() as db:
        # Keep it simple and reliable: check a few anchor records.
        users = UserService(db)
        if await users.get_by_username("admin") is None:
            await users.create(username="admin", password="admin123", role=UserRole.admin)

        team_a_id = await _get_or_create_team(db, "Team A")
        team_b_id = await _get_or_create_team(db, "Team B")

        if await users.get_by_username("player1") is None:
            await users.create(username="player1", password="password123", role=UserRole.player, team_id=team_a_id)
        if await users.get_by_username("player2") is None:
            await users.create(username="player2", password="password123", role=UserRole.player, team_id=team_b_id)

        q1_id = await _get_or_create_question(
            db,
            text="2+2?",
            options=["3", "4", "5", "22"],
            correct_option_index=1,
        )
        q2_id = await _get_or_create_question(
            db,
            text="Capital of France?",
            options=["Paris", "Rome", "Berlin", "Madrid"],
            correct_option_index=0,
        )
        q3_id = await _get_or_create_question(
            db,
            text="HTTP status for 'Not Found'?",
            options=["200", "301", "404", "500"],
            correct_option_index=2,
        )

        # Create a single draft game only if there are no games at all.
        res = await db.execute(select(func.count(Game.id)))
        games_count = int(res.scalar_one() or 0)
        if games_count == 0:
            scheduled_at = datetime.now(timezone.utc)
            await GameService(db).create(
                scheduled_at=scheduled_at,
                team_ids=[team_a_id, team_b_id],
                question_ids=[q1_id, q2_id, q3_id],
                created_by_user_id=None,
            )


def main() -> None:
    asyncio.run(seed_demo_data())


if __name__ == "__main__":
    main()
