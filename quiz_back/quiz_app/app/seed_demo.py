from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.models.game import Game, GameQuestion, GameStatus, GameTeam, TeamAnswer
from app.models.question import Question, QuestionOption
from app.models.team import Team
from app.models.user import User, UserRole
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


async def _get_option_ids(db: AsyncSession, question_id: int) -> list[int]:
    res = await db.execute(
        select(QuestionOption.id).where(QuestionOption.question_id == question_id).order_by(QuestionOption.id)
    )
    return [int(option_id) for option_id in res.scalars().all()]


async def _ensure_demo_profile(
    db: AsyncSession,
    user: User,
    *,
    full_name: str,
    email: str,
    team_id: int,
) -> None:
    changed = False
    if user.full_name is None:
        user.full_name = full_name
        changed = True
    if user.email is None:
        user.email = email
        changed = True
    if user.team_id is None:
        user.team_id = team_id
        changed = True
    if changed:
        await db.commit()


async def _seed_finished_demo_game(
    db: AsyncSession,
    *,
    team_a_id: int,
    team_b_id: int,
    question_ids: list[int],
    admin_id: int | None,
) -> None:
    res = await db.execute(select(func.count(Game.id)).where(Game.status == GameStatus.finished))
    finished_games_count = int(res.scalar_one() or 0)
    if finished_games_count > 0:
        return

    q1_options = await _get_option_ids(db, question_ids[0])
    q2_options = await _get_option_ids(db, question_ids[1])
    q3_options = await _get_option_ids(db, question_ids[2])

    game = Game(
        scheduled_at=datetime(2026, 5, 28, 12, 0, tzinfo=timezone.utc),
        status=GameStatus.finished,
        created_by_user_id=admin_id,
    )
    game.questions = [
        GameQuestion(question_id=question_id, order_index=index)
        for index, question_id in enumerate(question_ids)
    ]
    game.teams = [
        GameTeam(team_id=team_a_id, points=2),
        GameTeam(team_id=team_b_id, points=1),
    ]
    db.add(game)
    await db.flush()

    db.add_all(
        [
            TeamAnswer(
                game_id=game.id,
                team_id=team_a_id,
                question_id=question_ids[0],
                option_id=q1_options[1],
                is_correct=True,
                within_time=True,
                elapsed_ms=8200,
            ),
            TeamAnswer(
                game_id=game.id,
                team_id=team_b_id,
                question_id=question_ids[0],
                option_id=q1_options[0],
                is_correct=False,
                within_time=True,
                elapsed_ms=9300,
            ),
            TeamAnswer(
                game_id=game.id,
                team_id=team_a_id,
                question_id=question_ids[1],
                option_id=q2_options[0],
                is_correct=True,
                within_time=True,
                elapsed_ms=11800,
            ),
            TeamAnswer(
                game_id=game.id,
                team_id=team_b_id,
                question_id=question_ids[1],
                option_id=q2_options[0],
                is_correct=True,
                within_time=True,
                elapsed_ms=14100,
            ),
            TeamAnswer(
                game_id=game.id,
                team_id=team_a_id,
                question_id=question_ids[2],
                option_id=q3_options[1],
                is_correct=False,
                within_time=True,
                elapsed_ms=15400,
            ),
            TeamAnswer(
                game_id=game.id,
                team_id=team_b_id,
                question_id=question_ids[2],
                option_id=q3_options[3],
                is_correct=False,
                within_time=True,
                elapsed_ms=17600,
            ),
        ]
    )

    team_a = await db.get(Team, team_a_id)
    team_b = await db.get(Team, team_b_id)
    if team_a is not None:
        team_a.total_points += 2
        team_a.games_played += 1
        team_a.wins += 1
    if team_b is not None:
        team_b.total_points += 1
        team_b.games_played += 1
        team_b.losses += 1

    await db.commit()


async def seed_demo_data() -> None:
    """
    Seeds a minimal demo dataset so Swagger/UI checks are fast:
    - admin user: admin / admin123
    - two teams
    - two players (assigned to teams)
    - a few questions (with options)
    - one finished demo game with answers/results
    - one draft game referencing the seeded teams/questions

    The seeding is idempotent-ish: it won't recreate entities if they already exist.
    """
    async with AsyncSessionLocal() as db:
        # Keep it simple and reliable: check a few anchor records.
        users = UserService(db)
        admin = await users.get_by_username("admin")
        if admin is None:
            admin = await users.create(username="admin", password="admin123", role=UserRole.admin)

        team_a_id = await _get_or_create_team(db, "Team A")
        team_b_id = await _get_or_create_team(db, "Team B")

        player1 = await users.get_by_username("player1")
        if player1 is None:
            player1 = await users.create(
                username="player1",
                password="password123",
                role=UserRole.player,
                full_name="Ivan Petrov",
                email="player1@example.com",
                team_id=team_a_id,
            )
        else:
            await _ensure_demo_profile(
                db,
                player1,
                full_name="Ivan Petrov",
                email="player1@example.com",
                team_id=team_a_id,
            )

        player2 = await users.get_by_username("player2")
        if player2 is None:
            player2 = await users.create(
                username="player2",
                password="password123",
                role=UserRole.player,
                full_name="Anna Sidorova",
                email="player2@example.com",
                team_id=team_b_id,
            )
        else:
            await _ensure_demo_profile(
                db,
                player2,
                full_name="Anna Sidorova",
                email="player2@example.com",
                team_id=team_b_id,
            )

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

        await _seed_finished_demo_game(
            db,
            team_a_id=team_a_id,
            team_b_id=team_b_id,
            question_ids=[q1_id, q2_id, q3_id],
            admin_id=admin.id if admin else None,
        )

        # Create a draft game for manual realtime checks only if there is no draft.
        res = await db.execute(select(func.count(Game.id)))
        games_count = int(res.scalar_one() or 0)
        res = await db.execute(select(func.count(Game.id)).where(Game.status == GameStatus.draft))
        draft_games_count = int(res.scalar_one() or 0)
        if games_count == 0 or draft_games_count == 0:
            scheduled_at = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
            await GameService(db).create(
                scheduled_at=scheduled_at,
                team_ids=[team_a_id, team_b_id],
                question_ids=[q1_id, q2_id, q3_id],
                created_by_user_id=admin.id if admin else None,
            )


def main() -> None:
    asyncio.run(seed_demo_data())


if __name__ == "__main__":
    main()
