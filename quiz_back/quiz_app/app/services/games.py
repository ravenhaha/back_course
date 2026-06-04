from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.game import Game, GameQuestion, GameStatus, GameTeam, TeamAnswer
from app.models.question import QuestionOption
from app.models.team import Team
from app.services.game_realtime import GameRealtime


class GameService:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def list(self, *, offset: int = 0, limit: int = 100) -> list[Game]:
        res = await self.db.execute(select(Game).offset(offset).limit(limit).order_by(Game.id))
        return list(res.scalars().all())

    async def get(self, game_id: int) -> Game | None:
        res = await self.db.execute(
            select(Game)
            .options(selectinload(Game.questions), selectinload(Game.teams))
            .where(Game.id == game_id)
        )
        return res.scalar_one_or_none()

    async def create(
        self,
        *,
        scheduled_at: datetime,
        team_ids: list[int],
        question_ids: list[int],
        created_by_user_id: int | None,
    ) -> Game:
        game = Game(scheduled_at=scheduled_at, status=GameStatus.draft, created_by_user_id=created_by_user_id)
        game.questions = [
            GameQuestion(question_id=q_id, order_index=idx) for idx, q_id in enumerate(question_ids)
        ]
        game.teams = [GameTeam(team_id=t_id) for t_id in team_ids]
        self.db.add(game)
        await self.db.commit()
        await self.db.refresh(game)
        return game

    async def update(
        self,
        game: Game,
        *,
        scheduled_at: datetime | None = None,
        team_ids: list[int] | None = None,
        question_ids: list[int] | None = None,
    ) -> Game:
        if scheduled_at is not None:
            game.scheduled_at = scheduled_at
        if team_ids is not None:
            game.teams = []
            await self.db.flush()
            game.teams = [GameTeam(team_id=t_id) for t_id in team_ids]
        if question_ids is not None:
            game.questions = []
            await self.db.flush()
            game.questions = [
                GameQuestion(question_id=q_id, order_index=idx) for idx, q_id in enumerate(question_ids)
            ]
        await self.db.commit()
        await self.db.refresh(game)
        return game

    async def delete(self, game_id: int) -> bool:
        res = await self.db.execute(delete(Game).where(Game.id == game_id))
        await self.db.commit()
        return bool(res.rowcount)

    async def start(self, game: Game) -> tuple[str, int]:
        if game.status != GameStatus.draft:
            raise ValueError("Game already started/finished")

        # Load ordered question ids
        res = await self.db.execute(
            select(GameQuestion).where(GameQuestion.game_id == game.id).order_by(GameQuestion.order_index)
        )
        gqs = list(res.scalars().all())
        question_ids = [gq.question_id for gq in gqs]
        if not question_ids:
            raise ValueError("Game has no questions")

        game.status = GameStatus.started
        await self.db.commit()

        realtime = GameRealtime()
        state = await realtime.start(game.id, question_ids)
        qid = state.current_question_id()
        if qid is None:
            raise RuntimeError("Failed to start game")
        return realtime.channel(game.id), qid

    async def next_question(self, game: Game) -> tuple[str, int | None]:
        if game.status != GameStatus.started:
            raise ValueError("Game is not started")

        realtime = GameRealtime()
        state = await realtime.next_question(game.id)
        if state is None:
            raise RuntimeError("Game state not found in Redis")

        if state.status == "finished":
            game.status = GameStatus.finished
            await self.db.commit()
            await self._finalize_game(game.id)
            return state.status, None

        return state.status, state.current_question_id()

    async def submit_answer(self, *, game_id: int, team_id: int, option_id: int) -> TeamAnswer:
        realtime = GameRealtime()
        state = await realtime.get_state(game_id)
        if state is None or state.status != "started":
            raise ValueError("Game is not active")

        question_id = state.current_question_id()
        if question_id is None:
            raise ValueError("No active question")

        # Validate that team participates in the game
        res = await self.db.execute(
            select(GameTeam).where(GameTeam.game_id == game_id, GameTeam.team_id == team_id)
        )
        game_team = res.scalar_one_or_none()
        if game_team is None:
            raise ValueError("Team is not part of this game")

        # Validate option belongs to current question
        opt = await self.db.get(QuestionOption, option_id)
        if opt is None or opt.question_id != question_id:
            raise ValueError("Option does not belong to current question")

        # Check timer: 30 seconds
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        elapsed_ms = max(0, now_ms - state.question_started_at_ms)
        within_time = elapsed_ms <= state.round_seconds * 1000
        if not within_time:
            raise ValueError("Answer time expired (30 seconds)")

        is_correct = bool(opt.is_correct)

        ans = TeamAnswer(
            game_id=game_id,
            team_id=team_id,
            question_id=question_id,
            option_id=option_id,
            is_correct=is_correct,
            within_time=True,
            elapsed_ms=elapsed_ms,
        )
        self.db.add(ans)

        if is_correct:
            game_team.points += 1
            team = await self.db.get(Team, team_id)
            if team is not None:
                team.total_points += 1

        await self.db.commit()
        await self.db.refresh(ans)
        return ans

    async def _finalize_game(self, game_id: int) -> None:
        # Update games_played/wins/losses per team based on points.
        res = await self.db.execute(select(GameTeam).where(GameTeam.game_id == game_id))
        rows = list(res.scalars().all())
        if not rows:
            return
        max_points = max(r.points for r in rows)
        winner_team_ids = {r.team_id for r in rows if r.points == max_points}

        for r in rows:
            team = await self.db.get(Team, r.team_id)
            if team is None:
                continue
            team.games_played += 1
            if r.team_id in winner_team_ids:
                team.wins += 1
            else:
                team.losses += 1
        await self.db.commit()
