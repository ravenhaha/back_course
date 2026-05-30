from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import AdminUser, CurrentUser
from app.schemas.games import (
    CurrentQuestionResponse,
    GameCreate,
    GameOut,
    GameUpdate,
    NextQuestionResponse,
    StartGameResponse,
    SubmitAnswerRequest,
    SubmitAnswerResponse,
)
from app.services.games import GameService
from app.services.game_realtime import GameRealtime
from app.services.questions import QuestionService

router = APIRouter(prefix="/games", tags=["games"])
DbDep = Annotated[AsyncSession, Depends(get_db)]


@router.get("", response_model=list[GameOut])
async def list_games(db: DbDep, _: AdminUser, offset: int = 0, limit: int = 100):
    return await GameService(db).list(offset=offset, limit=limit)


@router.post("", response_model=GameOut, status_code=201)
async def create_game(db: DbDep, admin: AdminUser, data: GameCreate):
    return await GameService(db).create(
        scheduled_at=data.scheduled_at,
        team_ids=data.team_ids,
        question_ids=data.question_ids,
        created_by_user_id=admin.id,
    )


@router.get("/{game_id}", response_model=GameOut)
async def get_game(db: DbDep, _: AdminUser, game_id: int):
    game = await GameService(db).get(game_id)
    if game is None:
        raise HTTPException(status_code=404, detail="Game not found")
    return game


@router.patch("/{game_id}", response_model=GameOut)
async def update_game(db: DbDep, _: AdminUser, game_id: int, data: GameUpdate):
    gs = GameService(db)
    game = await gs.get(game_id)
    if game is None:
        raise HTTPException(status_code=404, detail="Game not found")
    return await gs.update(
        game,
        scheduled_at=data.scheduled_at,
        team_ids=data.team_ids,
        question_ids=data.question_ids,
    )


@router.delete("/{game_id}", status_code=204)
async def delete_game(db: DbDep, _: AdminUser, game_id: int):
    ok = await GameService(db).delete(game_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Game not found")


@router.post("/{game_id}/start", response_model=StartGameResponse)
async def start_game(db: DbDep, _: AdminUser, game_id: int):
    gs = GameService(db)
    game = await gs.get(game_id)
    if game is None:
        raise HTTPException(status_code=404, detail="Game not found")
    try:
        channel, qid = await gs.start(game)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return StartGameResponse(game_id=game_id, redis_channel=channel, current_question_id=qid)


@router.post("/{game_id}/next-question", response_model=NextQuestionResponse)
async def next_question(db: DbDep, _: AdminUser, game_id: int):
    gs = GameService(db)
    game = await gs.get(game_id)
    if game is None:
        raise HTTPException(status_code=404, detail="Game not found")
    try:
        status_str, qid = await gs.next_question(game)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return NextQuestionResponse(game_id=game_id, current_question_id=qid, status=status_str)


@router.post("/{game_id}/answer", response_model=SubmitAnswerResponse)
async def submit_answer(db: DbDep, _: CurrentUser, game_id: int, data: SubmitAnswerRequest):
    # Player auth is required, but we allow any authenticated user to submit (simple).
    # Team membership is checked in the service.
    gs = GameService(db)
    try:
        ans = await gs.submit_answer(game_id=game_id, team_id=data.team_id, option_id=data.option_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        # unique constraint => already answered
        raise HTTPException(status_code=409, detail="Answer already submitted for this question") from e
    return SubmitAnswerResponse(
        game_id=ans.game_id,
        team_id=ans.team_id,
        question_id=ans.question_id,
        is_correct=ans.is_correct,
        within_time=ans.within_time,
        elapsed_ms=ans.elapsed_ms,
    )


@router.get("/{game_id}/current-question", response_model=CurrentQuestionResponse)
async def get_current_question(db: DbDep, _: CurrentUser, game_id: int):
    # Validate game exists in DB (for a clean 404 instead of "redis state missing").
    game = await GameService(db).get(game_id)
    if game is None:
        raise HTTPException(status_code=404, detail="Game not found")

    realtime = GameRealtime()
    state = await realtime.get_state(game_id)
    if state is None:
        raise HTTPException(status_code=400, detail="Game is not active")

    qid = state.current_question_id()
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    if state.status != "started" or qid is None:
        return CurrentQuestionResponse(
            game_id=game_id,
            status=state.status,
            current_question_id=qid,
            question_started_at_ms=state.question_started_at_ms,
            round_seconds=state.round_seconds,
            server_now_ms=now_ms,
            time_left_ms=0,
            question=None,
        )

    elapsed_ms = max(0, now_ms - state.question_started_at_ms)
    time_left_ms = max(0, (state.round_seconds * 1000) - elapsed_ms)

    q = await QuestionService(db).get(qid)
    if q is None:
        raise HTTPException(status_code=404, detail="Question not found")

    return CurrentQuestionResponse(
        game_id=game_id,
        status=state.status,
        current_question_id=qid,
        question_started_at_ms=state.question_started_at_ms,
        round_seconds=state.round_seconds,
        server_now_ms=now_ms,
        time_left_ms=time_left_ms,
        question=q,
    )
