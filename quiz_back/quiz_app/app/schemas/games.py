from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from app.schemas.questions import QuestionOut


class GameCreate(BaseModel):
    scheduled_at: datetime
    team_ids: list[int] = Field(min_length=1)
    question_ids: list[int] = Field(min_length=1)


class GameUpdate(BaseModel):
    scheduled_at: datetime | None = None
    team_ids: list[int] | None = None
    question_ids: list[int] | None = None


class GameOut(BaseModel):
    id: int
    scheduled_at: datetime
    status: str

    model_config = {"from_attributes": True}


class StartGameResponse(BaseModel):
    game_id: int
    redis_channel: str
    current_question_id: int
    round_seconds: int = 30


class NextQuestionResponse(BaseModel):
    game_id: int
    current_question_id: int | None
    status: str


class SubmitAnswerRequest(BaseModel):
    team_id: int
    option_id: int


class SubmitAnswerResponse(BaseModel):
    game_id: int
    team_id: int
    question_id: int
    is_correct: bool
    within_time: bool
    elapsed_ms: int


class CurrentQuestionResponse(BaseModel):
    game_id: int
    status: str
    current_question_id: int | None
    question_started_at_ms: int | None
    round_seconds: int = 30
    server_now_ms: int
    time_left_ms: int
    question: QuestionOut | None = None
