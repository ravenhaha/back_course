from __future__ import annotations

import enum
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class GameStatus(str, enum.Enum):
    draft = "draft"
    started = "started"
    finished = "finished"


class Game(Base):
    __tablename__ = "games"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[GameStatus] = mapped_column(Enum(GameStatus, name="game_status"), nullable=False, default=GameStatus.draft)

    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    questions = relationship("GameQuestion", back_populates="game", cascade="all, delete-orphan")
    teams = relationship("GameTeam", back_populates="game", cascade="all, delete-orphan")


class GameQuestion(Base):
    __tablename__ = "game_questions"
    __table_args__ = (
        UniqueConstraint("game_id", "order_index", name="uq_game_question_order"),
        UniqueConstraint("game_id", "question_id", name="uq_game_question_question"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.id"), index=True, nullable=False)
    question_id: Mapped[int] = mapped_column(ForeignKey("questions.id"), index=True, nullable=False)
    order_index: Mapped[int] = mapped_column(Integer, nullable=False)

    game = relationship("Game", back_populates="questions")
    question = relationship("Question")


class GameTeam(Base):
    __tablename__ = "game_teams"
    __table_args__ = (UniqueConstraint("game_id", "team_id", name="uq_game_team"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.id"), index=True, nullable=False)
    team_id: Mapped[int] = mapped_column(ForeignKey("teams.id"), index=True, nullable=False)
    points: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    game = relationship("Game", back_populates="teams")
    team = relationship("Team")


class TeamAnswer(Base):
    __tablename__ = "team_answers"
    __table_args__ = (
        UniqueConstraint("game_id", "team_id", "question_id", name="uq_answer_once_per_question"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.id"), index=True, nullable=False)
    team_id: Mapped[int] = mapped_column(ForeignKey("teams.id"), index=True, nullable=False)
    question_id: Mapped[int] = mapped_column(ForeignKey("questions.id"), index=True, nullable=False)
    option_id: Mapped[int] = mapped_column(ForeignKey("question_options.id"), index=True, nullable=False)

    is_correct: Mapped[bool] = mapped_column(Boolean, nullable=False)
    within_time: Mapped[bool] = mapped_column(Boolean, nullable=False)
    elapsed_ms: Mapped[int] = mapped_column(Integer, nullable=False)

    answered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

