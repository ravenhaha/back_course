"""init quiz schema

Revision ID: 0001_init_quiz
Revises:
Create Date: 2026-05-28

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001_init_quiz"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teams",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("total_points", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("games_played", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("wins", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("losses", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index("ix_teams_name", "teams", ["name"], unique=True)

    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("username", sa.String(length=50), nullable=False),
        sa.Column("password_hash", sa.String(length=128), nullable=False),
        sa.Column("password_salt", sa.String(length=64), nullable=False),
        sa.Column("role", sa.Enum("admin", "player", name="user_role"), nullable=False),
        sa.Column("full_name", sa.String(length=200), nullable=True),
        sa.Column("email", sa.String(length=200), nullable=True),
        sa.Column("team_id", sa.Integer(), sa.ForeignKey("teams.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_users_username", "users", ["username"], unique=True)
    op.create_index("ix_users_email", "users", ["email"], unique=True)
    op.create_index("ix_users_team_id", "users", ["team_id"])

    op.create_table(
        "auth_tokens",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("access_token_hash", sa.String(length=64), nullable=False),
        sa.Column("refresh_token_hash", sa.String(length=64), nullable=False),
        sa.Column("access_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refresh_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_auth_tokens_user_id", "auth_tokens", ["user_id"])
    op.create_index("ix_auth_tokens_access_token_hash", "auth_tokens", ["access_token_hash"], unique=True)
    op.create_index("ix_auth_tokens_refresh_token_hash", "auth_tokens", ["refresh_token_hash"], unique=True)
    op.create_index("ix_auth_tokens_access_expires_at", "auth_tokens", ["access_expires_at"])
    op.create_index("ix_auth_tokens_refresh_expires_at", "auth_tokens", ["refresh_expires_at"])

    op.create_table(
        "questions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("text", sa.Text(), nullable=False),
    )

    op.create_table(
        "question_options",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("question_id", sa.Integer(), sa.ForeignKey("questions.id"), nullable=False),
        sa.Column("text", sa.String(length=500), nullable=False),
        sa.Column("is_correct", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.create_index("ix_question_options_question_id", "question_options", ["question_id"])

    op.create_table(
        "games",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.Enum("draft", "started", "finished", name="game_status"), nullable=False, server_default="draft"),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_games_created_by_user_id", "games", ["created_by_user_id"])

    op.create_table(
        "game_questions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("game_id", sa.Integer(), sa.ForeignKey("games.id"), nullable=False),
        sa.Column("question_id", sa.Integer(), sa.ForeignKey("questions.id"), nullable=False),
        sa.Column("order_index", sa.Integer(), nullable=False),
        sa.UniqueConstraint("game_id", "order_index", name="uq_game_question_order"),
        sa.UniqueConstraint("game_id", "question_id", name="uq_game_question_question"),
    )
    op.create_index("ix_game_questions_game_id", "game_questions", ["game_id"])
    op.create_index("ix_game_questions_question_id", "game_questions", ["question_id"])

    op.create_table(
        "game_teams",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("game_id", sa.Integer(), sa.ForeignKey("games.id"), nullable=False),
        sa.Column("team_id", sa.Integer(), sa.ForeignKey("teams.id"), nullable=False),
        sa.Column("points", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint("game_id", "team_id", name="uq_game_team"),
    )
    op.create_index("ix_game_teams_game_id", "game_teams", ["game_id"])
    op.create_index("ix_game_teams_team_id", "game_teams", ["team_id"])

    op.create_table(
        "team_answers",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("game_id", sa.Integer(), sa.ForeignKey("games.id"), nullable=False),
        sa.Column("team_id", sa.Integer(), sa.ForeignKey("teams.id"), nullable=False),
        sa.Column("question_id", sa.Integer(), sa.ForeignKey("questions.id"), nullable=False),
        sa.Column("option_id", sa.Integer(), sa.ForeignKey("question_options.id"), nullable=False),
        sa.Column("is_correct", sa.Boolean(), nullable=False),
        sa.Column("within_time", sa.Boolean(), nullable=False),
        sa.Column("elapsed_ms", sa.Integer(), nullable=False),
        sa.Column("answered_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("game_id", "team_id", "question_id", name="uq_answer_once_per_question"),
    )
    op.create_index("ix_team_answers_game_id", "team_answers", ["game_id"])
    op.create_index("ix_team_answers_team_id", "team_answers", ["team_id"])
    op.create_index("ix_team_answers_question_id", "team_answers", ["question_id"])
    op.create_index("ix_team_answers_option_id", "team_answers", ["option_id"])


def downgrade() -> None:
    op.drop_table("team_answers")
    op.drop_table("game_teams")
    op.drop_table("game_questions")
    op.drop_table("games")
    op.drop_table("question_options")
    op.drop_table("questions")
    op.drop_table("auth_tokens")
    op.drop_index("ix_users_team_id", table_name="users")
    op.drop_index("ix_users_email", table_name="users")
    op.drop_index("ix_users_username", table_name="users")
    op.drop_table("users")
    op.drop_index("ix_teams_name", table_name="teams")
    op.drop_table("teams")

    # Clean up enums in Postgres
    op.execute("DROP TYPE IF EXISTS user_role")
    op.execute("DROP TYPE IF EXISTS game_status")

