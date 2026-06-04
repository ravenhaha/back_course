from __future__ import annotations

import asyncio
from datetime import datetime

import pytest
from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models.game import GameQuestion, GameTeam
from app.models.base import Base
from app.models.game import Game, GameStatus, TeamAnswer
from app.models.team import Team
from app.models.user import User, UserRole
from app.schemas.auth import ChangePasswordRequest, LoginRequest, RefreshRequest, RegisterRequest
from app.schemas.questions import QuestionCreate, QuestionUpdate
from app.schemas.teams import TeamCreate, TeamUpdate
from app.schemas.users import UserCreate, UserUpdate
from app.services.tokens import TokenService
from app.services.users import UserService
from app.services import redis_client
from app.services.game_realtime import GameRealtime
from app.services.games import GameService
from app.services.questions import QuestionService
from app.services.teams import TeamService


async def _register(client, username: str, password: str = "password123", role: str = "admin", **extra):
    payload = {"username": username, "password": password, "role": role, **extra}
    r = await client.post("/auth/register", json=payload)
    assert r.status_code == 201, r.text
    return r.json()


async def _login(client, username: str, password: str = "password123"):
    r = await client.post("/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return r.json()


def _headers(token: str):
    return {"Authorization": f"Bearer {token}"}


async def _admin_headers(client, username: str = "admin_edges"):
    await _register(client, username)
    tokens = await _login(client, username)
    return _headers(tokens["access_token"])


async def _create_team(client, headers, name: str = "Team X"):
    r = await client.post("/teams", headers=headers, json={"name": name})
    assert r.status_code == 201, r.text
    return r.json()


async def _create_question(client, headers, text: str = "Question?"):
    r = await client.post(
        "/questions",
        headers=headers,
        json={"text": text, "options": ["A", "B", "C"], "correct_option_index": 1},
    )
    assert r.status_code == 201, r.text
    return r.json()


async def _create_game(client, headers):
    team_a = await _create_team(client, headers, "Team A")
    team_b = await _create_team(client, headers, "Team B")
    q1 = await _create_question(client, headers, "Q1?")
    q2 = await _create_question(client, headers, "Q2?")
    r = await client.post(
        "/games",
        headers=headers,
        json={
            "scheduled_at": "2026-01-01T00:00:00Z",
            "team_ids": [team_a["id"], team_b["id"]],
            "question_ids": [q1["id"], q2["id"]],
        },
    )
    assert r.status_code == 201, r.text
    return r.json(), team_a, team_b, q1, q2


async def test_auth_error_paths_and_admin_guards(client):
    admin_h = await _admin_headers(client)

    r = await client.post("/auth/register", json={"username": "admin_edges", "password": "password123", "role": "admin"})
    assert r.status_code == 409

    r = await client.post("/auth/login", json={"username": "admin_edges", "password": "wrong-password"})
    assert r.status_code == 401

    r = await client.post("/auth/refresh", json={"refresh_token": "missing"})
    assert r.status_code == 401

    r = await client.post(
        "/auth/change-password",
        headers=admin_h,
        json={"current_password": "wrong-password", "new_password": "password456"},
    )
    assert r.status_code == 400

    r = await client.get("/auth/me")
    assert r.status_code == 401

    await _register(client, "player_edges", role="player")
    player_tokens = await _login(client, "player_edges")
    player_h = _headers(player_tokens["access_token"])

    r = await client.get("/users", headers=player_h)
    assert r.status_code == 403

    r = await client.get("/teams", headers={"Authorization": "Bearer bad-token"})
    assert r.status_code == 401


async def test_team_question_user_crud_edges(client):
    h = await _admin_headers(client, "admin_crud_edges")

    team = await _create_team(client, h, "Unique Team")
    r = await client.post("/teams", headers=h, json={"name": "Unique Team"})
    assert r.status_code == 409

    r = await client.get("/teams/999", headers=h)
    assert r.status_code == 404

    r = await client.patch(f"/teams/{team['id']}", headers=h, json={"name": "Unique Team Updated"})
    assert r.status_code == 200

    other = await _create_team(client, h, "Other Team")
    r = await client.patch(f"/teams/{other['id']}", headers=h, json={"name": "Unique Team Updated"})
    assert r.status_code == 409

    r = await client.delete("/teams/999", headers=h)
    assert r.status_code == 404

    r = await client.post(
        "/questions",
        headers=h,
        json={"text": "Bad?", "options": ["A", "B"], "correct_option_index": 5},
    )
    assert r.status_code == 400

    q = await _create_question(client, h, "Edge question?")
    r = await client.get(f"/questions/{q['id']}", headers=h)
    assert r.status_code == 200

    r = await client.get("/questions/999", headers=h)
    assert r.status_code == 404

    r = await client.patch(f"/questions/{q['id']}", headers=h, json={"correct_option_index": 99})
    assert r.status_code == 400

    r = await client.patch(f"/questions/{q['id']}", headers=h, json={"options": ["Only one"], "correct_option_index": 1})
    assert r.status_code == 400

    r = await client.delete("/questions/999", headers=h)
    assert r.status_code == 404

    r = await client.post("/users", headers=h, json={"username": "edge_user", "password": "password123", "role": "player"})
    assert r.status_code == 201
    user = r.json()

    r = await client.post("/users", headers=h, json={"username": "edge_user", "password": "password123", "role": "player"})
    assert r.status_code == 409

    r = await client.get("/users/999", headers=h)
    assert r.status_code == 404

    r = await client.patch(f"/users/{user['id']}", headers=h, json={"role": "admin", "email": "edge@example.com"})
    assert r.status_code == 200

    r = await client.patch("/users/999", headers=h, json={"full_name": "Nobody"})
    assert r.status_code == 404

    r = await client.delete("/users/999", headers=h)
    assert r.status_code == 404


async def test_game_results_and_error_paths(client):
    h = await _admin_headers(client, "admin_game_edges")
    game, team_a, team_b, q1, q2 = await _create_game(client, h)

    r = await client.get("/games", headers=h)
    assert r.status_code == 200

    r = await client.get(f"/games/{game['id']}", headers=h)
    assert r.status_code == 200

    r = await client.get("/games/999", headers=h)
    assert r.status_code == 404

    r = await client.get("/games/999/results", headers=h)
    assert r.status_code == 404

    r = await client.patch(
        f"/games/{game['id']}",
        headers=h,
        json={
            "scheduled_at": "2026-01-02T00:00:00Z",
            "team_ids": [team_a["id"], team_b["id"]],
            "question_ids": [q1["id"], q2["id"]],
        },
    )
    assert r.status_code == 200

    r = await client.patch("/games/999", headers=h, json={"scheduled_at": "2026-01-02T00:00:00Z"})
    assert r.status_code == 404

    r = await client.post("/games/999/start", headers=h)
    assert r.status_code == 404

    r = await client.get("/games/999/current-question", headers=h)
    assert r.status_code == 404

    r = await client.get(f"/games/{game['id']}/current-question", headers=h)
    assert r.status_code == 400

    r = await client.post(f"/games/{game['id']}/answer", headers=h, json={"team_id": team_a["id"], "option_id": q1["options"][0]["id"]})
    assert r.status_code == 400

    r = await client.post(f"/games/{game['id']}/start", headers=h)
    assert r.status_code == 200

    r = await client.post(f"/games/{game['id']}/start", headers=h)
    assert r.status_code == 400

    r = await client.post(f"/games/{game['id']}/answer", headers=h, json={"team_id": 999, "option_id": q1["options"][0]["id"]})
    assert r.status_code == 400

    r = await client.post(f"/games/{game['id']}/answer", headers=h, json={"team_id": team_a["id"], "option_id": q2["options"][0]["id"]})
    assert r.status_code == 400

    r = await client.post(f"/games/{game['id']}/answer", headers=h, json={"team_id": team_a["id"], "option_id": q1["options"][1]["id"]})
    assert r.status_code == 200

    r = await client.post(f"/games/{game['id']}/next-question", headers=h)
    assert r.status_code == 200

    r = await client.get(f"/games/{game['id']}/current-question", headers=h)
    assert r.status_code == 200
    assert r.json()["question"]["id"] == q2["id"]

    r = await client.post(f"/games/{game['id']}/next-question", headers=h)
    assert r.status_code == 200
    assert r.json()["status"] == "finished"

    r = await client.post(f"/games/{game['id']}/next-question", headers=h)
    assert r.status_code == 400

    r = await client.get(f"/games/{game['id']}/results", headers=h)
    assert r.status_code == 200
    assert r.json()["answers"]

    r = await client.delete(f"/games/{game['id']}", headers=h)
    assert r.status_code == 204

    r = await client.delete("/games/999", headers=h)
    assert r.status_code == 404


async def test_game_realtime_state_edges(client):
    h = await _admin_headers(client, "admin_realtime_edges")
    game, *_ = await _create_game(client, h)
    realtime = GameRealtime()

    assert realtime.channel(game["id"]) == f"quiz:game:{game['id']}"
    assert await realtime.get_state(game["id"]) is None
    assert await realtime.next_question(game["id"]) is None


async def test_root_redirect(client):
    r = await client.get("/", follow_redirects=False)
    assert r.status_code in {307, 308}
    assert r.headers["location"] == "/ui/"


async def test_seed_demo_data_and_direct_services(monkeypatch):
    import app.seed_demo as seed_demo

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    monkeypatch.setattr(seed_demo, "AsyncSessionLocal", session_factory)

    await seed_demo.seed_demo_data()
    await seed_demo.seed_demo_data()

    async with session_factory() as db:
        users_count = int((await db.execute(select(func.count(User.id)))).scalar_one())
        teams_count = int((await db.execute(select(func.count(Team.id)))).scalar_one())
        finished_games_count = int(
            (await db.execute(select(func.count(Game.id)).where(Game.status == GameStatus.finished))).scalar_one()
        )
        draft_games_count = int(
            (await db.execute(select(func.count(Game.id)).where(Game.status == GameStatus.draft))).scalar_one()
        )
        answers_count = int((await db.execute(select(func.count(TeamAnswer.id)))).scalar_one())

        assert users_count == 3
        assert teams_count == 2
        assert finished_games_count == 1
        assert draft_games_count == 1
        assert answers_count == 6

        users = UserService(db)
        assert await users.get_by_id(999) is None
        assert await users.authenticate("missing", "password123") is None
        assert await users.authenticate("admin", "bad") is None
        admin = await users.authenticate("admin", "admin123")
        assert admin is not None
        assert len(await users.list()) == 3

        tokens = TokenService(db)
        access, refresh = await tokens.issue_pair(admin.id)
        assert await tokens.get_user_by_access_token(access) == admin
        assert await tokens.refresh_pair("missing") is None
        assert await tokens.revoke_pair_by_refresh(refresh) is True
        assert await tokens.revoke_pair_by_refresh(refresh) is False

    await engine.dispose()


async def test_direct_auth_router_handlers():
    import app.routers.auth as auth_router

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with session_factory() as db:
        created = await auth_router.register(
            RegisterRequest(username="direct_admin", password="password123", role=UserRole.admin),
            db,
        )
        assert created.username == "direct_admin"

        with pytest.raises(HTTPException) as exc:
            await auth_router.register(
                RegisterRequest(username="direct_admin", password="password123", role=UserRole.admin),
                db,
            )
        assert exc.value.status_code == 409

        tokens = await auth_router.login(LoginRequest(username="direct_admin", password="password123"), db)
        assert tokens.access_token

        with pytest.raises(HTTPException) as exc:
            await auth_router.login(LoginRequest(username="direct_admin", password="bad-password"), db)
        assert exc.value.status_code == 401

        refreshed = await auth_router.refresh(RefreshRequest(refresh_token=tokens.refresh_token), db)
        assert refreshed.access_token != tokens.access_token

        with pytest.raises(HTTPException) as exc:
            await auth_router.refresh(RefreshRequest(refresh_token="missing"), db)
        assert exc.value.status_code == 401

        user = await TokenService(db).get_user_by_access_token(refreshed.access_token)
        assert user is not None
        assert await auth_router.me(user) == user

        with pytest.raises(HTTPException) as exc:
            await auth_router.change_password(
                ChangePasswordRequest(current_password="bad-password", new_password="password456"),
                user,
                db,
            )
        assert exc.value.status_code == 400

        await auth_router.change_password(
            ChangePasswordRequest(current_password="password123", new_password="password456"),
            user,
            db,
        )

    await engine.dispose()


async def test_direct_game_results_and_current_question_branches(monkeypatch):
    import app.routers.games as games_router

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with session_factory() as db:
        team = await TeamService(db).create("Direct Team")
        q = await QuestionService(db).create(text="Direct Q?", options=["A", "B"], correct_option_index=0)
        game = await GameService(db).create(
            scheduled_at=datetime(2026, 1, 1),
            team_ids=[team.id],
            question_ids=[q.id],
            created_by_user_id=None,
        )
        game.status = GameStatus.finished
        await db.commit()

        db.add(
            TeamAnswer(
                game_id=game.id,
                team_id=team.id,
                question_id=q.id,
                option_id=q.options[0].id,
                is_correct=True,
                within_time=True,
                elapsed_ms=1000,
            )
        )
        game_team = (await db.execute(select(GameTeam).where(GameTeam.game_id == game.id))).scalar_one()
        game_team.points = 1
        await db.commit()

        results = await games_router.get_game_results(db, object(), game.id)
        assert results.teams[0].points == 1
        assert results.answers[0].is_correct is True

        with pytest.raises(HTTPException) as exc:
            await games_router.get_game_results(db, object(), 999)
        assert exc.value.status_code == 404

        class FinishedState:
            status = "finished"
            question_started_at_ms = None
            round_seconds = 30

            def current_question_id(self):
                return None

        class StartedState:
            status = "started"
            question_started_at_ms = 0
            round_seconds = 30

            def current_question_id(self):
                return q.id

        class MissingQuestionState(StartedState):
            def current_question_id(self):
                return 999

        class FakeRealtime:
            state = FinishedState()

            async def get_state(self, game_id):
                return self.state

        fake_realtime = FakeRealtime()
        monkeypatch.setattr(games_router, "GameRealtime", lambda: fake_realtime)

        current = await games_router.get_current_question(db, object(), game.id)
        assert current.status == "finished"
        assert current.question is None

        fake_realtime.state = StartedState()
        current = await games_router.get_current_question(db, object(), game.id)
        assert current.question is not None
        assert current.question.id == q.id

        fake_realtime.state = MissingQuestionState()
        with pytest.raises(HTTPException) as exc:
            await games_router.get_current_question(db, object(), game.id)
        assert exc.value.status_code == 404

    await engine.dispose()


async def test_direct_game_service_rare_errors(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with session_factory() as db:
        team = await TeamService(db).create("Rare Team")
        q = await QuestionService(db).create(text="Rare Q?", options=["A", "B"], correct_option_index=0)
        service = GameService(db)

        empty_game = Game(scheduled_at=datetime(2026, 1, 1), status=GameStatus.draft)
        db.add(empty_game)
        await db.commit()
        await db.refresh(empty_game)

        with pytest.raises(ValueError, match="no questions"):
            await service.start(empty_game)

        game = await service.create(
            scheduled_at=datetime(2026, 1, 1),
            team_ids=[team.id],
            question_ids=[q.id],
            created_by_user_id=None,
        )

        class NoCurrentState:
            status = "started"
            question_started_at_ms = 0
            round_seconds = 30

            def current_question_id(self):
                return None

        class ExpiredState:
            status = "started"
            question_started_at_ms = -10**12
            round_seconds = 30

            def current_question_id(self):
                return q.id

        class FakeRealtime:
            state = NoCurrentState()

            async def get_state(self, game_id):
                return self.state

            async def next_question(self, game_id):
                return None

        fake = FakeRealtime()
        monkeypatch.setattr("app.services.games.GameRealtime", lambda: fake)

        game.status = GameStatus.started
        await db.commit()

        with pytest.raises(RuntimeError, match="state not found"):
            await service.next_question(game)

        with pytest.raises(ValueError, match="No active question"):
            await service.submit_answer(game_id=game.id, team_id=team.id, option_id=q.options[0].id)

        fake.state = ExpiredState()
        with pytest.raises(ValueError, match="expired"):
            await service.submit_answer(game_id=game.id, team_id=team.id, option_id=q.options[0].id)

        await service._finalize_game(999)

    await engine.dispose()


async def test_direct_crud_router_handlers():
    import app.routers.questions as questions_router
    import app.routers.teams as teams_router
    import app.routers.users as users_router

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with session_factory() as db:
        admin = await UserService(db).create(username="router_admin", password="password123", role=UserRole.admin)

        teams = await teams_router.list_teams(db, admin)
        assert teams == []

        team = await teams_router.create_team(db, admin, TeamCreate(name="Router Team"))
        assert team.name == "Router Team"

        with pytest.raises(HTTPException) as exc:
            await teams_router.create_team(db, admin, TeamCreate(name="Router Team"))
        assert exc.value.status_code == 409

        assert await teams_router.get_team(db, admin, team.id) == team

        with pytest.raises(HTTPException) as exc:
            await teams_router.get_team(db, admin, 999)
        assert exc.value.status_code == 404

        updated_team = await teams_router.update_team(db, admin, team.id, TeamUpdate(name="Router Team Updated"))
        assert updated_team.name == "Router Team Updated"

        other_team = await teams_router.create_team(db, admin, TeamCreate(name="Other Router Team"))
        with pytest.raises(HTTPException) as exc:
            await teams_router.update_team(db, admin, other_team.id, TeamUpdate(name="Router Team Updated"))
        assert exc.value.status_code == 409

        with pytest.raises(HTTPException) as exc:
            await teams_router.update_team(db, admin, 999, TeamUpdate(name="Missing"))
        assert exc.value.status_code == 404

        await teams_router.delete_team(db, admin, other_team.id)
        with pytest.raises(HTTPException) as exc:
            await teams_router.delete_team(db, admin, 999)
        assert exc.value.status_code == 404

        questions = await questions_router.list_questions(db, admin)
        assert questions == []

        with pytest.raises(HTTPException) as exc:
            await questions_router.create_question(
                db,
                admin,
                QuestionCreate(text="Bad?", options=["A", "B"], correct_option_index=3),
            )
        assert exc.value.status_code == 400

        question = await questions_router.create_question(
            db,
            admin,
            QuestionCreate(text="Router Q?", options=["A", "B"], correct_option_index=0),
        )
        assert question.text == "Router Q?"

        assert (await questions_router.get_question(db, admin, question.id)).id == question.id

        with pytest.raises(HTTPException) as exc:
            await questions_router.get_question(db, admin, 999)
        assert exc.value.status_code == 404

        updated_question = await questions_router.update_question(
            db,
            admin,
            question.id,
            QuestionUpdate(text="Router Q updated?", options=["Yes", "No"], correct_option_index=1),
        )
        assert updated_question.text == "Router Q updated?"

        with pytest.raises(HTTPException) as exc:
            await questions_router.update_question(db, admin, 999, QuestionUpdate(text="Missing?"))
        assert exc.value.status_code == 404

        with pytest.raises(HTTPException) as exc:
            await questions_router.update_question(
                db,
                admin,
                question.id,
                QuestionUpdate(options=["Only"], correct_option_index=2),
            )
        assert exc.value.status_code == 400

        await questions_router.delete_question(db, admin, question.id)
        with pytest.raises(HTTPException) as exc:
            await questions_router.delete_question(db, admin, 999)
        assert exc.value.status_code == 404

        users = await users_router.list_users(db, admin)
        assert len(users) == 1

        created_user = await users_router.create_user(
            db,
            admin,
            UserCreate(username="router_player", password="password123", role=UserRole.player, team_id=team.id),
        )
        assert created_user.username == "router_player"

        with pytest.raises(HTTPException) as exc:
            await users_router.create_user(
                db,
                admin,
                UserCreate(username="router_player", password="password123", role=UserRole.player),
            )
        assert exc.value.status_code == 409

        assert (await users_router.get_user(db, admin, created_user.id)).id == created_user.id

        with pytest.raises(HTTPException) as exc:
            await users_router.get_user(db, admin, 999)
        assert exc.value.status_code == 404

        updated_user = await users_router.update_user(
            db,
            admin,
            created_user.id,
            UserUpdate(full_name="Router Player", email="router@example.com", team_id=team.id),
        )
        assert updated_user.full_name == "Router Player"

        with pytest.raises(HTTPException) as exc:
            await users_router.update_user(db, admin, 999, UserUpdate(full_name="Missing"))
        assert exc.value.status_code == 404

        await users_router.delete_user(db, admin, created_user.id)
        with pytest.raises(HTTPException) as exc:
            await users_router.delete_user(db, admin, 999)
        assert exc.value.status_code == 404

    await engine.dispose()


async def _reader_with(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


class FakeWriter:
    def __init__(self) -> None:
        self.data = b""
        self.closed = False

    def write(self, data: bytes) -> None:
        self.data += data

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


async def test_redis_resp_parser_success_and_errors():
    assert redis_client._encode_command("PING") == b"*1\r\n$4\r\nPING\r\n"

    reader = await _reader_with(b"+OK\r\n")
    assert await redis_client._read_resp(reader) == "OK"

    reader = await _reader_with(b":12\r\n")
    assert await redis_client._read_resp(reader) == 12

    reader = await _reader_with(b"$5\r\nhello\r\n")
    assert await redis_client._read_resp(reader) == "hello"

    reader = await _reader_with(b"$-1\r\n")
    assert await redis_client._read_resp(reader) is None

    reader = await _reader_with(b"*3\r\n+message\r\n+chan\r\n$7\r\npayload\r\n")
    assert await redis_client._read_resp(reader) == ["message", "chan", "payload"]

    reader = await _reader_with(b"-ERR nope\r\n")
    with pytest.raises(redis_client.RedisError, match="ERR nope"):
        await redis_client._read_resp(reader)

    reader = await _reader_with(b"?bad\r\n")
    with pytest.raises(redis_client.RedisError, match="Unknown RESP prefix"):
        await redis_client._read_resp(reader)

    reader = await _reader_with(b"+OK\n")
    with pytest.raises(redis_client.RedisError, match="no CRLF"):
        await redis_client._read_resp(reader)

    reader = await _reader_with(b"")
    with pytest.raises(redis_client.RedisError, match="Redis connection closed"):
        await redis_client._read_line(reader)


async def test_redis_client_commands(monkeypatch):
    async def connect_with(payload: bytes):
        writer = FakeWriter()
        return await _reader_with(payload), writer

    client = redis_client.RedisClient(host="example", port=1234)

    monkeypatch.setattr(client, "_connect", lambda: connect_with(b"+PONG\r\n"))
    assert await client.ping() is True

    monkeypatch.setattr(client, "_connect", lambda: connect_with(b"$5\r\nvalue\r\n"))
    assert await client.get("key") == "value"

    monkeypatch.setattr(client, "_connect", lambda: connect_with(b"+OK\r\n"))
    assert await client.set("key", "value") is True

    monkeypatch.setattr(client, "_connect", lambda: connect_with(b"+OK\r\n"))
    assert await client.set("key", "value", ex_seconds=10) is True

    monkeypatch.setattr(client, "_connect", lambda: connect_with(b":2\r\n"))
    assert await client.publish("chan", "payload") == 2

    payload = b"*3\r\n+subscribe\r\n+chan\r\n:1\r\n*3\r\n+message\r\n+chan\r\n$7\r\npayload\r\n"
    monkeypatch.setattr(client, "_connect", lambda: connect_with(payload))
    sub = client.subscribe("chan")
    msg = await anext(sub)
    assert msg == redis_client.PubSubMessage(channel="chan", payload="payload")
    await sub.aclose()
