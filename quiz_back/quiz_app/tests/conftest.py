import pytest
from httpx import ASGITransport, AsyncClient
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core import database as database_module
from app.main import app as fastapi_app
from app.models.base import Base
import app.models as _models  # noqa: F401
from tests.fakes import FakeRedis
import asyncio


@pytest.fixture()
async def client(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async def override_get_db():
        async with session_factory() as session:
            yield session

    fastapi_app.dependency_overrides[database_module.get_db] = override_get_db

    # Fake Redis (so /games/start works without a real redis server in unit tests)
    fake_redis = FakeRedis()
    import app.services.redis_client as redis_client_module
    import app.services.game_realtime as game_realtime_module
    import app.routers.ws as ws_router_module

    monkeypatch.setattr(redis_client_module, "RedisClient", lambda *args, **kwargs: fake_redis)
    monkeypatch.setattr(game_realtime_module, "RedisClient", lambda *args, **kwargs: fake_redis)
    monkeypatch.setattr(ws_router_module, "RedisClient", lambda *args, **kwargs: fake_redis)

    transport = ASGITransport(app=fastapi_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    fastapi_app.dependency_overrides.clear()
    await engine.dispose()


@pytest.fixture()
def sync_client(monkeypatch):
    """
    Sync client is used for WebSocket tests (TestClient.websocket_connect).
    """
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)

    async def init_db():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    asyncio.run(init_db())

    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async def override_get_db():
        async with session_factory() as session:
            yield session

    fastapi_app.dependency_overrides[database_module.get_db] = override_get_db

    fake_redis = FakeRedis()
    import app.services.redis_client as redis_client_module
    import app.services.game_realtime as game_realtime_module
    import app.routers.ws as ws_router_module

    monkeypatch.setattr(redis_client_module, "RedisClient", lambda *args, **kwargs: fake_redis)
    monkeypatch.setattr(game_realtime_module, "RedisClient", lambda *args, **kwargs: fake_redis)
    monkeypatch.setattr(ws_router_module, "RedisClient", lambda *args, **kwargs: fake_redis)

    with TestClient(fastapi_app) as tc:
        yield tc

    fastapi_app.dependency_overrides.clear()
    asyncio.run(engine.dispose())
