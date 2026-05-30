from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

from app.services.redis_client import RedisClient


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


@dataclass
class GameState:
    status: str
    question_ids: list[int]
    current_index: int
    question_started_at_ms: int
    round_seconds: int = 30

    def current_question_id(self) -> int | None:
        if 0 <= self.current_index < len(self.question_ids):
            return self.question_ids[self.current_index]
        return None

    def to_json(self) -> str:
        return json.dumps(
            {
                "status": self.status,
                "question_ids": self.question_ids,
                "current_index": self.current_index,
                "question_started_at_ms": self.question_started_at_ms,
                "round_seconds": self.round_seconds,
            },
            ensure_ascii=True,
        )

    @staticmethod
    def from_json(raw: str) -> "GameState":
        obj = json.loads(raw)
        return GameState(
            status=str(obj["status"]),
            question_ids=[int(x) for x in obj["question_ids"]],
            current_index=int(obj["current_index"]),
            question_started_at_ms=int(obj["question_started_at_ms"]),
            round_seconds=int(obj.get("round_seconds", 30)),
        )


class GameRealtime:
    def __init__(self, redis: RedisClient | None = None) -> None:
        self.redis = redis or RedisClient()

    @staticmethod
    def channel(game_id: int) -> str:
        return f"quiz:game:{game_id}"

    @staticmethod
    def state_key(game_id: int) -> str:
        return f"quiz:game:{game_id}:state"

    async def set_state(self, game_id: int, state: GameState, *, ex_seconds: int = 24 * 3600) -> None:
        ok = await self.redis.set(self.state_key(game_id), state.to_json(), ex_seconds=ex_seconds)
        if not ok:
            raise RuntimeError("Redis SET failed")

    async def get_state(self, game_id: int) -> GameState | None:
        raw = await self.redis.get(self.state_key(game_id))
        if raw is None:
            return None
        return GameState.from_json(raw)

    async def start(self, game_id: int, question_ids: list[int]) -> GameState:
        state = GameState(
            status="started",
            question_ids=question_ids,
            current_index=0,
            question_started_at_ms=_now_ms(),
            round_seconds=30,
        )
        await self.set_state(game_id, state)
        await self.publish_question(game_id, state.current_question_id())
        return state

    async def publish_question(self, game_id: int, question_id: int | None) -> None:
        payload = json.dumps(
            {"type": "question", "game_id": game_id, "question_id": question_id},
            ensure_ascii=True,
        )
        await self.redis.publish(self.channel(game_id), payload)

    async def next_question(self, game_id: int) -> GameState | None:
        state = await self.get_state(game_id)
        if state is None or state.status != "started":
            return None
        state.current_index += 1
        if state.current_index >= len(state.question_ids):
            state.status = "finished"
            await self.set_state(game_id, state)
            await self.redis.publish(self.channel(game_id), json.dumps({"type": "finished", "game_id": game_id}))
            return state
        state.question_started_at_ms = _now_ms()
        await self.set_state(game_id, state)
        await self.publish_question(game_id, state.current_question_id())
        return state

