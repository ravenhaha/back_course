from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import AsyncIterator


@dataclass(frozen=True)
class FakePubSubMessage:
    channel: str
    payload: str


class FakeRedis:
    def __init__(self) -> None:
        self._kv: dict[str, str] = {}
        self._channels: dict[str, list[str]] = defaultdict(list)
        self._waiters: dict[str, list[asyncio.Event]] = defaultdict(list)

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self._kv.get(key)

    async def set(self, key: str, value: str, *, ex_seconds: int | None = None) -> bool:
        self._kv[key] = value
        return True

    async def publish(self, channel: str, message: str) -> int:
        self._channels[channel].append(message)
        for ev in self._waiters[channel]:
            ev.set()
        self._waiters[channel].clear()
        return 1

    async def subscribe(self, channel: str) -> AsyncIterator[FakePubSubMessage]:
        idx = 0
        while True:
            if idx < len(self._channels[channel]):
                payload = self._channels[channel][idx]
                idx += 1
                yield FakePubSubMessage(channel=channel, payload=payload)
                continue
            ev = asyncio.Event()
            self._waiters[channel].append(ev)
            await ev.wait()

