from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import AsyncIterator

from app.core.config import settings


class RedisError(RuntimeError):
    pass


def _encode_command(*parts: str) -> bytes:
    out = [f"*{len(parts)}\r\n".encode("utf-8")]
    for p in parts:
        b = p.encode("utf-8")
        out.append(f"${len(b)}\r\n".encode("utf-8"))
        out.append(b)
        out.append(b"\r\n")
    return b"".join(out)


async def _read_line(reader: asyncio.StreamReader) -> bytes:
    line = await reader.readline()
    if not line:
        raise RedisError("Redis connection closed")
    if not line.endswith(b"\r\n"):
        raise RedisError("Invalid Redis response (no CRLF)")
    return line[:-2]


async def _read_resp(reader: asyncio.StreamReader):
    prefix = await reader.readexactly(1)
    if prefix == b"+":
        return (await _read_line(reader)).decode("utf-8")
    if prefix == b"-":
        msg = (await _read_line(reader)).decode("utf-8", errors="replace")
        raise RedisError(msg)
    if prefix == b":":
        return int((await _read_line(reader)).decode("utf-8"))
    if prefix == b"$":
        n = int((await _read_line(reader)).decode("utf-8"))
        if n == -1:
            return None
        data = await reader.readexactly(n + 2)
        if not data.endswith(b"\r\n"):
            raise RedisError("Invalid bulk string")
        return data[:-2].decode("utf-8")
    if prefix == b"*":
        n = int((await _read_line(reader)).decode("utf-8"))
        if n == -1:
            return None
        arr = []
        for _ in range(n):
            arr.append(await _read_resp(reader))
        return arr
    raise RedisError(f"Unknown RESP prefix: {prefix!r}")


@dataclass(frozen=True)
class PubSubMessage:
    channel: str
    payload: str


class RedisClient:
    def __init__(self, host: str | None = None, port: int | None = None) -> None:
        self.host = host or settings.redis_host
        self.port = port or settings.redis_port

    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_connection(self.host, self.port)

    async def ping(self) -> bool:
        reader, writer = await self._connect()
        try:
            writer.write(_encode_command("PING"))
            await writer.drain()
            resp = await _read_resp(reader)
            return resp == "PONG"
        finally:
            writer.close()
            await writer.wait_closed()

    async def get(self, key: str) -> str | None:
        reader, writer = await self._connect()
        try:
            writer.write(_encode_command("GET", key))
            await writer.drain()
            return await _read_resp(reader)
        finally:
            writer.close()
            await writer.wait_closed()

    async def set(self, key: str, value: str, *, ex_seconds: int | None = None) -> bool:
        reader, writer = await self._connect()
        try:
            if ex_seconds is None:
                writer.write(_encode_command("SET", key, value))
            else:
                writer.write(_encode_command("SET", key, value, "EX", str(ex_seconds)))
            await writer.drain()
            resp = await _read_resp(reader)
            return resp == "OK"
        finally:
            writer.close()
            await writer.wait_closed()

    async def publish(self, channel: str, message: str) -> int:
        reader, writer = await self._connect()
        try:
            writer.write(_encode_command("PUBLISH", channel, message))
            await writer.drain()
            resp = await _read_resp(reader)
            return int(resp)
        finally:
            writer.close()
            await writer.wait_closed()

    async def subscribe(self, channel: str) -> AsyncIterator[PubSubMessage]:
        """
        Async iterator of published messages for a channel.
        Uses Redis SUBSCRIBE and yields only ["message", <channel>, <payload>] events.
        """
        reader, writer = await self._connect()
        writer.write(_encode_command("SUBSCRIBE", channel))
        await writer.drain()

        # First response is subscribe confirmation: ["subscribe", channel, 1]
        await _read_resp(reader)

        try:
            while True:
                resp = await _read_resp(reader)
                if (
                    isinstance(resp, list)
                    and len(resp) == 3
                    and resp[0] == "message"
                    and isinstance(resp[1], str)
                    and isinstance(resp[2], str)
                ):
                    yield PubSubMessage(channel=resp[1], payload=resp[2])
        finally:
            writer.close()
            await writer.wait_closed()

