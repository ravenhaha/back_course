from __future__ import annotations

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services.game_realtime import GameRealtime
from app.services.redis_client import RedisClient

router = APIRouter(tags=["realtime"])


@router.websocket("/ws/games/{game_id}")
async def game_ws(websocket: WebSocket, game_id: int):
    """
    Simple realtime channel for a game.
    The server subscribes to Redis Pub/Sub and forwards JSON payloads to the client.
    """
    await websocket.accept()
    realtime = GameRealtime()
    channel = realtime.channel(game_id)
    redis = RedisClient()
    try:
        async for msg in redis.subscribe(channel):
            await websocket.send_text(msg.payload)
    except WebSocketDisconnect:
        return

