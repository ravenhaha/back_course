import json
from typing import Any, Callable

import redis
from fastapi.encoders import jsonable_encoder

REDIS_URL = "redis://localhost:6379/0"
DEFAULT_TTL = 60  # секунд

_client: redis.Redis | None = None


def get_client() -> redis.Redis | None:
    global _client
    if _client is None:
        try:
            _client = redis.from_url(REDIS_URL, decode_responses=True)
            _client.ping()
        except redis.RedisError:
            _client = None
    return _client


def cache_or_compute(key: str, compute_fn: Callable[[], Any], ttl: int = DEFAULT_TTL) -> Any:
    client = get_client()
    if client is None:
        return compute_fn()
    try:
        cached = client.get(key)
        if cached is not None:
            return json.loads(cached)
        value = compute_fn()
        client.setex(key, ttl, json.dumps(jsonable_encoder(value)))
        return value
    except redis.RedisError:
        return compute_fn()


def invalidate(prefix: str) -> None:
    client = get_client()
    if client is None:
        return
    try:
        for key in client.scan_iter(match=f"{prefix}*"):
            client.delete(key)
    except redis.RedisError:
        pass
