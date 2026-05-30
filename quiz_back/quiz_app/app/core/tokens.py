from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone

from app.core.config import settings


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def hash_token(token: str) -> str:
    return _sha256_hex(token)


def new_token() -> str:
    # Short but URL-safe, good for Authorization: Bearer <token>
    return secrets.token_urlsafe(32)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def access_expiry() -> datetime:
    return utcnow() + timedelta(seconds=settings.access_token_ttl_seconds)


def refresh_expiry() -> datetime:
    return utcnow() + timedelta(seconds=settings.refresh_token_ttl_seconds)

