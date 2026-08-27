"""Redis transport — atomic mirror batches (book + rows) for zero-behavior split."""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

CHANNEL_BATCH = "hl:mirror:batch"
CHANNEL_MIRROR = "hl:mirror:rows"
CHANNEL_BOT_STATE = "hl:bot:state"
STREAM_KEY = "hl:mirror:stream"


def _channel(name: str) -> str:
    prefix = (os.getenv("HL_REDIS_CHANNEL_PREFIX") or "").strip()
    if prefix:
        return f"{prefix}:{name}"
    return name


def _stream_key() -> str:
    prefix = (os.getenv("HL_REDIS_CHANNEL_PREFIX") or "").strip()
    return f"{prefix}:{STREAM_KEY}" if prefix else STREAM_KEY


def _transport() -> str:
    return (os.getenv("HL_EVENT_TRANSPORT") or "redis").strip().lower()


def _use_stream() -> bool:
    return _env_truthy("HL_MIRROR_USE_STREAM", default=True)


def _env_truthy(name: str, *, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _envelope(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 2,
        "event_type": event_type,
        "published_at": datetime.now(timezone.utc).isoformat(),
        "batch_id": str(uuid.uuid4()),
        "payload": payload,
    }


class EventBus:
    def publish_batch(self, payload: dict[str, Any]) -> None:
        raise NotImplementedError


class RedisEventBus(EventBus):
    def __init__(self) -> None:
        import redis

        url = (os.getenv("REDIS_URL") or "").strip()
        if not url:
            raise RuntimeError("REDIS_URL is required for HL_EVENT_TRANSPORT=redis")
        self._client = redis.from_url(url, decode_responses=True)

    def publish_batch(self, payload: dict[str, Any]) -> None:
        body = json.dumps(_envelope("mirror_batch", payload), ensure_ascii=False)
        if _use_stream():
            try:
                self._client.xadd(
                    _stream_key(),
                    {"data": body},
                    maxlen=int(os.getenv("HL_MIRROR_STREAM_MAXLEN", "2000") or 2000),
                    approximate=True,
                )
            except Exception as exc:
                logger.warning("mirror stream xadd failed, falling back to pubsub: %s", exc)
        self._client.publish(_channel(CHANNEL_BATCH), body)


class LogEventBus(EventBus):
    def publish_batch(self, payload: dict[str, Any]) -> None:
        rows = payload.get("rows") or []
        bots = payload.get("book", {}).get("bots") or {}
        logger.info(
            "HL bus mirror_batch bots=%s rows=%s immediate=%s",
            len(bots) if isinstance(bots, dict) else 0,
            len(rows) if isinstance(rows, list) else 0,
            payload.get("immediate"),
        )


_bus: EventBus | None = None


def get_bus() -> EventBus:
    global _bus
    if _bus is not None:
        return _bus
    mode = _transport()
    if mode == "redis":
        _bus = RedisEventBus()
    else:
        _bus = LogEventBus()
    return _bus


def publish_mirror_batch(
    book: dict[str, Any],
    rows: list[dict[str, Any]] | None = None,
    *,
    immediate: bool = False,
) -> None:
    payload = {
        "book": book,
        "rows": list(rows or []),
        "immediate": bool(immediate),
    }
    get_bus().publish_batch(payload)


def parse_message(raw: str) -> dict[str, Any] | None:
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return msg if isinstance(msg, dict) else None
