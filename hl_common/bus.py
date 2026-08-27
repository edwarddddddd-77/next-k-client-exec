"""Redis pub/sub transport for HL mirror rows and bot state."""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

CHANNEL_MIRROR = "hl:mirror:rows"
CHANNEL_BOT_STATE = "hl:bot:state"


def _channel(name: str) -> str:
    prefix = (os.getenv("HL_REDIS_CHANNEL_PREFIX") or "").strip()
    if prefix:
        return f"{prefix}:{name}"
    return name


def _transport() -> str:
    return (os.getenv("HL_EVENT_TRANSPORT") or "redis").strip().lower()


def _envelope(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "event_type": event_type,
        "published_at": datetime.now(timezone.utc).isoformat(),
        "batch_id": str(uuid.uuid4()),
        "payload": payload,
    }


class EventBus:
    def publish(self, channel_name: str, event_type: str, payload: dict[str, Any]) -> None:
        raise NotImplementedError


class RedisEventBus(EventBus):
    def __init__(self) -> None:
        import redis

        url = (os.getenv("REDIS_URL") or "").strip()
        if not url:
            raise RuntimeError("REDIS_URL is required for HL_EVENT_TRANSPORT=redis")
        self._client = redis.from_url(url, decode_responses=True)

    def publish(self, channel_name: str, event_type: str, payload: dict[str, Any]) -> None:
        body = json.dumps(_envelope(event_type, payload), ensure_ascii=False)
        self._client.publish(_channel(channel_name), body)


class LogEventBus(EventBus):
    def publish(self, channel_name: str, event_type: str, payload: dict[str, Any]) -> None:
        logger.info("HL bus publish channel=%s type=%s n=%s", channel_name, event_type, len(payload))


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


def publish_mirror_rows(rows: list[dict[str, Any]], *, immediate: bool = False) -> None:
    if not rows:
        return
    get_bus().publish(
        CHANNEL_MIRROR,
        "mirror_rows",
        {"rows": rows, "immediate": bool(immediate)},
    )


def publish_bot_state(bot_id: str, bot: dict[str, Any]) -> None:
    if not bot_id:
        return
    payload = {
        "bot_id": bot_id,
        "address": bot.get("address"),
        "live_only": bot.get("live_only"),
        "copy_current": bot.get("copy_current"),
        "allow_coins": bot.get("allow_coins"),
        "target_av": bot.get("target_av"),
        "target_equity": bot.get("target_equity"),
        "target_spot_usdc": bot.get("target_spot_usdc"),
        "target_positions": bot.get("target_positions"),
        "target_lev_by_coin": bot.get("target_lev_by_coin"),
        "target_last_fill_at": bot.get("target_last_fill_at"),
        "paper": bot.get("paper"),
        "live": bot.get("live"),
        "venue": bot.get("venue"),
    }
    get_bus().publish(CHANNEL_BOT_STATE, "bot_state", payload)


def parse_message(raw: str) -> dict[str, Any] | None:
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return msg if isinstance(msg, dict) else None
