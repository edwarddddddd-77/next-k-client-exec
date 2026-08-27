"""Redis consumer: mirror rows → Bitget executor."""

from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
from typing import Any

from hl_common.bus import CHANNEL_BOT_STATE, CHANNEL_MIRROR, _channel, parse_message
from hl_exec.bot_state_store import get_store

logger = logging.getLogger(__name__)


def _jitter_seconds() -> float:
    raw = (os.getenv("HL_EXEC_JITTER_MS") or "0").strip()
    if "-" in raw:
        lo_s, hi_s = raw.split("-", 1)
        lo = max(0.0, float(lo_s.strip() or 0) / 1000.0)
        hi = max(lo, float(hi_s.strip() or lo_s.strip() or 0) / 1000.0)
        return random.uniform(lo, hi)
    ms = max(0.0, float(raw or 0))
    return ms / 1000.0


def _handle_mirror_payload(payload: dict[str, Any]) -> None:
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        return
    immediate = bool(payload.get("immediate"))
    delay = 0.0 if immediate else _jitter_seconds()
    if delay > 0:
        time.sleep(delay)
    from utils.hl_bitget_executor import maybe_execute_rows_async

    maybe_execute_rows_async(rows, immediate=immediate)
    logger.info(
        "exec mirror_rows n=%s immediate=%s delay=%.2fs",
        len(rows),
        immediate,
        delay,
    )


def _handle_bot_state_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        return
    get_store().update(payload)
    logger.debug("bot_state updated bot=%s", payload.get("bot_id"))


def _dispatch(envelope: dict[str, Any]) -> None:
    event_type = str(envelope.get("event_type") or "")
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return
    if event_type == "mirror_rows":
        _handle_mirror_payload(payload)
        return
    if event_type == "bot_state":
        _handle_bot_state_payload(payload)


class ExecConsumer:
    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="hl-exec-consumer", daemon=True)
        self._thread.start()
        logger.info("HL exec consumer started")

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=8)
        logger.info("HL exec consumer stopped")

    def _run(self) -> None:
        import redis

        url = (os.getenv("REDIS_URL") or "").strip()
        if not url:
            logger.error("REDIS_URL missing — exec consumer idle")
            return
        client = redis.from_url(url, decode_responses=True)
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        channels = [_channel(CHANNEL_MIRROR), _channel(CHANNEL_BOT_STATE)]
        pubsub.subscribe(*channels)
        logger.info("subscribed channels=%s", channels)
        while not self._stop.is_set():
            msg = pubsub.get_message(timeout=1.0)
            if not msg or msg.get("type") != "message":
                continue
            data = msg.get("data")
            if not isinstance(data, str):
                continue
            envelope = parse_message(data)
            if not envelope:
                continue
            try:
                _dispatch(envelope)
            except Exception:
                logger.exception("exec dispatch failed")


exec_consumer = ExecConsumer()
