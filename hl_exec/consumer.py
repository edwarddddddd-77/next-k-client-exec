"""Redis consumer — atomic mirror_batch → book + Bitget executor."""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from typing import Any

from hl_common.bus import CHANNEL_BATCH, _channel, _stream_key, parse_message
from hl_exec.bot_state_store import get_store

logger = logging.getLogger(__name__)


def _jitter_seconds(immediate: bool) -> float:
    if immediate:
        return 0.0
    raw = (os.getenv("HL_EXEC_JITTER_MS") or "0").strip()
    if not raw or raw == "0":
        return 0.0
    if "-" in raw:
        lo_s, hi_s = raw.split("-", 1)
        lo = max(0.0, float(lo_s.strip() or 0) / 1000.0)
        hi = max(lo, float(hi_s.strip() or lo_s.strip() or 0) / 1000.0)
        return random.uniform(lo, hi)
    return max(0.0, float(raw) / 1000.0)


def _apply_batch_payload(payload: dict[str, Any]) -> None:
    book = payload.get("book")
    if isinstance(book, dict):
        get_store().replace_book(book)
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        return
    immediate = bool(payload.get("immediate"))
    delay = _jitter_seconds(immediate)
    if delay > 0:
        time.sleep(delay)
    from utils.hl_bitget_executor import maybe_execute_rows_async

    maybe_execute_rows_async(rows, immediate=immediate)
    logger.info(
        "exec mirror_batch rows=%s immediate=%s delay=%.2fs",
        len(rows),
        immediate,
        delay,
    )


def _dispatch_envelope(envelope: dict[str, Any]) -> None:
    event_type = str(envelope.get("event_type") or "")
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return
    if event_type == "mirror_batch":
        _apply_batch_payload(payload)
        return
    # Legacy v1: best-effort (bot_state then rows — still racy)
    if event_type == "bot_state":
        bot_id = str(payload.get("bot_id") or "")
        if bot_id:
            book = get_store().as_book()
            bots = book.setdefault("bots", {})
            prev = dict(bots.get(bot_id) or {})
            prev.update(payload)
            prev["id"] = bot_id
            bots[bot_id] = prev
            get_store().replace_book(book)
        return
    if event_type == "mirror_rows":
        _apply_batch_payload({"book": get_store().as_book(), **payload})


def _replay_stream_tail(client: Any) -> None:
    if not _env_truthy("HL_MIRROR_REPLAY_ON_START", default=True):
        return
    try:
        entries = client.xrevrange(_stream_key(), count=1)
    except Exception as exc:
        logger.warning("mirror stream replay skipped: %s", exc)
        return
    if not entries:
        return
    for _entry_id, fields in entries:
        raw = fields.get("data") if isinstance(fields, dict) else None
        if not isinstance(raw, str):
            continue
        envelope = parse_message(raw)
        if envelope and envelope.get("event_type") == "mirror_batch":
            payload = envelope.get("payload")
            if isinstance(payload, dict) and isinstance(payload.get("book"), dict):
                get_store().replace_book(payload["book"])
                logger.info("replayed mirror stream tail into book")
                return


def _env_truthy(name: str, *, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


class ExecConsumer:
    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        try:
            from hl_exec.ingest_client import sync_from_ingest

            sync_from_ingest()
        except Exception as exc:
            logger.warning("exec startup ingest sync: %s", exc)
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
        _replay_stream_tail(client)
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        channel = _channel(CHANNEL_BATCH)
        pubsub.subscribe(channel)
        logger.info("subscribed channel=%s", channel)
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
                _dispatch_envelope(envelope)
            except Exception:
                logger.exception("exec dispatch failed")


exec_consumer = ExecConsumer()
