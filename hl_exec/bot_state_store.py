"""Bot state cache for client-exec (replaces ingest paper book)."""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any


class BotStateStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._bots: dict[str, dict[str, Any]] = {}

    def update(self, payload: dict[str, Any]) -> None:
        bot_id = str(payload.get("bot_id") or "").strip()
        if not bot_id:
            return
        with self._lock:
            prev = dict(self._bots.get(bot_id) or {})
            prev.update(payload)
            prev["id"] = bot_id
            self._bots[bot_id] = prev

    def as_book(self) -> dict[str, Any]:
        with self._lock:
            return {
                "bots": {k: dict(v) for k, v in self._bots.items()},
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

    def get_bot(self, bot_id: str) -> dict[str, Any]:
        with self._lock:
            bot = self._bots.get(bot_id)
            return dict(bot) if isinstance(bot, dict) else {}


_store = BotStateStore()


def get_store() -> BotStateStore:
    return _store
