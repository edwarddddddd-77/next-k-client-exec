"""In-memory paper book mirror (fed atomically by hl-event batches)."""

from __future__ import annotations

import copy
import threading
from datetime import datetime, timezone
from typing import Any


class BotStateStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._book: dict[str, Any] = {"bots": {}}

    def replace_book(self, book: dict[str, Any]) -> None:
        if not isinstance(book, dict):
            return
        with self._lock:
            self._book = copy.deepcopy(book)

    def as_book(self) -> dict[str, Any]:
        with self._lock:
            if not self._book:
                return {"bots": {}, "updated_at": datetime.now(timezone.utc).isoformat()}
            return copy.deepcopy(self._book)

    def get_bot(self, bot_id: str) -> dict[str, Any]:
        with self._lock:
            bots = self._book.get("bots") or {}
            bot = bots.get(bot_id)
            if isinstance(bot, dict):
                return copy.deepcopy(bot)
            for b in bots.values():
                if isinstance(b, dict) and str(b.get("id") or "") == bot_id:
                    return copy.deepcopy(b)
            return {}


_store = BotStateStore()


def get_store() -> BotStateStore:
    return _store
