"""Fetch paper book snapshot from hl-event (cold start / catch-up refresh)."""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

logger = logging.getLogger(__name__)


def ingest_base_url() -> str:
    return (os.getenv("HL_EVENT_INGEST_URL") or "").strip().rstrip("/")


def fetch_paper_snapshot() -> dict[str, Any] | None:
    base = ingest_base_url()
    if not base:
        return None
    token = (os.getenv("HL_INTERNAL_TOKEN") or "").strip()
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    url = f"{base}/internal/paper-snapshot"
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        book = data.get("book") if isinstance(data, dict) else None
        return book if isinstance(book, dict) else None
    except Exception as exc:
        logger.warning("ingest paper snapshot fetch failed: %s", exc)
        return None


def sync_from_ingest() -> bool:
    book = fetch_paper_snapshot()
    if not book:
        return False
    from hl_exec.bot_state_store import get_store

    get_store().replace_book(book)
    logger.info("synced paper book from ingest bots=%s", len(book.get("bots") or {}))
    return True
