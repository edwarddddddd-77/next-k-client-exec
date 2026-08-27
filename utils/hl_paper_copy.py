"""Paper book adapter — same surface as monolith hl_paper_copy for executor."""

from __future__ import annotations

from typing import Any

from hl_exec.bot_state_store import get_store


def load_paper() -> dict[str, Any]:
    return get_store().as_book()


def refresh_target_health(*, force: bool = False) -> dict[str, Any]:
    if force:
        try:
            from hl_exec.ingest_client import sync_from_ingest
        except Exception:
            pass
        else:
            sync_from_ingest()
    return load_paper()


def target_sizing_equity(bot: dict[str, Any]) -> float:
    perp = float(bot.get("target_av") or 0)
    spot = float(bot.get("target_spot_usdc") or 0)
    equity = max(0.0, perp) + spot
    bot["target_equity"] = round(equity, 4)
    return equity


def _bot_copy_current(bot: dict[str, Any] | None) -> bool:
    if not isinstance(bot, dict):
        return False
    raw = bot.get("copy_current")
    if raw is True:
        return True
    if raw is False or raw is None:
        return False
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def is_live_only_bot(bot: dict[str, Any] | None) -> bool:
    if not isinstance(bot, dict):
        return False
    if bot.get("paper") is True:
        return False
    if bot.get("live_only") is True:
        return True
    if bot.get("paper") is False:
        return True
    venue = str(bot.get("venue") or "").strip().lower()
    if venue in ("binance", "bitget") and bot.get("live") is True and bot.get("paper") is not True:
        return True
    return False


def _coin_base(coin: str) -> str:
    raw = str(coin or "").strip()
    if not raw:
        return ""
    if ":" in raw:
        return raw.split(":", 1)[1]
    return raw


def _scope_keys_for_coin(coin: str) -> set[str]:
    raw = str(coin or "").strip()
    if not raw:
        return set()
    out = {raw.upper()}
    base = _coin_base(raw)
    if base:
        out.add(base.upper())
    return out
