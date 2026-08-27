"""HL → Bitget sub-account routing config.

Each entry binds one paper bot (watchlist id) to one Bitget API key set,
optionally limited to a coin allowlist.

Credentials live in Railway/env only (never in JSON):
  {env_prefix}_API_KEY / _API_SECRET / _PASSPHRASE

Enable live seats via Railway (JSON stays enabled:false). Enabled seats are
live_only (no paper); size = leader × (bitget_eq / AV):
  HL_BITGET_ENABLE_BOTS=bot_c,bot_a   # or C,A
  HL_BITGET_SUB_C_ENABLED=1           # per route id

Desk habit: only main BITGET_* keys are provisioned. When ENABLE_BOTS flips a
seat whose JSON still says BITGET_SUB_* (empty keys), enabled routes auto-fall
back to main BITGET_* so UI never shows 实盘 + credentials_missing.

Hard cap: at most HL_BITGET_MAX_SUBACCOUNTS enabled routes (default 10, ceiling 32).
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from utils.paths import PROJECT_ROOT, resolve_data_dir

logger = logging.getLogger(__name__)

CONFIG_NAME = "hl_bitget_subaccounts.json"
DEFAULT_MAX_SUBACCOUNTS = 10

_lock = threading.Lock()
_cache: dict[str, Any] | None = None
_cache_mtime: float | None = None


def _env_truthy(raw: str) -> bool | None:
    s = str(raw or "").strip().lower()
    if not s:
        return None
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return None


def _env_csv_set(*names: str) -> set[str]:
    out: set[str] = set()
    for name in names:
        raw = (os.getenv(name) or "").strip()
        if not raw:
            continue
        for part in raw.replace(";", ",").split(","):
            tok = part.strip()
            if tok:
                out.add(tok)
    return out


def _expand_seat_tokens(tokens: set[str]) -> set[str]:
    """Accept bot_c / C / c → match bot_id or route id."""
    expanded: set[str] = set()
    for tok in tokens:
        t = str(tok or "").strip()
        if not t:
            continue
        expanded.add(t)
        expanded.add(t.lower())
        expanded.add(t.upper())
        low = t.lower()
        if low.startswith("bot_"):
            suffix = low[4:]
            if suffix:
                expanded.add(suffix)
                expanded.add(suffix.upper())
                expanded.add(f"bot_{suffix}")
        else:
            expanded.add(f"bot_{low}")
            expanded.add(low.upper())
    return expanded


def env_enable_bot_tokens() -> set[str]:
    """Railway: which seats go Bitget live_only (no paper book).

    Accepts HL_BITGET_ENABLE_BOTS, plus aliases HL_LIVE_ONLY_BOTS /
    HL_BITGET_LIVE_ONLY_BOTS.
    """
    return _expand_seat_tokens(
        _env_csv_set(
            "HL_BITGET_ENABLE_BOTS",
            "HL_LIVE_ONLY_BOTS",
            "HL_BITGET_LIVE_ONLY_BOTS",
        )
    )


def seat_enabled_by_env(*, route_id: str, bot_id: str) -> bool | None:
    """True/False if Railway forces enable/disable; None = use JSON.

    When HL_BITGET_ENABLE_BOTS (or aliases) is set, it is an allowlist:
    unmatched seats are forced off (do not fall through to JSON enabled:true).
    Per-route HL_BITGET_SUB_<ID>_ENABLED still wins first.
    """
    rid = str(route_id or "").strip()
    bid = str(bot_id or "").strip()
    # Per-route: HL_BITGET_SUB_C_ENABLED=1
    if rid:
        forced = _env_truthy(os.getenv(f"HL_BITGET_SUB_{rid.upper()}_ENABLED", ""))
        if forced is not None:
            return forced
    tokens = env_enable_bot_tokens()
    if not tokens:
        return None
    if rid in tokens or rid.upper() in tokens or rid.lower() in tokens:
        return True
    if bid in tokens or bid.lower() in tokens:
        return True
    return False


def route_id_for_bot(bot_id: str) -> str:
    """Resolve Bitget route id for a watchlist bot (bot_c → C)."""
    bid = str(bot_id or "").strip()
    if not bid:
        return ""
    for r in parse_routes():
        if r.bot_id == bid:
            return r.id
    if bid.lower().startswith("bot_") and len(bid) > 4:
        return bid[4:].upper()
    return ""


MAIN_ENV_PREFIX = "BITGET"
_fallback_logged: set[str] = set()


def normalize_env_prefix(prefix: str) -> str:
    """Empty / BITGET → main account prefix."""
    p = str(prefix or "").strip().rstrip("_")
    if not p or p.upper() == MAIN_ENV_PREFIX:
        return MAIN_ENV_PREFIX
    return p


def _bitget_creds_ok(prefix: str) -> bool:
    try:
        from quant.engine.exchanges.bitget.account import load_creds_from_env
    except Exception:
        return False
    p = normalize_env_prefix(prefix)
    try:
        return bool(load_creds_from_env(p).ok())
    except Exception:
        return False


def resolve_live_env_prefix(configured: str, *, route_id: str = "", enabled: bool = False) -> str:
    """Pick env prefix for a seat.

    Enabled seats: configured keys if present; else main BITGET_* when available.
    Disabled seats keep JSON prefix unchanged (no live overlay).
    """
    configured = str(configured or "").strip().rstrip("_")
    if not enabled:
        return configured
    if configured and _bitget_creds_ok(configured):
        return normalize_env_prefix(configured)
    if _bitget_creds_ok(MAIN_ENV_PREFIX):
        if normalize_env_prefix(configured) != MAIN_ENV_PREFIX:
            key = str(route_id or configured or "?")
            if key not in _fallback_logged:
                _fallback_logged.add(key)
                logger.warning(
                    "route %s: %s credentials missing — using main BITGET_* "
                    "(set BITGET_SUB_* keys or point env_prefix at BITGET)",
                    route_id or "?",
                    configured or "(empty)",
                )
        return MAIN_ENV_PREFIX
    return configured or MAIN_ENV_PREFIX


@dataclass(frozen=True)
class SubAccountRoute:
    id: str
    label: str
    bot_id: str
    coins: frozenset[str] | None  # None = all coins for that bot
    enabled: bool
    env_prefix: str
    scale: float

    def allows_coin(self, coin: str) -> bool:
        if self.coins is None:
            return True
        from utils.hl_bitget_symbol_map import hl_base_ticker

        base = hl_base_ticker(coin)
        if not base:
            return False
        allowed = {hl_base_ticker(c) or str(c).strip().upper() for c in self.coins}
        return base in allowed


def max_subaccounts() -> int:
    try:
        n = int(os.getenv("HL_BITGET_MAX_SUBACCOUNTS", str(DEFAULT_MAX_SUBACCOUNTS)) or DEFAULT_MAX_SUBACCOUNTS)
    except (TypeError, ValueError):
        n = DEFAULT_MAX_SUBACCOUNTS
    # Hard ceiling above desk A–J; env can raise up to this.
    return max(1, min(32, n))


def _config_path() -> Path:
    """Prefer repo config when present (same deploy-config rule as watchlist)."""
    root = PROJECT_ROOT / CONFIG_NAME
    if root.is_file():
        return root
    return resolve_data_dir() / CONFIG_NAME


def invalidate_cache() -> None:
    global _cache, _cache_mtime
    with _lock:
        _cache = None
        _cache_mtime = None


def load_subaccounts_doc(*, force: bool = False) -> dict[str, Any]:
    global _cache, _cache_mtime
    path = _config_path()
    with _lock:
        try:
            mtime = path.stat().st_mtime if path.exists() else None
        except OSError:
            mtime = None
        if not force and _cache is not None and mtime == _cache_mtime:
            return _cache
        if not path.exists():
            doc = {"updated": None, "subaccounts": [], "error": f"missing: {path}"}
            _cache = doc
            _cache_mtime = mtime
            return doc
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(doc, dict):
                doc = {"subaccounts": [], "error": "invalid root"}
        except Exception as exc:
            logger.warning("hl_bitget_subaccounts load failed: %s", exc)
            doc = {"subaccounts": [], "error": str(exc)}
        _cache = doc
        _cache_mtime = mtime
        return doc


def parse_routes(doc: dict[str, Any] | None = None) -> list[SubAccountRoute]:
    raw = doc if doc is not None else load_subaccounts_doc()
    out: list[SubAccountRoute] = []
    seen_ids: set[str] = set()
    for row in raw.get("subaccounts") or []:
        if not isinstance(row, dict):
            continue
        rid = str(row.get("id") or "").strip()
        bot_id = str(row.get("bot_id") or "").strip()
        if not rid or not bot_id:
            continue
        if rid in seen_ids:
            logger.warning("duplicate subaccount id skipped: %s", rid)
            continue
        seen_ids.add(rid)
        coins_raw = row.get("coins")
        coins: frozenset[str] | None
        if coins_raw is None or coins_raw == [] or coins_raw == "*":
            coins = None
        elif isinstance(coins_raw, str):
            coins = frozenset(c.strip().upper() for c in coins_raw.split(",") if c.strip())
        else:
            coins = frozenset(str(c).strip().upper() for c in coins_raw if str(c).strip())
            if not coins:
                coins = None
        try:
            scale = max(0.0, float(row.get("scale", 1) or 1))
        except (TypeError, ValueError):
            scale = 1.0
        enabled = bool(row.get("enabled", False))
        # Railway overrides JSON enabled (see seat_enabled_by_env).
        forced = seat_enabled_by_env(route_id=rid, bot_id=bot_id)
        if forced is not None:
            enabled = forced
        configured_prefix = str(row.get("env_prefix") or "").strip()
        env_prefix = resolve_live_env_prefix(
            configured_prefix, route_id=rid, enabled=enabled
        )
        out.append(
            SubAccountRoute(
                id=rid,
                label=str(row.get("label") or rid).strip(),
                bot_id=bot_id,
                coins=coins,
                enabled=enabled,
                env_prefix=env_prefix,
                scale=scale,
            )
        )
    return out


def enabled_routes() -> list[SubAccountRoute]:
    """Enabled routes, hard-capped at max_subaccounts() (fail-closed over cap).

    Also refuse when 2+ enabled seats resolve to the same API key prefix
    (usually both falling back to main BITGET_*).
    """
    enabled = [r for r in parse_routes() if r.enabled]
    cap = max_subaccounts()
    if len(enabled) > cap:
        logger.error(
            "enabled subaccounts %d > max %d — refusing all until trimmed",
            len(enabled),
            cap,
        )
        return []
    by_prefix: dict[str, list[str]] = {}
    for r in enabled:
        by_prefix.setdefault(normalize_env_prefix(r.env_prefix), []).append(r.id)
    clashes = {p: ids for p, ids in by_prefix.items() if len(ids) > 1}
    if clashes:
        logger.error(
            "enabled seats share Bitget API prefix %s — refusing all until "
            "ENABLE_BOTS is a single seat or each has own keys",
            clashes,
        )
        return []
    return enabled


def routes_for_bot(bot_id: str) -> list[SubAccountRoute]:
    bid = str(bot_id or "").strip()
    return [r for r in enabled_routes() if r.bot_id == bid]


def route_for_bot_any(bot_id: str) -> SubAccountRoute | None:
    """Configured route for bot even when seat is disabled (leave-live flatten)."""
    bid = str(bot_id or "").strip()
    if not bid:
        return None
    for r in parse_routes():
        if r.bot_id == bid:
            return r
    return None


def route_for_flatten(bot_id: str) -> SubAccountRoute | None:
    """Route for forced flatten after DISABLE / leave live.

    Resolves credentials as if the seat were enabled so SUB→main fallback still
    works when cleaning up the account the desk was trading on.

    Refuses when the resolved API prefix is still owned by a *different*
    enabled live seat (avoids paper prune of bot_a wiping bot_c on BITGET_*).
    """
    from dataclasses import replace

    bid = str(bot_id or "").strip()
    raw = route_for_bot_any(bid)
    if raw is None:
        return None
    prefix = resolve_live_env_prefix(raw.env_prefix, route_id=raw.id, enabled=True)
    pref = normalize_env_prefix(prefix)
    for other in enabled_routes():
        if other.bot_id == bid:
            continue
        if normalize_env_prefix(other.env_prefix) == pref:
            logger.warning(
                "skip flatten bot=%s: resolved prefix %s still owned by live %s",
                bid,
                pref,
                other.bot_id,
            )
            return None
    return replace(raw, enabled=True, env_prefix=prefix)


def validate_routes(routes: list[SubAccountRoute] | None = None) -> list[str]:
    """Return human-readable config problems (empty = ok)."""
    routes = routes if routes is not None else parse_routes()
    problems: list[str] = []
    enabled = [r for r in routes if r.enabled]
    cap = max_subaccounts()
    if len(enabled) > cap:
        problems.append(f"enabled subaccounts {len(enabled)} > max {cap}")

    seen_bots: dict[str, str] = {}
    seen_prefix: dict[str, str] = {}
    for r in enabled:
        if not r.env_prefix:
            problems.append(
                f"route {r.id}: env_prefix required (Railway: {r.id.upper()} API keys)"
            )
        pref = normalize_env_prefix(r.env_prefix)
        if pref in seen_prefix and seen_prefix[pref] != r.id:
            problems.append(
                f"routes {seen_prefix[pref]} and {r.id} share API prefix {pref}"
            )
        else:
            seen_prefix[pref] = r.id
        if r.bot_id in seen_bots and seen_bots[r.bot_id] != r.id:
            # one bot → one subaccount recommended; coin-split still allowed via validate below
            pass
        seen_bots[r.bot_id] = r.id

    for i, a in enumerate(enabled):
        for b in enabled[i + 1 :]:
            if a.bot_id != b.bot_id:
                continue
            if a.coins is None or b.coins is None:
                problems.append(
                    f"bot {a.bot_id} mapped to both {a.id} and {b.id} with overlapping all-coins"
                )
                continue
            overlap = a.coins & b.coins
            if overlap:
                problems.append(
                    f"bot {a.bot_id} coin overlap {sorted(overlap)} on {a.id} and {b.id}"
                )
    return problems
