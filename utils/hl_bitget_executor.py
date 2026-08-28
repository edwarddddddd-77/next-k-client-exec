"""HL → Bitget USDT-M (vnpy REST).

MODE=sub (default): each enabled bot maps to a Bitget sub-account
(hl_bitget_subaccounts.json). Positions never net across sub-accounts.

Railway-enabled seats are live_only (no paper):
  open size ≈ leader_sz × (bitget_eq / target_equity) × scale
  target_equity = perp AV (main+xyz) + Core spot USDC
Desk UI overlays Bitget wallet/positions for those seats.

Mature copy_current=off policy (event-driven, not target-chase):
  • Open only on leader flat→open fills (per-row startPosition≈0, batch inferred
    near-flat open burst with dust notional cap, or pending_fresh after a failed
    open).
  • Size-UP only when this batch carries a leader fill signal (target_delta /
    flat→open). No signal → hold (never top-up on AV drift / paper rebuild /
    enter-live align).
  • Size-DOWN / flatten only when leader that coin actually reduces/flattens
    (fill signal or leader_sz≈0). Never shrink BTC/ETH just because AV drifted.
  • copy_current=on keeps full desired sync (explicit catch-up).

MODE=net: legacy single-account sum of bots.
MODE=delta: per-row intents (single bot / single account only).

Default dry-run. Live: HL_BITGET_LIVE=1, DRY_RUN=0, plus enabled subaccounts
with credentials (sub mode) or ALLOW_COINS+BOT_IDS (net/delta).

Burst fills: HL_BITGET_DEBOUNCE_MS (default 10000) coalesces HL fills into
one Bitget position sync.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from utils.paths import resolve_data_dir

logger = logging.getLogger(__name__)

LEDGER_NAME = "hl_bitget_live.jsonl"

# Leader flat→open: exact zero from HL startPosition.
_FRESH_OPEN_SP_EPS = 1e-12
# Inferred pre-batch leg vs post (burst from near-flat when WS misses first fill).
# Ratio alone is not enough — a 50→5050 mid-book scale-up is also ~1%.
_NEAR_FLAT_PRE_RATIO = 0.01
# Absolute dust notional (USD) for inferred pre; SNDK residual was ~$240.
_NEAR_FLAT_DUST_NOTIONAL = 500.0

_symbol_locks: dict[str, threading.Lock] = {}
_symbol_locks_guard = threading.Lock()
_mode_ready_accounts: set[str] = set()
_mode_lock = threading.Lock()
_bg_lock = threading.Lock()

# Coalesce rapid paper fills into one Bitget position sync.
_debounce_lock = threading.Lock()
_debounce_timer: threading.Timer | None = None
_debounce_pending: list[dict[str, Any]] = []
_debounce_gen = 0

# After a flat→open is seen (or open place fails), keep the symbol eligible for
# open while copy_current=off — so a later mid-book add does not orphan-skip forever.
_pending_fresh_lock = threading.Lock()
_pending_fresh_opens: dict[str, set[str]] = {}
_open_retry_lock = threading.Lock()
_open_retry_at: dict[str, float] = {}


def _env_truthy(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name, "")
    if not str(raw).strip():
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def live_enabled() -> bool:
    return _env_truthy("HL_BITGET_LIVE", default=False)


def dry_run() -> bool:
    return _env_truthy("HL_BITGET_DRY_RUN", default=True)


def scale() -> float:
    try:
        return max(0.0, float(os.getenv("HL_BITGET_SCALE", "1") or 1))
    except (TypeError, ValueError):
        return 1.0


def max_notional() -> float:
    try:
        return max(0.0, float(os.getenv("HL_BITGET_MAX_NOTIONAL", "0") or 0))
    except (TypeError, ValueError):
        return 0.0


def min_notional() -> float:
    try:
        return max(0.0, float(os.getenv("HL_BITGET_MIN_NOTIONAL", "5") or 5))
    except (TypeError, ValueError):
        return 5.0


def debounce_ms() -> float:
    """Wait this many ms after the last paper fill before Bitget sync (0 = off)."""
    try:
        return max(0.0, float(os.getenv("HL_BITGET_DEBOUNCE_MS", "10000") or 10000))
    except (TypeError, ValueError):
        return 1000.0


def allow_coins() -> set[str] | None:
    """Global allowlist (net/delta). Sub mode prefers per-route coins."""
    raw = (os.getenv("HL_BITGET_ALLOW_COINS") or "").strip()
    if not raw:
        return None
    return {c.strip().upper() for c in raw.split(",") if c.strip()}


def allow_bot_ids() -> set[str] | None:
    raw = (os.getenv("HL_BITGET_BOT_IDS") or "").strip()
    if not raw:
        return None
    return {c.strip() for c in raw.split(",") if c.strip()}


def skip_prefixes() -> tuple[str, ...]:
    """Prefixes to drop before mapping. Default empty — xyz: stocks are mapped.
    Example skip: HL_BITGET_SKIP_PREFIX=flx:,vntl:
    """
    raw = (os.getenv("HL_BITGET_SKIP_PREFIX") or "").strip()
    if not raw:
        return ()
    return tuple(p.strip().lower() for p in raw.split(",") if p.strip())


def log_skips() -> bool:
    return _env_truthy("HL_BITGET_LOG_SKIPS", default=False)


def exec_mode() -> str:
    """sub | net | delta. Default sub (isolated Bitget sub-accounts)."""
    raw = (os.getenv("HL_BITGET_MODE") or "sub").strip().lower()
    if raw in ("delta", "per_bot", "row"):
        return "delta"
    if raw in ("net", "sum", "legacy"):
        return "net"
    return "sub"


def status() -> dict[str, Any]:
    allow = allow_coins()
    bots = allow_bot_ids()
    ready, ready_reason = live_ready()
    sub_summary = _subaccount_status()
    return {
        "live_enabled": live_enabled(),
        "dry_run": dry_run(),
        "mode": exec_mode(),
        "live_ready": ready,
        "live_ready_reason": ready_reason,
        "scale": scale(),
        "max_notional": max_notional() or None,
        "min_notional": min_notional(),
        "debounce_ms": debounce_ms(),
        "allow_coins": sorted(allow) if allow is not None else None,
        "allow_bot_ids": sorted(bots) if bots is not None else None,
        "skip_prefixes": list(skip_prefixes()),
        "credentials": _credentials_ok(),
        "ledger": str(_ledger_path()),
        "subaccounts": sub_summary,
        "symbol_map": _symbol_map_status(),
    }


def _symbol_map_status() -> dict[str, Any]:
    try:
        from utils.hl_bitget_symbol_map import (
            bitget_contract_set,
            coin_aliases,
            verify_symbols_enabled,
        )

        known = bitget_contract_set()
        return {
            "verify_symbols": verify_symbols_enabled(),
            "aliases": coin_aliases(),
            "bitget_contracts": len(known) if known is not None else None,
            "unlisted_policy": "auto_skip",
            "samples": {
                "BTC": "BTCUSDT",
                "xyz:TSLA": "TSLAUSDT",
                "xyz:SILVER": "XAGUSDT",
                "xyz:GOOG": "GOOGLUSDT",
                "xyz:BRENTOIL": "BZUSDT",
                "xyz:SKHX": "SKHYNIXUSDT",
            },
        }
    except Exception as exc:
        return {"error": str(exc)}


def _subaccount_status() -> dict[str, Any]:
    try:
        from utils.hl_bitget_subaccounts import (
            load_subaccounts_doc,
            max_subaccounts,
            parse_routes,
            validate_routes,
        )
        from quant.engine.exchanges.bitget.account import load_creds_from_env
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    doc = load_subaccounts_doc()
    routes = parse_routes(doc)
    problems = validate_routes(routes)
    rows = []
    for r in routes:
        creds = load_creds_from_env(r.env_prefix)
        rows.append(
            {
                "id": r.id,
                "label": r.label,
                "bot_id": r.bot_id,
                "coins": sorted(r.coins) if r.coins is not None else None,
                "enabled": r.enabled,
                "env_prefix": r.env_prefix or "(missing — set in JSON)",
                "scale": r.scale,
                "credentials_ok": creds.ok(),
                "railway_keys": [
                    f"{r.env_prefix}_API_KEY",
                    f"{r.env_prefix}_API_SECRET",
                    f"{r.env_prefix}_PASSPHRASE",
                ]
                if r.env_prefix
                else [],
            }
        )
    try:
        from utils.hl_bitget_subaccounts import env_enable_bot_tokens

        env_enable = sorted(env_enable_bot_tokens())
    except Exception:
        env_enable = []
    return {
        "ok": not problems,
        "max_subaccounts": max_subaccounts(),
        "config_error": doc.get("error"),
        "problems": problems,
        "routes": rows,
        "enabled_count": sum(1 for r in routes if r.enabled),
        "env_enable_bots": env_enable or None,
        "live_only_when_enabled": True,
    }


def live_ready() -> tuple[bool, str]:
    """Whether real (non-dry) orders are allowed."""
    if not live_enabled():
        return False, "HL_BITGET_LIVE=0"
    if dry_run():
        return False, "HL_BITGET_DRY_RUN=1"

    mode = exec_mode()
    if mode == "sub":
        try:
            from utils.hl_bitget_subaccounts import (
                enabled_routes,
                max_subaccounts,
                validate_routes,
            )
            from quant.engine.exchanges.bitget.account import load_creds_from_env
        except Exception as exc:
            return False, f"subaccounts_import: {exc}"
        routes = enabled_routes()
        if not routes:
            problems = validate_routes()
            if any("max" in p for p in problems):
                return False, problems[0]
            return False, "no enabled subaccounts (set enabled or HL_BITGET_SUB_<ID>_ENABLED=1)"
        problems = validate_routes()
        if problems:
            return False, "; ".join(problems[:3])
        if len(routes) > max_subaccounts():
            return False, f"enabled > max {max_subaccounts()}"
        missing = [r.id for r in routes if not load_creds_from_env(r.env_prefix).ok()]
        if missing:
            return False, f"Railway credentials missing for: {','.join(missing)}"
        return True, "ok"

    if not _credentials_ok():
        return False, "bitget_credentials_missing"
    if not allow_coins():
        return False, "HL_BITGET_ALLOW_COINS required for live"
    if not allow_bot_ids():
        return False, "HL_BITGET_BOT_IDS required (bots included in net book)"
    if mode == "delta" and len(allow_bot_ids() or []) > 1:
        return False, "HL_BITGET_MODE=delta only safe with one BOT_ID (use sub or net)"
    return True, "ok"


def _credentials_ok() -> bool:
    try:
        from quant.engine.exchanges.bitget.gateway import bitget_credentials_configured

        return bool(bitget_credentials_configured())
    except Exception:
        key = (os.getenv("BITGET_API_KEY") or "").strip()
        sec = (os.getenv("BITGET_API_SECRET") or "").strip()
        pwd = (os.getenv("BITGET_PASSPHRASE") or os.getenv("BITGET_API_PASSPHRASE") or "").strip()
        return bool(key and sec and pwd)


def _ledger_path() -> Path:
    return resolve_data_dir() / LEDGER_NAME


def _append_ledger(row: dict[str, Any]) -> None:
    path = _ledger_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(row, ensure_ascii=False)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def _symbol_lock(sym: str, *, account_id: str = "main") -> threading.Lock:
    key = f"{account_id}:{sym}"
    with _symbol_locks_guard:
        lk = _symbol_locks.get(key)
        if lk is None:
            lk = threading.Lock()
            _symbol_locks[key] = lk
        return lk


def hl_coin_to_bitget(
    coin: str,
    *,
    route_coins: set[str] | frozenset[str] | None = None,
) -> str | None:
    """Map HL coin (crypto / xyz: stocks) to Bitget USDT-M; None = auto-skip."""
    from utils.hl_bitget_symbol_map import hl_base_ticker, resolve_bitget_symbol

    raw = str(coin or "").strip()
    if not raw:
        return None
    low = raw.lower()
    for pref in skip_prefixes():
        if low.startswith(pref):
            return None
    if raw.startswith("@"):
        return None

    base = hl_base_ticker(raw)
    if not base:
        return None

    def _in_allow(allow: set[str] | frozenset[str]) -> bool:
        normalized = {hl_base_ticker(c) or str(c).strip().upper() for c in allow}
        return base in normalized

    if route_coins is not None:
        if not _in_allow(route_coins):
            return None
    else:
        allow = allow_coins()
        if allow is not None and not _in_allow(allow):
            return None

    sym, _reason = resolve_bitget_symbol(raw)
    return sym


def hl_coin_map_detail(coin: str) -> dict[str, Any]:
    """For ledger / debugging: full map result including skip_reason."""
    from utils.hl_bitget_symbol_map import describe_mapping, hl_base_ticker

    raw = str(coin or "").strip()
    detail = describe_mapping(raw)
    if not raw:
        return detail
    low = raw.lower()
    for pref in skip_prefixes():
        if low.startswith(pref):
            return {**detail, "bitget": None, "ok": False, "skip_reason": "skip_prefix"}
    allow = allow_coins()
    base = hl_base_ticker(raw)
    if allow is not None and base and base not in {
        hl_base_ticker(c) or str(c).strip().upper() for c in allow
    }:
        return {**detail, "bitget": None, "ok": False, "skip_reason": "not_in_allow_coins"}
    return detail


def make_client_oid(
    *,
    bot_id: str,
    action: str,
    coin: str,
    tid: str | None,
    fp: str | None,
) -> str:
    """Stable idempotency key — do NOT include qty (ratio drift would re-fire)."""
    seed = "|".join(
        [
            str(bot_id or ""),
            str(action or ""),
            str(coin or "").upper(),
            str(tid or ""),
            str(fp or ""),
        ]
    )
    digest = hashlib.sha1(seed.encode()).hexdigest()[:20]
    return f"hl{digest}"


def _ensure_one_way_once(*, account_id: str = "main") -> None:
    with _mode_lock:
        if account_id in _mode_ready_accounts:
            return
        try:
            from quant.engine.exchanges.bitget.account import ensure_one_way_mode

            ensure_one_way_mode()
            _mode_ready_accounts.add(account_id)
        except Exception as exc:
            logger.warning("bitget ensure one-way failed [%s]: %s", account_id, exc)


def _close_side_from_row(row: dict[str, Any]) -> str | None:
    side = str(row.get("side") or "").lower()
    if side in ("buy", "sell"):
        return side
    return None


def _clamp_reduce_size(sym: str, side: str, qty: float) -> float:
    try:
        from quant.engine.exchanges.bitget.account import fetch_signed_position

        pos = fetch_signed_position(sym)
    except Exception as exc:
        logger.warning("clamp reduce: position fetch failed %s: %s", sym, exc)
        return qty
    if abs(pos) < 1e-12:
        return 0.0
    if side == "sell" and pos > 0:
        return min(qty, abs(pos))
    if side == "buy" and pos < 0:
        return min(qty, abs(pos))
    return 0.0


def row_to_intent(row: dict[str, Any]) -> dict[str, Any] | None:
    action = str(row.get("action") or "").replace("sync_", "")
    if action not in ("open", "increase", "reduce", "close"):
        return None

    bot_id = str(row.get("source") or "")
    bots = allow_bot_ids()
    if bots is not None and bot_id not in bots:
        return {
            "skip": True,
            "reason": "bot_not_allowed",
            "bot_id": bot_id,
            "action": action,
            "coin": row.get("coin"),
        }

    coin = str(row.get("coin") or "")
    detail = hl_coin_map_detail(coin)
    sym = detail.get("bitget")
    if not sym:
        return {
            "skip": True,
            "reason": str(detail.get("skip_reason") or "unmapped_or_filtered"),
            "coin": coin,
            "action": action,
            "bot_id": bot_id,
        }

    try:
        qty = abs(float(row.get("our_sz") or 0)) * scale()
    except (TypeError, ValueError):
        return {"skip": True, "reason": "bad_qty", "coin": coin, "symbol": sym}
    try:
        px = float(row.get("px") or 0)
    except (TypeError, ValueError):
        px = 0.0
    try:
        notion = qty * px if px > 0 else abs(float(row.get("notional") or 0)) * scale()
    except (TypeError, ValueError):
        notion = 0.0

    if qty <= 0:
        return {"skip": True, "reason": "zero_qty", "coin": coin, "symbol": sym}
    if notion > 0 and notion < min_notional():
        return {
            "skip": True,
            "reason": "below_min_notional",
            "coin": coin,
            "symbol": sym,
            "notional": notion,
        }
    cap = max_notional()
    if cap > 0 and notion > cap:
        if px > 0:
            qty = cap / px
            notion = cap
        else:
            return {"skip": True, "reason": "above_max_notional", "coin": coin, "symbol": sym}

    reduce_only = action in ("reduce", "close")
    if reduce_only:
        side = _close_side_from_row(row)
        if not side:
            return {"skip": True, "reason": "missing_close_side", "coin": coin, "symbol": sym}
    else:
        side = str(row.get("side") or "").lower()
        if side not in ("buy", "sell"):
            return {"skip": True, "reason": "missing_side", "coin": coin, "symbol": sym}

    tid = None
    tids = row.get("target_tids") or []
    if isinstance(tids, list) and tids:
        tid = str(tids[0])
    elif row.get("target_tid"):
        tid = str(row.get("target_tid"))
    fp = str(row.get("target_fp") or "") or None
    if not tid and not fp:
        fp = str(row.get("id") or "") or None

    oid = make_client_oid(
        bot_id=bot_id,
        action=action,
        coin=coin,
        tid=tid,
        fp=fp,
    )
    lev = row.get("leverage")
    try:
        lev_i = int(float(lev)) if lev is not None else None
    except (TypeError, ValueError):
        lev_i = None

    return {
        "skip": False,
        "action": action,
        "coin": coin,
        "symbol": sym,
        "side": side,
        "size": qty,
        "notional": notion,
        "reduce_only": reduce_only,
        "client_oid": oid,
        "leverage": lev_i,
        "bot_id": bot_id,
        "target_tid": tid,
        "paper_row_id": row.get("id"),
    }


def execute_intent(intent: dict[str, Any], *, account_id: str = "main") -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    if intent.get("skip"):
        out = {**intent, "ts": now, "status": "skipped", "account_id": account_id}
        if log_skips():
            _append_ledger(out)
        return out

    sym = str(intent["symbol"])
    payload = {
        **intent,
        "ts": now,
        "dry_run": dry_run(),
        "live_enabled": live_enabled(),
        "account_id": account_id,
    }

    if not live_enabled():
        payload["status"] = "disabled"
        return payload

    if dry_run():
        payload["status"] = "dry_run"
        logger.info(
            "HL→Bitget DRY [%s] %s %s %s size=%.6f oid=%s bot=%s",
            account_id,
            intent.get("action"),
            intent.get("side"),
            sym,
            float(intent.get("size") or 0),
            intent.get("client_oid"),
            intent.get("bot_id"),
        )
        _append_ledger(payload)
        return payload

    ready, reason = live_ready()
    if not ready:
        payload["status"] = "blocked"
        payload["error"] = reason
        _append_ledger(payload)
        logger.warning("HL→Bitget blocked: %s", reason)
        return payload

    _ensure_one_way_once(account_id=account_id)
    lk = _symbol_lock(sym, account_id=account_id)
    with lk:
        try:
            from quant.engine.exchanges.bitget.account import place_market_order

            size = float(intent["size"])
            side = str(intent["side"])
            if intent.get("reduce_only"):
                size = _clamp_reduce_size(sym, side, size)
                if size <= 0:
                    payload["status"] = "skipped"
                    payload["reason"] = "no_position_to_reduce"
                    _append_ledger(payload)
                    return payload
                payload["size"] = size

            result = place_market_order(
                symbol=sym,
                side=side,
                size=size,
                client_oid=str(intent["client_oid"]),
                reduce_only=bool(intent.get("reduce_only")),
                leverage=intent.get("leverage"),
            )
            payload["status"] = "deduped" if result.get("deduped") else "sent"
            payload["exchange"] = result
        except Exception as exc:
            logger.exception("HL→Bitget place failed %s [%s]", sym, account_id)
            payload["status"] = "error"
            payload["error"] = str(exc)
    _append_ledger(payload)
    return payload


def apply_mirror_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Execute after paper mirror."""
    if not rows:
        return []
    if not live_enabled():
        return []

    mode = exec_mode()
    # Paper seat reset has no fill delta — always position-sync to flatten.
    if any(str(r.get("action") or "").lower() == "reset" for r in rows):
        if mode == "sub":
            return sync_subaccounts_from_paper(rows)
        return sync_net_from_paper(rows)
    if mode == "sub":
        return sync_subaccounts_from_paper(rows)
    if mode == "net":
        return sync_net_from_paper(rows)

    out: list[dict[str, Any]] = []
    for row in rows:
        if row.get("skipped"):
            continue
        intent = row_to_intent(row)
        if not intent:
            continue
        result = execute_intent(intent)
        out.append(result)
        if result.get("status") == "sent":
            time.sleep(0.05)
    return out


def _load_bot(bot_id: str) -> dict[str, Any]:
    from utils.hl_paper_copy import load_paper

    book = load_paper()
    bot = (book.get("bots") or {}).get(bot_id) or {}
    if bot:
        return bot if isinstance(bot, dict) else {}
    for b in (book.get("bots") or {}).values():
        if str(b.get("id") or "") == bot_id:
            return b if isinstance(b, dict) else {}
    return {}


def _fetch_bitget_equity(env_prefix: str = "") -> float | None:
    """Signed sub-account equity, or None if unavailable."""
    try:
        from quant.engine.exchanges.bitget.account import (
            bitget_creds,
            fetch_account_equity,
            load_creds_from_env,
        )

        creds = load_creds_from_env(env_prefix) if env_prefix else load_creds_from_env("")
        if not creds.ok():
            return None
        with bitget_creds(creds):
            return float(fetch_account_equity().get("equity") or 0)
    except Exception as exc:
        logger.warning("bitget equity fetch failed [%s]: %s", env_prefix or "main", exc)
        return None


def _desired_from_target_book(
    bot: dict[str, Any],
    *,
    route_coins: frozenset[str] | set[str] | None = None,
    route_scale: float = 1.0,
    env_prefix: str = "",
) -> dict[str, float] | None:
    """live_only: our_sz = target_sz × (our_eq / target_equity) × scale.

    ``target_equity`` = perp AV (main+xyz) + Core spot USDC (spot defaults to 0
    until first sample).

    Returns None when sizing inputs are unavailable (caller must NOT flatten).
    Returns {} when target book is flat (caller may flatten).
    """
    from utils.hl_paper_copy import target_sizing_equity

    try:
        av = float(target_sizing_equity(bot) or 0)
    except (TypeError, ValueError):
        av = 0.0
    if av <= 1e-9:
        return None
    tpos = bot.get("target_positions") if isinstance(bot.get("target_positions"), dict) else {}
    # Explicit empty target book → flat (distinguish from missing meta above).
    if not tpos:
        return {}
    eq = _fetch_bitget_equity(env_prefix)
    if eq is None or eq <= 0:
        # dry-run without keys: do not invent size; skip rather than flatten
        return None
    ratio = (eq / av) * float(route_scale or 1.0)
    net: dict[str, float] = {}
    for coin, tp in tpos.items():
        if not isinstance(tp, dict):
            continue
        sym = hl_coin_to_bitget(str(coin), route_coins=route_coins)
        if not sym:
            continue
        try:
            sz = float(tp.get("sz") or 0) * ratio
        except (TypeError, ValueError):
            continue
        if abs(sz) < 1e-16:
            continue
        net[sym] = net.get(sym, 0.0) + sz
    return net


def _paper_equity(bot: dict[str, Any]) -> float:
    try:
        eq = float(bot.get("equity") or 0)
    except (TypeError, ValueError):
        eq = 0.0
    if eq > 1e-9:
        return eq
    try:
        return float(bot.get("balance") or bot.get("paper_balance") or 0)
    except (TypeError, ValueError):
        return 0.0


def _desired_paper_scaled_to_bitget(
    bot: dict[str, Any],
    *,
    route_coins: frozenset[str] | set[str] | None = None,
    route_scale: float = 1.0,
    env_prefix: str = "",
) -> dict[str, float] | None:
    """Paper legs × (bitget_eq / paper_eq) × scale.

    Keeps trade-copy / orphan / risk decisions on paper; live notional follows
    real sub-account equity. None = equity missing (do not flatten).
    """
    paper_eq = _paper_equity(bot)
    positions = bot.get("positions") if isinstance(bot.get("positions"), dict) else {}
    # Paper flat → Bitget should flatten (caller merges open book).
    if not positions or paper_eq <= 1e-9:
        return {}

    bitget_eq = _fetch_bitget_equity(env_prefix)
    if bitget_eq is None:
        if dry_run():
            bitget_eq = paper_eq  # dry-run without keys: 1× paper
        else:
            return None
    if bitget_eq <= 0:
        return None

    factor = (bitget_eq / paper_eq) * float(route_scale or 1.0)
    net: dict[str, float] = {}
    for pos in positions.values():
        if not isinstance(pos, dict):
            continue
        coin = str(pos.get("coin") or "")
        sym = hl_coin_to_bitget(coin, route_coins=route_coins)
        if not sym:
            continue
        try:
            sz = float(pos.get("sz") or 0) * factor
        except (TypeError, ValueError):
            continue
        if abs(sz) < 1e-16:
            continue
        net[sym] = net.get(sym, 0.0) + sz
    return net


def compute_bot_desired(
    bot_id: str,
    *,
    route_coins: frozenset[str] | set[str] | None = None,
    route_scale: float = 1.0,
    env_prefix: str = "",
) -> dict[str, float] | None:
    """Desired Bitget sizes for one bot.

    - live_only (Railway enable): target × (bitget_eq / AV)
    - legacy paper twin: paper legs × (bitget_eq / paper_eq)
    - None = skip sync (sizing unavailable). {} = flat.
    """
    from utils.hl_paper_copy import is_live_only_bot

    bot = _load_bot(bot_id)
    sc = scale() * float(route_scale or 1.0)
    if is_live_only_bot(bot):
        return _desired_from_target_book(
            bot,
            route_coins=route_coins,
            route_scale=sc,
            env_prefix=env_prefix,
        )
    # Enabled Bitget routes are live_only; paper path is legacy/fallback only.
    return _desired_paper_scaled_to_bitget(
        bot,
        route_coins=route_coins,
        route_scale=sc,
        env_prefix=env_prefix,
    )


def compute_net_desired() -> tuple[dict[str, float], dict[str, dict[str, float]]]:
    """Sum paper bot positions → Bitget symbol net size (signed). Legacy net mode."""
    from utils.hl_paper_copy import load_paper

    book = load_paper()
    bots_filter = allow_bot_ids()
    net: dict[str, float] = {}
    parts: dict[str, dict[str, float]] = {}
    for bot in (book.get("bots") or {}).values():
        bid = str(bot.get("id") or "")
        if bots_filter is not None and bid not in bots_filter:
            continue
        for pos in (bot.get("positions") or {}).values():
            coin = str(pos.get("coin") or "")
            sym = hl_coin_to_bitget(coin)
            if not sym:
                continue
            try:
                sz = float(pos.get("sz") or 0) * scale()
            except (TypeError, ValueError):
                continue
            if abs(sz) < 1e-16:
                continue
            net[sym] = net.get(sym, 0.0) + sz
            parts.setdefault(sym, {})[bid] = parts.get(sym, {}).get(bid, 0.0) + sz
    return net, parts


def make_net_client_oid(*, symbol: str, tid: str | None, desired: float, account_id: str = "main") -> str:
    seed = f"sub|{account_id}|{symbol}|{tid or ''}|{desired:.8f}"
    digest = hashlib.sha1(seed.encode()).hexdigest()[:20]
    return f"hs{digest}"


def _coin_from_bitget_symbol(symbol: str) -> str:
    sym = str(symbol or "").upper()
    return sym[:-4] if sym.endswith("USDT") else sym


def leader_leverage_for_symbol(bot_id: str, symbol: str) -> int | None:
    """Resolve HL leader leverage for a Bitget symbol (e.g. BTCUSDT → 20).

    Uses raw leader book leverage — not the paper ``_adjusted_leverage`` asset
    cap (default 10), which would wrongly clamp HIP-3 stocks like GOOGL.
    """
    from utils.hl_paper_copy import _scope_keys_for_coin

    bot = _load_bot(bot_id)
    if not bot:
        return None
    coin = _coin_from_bitget_symbol(symbol)
    if not coin:
        return None
    want = _scope_keys_for_coin(coin)
    lev_map = bot.get("target_lev_by_coin") if isinstance(bot.get("target_lev_by_coin"), dict) else {}
    for key, val in lev_map.items():
        if want & _scope_keys_for_coin(str(key)):
            try:
                return max(1, int(round(float(val))))
            except (TypeError, ValueError):
                break
    tpos = bot.get("target_positions") if isinstance(bot.get("target_positions"), dict) else {}
    for tcoin, tp in tpos.items():
        if not isinstance(tp, dict) or tp.get("leverage") is None:
            continue
        if want & _scope_keys_for_coin(str(tcoin)):
            try:
                return max(1, int(round(float(tp.get("leverage")))))
            except (TypeError, ValueError):
                return None
    return None


def _place_one(
    *,
    symbol: str,
    side: str,
    size: float,
    client_oid: str,
    reduce_only: bool,
    meta: dict[str, Any],
    account_id: str = "main",
    leverage: int | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        **meta,
        "ts": now,
        "account_id": account_id,
        "symbol": symbol,
        "side": side,
        "size": size,
        "reduce_only": reduce_only,
        "client_oid": client_oid,
        "dry_run": dry_run(),
        "live_enabled": live_enabled(),
        "leverage": leverage,
    }
    if size <= 0:
        payload["status"] = "skipped"
        payload["reason"] = "zero_size"
        return payload

    if dry_run():
        payload["status"] = "dry_run"
        logger.info(
            "HL→Bitget [%s] DRY %s %s size=%.6f reduceOnly=%s lev=%s oid=%s desired=%s have=%s",
            account_id,
            side,
            symbol,
            size,
            reduce_only,
            leverage,
            client_oid,
            meta.get("desired"),
            meta.get("have"),
        )
        _append_ledger(payload)
        return payload

    ready, reason = live_ready()
    if not ready:
        payload["status"] = "blocked"
        payload["error"] = reason
        _append_ledger(payload)
        return payload

    _ensure_one_way_once(account_id=account_id)
    lk = _symbol_lock(symbol, account_id=account_id)
    with lk:
        try:
            from quant.engine.exchanges.bitget.account import place_market_order

            qty = size
            if reduce_only:
                qty = _clamp_reduce_size(symbol, side, qty)
                if qty <= 0:
                    payload["status"] = "skipped"
                    payload["reason"] = "no_position_to_reduce"
                    _append_ledger(payload)
                    return payload
                payload["size"] = qty
            result = place_market_order(
                symbol=symbol,
                side=side,
                size=qty,
                client_oid=client_oid,
                reduce_only=reduce_only,
                leverage=None if reduce_only else leverage,
            )
            payload["status"] = "deduped" if result.get("deduped") else "sent"
            payload["exchange"] = result
        except Exception as exc:
            logger.exception("HL→Bitget place failed %s [%s]", symbol, account_id)
            payload["status"] = "error"
            payload["error"] = str(exc)
    _append_ledger(payload)
    return payload


def sync_account_symbol(
    symbol: str,
    desired: float,
    *,
    account_id: str = "main",
    parts: dict[str, float] | None = None,
    trigger_tid: str | None = None,
    mode_tag: str = "sync",
    bot_id: str | None = None,
    leverage: int | None = None,
) -> list[dict[str, Any]]:
    """Move one Bitget account's position to desired (signed)."""
    from quant.engine.exchanges.bitget.account import fetch_signed_position

    eps = 1e-12
    desired = float(desired)
    have = 0.0
    try:
        have = float(fetch_signed_position(symbol))
    except Exception as exc:
        if not dry_run():
            logger.warning("sync fetch pos %s [%s]: %s", symbol, account_id, exc)
            return [
                {
                    "status": "error",
                    "symbol": symbol,
                    "account_id": account_id,
                    "error": f"fetch_position: {exc}",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            ]
        have = 0.0

    lev = leverage
    if lev is None:
        bid = bot_id
        if not bid and parts:
            bid = next(iter(parts.keys()), None)
        if bid:
            lev = leader_leverage_for_symbol(str(bid), symbol)

    delta = desired - have
    meta = {
        "action": mode_tag,
        "desired": desired,
        "have": have,
        "delta": delta,
        "parts": parts or {},
        "trigger_tid": trigger_tid,
        "mode": exec_mode(),
        "leverage": lev,
        "bot_id": bot_id,
    }

    if abs(delta) < eps:
        # Hold existing size/leverage — do not rewrite venue lev on idle sync.
        out = {
            **meta,
            "status": "synced",
            "symbol": symbol,
            "account_id": account_id,
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        if log_skips():
            _append_ledger(out)
        return [out]

    results: list[dict[str, Any]] = []

    if abs(have) > eps and (desired == 0 or have * desired < 0):
        close_side = "sell" if have > 0 else "buy"
        oid_c = make_net_client_oid(
            symbol=symbol, tid=trigger_tid, desired=0.0, account_id=account_id
        ) + "c"
        oid_c = oid_c[:32]
        flat_res = _place_one(
            symbol=symbol,
            side=close_side,
            size=abs(have),
            client_oid=oid_c,
            reduce_only=True,
            meta={**meta, "leg": "flatten"},
            account_id=account_id,
            leverage=None,
        )
        results.append(flat_res)
        st = str(flat_res.get("status") or "")
        flat_ok = st in ("sent", "dry_run", "deduped") or (
            st == "skipped"
            and str(flat_res.get("reason") or "")
            in ("no_position_to_reduce", "zero_size")
        )
        if not flat_ok:
            # Never assume flat after a failed close — opening the other side
            # would create a double-sided book.
            return results
        have = 0.0
        delta = desired - have
        if abs(desired) < eps or abs(delta) < eps:
            return results
        time.sleep(0.05)

    side = "buy" if delta > 0 else "sell"
    reduce_only = abs(have) > eps and abs(desired) < abs(have) - eps and have * desired >= 0
    # Set leader leverage only on flat→open (or flip reopen). Never retouch an
    # already-open symbol's leverage on size-up / idle hold.
    apply_lev = None if reduce_only or abs(have) > eps else lev
    oid = make_net_client_oid(
        symbol=symbol, tid=trigger_tid, desired=desired, account_id=account_id
    )
    results.append(
        _place_one(
            symbol=symbol,
            side=side,
            size=abs(delta),
            client_oid=oid,
            reduce_only=reduce_only,
            meta={**meta, "leg": "adjust"},
            account_id=account_id,
            leverage=apply_lev,
        )
    )
    return results


# Back-compat alias
def sync_net_symbol(
    symbol: str,
    desired: float,
    *,
    parts: dict[str, float] | None = None,
    trigger_tid: str | None = None,
) -> list[dict[str, Any]]:
    return sync_account_symbol(
        symbol,
        desired,
        account_id="main",
        parts=parts,
        trigger_tid=trigger_tid,
        mode_tag="net_sync",
    )


def _trigger_meta(rows: list[dict[str, Any]] | None) -> tuple[set[str], set[str], str | None]:
    """coins/symbols touched + bot ids + first tid."""
    coins: set[str] = set()
    bots: set[str] = set()
    trigger_tid = None
    for row in rows or []:
        if row.get("skipped"):
            continue
        bid = str(row.get("source") or "")
        if bid:
            bots.add(bid)
        c = str(row.get("coin") or "")
        if c:
            coins.add(c)
        tids = row.get("target_tids") or []
        if not trigger_tid and isinstance(tids, list) and tids:
            trigger_tid = str(tids[0])
        elif not trigger_tid and row.get("target_tid"):
            trigger_tid = str(row.get("target_tid"))
    return coins, bots, trigger_tid


def _leader_near_flat_before_burst(
    pre: float,
    post: float,
    *,
    px: float | None = None,
) -> bool:
    """True when leader leg was ≈flat before this debounced fill batch.

    Requires both a small pre/post ratio *and* small pre notional when px is
    known — otherwise a mid-book 50→5050 scale-up (~1%) would look "fresh".
    """
    if abs(pre) <= _FRESH_OPEN_SP_EPS:
        return True
    if abs(post) <= _FRESH_OPEN_SP_EPS:
        return False
    scale = max(abs(post), abs(pre))
    if abs(pre) / scale > _NEAR_FLAT_PRE_RATIO:
        return False
    if px is not None and px > 0:
        return abs(pre) * float(px) <= _NEAR_FLAT_DUST_NOTIONAL
    # No price: only accept near-exact flat (ratio already passed above is not
    # enough alone — tighten to 0.1% of post).
    return abs(pre) / scale <= 0.001


def _batch_is_leader_open(pre: float, post: float) -> bool:
    """Batch must grow inventory (flat→open / dust→open), not reduce/flip."""
    if abs(post) <= _FRESH_OPEN_SP_EPS:
        return False
    if abs(post) <= abs(pre) + _FRESH_OPEN_SP_EPS:
        return False
    if abs(pre) > _FRESH_OPEN_SP_EPS and pre * post < 0:
        return False
    return True


def _iter_copy_signal_rows(rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Rows that may carry leader fill signals (not align / catch_up / skipped)."""
    out: list[dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict) or row.get("skipped"):
            continue
        action = str(row.get("action") or "").strip().lower()
        rid = str(row.get("id") or "")
        if action in ("live_align", "reset", "reset_flat", "catch_up") or rid.startswith(
            "align-"
        ):
            continue
        out.append(row)
    return out


def _row_is_fresh_open(row: dict[str, Any]) -> bool:
    """True only when startPosition≈0 (flat→open).

    Never trust HL Open* alone — mid-book adds are often labeled Open*.
    Missing startPosition must be stamped from snap upstream; if still missing,
    treat as unknown (not fresh) so copy_current=off does not catch up.
    """
    sp = row.get("start_position")
    if sp is None or sp == "":
        return False
    try:
        return abs(float(sp)) <= _FRESH_OPEN_SP_EPS
    except (TypeError, ValueError):
        return False


def _row_is_catch_up(row: dict[str, Any]) -> bool:
    return str(row.get("action") or "").strip().lower() == "catch_up"


def _fresh_open_bitget_symbols(
    rows: list[dict[str, Any]] | None,
    *,
    bot: dict[str, Any] | None = None,
    route_coins: frozenset[str] | set[str] | None = None,
) -> set[str]:
    """Symbols whose batch qualifies as leader flat→open for copy_current=off.

    Per-row startPosition≈0, plus batch inference when debounce coalesces many
    fills but WS drops the first sp=0 tick (SNDK-style miss).

    Manual ``catch_up`` rows are excluded — they use ``_catch_up_force_symbols``
    and must not pollute ``pending_fresh`` (would leave a long-lived orphan gate).
    """
    out: set[str] = set()
    signal_rows = _iter_copy_signal_rows(rows)
    nearest_sp: dict[str, float] = {}
    sum_delta: dict[str, float] = {}
    px_by_sym: dict[str, float] = {}

    for row in signal_rows:
        coin = str(row.get("coin") or "").strip()
        if not coin:
            continue
        sym = hl_coin_to_bitget(coin, route_coins=route_coins)
        if not sym:
            continue
        if _row_is_fresh_open(row):
            out.add(sym)
        try:
            td = float(row.get("target_delta") or 0.0)
        except (TypeError, ValueError):
            td = 0.0
        if abs(td) > 1e-16:
            sum_delta[sym] = sum_delta.get(sym, 0.0) + td
        try:
            px = float(row.get("px") or 0.0)
        except (TypeError, ValueError):
            px = 0.0
        if px > 0 and (sym not in px_by_sym or px > px_by_sym[sym]):
            px_by_sym[sym] = px
        sp = row.get("start_position")
        if sp is None or sp == "":
            continue
        try:
            sp_f = float(sp)
        except (TypeError, ValueError):
            continue
        if sym not in nearest_sp or abs(sp_f) < abs(nearest_sp[sym]):
            nearest_sp[sym] = sp_f

    if bot is None:
        return out

    candidates = set(sum_delta) | set(nearest_sp)
    for sym in candidates:
        post = _leader_sz_for_bitget_sym(bot, sym, route_coins=route_coins)
        if abs(post) <= _FRESH_OPEN_SP_EPS:
            continue
        dlt = sum_delta.get(sym)
        if dlt is not None and abs(dlt) > 1e-16:
            pre = post - dlt
        elif sym in nearest_sp:
            # Incomplete batch (no deltas): earliest visible startPosition.
            pre = nearest_sp[sym]
        else:
            continue
        if not _batch_is_leader_open(pre, post):
            continue
        px = px_by_sym.get(sym)
        if _leader_near_flat_before_burst(pre, post, px=px):
            out.add(sym)

    return out


def _catch_up_force_symbols(
    rows: list[dict[str, Any]] | None,
    *,
    route_coins: frozenset[str] | set[str] | None = None,
) -> set[str]:
    """One-shot catch-up symbols (do not persist into pending_fresh)."""
    out: set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict) or row.get("skipped"):
            continue
        if not _row_is_catch_up(row):
            continue
        coin = str(row.get("coin") or "").strip()
        if not coin:
            continue
        sym = hl_coin_to_bitget(coin, route_coins=route_coins)
        if sym:
            out.add(sym)
    return out


def _pending_fresh_path() -> Path:
    return resolve_data_dir() / "hl_bitget_pending_fresh.json"


def _load_pending_fresh_opens() -> None:
    """Restore pending fresh symbols after process restart."""
    path = _pending_fresh_path()
    if not path.is_file():
        return
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(raw, dict):
        return
    with _pending_fresh_lock:
        for aid, syms in raw.items():
            if not isinstance(syms, list):
                continue
            bucket = {str(s).upper() for s in syms if s}
            if bucket:
                _pending_fresh_opens[str(aid)] = bucket


def _persist_pending_fresh_opens() -> None:
    with _pending_fresh_lock:
        payload = {aid: sorted(syms) for aid, syms in _pending_fresh_opens.items() if syms}
    path = _pending_fresh_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not payload:
            if path.is_file():
                path.unlink()
            return
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        logger.warning("HL→Bitget pending fresh persist failed", exc_info=True)


_pending_fresh_loaded = False


def _ensure_pending_fresh_loaded() -> None:
    global _pending_fresh_loaded
    if _pending_fresh_loaded:
        return
    _load_pending_fresh_opens()
    _pending_fresh_loaded = True


def _mark_pending_fresh_opens(account_id: str, symbols: set[str] | frozenset[str]) -> None:
    if not account_id or not symbols:
        return
    _ensure_pending_fresh_loaded()
    with _pending_fresh_lock:
        bucket = _pending_fresh_opens.setdefault(account_id, set())
        before = len(bucket)
        bucket.update(str(s).upper() for s in symbols)
        changed = len(bucket) != before
    if changed:
        logger.info(
            "HL→Bitget [%s] pending fresh opens += %s",
            account_id,
            sorted(symbols),
        )
        _persist_pending_fresh_opens()


def _clear_pending_fresh_opens(account_id: str, symbols: set[str] | frozenset[str]) -> None:
    if not account_id or not symbols:
        return
    _ensure_pending_fresh_loaded()
    with _pending_fresh_lock:
        bucket = _pending_fresh_opens.get(account_id)
        if not bucket:
            return
        for sym in symbols:
            bucket.discard(str(sym).upper())
        if not bucket:
            _pending_fresh_opens.pop(account_id, None)
    _persist_pending_fresh_opens()


def clear_pending_fresh_account(account_id: str) -> None:
    """Drop all pending fresh opens for a route (leave-live / reset)."""
    if not account_id:
        return
    _ensure_pending_fresh_loaded()
    with _pending_fresh_lock:
        if account_id not in _pending_fresh_opens:
            return
        _pending_fresh_opens.pop(account_id, None)
    _persist_pending_fresh_opens()
    logger.info("HL→Bitget [%s] cleared pending fresh opens", account_id)


def _pending_fresh_open_symbols(account_id: str) -> set[str]:
    if not account_id:
        return set()
    _ensure_pending_fresh_loaded()
    with _pending_fresh_lock:
        return set(_pending_fresh_opens.get(account_id) or ())


def _leader_sz_for_bitget_sym(
    bot: dict[str, Any],
    sym: str,
    *,
    route_coins: frozenset[str] | set[str] | None = None,
) -> float:
    tpos = bot.get("target_positions") if isinstance(bot.get("target_positions"), dict) else {}
    for coin, tp in tpos.items():
        mapped = hl_coin_to_bitget(str(coin), route_coins=route_coins)
        if mapped != sym:
            continue
        if not isinstance(tp, dict):
            continue
        try:
            return float(tp.get("sz") or 0)
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def _schedule_failed_open_retry(
    *,
    account_id: str,
    bot_id: str,
    symbol: str,
    coin: str,
    rows: list[dict[str, Any]] | None,
) -> None:
    """One short retry after a failed open place (same clientOid is idempotent)."""
    key = f"{account_id}|{symbol}"
    now = time.time()
    with _open_retry_lock:
        last = float(_open_retry_at.get(key) or 0.0)
        if now - last < 25.0:
            return
        _open_retry_at[key] = now

    retry_rows = [
        r
        for r in (rows or [])
        if isinstance(r, dict)
        and not r.get("skipped")
        and str(r.get("source") or r.get("bot_id") or "") in ("", bot_id)
        and (
            str(r.get("coin") or "").strip().upper() == coin.upper()
            or hl_coin_to_bitget(str(r.get("coin") or "")) == symbol
        )
    ]
    if not retry_rows:
        retry_rows = [
            {
                "action": "live_sync",
                "source": bot_id,
                "bot_id": bot_id,
                "coin": coin,
                "start_position": 0.0,
                "dir": "Open Long",
                "live_only": True,
            }
        ]
    else:
        # Ensure gate still treats this as a fresh open on retry.
        patched: list[dict[str, Any]] = []
        for r in retry_rows:
            rr = dict(r)
            if rr.get("start_position") in (None, ""):
                rr["start_position"] = 0.0
            patched.append(rr)
        retry_rows = patched

    def _run() -> None:
        time.sleep(3.0)
        logger.warning(
            "HL→Bitget [%s] retry open sync %s bot=%s",
            account_id,
            symbol,
            bot_id,
        )
        with _bg_lock:
            maybe_execute_rows(retry_rows)

    try:
        threading.Thread(
            target=_run, name=f"hl-bitget-retry-{account_id}-{symbol}", daemon=True
        ).start()
    except Exception:
        logger.exception("HL→Bitget open retry dispatch failed %s", symbol)


def _rows_are_live_align(rows: list[dict[str, Any]] | None) -> bool:
    """Enter-live / resume align — never a size-up signal."""
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        action = str(r.get("action") or "").strip().lower()
        rid = str(r.get("id") or "")
        if action == "live_align" or rid.startswith("align-"):
            return True
    return False


def _symbols_with_leader_size_up_signal(
    rows: list[dict[str, Any]] | None,
    open_pos: dict[str, float],
    *,
    route_coins: frozenset[str] | set[str] | None = None,
) -> set[str]:
    """Symbols with a leader fill that extends inventory (event-driven size-up).

    • flat→open (startPosition≈0), or
    • target_delta same sign as current Bitget ``have`` (true add).

    A reduce fill in the batch must NOT unlock ratio top-up. Ignores live_align /
    reset so paper rebuild cannot invent a signal.
    """
    out: set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict) or row.get("skipped"):
            continue
        action = str(row.get("action") or "").strip().lower()
        rid = str(row.get("id") or "")
        if action in ("live_align", "reset", "reset_flat") or rid.startswith("align-"):
            continue
        coin = str(row.get("coin") or "").strip()
        if not coin:
            continue
        sym = hl_coin_to_bitget(coin, route_coins=route_coins)
        if not sym:
            continue
        if _row_is_fresh_open(row):
            out.add(sym)
            continue
        try:
            td = float(row.get("target_delta") or 0.0)
        except (TypeError, ValueError):
            td = 0.0
        if abs(td) <= 1e-16:
            continue
        have = float(open_pos.get(sym) or 0.0)
        # Extending an existing leg, or opening while still flat on Bitget.
        if abs(have) <= 1e-12 or have * td > 0:
            out.add(sym)
    return out


def _symbols_with_leader_reduce_signal(
    rows: list[dict[str, Any]] | None,
    open_pos: dict[str, float],
    *,
    route_coins: frozenset[str] | set[str] | None = None,
) -> set[str]:
    """Symbols where this batch shows leader cutting the Bitget leg we hold."""
    out: set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict) or row.get("skipped"):
            continue
        action = str(row.get("action") or "").strip().lower()
        rid = str(row.get("id") or "")
        if action in ("live_align", "reset", "reset_flat") or rid.startswith("align-"):
            continue
        coin = str(row.get("coin") or "").strip()
        if not coin:
            continue
        sym = hl_coin_to_bitget(coin, route_coins=route_coins)
        if not sym:
            continue
        have = float(open_pos.get(sym) or 0.0)
        if abs(have) <= 1e-12:
            continue
        try:
            td = float(row.get("target_delta") or 0.0)
        except (TypeError, ValueError):
            td = 0.0
        if abs(td) > 1e-16 and have * td < 0:
            out.add(sym)
            continue
        direction = str(row.get("dir") or "").lower()
        if "close" in direction or action in ("reduce", "close"):
            if "short" in direction and have < 0:
                out.add(sym)
            elif "long" in direction and have > 0:
                out.add(sym)
            elif "close" in direction or action in ("reduce", "close"):
                # Generic close/reduce without long/short token.
                out.add(sym)
    return out


def _augment_desired_from_fresh_fills(
    bot: dict[str, Any],
    desired: dict[str, float],
    rows: list[dict[str, Any]] | None,
    *,
    route_coins: frozenset[str] | set[str] | None = None,
    route_scale: float = 1.0,
    env_prefix: str = "",
) -> dict[str, float]:
    """If snap missed a HIP-3 coin, still size flat→open from fill delta × eq/equity."""
    from utils.hl_paper_copy import target_sizing_equity

    out = dict(desired)
    try:
        av = float(target_sizing_equity(bot) or 0)
    except (TypeError, ValueError):
        av = 0.0
    if av <= 1e-9:
        return out
    eq = _fetch_bitget_equity(env_prefix)
    if eq is None or eq <= 0:
        return out
    ratio = (eq / av) * float(route_scale or 1.0)
    for row in rows or []:
        if not isinstance(row, dict) or row.get("skipped"):
            continue
        if not _row_is_fresh_open(row):
            continue
        coin = str(row.get("coin") or "").strip()
        if not coin:
            continue
        sym = hl_coin_to_bitget(coin, route_coins=route_coins)
        if not sym:
            continue
        if abs(float(out.get(sym) or 0.0)) > 1e-12:
            continue
        try:
            td = float(row.get("target_delta") or 0.0)
        except (TypeError, ValueError):
            td = 0.0
        if abs(td) <= 1e-16:
            continue
        sized = td * ratio
        if abs(sized) <= 1e-16:
            continue
        out[sym] = out.get(sym, 0.0) + sized
        logger.info(
            "HL→Bitget seed desired from fresh fill %s td=%s ratio=%.6g -> %.6g",
            sym,
            td,
            ratio,
            out[sym],
        )
    return out


def _gate_desired_no_copy_current(
    bot: dict[str, Any],
    desired: dict[str, float],
    open_pos: dict[str, float],
    rows: list[dict[str, Any]] | None,
    *,
    route_coins: frozenset[str] | set[str] | None = None,
    account_id: str = "",
) -> dict[str, float]:
    """copy_current=false: event-driven Bitget sync (mature hold-without-signal).

    • Flat → open only on true flat→open fills (pending_fresh keeps retries).
      Debounced bursts may infer near-flat open when the first sp≈0 tick is
      missing, but only with dust-notional + inventory-growth checks.
    • Already holding → size-UP only if this batch has a leader fill signal.
      No signal (AV drift, paper rebuild, enter-live align) → hold ``have``.
    • Size-DOWN only on leader reduce/flatten for that coin (fill signal or
      leader_sz≈0). AV-ratio shrink alone must not cut BTC/ETH.
    """
    from utils.hl_paper_copy import _bot_copy_current, is_live_only_bot

    if not is_live_only_bot(bot) or _bot_copy_current(bot):
        return desired

    align_block = _rows_are_live_align(rows)
    fill_signal = _symbols_with_leader_size_up_signal(
        rows, open_pos, route_coins=route_coins
    )
    reduce_signal = _symbols_with_leader_reduce_signal(
        rows, open_pos, route_coins=route_coins
    )
    force_open = _catch_up_force_symbols(rows, route_coins=route_coins)
    batch_fresh = _fresh_open_bitget_symbols(
        rows, bot=bot, route_coins=route_coins
    )
    _mark_pending_fresh_opens(account_id, batch_fresh)
    fresh = set(batch_fresh) | _pending_fresh_open_symbols(account_id)

    # Drop pending only when we hold the leg, or leader is truly flat on it.
    # Do NOT clear on a single want≈0 glitch (sizing/AV blip) — that recreated
    # the J miss: failed open → pending cleared → later adds orphan-skipped.
    clear_syms: set[str] = set()
    for sym in list(fresh):
        have = float(open_pos.get(sym) or 0.0)
        if abs(have) > 1e-12:
            clear_syms.add(sym)
            continue
        leader_sz = _leader_sz_for_bitget_sym(bot, sym, route_coins=route_coins)
        want = float(desired.get(sym) or 0.0)
        if abs(leader_sz) <= 1e-12 and abs(want) <= 1e-12 and sym not in batch_fresh:
            clear_syms.add(sym)
    _clear_pending_fresh_opens(account_id, clear_syms)
    fresh -= clear_syms

    out: dict[str, float] = {}
    for sym, want in desired.items():
        have = float(open_pos.get(sym) or 0.0)
        want_f = float(want)
        if abs(have) > 1e-12:
            leader_sz = _leader_sz_for_bitget_sym(bot, sym, route_coins=route_coins)
            leader_flat = abs(leader_sz) <= 1e-12
            same_side = abs(want_f) > 1e-12 and have * want_f > 0
            size_up = same_side and abs(want_f) > abs(have) + 1e-12
            shrinking = abs(want_f) < abs(have) - 1e-12  # incl. flat / flip / ratio cut
            allow_up = (not align_block) and (sym in fill_signal)
            # Align may shrink/flatten to desired; never size-up on align alone.
            # Normal path: cut only on leader reduce signal or leader flat.
            allow_cut = bool(align_block) or (
                sym in reduce_signal or leader_flat
            )
            if size_up and not allow_up:
                logger.warning(
                    "HL→Bitget [%s] hold (no leader size-up signal) %s "
                    "have=%.6g want=%.6g align=%s",
                    account_id or "?",
                    sym,
                    have,
                    want_f,
                    align_block,
                )
                out[sym] = have
            elif shrinking and not allow_cut:
                # Opposite-side want only if leader actually flipped.
                if abs(want_f) > 1e-12 and have * want_f < 0 and leader_sz * have < 0:
                    out[sym] = want_f
                else:
                    logger.warning(
                        "HL→Bitget [%s] hold (no leader reduce signal) %s "
                        "have=%.6g want=%.6g leader_sz=%.6g align=%s",
                        account_id or "?",
                        sym,
                        have,
                        want_f,
                        leader_sz,
                        align_block,
                    )
                    out[sym] = have
            else:
                out[sym] = want_f
            continue
        if abs(want_f) <= 1e-12:
            continue
        if sym in fresh or sym in force_open:
            out[sym] = want_f
            continue
        logger.info(
            "HL→Bitget [%s] skip orphan open %s want=%.6g (copy_current=off)",
            account_id or "?",
            sym,
            want_f,
        )
    # Leftovers we hold: flatten only when leader is flat on that coin.
    for sym, have in open_pos.items():
        if abs(have) <= 1e-12 or sym in out:
            continue
        leader_sz = _leader_sz_for_bitget_sym(bot, sym, route_coins=route_coins)
        if abs(leader_sz) <= 1e-12:
            out[sym] = float(desired.get(sym) or 0.0)
        else:
            logger.warning(
                "HL→Bitget [%s] hold leftover %s have=%.6g (leader still %.6g)",
                account_id or "?",
                sym,
                have,
                leader_sz,
            )
            out[sym] = have
    return out


def _flatten_disabled_bot_routes(bot_ids: set[str]) -> list[dict[str, Any]]:
    """Force-flat Bitget for seats leaving live (route may already be disabled)."""
    from quant.engine.exchanges.bitget.account import bitget_creds, load_creds_from_env
    from utils.hl_bitget_subaccounts import route_for_flatten

    out: list[dict[str, Any]] = []
    for bid in sorted(bot_ids):
        route = route_for_flatten(bid)
        if route is None:
            out.append(
                {
                    "status": "blocked",
                    "bot_id": bid,
                    "error": "no_bitget_route_for_flatten",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            )
            continue
        clear_pending_fresh_account(route.id)
        creds = load_creds_from_env(route.env_prefix)
        if not creds.ok() and not dry_run():
            out.append(
                {
                    "status": "blocked",
                    "account_id": route.id,
                    "bot_id": bid,
                    "error": "credentials_missing",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            )
            continue
        open_pos: dict[str, float] = {}
        with bitget_creds(creds if creds.ok() else None):
            if creds.ok():
                try:
                    from quant.engine.exchanges.bitget.account import (
                        fetch_all_signed_positions,
                    )

                    open_pos = fetch_all_signed_positions()
                except Exception as exc:
                    logger.warning("flatten open-pos scan failed [%s]: %s", route.id, exc)
                    out.append(
                        {
                            "status": "error",
                            "account_id": route.id,
                            "bot_id": bid,
                            "error": f"fetch_positions: {exc}",
                            "ts": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                    continue
            symbols = {
                sym
                for sym, sz in open_pos.items()
                if abs(sz) > 1e-12
                and route.allows_coin(sym[:-4] if sym.endswith("USDT") else sym)
            }
            if not symbols:
                out.append(
                    {
                        "status": "synced",
                        "account_id": route.id,
                        "bot_id": bid,
                        "action": "reset_flat",
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }
                )
                continue
            logger.warning(
                "HL→Bitget [%s] leave-live flatten bot=%s symbols=%s",
                route.id,
                bid,
                sorted(symbols),
            )
            for sym in sorted(symbols):
                out.extend(
                    sync_account_symbol(
                        sym,
                        0.0,
                        account_id=route.id,
                        parts={bid: 0.0},
                        trigger_tid=None,
                        mode_tag="reset_flat",
                    )
                )
                time.sleep(0.05)
    return out


def sync_subaccounts_from_paper(rows: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Per sub-account: sync that bot's filtered paper book onto its Bitget keys."""
    from quant.engine.exchanges.bitget.account import bitget_creds, load_creds_from_env
    from utils.hl_bitget_subaccounts import enabled_routes, routes_for_bot

    reset_bots = {
        str(r.get("source") or r.get("bot_id") or "").strip()
        for r in (rows or [])
        if str(r.get("action") or "").lower() == "reset"
        and str(r.get("source") or r.get("bot_id") or "").strip()
    }
    if reset_bots:
        return _flatten_disabled_bot_routes(reset_bots)

    touched_coins, touched_bots, trigger_tid = _trigger_meta(rows)
    routes = enabled_routes()
    if not routes:
        logger.warning("HL→Bitget sub mode: no enabled subaccounts")
        return [
            {
                "status": "blocked",
                "error": "no enabled subaccounts",
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        ]

    # Only sync routes whose bot was touched; if no bot in rows, sync all enabled
    if touched_bots:
        routes = [r for r in routes if r.bot_id in touched_bots]
        # also include routes for bots that share coin triggers via routes_for_bot
        if not routes:
            for bid in touched_bots:
                routes.extend(routes_for_bot(bid))
            # dedupe
            seen: set[str] = set()
            uniq = []
            for r in routes:
                if r.id in seen:
                    continue
                seen.add(r.id)
                uniq.append(r)
            routes = uniq

    out: list[dict[str, Any]] = []
    for route in routes:
        creds = load_creds_from_env(route.env_prefix)
        if not creds.ok() and not dry_run():
            out.append(
                {
                    "status": "blocked",
                    "account_id": route.id,
                    "bot_id": route.bot_id,
                    "error": "credentials_missing",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            )
            continue

        desired = compute_bot_desired(
            route.bot_id,
            route_coins=route.coins,
            route_scale=route.scale,
            env_prefix=route.env_prefix,
        )
        if desired is None:
            logger.warning(
                "HL→Bitget skip sync [%s] bot=%s: sizing unavailable (not flattening)",
                route.id,
                route.bot_id,
            )
            out.append(
                {
                    "status": "skipped",
                    "account_id": route.id,
                    "bot_id": route.bot_id,
                    "error": "sizing_unavailable",
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            )
            continue

        # Symbols to touch: trigger coins + desired + (if paper flat) open Bitget book
        symbols: set[str] = set(desired.keys())
        for c in touched_coins:
            sym = hl_coin_to_bitget(c, route_coins=route.coins)
            if sym:
                symbols.add(sym)

        open_pos: dict[str, float] = {}
        with bitget_creds(creds if creds.ok() else None):
            # Always merge open book for this route so paper-flat / reset can flatten
            # leftovers even when other coins still have desired size.
            if creds.ok() and (not touched_bots or route.bot_id in touched_bots):
                try:
                    from quant.engine.exchanges.bitget.account import fetch_all_signed_positions

                    open_pos = fetch_all_signed_positions()
                    for sym, sz in open_pos.items():
                        if abs(sz) < 1e-12:
                            continue
                        base = sym[:-4] if sym.endswith("USDT") else sym
                        if not route.allows_coin(base):
                            continue
                        symbols.add(sym)
                except Exception as exc:
                    logger.warning("sub open-pos scan failed [%s]: %s", route.id, exc)

            bot = _load_bot(route.bot_id)
            # HIP-3/snap miss: seed flat→open size from fill delta when desired lacks coin.
            desired = _augment_desired_from_fresh_fills(
                bot,
                desired,
                rows,
                route_coins=route.coins,
                route_scale=scale() * float(route.scale or 1.0),
                env_prefix=route.env_prefix,
            )
            desired = _gate_desired_no_copy_current(
                bot,
                desired,
                open_pos,
                rows,
                route_coins=route.coins,
                account_id=route.id,
            )
            symbols = set(desired.keys())
            for sym, sz in open_pos.items():
                if abs(sz) > 1e-12:
                    symbols.add(sym)

            if not symbols:
                continue

            for sym in sorted(symbols):
                want = float(desired.get(sym) or 0.0)
                results = sync_account_symbol(
                    sym,
                    want,
                    account_id=route.id,
                    parts={route.bot_id: want},
                    trigger_tid=trigger_tid,
                    mode_tag="sub_sync",
                    bot_id=route.bot_id,
                )
                out.extend(results)
                # Open place failed → keep pending + one short retry (J ZEC miss mode).
                if abs(want) > 1e-12:
                    for res in results:
                        if res.get("status") != "error":
                            continue
                        if res.get("reduce_only"):
                            continue
                        _mark_pending_fresh_opens(route.id, {sym})
                        coin = sym[:-4] if sym.endswith("USDT") else sym
                        _schedule_failed_open_retry(
                            account_id=route.id,
                            bot_id=route.bot_id,
                            symbol=sym,
                            coin=coin,
                            rows=rows,
                        )
                        break
                time.sleep(0.05)
    return out


def sync_net_from_paper(rows: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Legacy: recompute net desires; sync on main BITGET_* account."""
    net, parts = compute_net_desired()
    coins_raw, _, trigger_tid = _trigger_meta(rows)
    coins: set[str] = set()
    for c in coins_raw:
        sym = hl_coin_to_bitget(c)
        if sym:
            coins.add(sym)
    coins.update(net.keys())
    # Paper flat / reset: still discover open main-account positions to flatten.
    if not coins:
        try:
            from quant.engine.exchanges.bitget.account import fetch_all_signed_positions

            for sym, sz in fetch_all_signed_positions().items():
                if abs(sz) < 1e-12:
                    continue
                coins.add(sym)
        except Exception as exc:
            logger.warning("net open-pos scan failed: %s", exc)

    out: list[dict[str, Any]] = []
    for sym in sorted(coins):
        desired = float(net.get(sym) or 0.0)
        out.extend(
            sync_account_symbol(
                sym,
                desired,
                account_id="main",
                parts=parts.get(sym),
                trigger_tid=trigger_tid,
                mode_tag="net_sync",
            )
        )
        time.sleep(0.05)
    return out


def maybe_execute_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    try:
        return apply_mirror_rows(rows)
    except Exception:
        logger.exception("HL Bitget executor failed")
        return []


def _normalize_catch_up_coin(raw: str) -> str:
    """GOOGL / xyz:GOOGL / GOOGLUSDT → HL-ish coin key for mapping."""
    s = str(raw or "").strip()
    if not s:
        return ""
    if s.upper().endswith("USDT") and ":" not in s:
        s = s[:-4]
    return s


def catch_up_orphan_coins(
    bot_id: str,
    coins: list[str] | None = None,
    *,
    refresh_target: bool = True,
) -> dict[str, Any]:
    """One-shot open mid-book orphans without ``copy_current`` full rebalance.

    Requires explicit ``coins``. Uses ``action=catch_up`` force-open (does not
    write ``pending_fresh``). Already-held legs stay put (no AV-ratio cut).
    """
    from utils.hl_bitget_subaccounts import enabled_routes, routes_for_bot
    from utils.hl_paper_copy import is_live_only_bot, refresh_target_health

    bid = str(bot_id or "").strip()
    if not bid:
        return {"ok": False, "error": "bot_id required"}
    if not live_enabled():
        return {"ok": False, "error": "live_disabled"}
    ready, ready_reason = live_ready()
    if not ready and not dry_run():
        return {"ok": False, "error": ready_reason or "live_not_ready"}

    coin_list = [c for c in (coins or []) if str(c or "").strip()]
    if not coin_list:
        return {"ok": False, "error": "coins_required"}

    if refresh_target:
        try:
            refresh_target_health(force=True)
        except Exception as exc:
            logger.warning("catch_up refresh_target_health failed: %s", exc)

    bot = _load_bot(bid)
    if not bot:
        return {"ok": False, "error": f"unknown bot {bid}"}
    if not is_live_only_bot(bot):
        return {"ok": False, "error": "not_live_only"}

    routes = list(routes_for_bot(bid)) or [
        r for r in enabled_routes() if r.bot_id == bid
    ]
    if not routes:
        return {"ok": False, "error": "no_enabled_route", "bot_id": bid}
    route = routes[0]

    desired = compute_bot_desired(
        bid,
        route_coins=route.coins,
        route_scale=route.scale,
        env_prefix=route.env_prefix,
    )
    if desired is None:
        return {"ok": False, "error": "sizing_unavailable", "bot_id": bid}

    from quant.engine.exchanges.bitget.account import bitget_creds, load_creds_from_env

    creds = load_creds_from_env(route.env_prefix)
    open_pos: dict[str, float] = {}
    if creds.ok():
        try:
            from quant.engine.exchanges.bitget.account import fetch_all_signed_positions

            with bitget_creds(creds):
                open_pos = fetch_all_signed_positions()
        except Exception as exc:
            return {"ok": False, "error": f"fetch_positions: {exc}", "bot_id": bid}
    elif not dry_run():
        return {"ok": False, "error": "credentials_missing", "bot_id": bid}

    want_syms: set[str] = set()
    for raw in coin_list:
        coin = _normalize_catch_up_coin(raw)
        if not coin:
            continue
        sym = hl_coin_to_bitget(coin, route_coins=route.coins)
        if not sym:
            return {
                "ok": False,
                "error": f"unmapped_coin:{raw}",
                "bot_id": bid,
            }
        want_syms.add(sym)
    if not want_syms:
        return {"ok": False, "error": "coins_required", "bot_id": bid}

    orphans: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    lev_fix: list[dict[str, Any]] = []
    tpos = bot.get("target_positions") if isinstance(bot.get("target_positions"), dict) else {}

    def _leader_coin_for_sym(sym: str) -> str:
        for tcoin, _tp in tpos.items():
            if hl_coin_to_bitget(str(tcoin), route_coins=route.coins) == sym:
                return str(tcoin)
        return sym[:-4] if sym.endswith("USDT") else sym

    for sym in sorted(want_syms):
        want = float(desired.get(sym) or 0.0)
        have = float(open_pos.get(sym) or 0.0)
        coin = _leader_coin_for_sym(sym)
        if abs(have) > 1e-12:
            # Size already open; optional leverage align only (no resize / flip).
            lev = leader_leverage_for_symbol(bid, sym)
            if lev is not None and lev > 0 and creds.ok() and not dry_run():
                try:
                    from quant.engine.exchanges.bitget.account import (
                        set_symbol_leverage,
                    )

                    lk = _symbol_lock(sym, account_id=route.id)
                    with _bg_lock:
                        with lk:
                            with bitget_creds(creds):
                                set_symbol_leverage(sym, int(lev))
                    lev_fix.append(
                        {"symbol": sym, "leverage": int(lev), "status": "set"}
                    )
                except Exception as exc:
                    lev_fix.append(
                        {
                            "symbol": sym,
                            "leverage": int(lev),
                            "status": "error",
                            "error": str(exc),
                        }
                    )
            orphans.append(
                {
                    "symbol": sym,
                    "coin": coin,
                    "status": "skipped",
                    "reason": "already_open",
                    "have": have,
                    "want": want,
                }
            )
            continue
        if abs(want) <= 1e-12:
            # Explicit coin with no sized desire = snap miss / leader flat.
            return {
                "ok": False,
                "error": f"leader_flat_or_snap_miss:{coin}",
                "bot_id": bid,
                "symbol": sym,
                "have": have,
                "want": want,
            }
        orphans.append(
            {
                "symbol": sym,
                "coin": coin,
                "status": "queued",
                "have": have,
                "want": want,
            }
        )
        rows.append(
            {
                "id": f"catch-up-{bid}-{sym}",
                "action": "catch_up",
                "source": bid,
                "bot_id": bid,
                "coin": coin,
                "start_position": 0.0,
                "dir": "Open Long" if want > 0 else "Open Short",
                "live_only": True,
            }
        )

    if not rows and not lev_fix:
        return {
            "ok": True,
            "bot_id": bid,
            "account_id": route.id,
            "opened": [],
            "orphans": orphans,
            "results": [],
            "leverage_fix": lev_fix,
            "note": "nothing_to_open",
        }

    results: list[dict[str, Any]] = []
    if rows:
        logger.warning(
            "HL→Bitget catch_up bot=%s route=%s symbols=%s",
            bid,
            route.id,
            [r["coin"] for r in rows],
        )
        with _bg_lock:
            results = maybe_execute_rows(rows)
    place_errors = [
        r
        for r in results
        if isinstance(r, dict) and str(r.get("status") or "") == "error"
    ]
    lev_errors = [
        r
        for r in lev_fix
        if isinstance(r, dict) and str(r.get("status") or "") == "error"
    ]
    return {
        "ok": not place_errors and not lev_errors,
        "bot_id": bid,
        "account_id": route.id,
        "opened": [r["coin"] for r in rows],
        "orphans": orphans,
        "results": results,
        "leverage_fix": lev_fix,
        "error": (
            (place_errors[0].get("error") if place_errors else None)
            or (lev_errors[0].get("error") if lev_errors else None)
        ),
    }


def _flush_debounced(gen: int) -> None:
    """Timer callback: sync once using all rows accumulated for this generation."""
    global _debounce_timer
    with _debounce_lock:
        if gen != _debounce_gen:
            return
        batch = list(_debounce_pending)
        _debounce_pending.clear()
        _debounce_timer = None
    if not batch:
        return
    logger.info(
        "HL→Bitget debounce flush n_rows=%s bots=%s",
        len(batch),
        sorted({str(r.get("source") or r.get("bot_id") or "") for r in batch if r}),
    )
    with _bg_lock:
        maybe_execute_rows(batch)


def maybe_execute_rows_async(
    rows: list[dict[str, Any]], *, immediate: bool = False
) -> None:
    """Queue Bitget sync after paper fills. Default: debounce burst fills (~10s).

    immediate=True bypasses debounce (paper reset / risk flatten).
    """
    if not rows or not live_enabled():
        return

    ms = 0.0 if immediate else debounce_ms()
    if ms <= 0:
        def _run() -> None:
            with _bg_lock:
                maybe_execute_rows(rows)

        try:
            threading.Thread(target=_run, name="hl-bitget-exec", daemon=True).start()
        except Exception:
            logger.exception("HL Bitget async dispatch failed")
            maybe_execute_rows(rows)
        return

    global _debounce_timer, _debounce_gen
    with _debounce_lock:
        _debounce_pending.extend(rows)
        _debounce_gen += 1
        gen = _debounce_gen
        if _debounce_timer is not None:
            try:
                _debounce_timer.cancel()
            except Exception:
                pass
        t = threading.Timer(ms / 1000.0, _flush_debounced, args=(gen,))
        t.daemon = True
        _debounce_timer = t
        t.start()


_overlay_diag_at: dict[str, float] = {}
_coin_pnl_at: dict[str, float] = {}
_coin_pnl_cache: dict[str, dict[str, Any]] = {}


def overlay_live_bots(book: dict[str, Any]) -> dict[str, Any]:
    """Mutate API response: fill Bitget live-only seats with wallet/positions.

    Paper→sub seats keep their paper book — do not overlay.
    """
    from utils.hl_paper_copy import is_live_only_bot
    from utils.hl_bitget_subaccounts import enabled_routes, parse_routes, routes_for_bot

    bots = book.get("bots") if isinstance(book.get("bots"), dict) else {}
    if not bots:
        return book
    try:
        from quant.engine.exchanges.bitget.account import (
            aggregate_coin_realized_pnl,
            bitget_creds,
            creds_diag,
            detect_egress_ip,
            fetch_account_equity,
            fetch_all_position_rows,
            fetch_history_positions,
            load_creds_from_env,
        )
    except Exception as exc:
        logger.warning("bitget overlay import failed: %s", exc)
        return book

    for bot in bots.values():
        if not is_live_only_bot(bot):
            continue
        venue = str(bot.get("venue") or "").strip().lower()
        if venue and venue != "bitget":
            continue
        bid = str(bot.get("id") or "")
        routes = routes_for_bot(bid) or [r for r in enabled_routes() if r.bot_id == bid]
        if not routes:
            routes = [r for r in parse_routes() if r.bot_id == bid][:1]
        if not routes:
            bot["live_error"] = "no_bitget_route"
            logger.warning("bitget overlay %s: no_bitget_route", bid)
            continue
        route = routes[0]
        creds = load_creds_from_env(route.env_prefix)
        now = time.time()
        last = _overlay_diag_at.get(bid, 0.0)
        if now - last >= 60:
            _overlay_diag_at[bid] = now
            try:
                egress = detect_egress_ip()
            except Exception:
                egress = ""
            logger.info(
                "bitget overlay diag bot=%s route=%s enabled=%s live=%s dry=%s egress_ip=%s %s",
                bid,
                route.id,
                route.enabled,
                live_enabled(),
                dry_run(),
                egress or "unknown",
                creds_diag(creds, route.env_prefix),
            )
        if not creds.ok():
            bot["live_error"] = "credentials_missing"
            bot["equity"] = None
            bot["balance"] = None
            logger.warning(
                "bitget overlay %s credentials_missing %s",
                bid,
                creds_diag(creds, route.env_prefix),
            )
            continue
        try:
            with bitget_creds(creds):
                eq = fetch_account_equity()
                rows = fetch_all_position_rows()
                # Throttled read-only history for per-coin realized PnL (UI only).
                coin_pnl = _coin_pnl_cache.get(bid)
                last_hist = _coin_pnl_at.get(bid, 0.0)
                if coin_pnl is None or (now - last_hist) >= 60.0:
                    try:
                        hist = fetch_history_positions(limit=100, pages=3)
                        coin_pnl = aggregate_coin_realized_pnl(hist)
                        # Drop coins the seat is not allowed to trade.
                        coin_pnl = {
                            c: v
                            for c, v in coin_pnl.items()
                            if route.allows_coin(c)
                        }
                        _coin_pnl_cache[bid] = coin_pnl
                        _coin_pnl_at[bid] = now
                    except Exception as hist_exc:
                        logger.warning(
                            "bitget overlay %s history-position failed: %s",
                            bid,
                            hist_exc,
                        )
                        if coin_pnl is None:
                            coin_pnl = {}
            bot["live_error"] = None
            bot["equity"] = eq.get("equity")
            bot["balance"] = eq.get("wallet")
            bot["u_pnl"] = eq.get("upnl")
            bot["live_available"] = eq.get("available")
            bot["paper_balance"] = eq.get("wallet")
            bot["coin_pnl"] = coin_pnl if isinstance(coin_pnl, dict) else {}
            try:
                bot["realized_pnl"] = round(
                    sum(
                        float(v.get("realized") or 0)
                        for v in (bot["coin_pnl"] or {}).values()
                        if isinstance(v, dict)
                    ),
                    4,
                )
            except (TypeError, ValueError):
                bot["realized_pnl"] = None
            positions: dict[str, Any] = {}
            for row in rows:
                if not isinstance(row, dict):
                    continue
                try:
                    total = float(row.get("total") or row.get("available") or 0)
                except (TypeError, ValueError):
                    continue
                if abs(total) < 1e-12:
                    continue
                side = str(row.get("holdSide") or "").lower()
                amt = total if side != "short" else -total
                sym = str(row.get("symbol") or "").upper()
                coin = sym[:-4] if sym.endswith("USDT") else sym
                if not route.allows_coin(coin):
                    continue
                try:
                    entry = float(row.get("openPriceAvg") or row.get("averageOpenPrice") or 0)
                except (TypeError, ValueError):
                    entry = 0.0
                try:
                    mark = float(row.get("markPrice") or row.get("marketPrice") or 0)
                except (TypeError, ValueError):
                    mark = entry
                try:
                    upnl = float(row.get("unrealizedPL") or row.get("unrealizedPnl") or 0)
                except (TypeError, ValueError):
                    upnl = 0.0
                try:
                    lev = float(row.get("leverage") or 0) or None
                except (TypeError, ValueError):
                    lev = None
                notional = abs(amt) * mark if mark > 0 else 0.0
                positions[f"{bid}:{coin}"] = {
                    "coin": coin,
                    "sz": amt,
                    "entry_px": entry,
                    "mark_px": mark,
                    "u_pnl": upnl,
                    "leverage": lev,
                    "notional": notional,
                    "source": bid,
                    "venue": "bitget",
                    "live": True,
                }
            bot["positions"] = positions
            bot["live_at"] = datetime.now(timezone.utc).isoformat()
            if now - last >= 60:
                logger.info(
                    "bitget overlay %s ok equity=%s available=%s positions=%d coin_pnl=%d",
                    bid,
                    eq.get("equity"),
                    eq.get("available"),
                    len(positions),
                    len(bot.get("coin_pnl") or {}),
                )
        except Exception as exc:
            msg = str(exc)
            # Short, UI-friendly reason for common Bitget auth failures.
            low = msg.lower()
            if "40018" in msg or "invalid ip" in low:
                bot["live_error"] = "Invalid IP（API Key IP 白名单未放行 Railway）"
            elif "40012" in msg or "password is incorrect" in low or "apikey/password" in low:
                bot["live_error"] = "40012 apikey/password incorrect（Key/Secret/Passphrase 错误或含多余引号空格）"
            elif "40001" in msg or "sign" in low:
                bot["live_error"] = "auth_failed（签名错误）"
            else:
                bot["live_error"] = msg[:160]
            try:
                egress = detect_egress_ip()
            except Exception:
                egress = ""
            logger.warning(
                "bitget overlay %s FAILED live_error=%s egress_ip=%s err=%s %s",
                bid,
                bot["live_error"],
                egress or "unknown",
                msg[:200],
                creds_diag(creds, route.env_prefix),
            )
            bot["equity"] = None
            bot["balance"] = None
            bot["live_available"] = None
    return book


def _desk_client_id() -> str:
    """Seat id for 映仓台: HL_DESK_CLIENT_ID or Railway service name → client_a."""
    raw = (os.getenv("HL_DESK_CLIENT_ID") or "").strip()
    if raw:
        return raw.replace("-", "_").lower()
    svc = (os.getenv("RAILWAY_SERVICE_NAME") or "").strip().lower()
    if svc.startswith("client-") or svc.startswith("client_"):
        return svc.replace("-", "_")
    bots = allow_bot_ids()
    if bots:
        return sorted(bots)[0]
    return "client"


def desk_seat_snapshot() -> dict[str, Any]:
    """One live Bitget seat for desk UI (equity / positions / errors)."""
    seat_id = _desk_client_id()
    # Overlay routes look up bot_c (HL_BITGET_BOT_IDS); rename after fetch.
    mirror_id = "bot_c"
    bots = allow_bot_ids()
    if bots and "bot_c" not in bots:
        mirror_id = sorted(bots)[0]
    seed = {
        "id": mirror_id,
        "live_only": True,
        "paper": False,
        "live": True,
        "venue": "bitget",
        "copy_current": False,
        "positions": {},
    }
    book = {"bots": {mirror_id: seed}, "positions": {}}
    overlay_live_bots(book)
    bot = dict(book.get("bots") or {}).get(mirror_id) or seed
    bot = dict(bot)
    bot["id"] = seat_id
    bot["follow_bot"] = mirror_id
    bot["label"] = seat_id
    # Remap nested position keys to seat id
    pos = bot.get("positions")
    if isinstance(pos, dict) and pos:
        remapped: dict[str, Any] = {}
        for _k, row in pos.items():
            if not isinstance(row, dict):
                continue
            coin = str(row.get("coin") or "").strip() or "UNK"
            row2 = dict(row)
            row2["source"] = seat_id
            remapped[f"{seat_id}:{coin}"] = row2
        bot["positions"] = remapped
    return bot
