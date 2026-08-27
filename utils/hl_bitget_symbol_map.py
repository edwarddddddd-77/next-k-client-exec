"""HL coin → Bitget USDT-M symbol mapping (crypto + xyz stock/commodity).

Flow:
1. Strip HL prefixes (``xyz:TSLA`` → ``TSLA``)
2. Apply aliases (``SILVER`` → ``XAG``)
3. Build ``{BASE}USDT``
4. If Bitget USDT-M does not list that contract → skip (crypto and stocks alike)

``HL_BITGET_VERIFY_SYMBOLS=0`` disables the listing check (not recommended).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any

from quant.common.kline_cache import norm_symbol
from utils.paths import PROJECT_ROOT, resolve_data_dir

logger = logging.getLogger(__name__)

MAP_NAME = "hl_bitget_symbol_map.json"

# HL ticker (after stripping prefix) → Bitget base coin.
# Only when the USDT-M listing uses a different ticker than HL (never guess
# substring matches: DXY≠DXYZ, XYZ100≠QQQ).
_BUILTIN_ALIASES: dict[str, str] = {
    "SILVER": "XAG",
    "GOLD": "XAU",
    "PLATINUM": "XPT",
    "PALLADIUM": "XPD",
    "BRENTOIL": "BZ",
    "GOOG": "GOOGL",
    "GOOGLE": "GOOGL",
    "SKHX": "SKHYNIX",
    "SMSN": "SAMSUNG",
    "GIGADEV": "GIGADEVICE",
    "NOK": "NOKSTOCK",
    "XYZ100": "NDX100",
}

_contracts_lock = threading.Lock()
_contracts_cache: set[str] | None = None
_contracts_fetched_at = 0.0
_CONTRACTS_TTL_SEC = 3600.0

_skip_log_lock = threading.Lock()
_skip_logged: set[str] = set()


def _env_aliases() -> dict[str, str]:
    """HL_BITGET_COIN_ALIASES=SILVER:XAG,GOLD:XAU"""
    raw = (os.getenv("HL_BITGET_COIN_ALIASES") or "").strip()
    out: dict[str, str] = {}
    if not raw:
        return out
    for part in raw.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        a, b = part.split(":", 1)
        a, b = a.strip().upper(), b.strip().upper()
        if a and b:
            out[a] = b
    return out


def _file_aliases() -> dict[str, str]:
    for path in (resolve_data_dir() / MAP_NAME, PROJECT_ROOT / MAP_NAME):
        if not path.exists():
            continue
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("symbol map load failed %s: %s", path, exc)
            continue
        aliases = doc.get("aliases") if isinstance(doc, dict) else None
        if not isinstance(aliases, dict):
            continue
        return {
            str(k).strip().upper(): str(v).strip().upper()
            for k, v in aliases.items()
            if str(k).strip() and str(v).strip()
        }
    return {}


def coin_aliases() -> dict[str, str]:
    out = dict(_BUILTIN_ALIASES)
    out.update(_file_aliases())
    out.update(_env_aliases())
    return out


def strip_hl_prefix(coin: str) -> str:
    """Normalize HL coin to bare ticker: xyz:TSLA → TSLA, BTC → BTC."""
    raw = str(coin or "").strip()
    if not raw:
        return ""
    if ":" in raw:
        raw = raw.split(":", 1)[1].strip()
    if raw.startswith("@"):
        return ""
    base = raw.upper().split("/")[0]
    if base.endswith("USDT"):
        base = base[:-4]
    return base


def hl_base_ticker(coin: str) -> str:
    """Bare ticker after alias (for allowlists / route coins)."""
    base = strip_hl_prefix(coin)
    if not base:
        return ""
    return coin_aliases().get(base, base)


def verify_symbols_enabled() -> bool:
    raw = (os.getenv("HL_BITGET_VERIFY_SYMBOLS") or "1").strip().lower()
    if not raw:
        return True
    return raw in ("1", "true", "yes", "on")


def _fetch_bitget_usdt_symbols() -> set[str]:
    import requests

    url = "https://api.bitget.com/api/v2/mix/market/contracts"
    resp = requests.get(url, params={"productType": "USDT-FUTURES"}, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if str(data.get("code", "")) != "00000":
        raise RuntimeError(f"bitget contracts: {data.get('code')} {data.get('msg')}")
    rows = data.get("data") or []
    out: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        # Prefer tradable contracts only
        status = str(row.get("symbolStatus") or "normal").strip().lower()
        if status and status not in ("normal", "listed", ""):
            continue
        sym = str(row.get("symbol") or "").strip().upper()
        if sym:
            out.add(sym)
    if not out:
        raise RuntimeError("bitget contracts empty")
    return out


def bitget_contract_set(*, force: bool = False) -> set[str] | None:
    """Cached Bitget USDT-M symbols; None if unavailable (no usable cache)."""
    global _contracts_cache, _contracts_fetched_at
    now = time.time()
    with _contracts_lock:
        if (
            not force
            and _contracts_cache is not None
            and (now - _contracts_fetched_at) < _CONTRACTS_TTL_SEC
        ):
            return _contracts_cache
        try:
            fresh = _fetch_bitget_usdt_symbols()
            _contracts_cache = fresh
            _contracts_fetched_at = now
            logger.info("bitget contracts cached: %d symbols", len(fresh))
            return fresh
        except Exception as exc:
            logger.warning("bitget contracts fetch failed: %s", exc)
            if _contracts_cache is not None:
                return _contracts_cache
            return None


def _log_skip_once(coin: str, sym: str, reason: str) -> None:
    key = f"{reason}|{sym}"
    with _skip_log_lock:
        if key in _skip_logged:
            return
        _skip_logged.add(key)
    logger.info("HL→Bitget auto-skip %s → %s (%s)", coin, sym or "-", reason)


def resolve_bitget_symbol(coin: str) -> tuple[str | None, str | None]:
    """Map HL coin → Bitget symbol.

    Returns ``(symbol, skip_reason)``. ``skip_reason`` is None when mapped OK.
    Reasons: empty_coin | bad_prefix | not_on_bitget | contracts_unavailable
    """
    raw = str(coin or "").strip()
    if not raw:
        return None, "empty_coin"
    if raw.startswith("@"):
        return None, "bad_prefix"

    base = hl_base_ticker(raw)
    if not base:
        return None, "bad_prefix"

    sym = norm_symbol(base)

    if not verify_symbols_enabled():
        return sym, None

    known = bitget_contract_set()
    if known is None:
        # Fail closed: do not place on unknown listings (crypto or stocks)
        _log_skip_once(raw, sym, "contracts_unavailable")
        return None, "contracts_unavailable"
    if sym not in known:
        _log_skip_once(raw, sym, "not_on_bitget")
        return None, "not_on_bitget"
    return sym, None


def map_hl_coin_to_bitget(coin: str) -> str | None:
    """Return Bitget symbol or None if missing / unlisted (crypto + stocks)."""
    sym, _reason = resolve_bitget_symbol(coin)
    return sym


def is_bitget_listed(coin: str) -> bool:
    return map_hl_coin_to_bitget(coin) is not None


def describe_mapping(coin: str) -> dict[str, Any]:
    raw = str(coin or "")
    stripped = strip_hl_prefix(raw)
    aliased = hl_base_ticker(raw)
    mapped, reason = resolve_bitget_symbol(raw)
    return {
        "coin": raw,
        "stripped": stripped,
        "aliased": aliased,
        "bitget": mapped,
        "ok": mapped is not None,
        "skip_reason": reason,
    }
