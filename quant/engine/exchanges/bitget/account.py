"""Bitget USDT 永续账户 REST（vnpy 实盘）。

Supports optional per-call credentials via ``bitget_creds()`` context
(for HL sub-account routing). Default = process env BITGET_*.
"""

from __future__ import annotations

import base64
import contextvars
import hashlib
import hmac
import json
import logging
import os
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

import requests

from quant.common.kline_cache import norm_symbol

logger = logging.getLogger(__name__)

_PRODUCT_TYPE = "USDT-FUTURES"
_MARGIN_COIN = "USDT"

_BITGET_BASES = {
    "REAL": "https://api.bitget.com",
    "DEMO": "https://api.bitget.com",
}


@dataclass(frozen=True)
class BitgetCreds:
    api_key: str
    api_secret: str
    passphrase: str
    server: str = "REAL"

    def ok(self) -> bool:
        return bool(self.api_key and self.api_secret and self.passphrase)


_creds_ctx: contextvars.ContextVar[Optional[BitgetCreds]] = contextvars.ContextVar(
    "bitget_creds", default=None
)


@contextmanager
def bitget_creds(creds: Optional[BitgetCreds]) -> Iterator[None]:
    """Temporarily use sub-account (or other) API keys for REST calls."""
    token = _creds_ctx.set(creds)
    try:
        yield
    finally:
        _creds_ctx.reset(token)


def _env_raw(name: str) -> Optional[str]:
    return os.getenv(name)


def _env_clean(name: str, default: str = "") -> str:
    """Strip whitespace and accidental surrounding quotes from Railway/env values."""
    raw = _env_raw(name)
    if raw is None:
        return default
    s = str(raw).strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        s = s[1:-1].strip()
    return s


def _env_had_wrapping_quotes(name: str) -> bool:
    raw = _env_raw(name)
    if raw is None:
        return False
    s = str(raw).strip()
    return len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"')


def load_creds_from_env(prefix: str = "") -> BitgetCreds:
    """Load keys from env. prefix='' → BITGET_*; prefix='BITGET_SUB_BTC' → BITGET_SUB_BTC_API_KEY etc."""
    p = (prefix or "").strip().rstrip("_")
    if p:
        key = _env_clean(f"{p}_API_KEY")
        sec = _env_clean(f"{p}_API_SECRET")
        pwd = _env_clean(f"{p}_PASSPHRASE") or _env_clean(f"{p}_API_PASSPHRASE")
        server = (_env_clean(f"{p}_SERVER") or _env_clean("BITGET_SERVER", "REAL")).upper()
    else:
        key = _env_clean("BITGET_API_KEY")
        sec = _env_clean("BITGET_API_SECRET")
        pwd = _env_clean("BITGET_PASSPHRASE") or _env_clean("BITGET_API_PASSPHRASE")
        server = _env_clean("BITGET_SERVER", "REAL").upper()
    if server not in _BITGET_BASES:
        server = "REAL"
    return BitgetCreds(api_key=key, api_secret=sec, passphrase=pwd, server=server)


def creds_diag(creds: BitgetCreds, env_prefix: str = "") -> str:
    """Safe one-line fingerprint for Railway logs (never logs full secrets)."""
    p = (env_prefix or "").strip().rstrip("_")
    if p:
        key_n, sec_n = f"{p}_API_KEY", f"{p}_API_SECRET"
        pwd_n = f"{p}_PASSPHRASE" if _env_raw(f"{p}_PASSPHRASE") is not None else f"{p}_API_PASSPHRASE"
    else:
        key_n, sec_n = "BITGET_API_KEY", "BITGET_API_SECRET"
        pwd_n = (
            "BITGET_PASSPHRASE"
            if _env_raw("BITGET_PASSPHRASE") is not None
            else "BITGET_API_PASSPHRASE"
        )
    k = creds.api_key or ""
    s = creds.api_secret or ""
    pw = creds.passphrase or ""
    quoted = [n for n in (key_n, sec_n, pwd_n) if _env_had_wrapping_quotes(n)]
    missing: list[str] = []
    if not k:
        missing.append(key_n)
    if not s:
        missing.append(sec_n)
    if not pw:
        missing.append(pwd_n)
    proxy = _proxies()
    proxy_s = "off"
    if proxy:
        host = _env_clean("BITGET_PROXY_HOST")
        port = _env_clean("BITGET_PROXY_PORT", "0")
        proxy_s = f"{host}:{port}"
    return (
        f"prefix={p or 'BITGET'} key={k[:10]}…{k[-4:] if len(k) >= 14 else ''} "
        f"len_key/sec/pwd={len(k)}/{len(s)}/{len(pw)} "
        f"pwd_first_last_ord={ord(pw[0]) if pw else -1}/{ord(pw[-1]) if pw else -1} "
        f"quoted_env={quoted or 'none'} missing={missing or 'none'} "
        f"server={creds.server} proxy={proxy_s}"
    )


_egress_ip_cache: tuple[float, str] = (0.0, "")


def detect_egress_ip(force: bool = False) -> str:
    """Best-effort public egress IP (for Bitget whitelist debugging). Cached 5 min."""
    global _egress_ip_cache
    now = time.time()
    ts, ip = _egress_ip_cache
    if not force and ip and now - ts < 300:
        return ip
    for url in (
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
    ):
        try:
            r = requests.get(url, timeout=5, proxies=_proxies())
            text = (r.text or "").strip()
            if r.status_code < 400 and text and " " not in text and len(text) < 64:
                _egress_ip_cache = (now, text)
                return text
        except Exception:
            continue
    return ip or ""


def _active_creds() -> BitgetCreds:
    override = _creds_ctx.get()
    if override is not None:
        return override
    return load_creds_from_env("")


def _api_key() -> str:
    return _active_creds().api_key


def _api_secret() -> str:
    return _active_creds().api_secret


def _passphrase() -> str:
    return _active_creds().passphrase


def _base_url() -> str:
    server = _active_creds().server.strip().upper()
    return _BITGET_BASES.get(server, _BITGET_BASES["REAL"])


def _sign(timestamp: str, method: str, path: str, body: str) -> str:
    payload = f"{timestamp}{method.upper()}{path}{body}"
    digest = hmac.new(_api_secret().encode(), payload.encode(), hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


def _headers(timestamp: str, sign: str) -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "ACCESS-KEY": _api_key(),
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": timestamp,
        "ACCESS-PASSPHRASE": _passphrase(),
    }


def _proxies() -> Optional[Dict[str, str]]:
    """Optional egress via BITGET_PROXY_HOST/PORT (must be one of API-key whitelist IPs)."""
    host = _env_clean("BITGET_PROXY_HOST")
    port_s = _env_clean("BITGET_PROXY_PORT", "0")
    try:
        port = int(port_s or "0")
    except ValueError:
        port = 0
    if not host or port <= 0:
        return None
    user = _env_clean("BITGET_PROXY_USER")
    pwd = _env_clean("BITGET_PROXY_PASS")
    if user:
        auth = f"{user}:{pwd}@" if pwd else f"{user}@"
    else:
        auth = ""
    url = f"http://{auth}{host}:{port}"
    return {"http": url, "https": url}


def _signed_request(method: str, path: str, params: Optional[Dict[str, Any]] = None, body: Optional[Dict[str, Any]] = None) -> Any:
    creds = _active_creds()
    if not creds.ok():
        raise RuntimeError("BITGET_API_KEY/SECRET/PASSPHRASE not configured")
    query = ""
    req_path = path
    if method.upper() == "GET" and params:
        parts = [f"{k}={v}" for k, v in sorted(params.items())]
        query = "?" + "&".join(parts)
        req_path = path + query
    body_s = json.dumps(body or {}, separators=(",", ":")) if method.upper() != "GET" else ""
    ts = str(int(time.time() * 1000))
    sign = _sign(ts, method, req_path, body_s)
    url = f"{_base_url()}{req_path}"
    headers = _headers(ts, sign)
    proxies = _proxies()
    if method.upper() == "GET":
        resp = requests.get(url, headers=headers, timeout=15, proxies=proxies)
    else:
        resp = requests.post(url, headers=headers, data=body_s, timeout=15, proxies=proxies)
    if resp.status_code >= 400:
        try:
            payload = resp.json()
        except Exception:
            payload = resp.text
        code = ""
        msg = ""
        if isinstance(payload, dict):
            code = str(payload.get("code") or "")
            msg = str(payload.get("msg") or "")
        if code in ("40012", "40018", "40036", "40037", "40038") or "invalid ip" in msg.lower():
            egress = ""
            try:
                egress = detect_egress_ip()
            except Exception:
                egress = ""
            logger.warning(
                "bitget AUTH_FAIL code=%s path=%s msg=%s egress_ip=%s %s",
                code or "?",
                path,
                msg[:120],
                egress or "unknown",
                creds_diag(creds),
            )
        raise RuntimeError(f"Bitget {path} HTTP {resp.status_code}: {payload}")
    data = resp.json()
    if str(data.get("code", "")) != "00000":
        raise RuntimeError(f"Bitget {path} code={data.get('code')} {data.get('msg')}")
    return data.get("data", data)


def set_symbol_leverage(symbol: str, leverage: int) -> None:
    """Set USDT-M leverage to match the leader (cross / one-way).

    Also sets longLeverage+shortLeverage so hedge-mode accounts do not keep a
    stale side-specific leverage when ``leverage`` alone is ignored.
    """
    sym = norm_symbol(symbol)
    lev = str(max(1, int(leverage)))
    body = {
        "symbol": sym,
        "productType": _PRODUCT_TYPE,
        "marginCoin": _MARGIN_COIN,
        "leverage": lev,
        "longLeverage": lev,
        "shortLeverage": lev,
    }
    try:
        _signed_request(
            "POST",
            "/api/v2/mix/account/set-leverage",
            body=body,
        )
        logger.info("[vnpy] bitget leverage %s -> %sx", sym, lev)
    except RuntimeError as exc:
        msg = str(exc).lower()
        if "no change" in msg or "not modified" in msg:
            return
        # Retry with only ``leverage`` (some accounts reject long/short fields).
        try:
            _signed_request(
                "POST",
                "/api/v2/mix/account/set-leverage",
                body={
                    "symbol": sym,
                    "productType": _PRODUCT_TYPE,
                    "marginCoin": _MARGIN_COIN,
                    "leverage": lev,
                },
            )
            logger.info("[vnpy] bitget leverage %s -> %sx (leverage-only)", sym, lev)
            return
        except RuntimeError as exc2:
            msg2 = str(exc2).lower()
            if "no change" in msg2 or "not modified" in msg2:
                return
            raise exc2 from exc


def ensure_one_way_mode() -> None:
    try:
        _signed_request(
            "POST",
            "/api/v2/mix/account/set-position-mode",
            body={"productType": _PRODUCT_TYPE, "posMode": "one_way_mode"},
        )
        logger.info("[vnpy] bitget position mode -> one-way")
    except RuntimeError as exc:
        msg = str(exc).lower()
        if "no change" in msg or "not modified" in msg:
            return
        raise


def fetch_account_equity() -> Dict[str, float]:
    """USDT-M equity snapshot for live desk display / live-only sizing.

    Raises RuntimeError on API/auth failure (caller should surface live_error).
    """
    data: Any = None
    last_exc: Exception | None = None
    try:
        data = _signed_request(
            "GET",
            "/api/v2/mix/account/accounts",
            params={"productType": _PRODUCT_TYPE},
        )
    except RuntimeError as exc:
        last_exc = exc
        try:
            data = _signed_request(
                "GET",
                "/api/v2/mix/account/account",
                params={
                    "productType": _PRODUCT_TYPE,
                    "marginCoin": _MARGIN_COIN,
                    "symbol": "BTCUSDT",
                },
            )
            last_exc = None
        except RuntimeError as exc2:
            last_exc = exc2
    if last_exc is not None:
        logger.warning("[vnpy] bitget account equity failed: %s", last_exc)
        raise last_exc
    if isinstance(data, list):
        # Prefer USDT marginCoin row when present.
        picked = None
        for row in data:
            if not isinstance(row, dict):
                continue
            if str(row.get("marginCoin") or "").upper() == _MARGIN_COIN:
                picked = row
                break
        data = picked or (data[0] if data else {})
    if not isinstance(data, dict):
        return {"equity": 0.0, "wallet": 0.0, "upnl": 0.0, "available": 0.0}

    def _f(*keys: str) -> float:
        for k in keys:
            if data.get(k) is None:
                continue
            try:
                return float(data.get(k) or 0)
            except (TypeError, ValueError):
                continue
        return 0.0

    equity = _f("accountEquity", "usdtEquity", "equity")
    avail = _f("available", "crossedMaxAvailable", "availableBalance")
    upnl = _f("unrealizedPL", "unrealizedPnl", "crossedUnrealizedPnl")
    wallet = _f("accountBalance", "crossedBalance", "walletBalance")
    if wallet <= 0 and equity > 0:
        wallet = max(0.0, equity - upnl)
    if equity <= 0 and wallet > 0:
        equity = wallet + upnl
    return {
        "equity": round(equity, 6),
        "wallet": round(wallet, 6),
        "upnl": round(upnl, 6),
        "available": round(avail, 6),
    }


def fetch_history_positions(
    *,
    limit: int = 100,
    pages: int = 3,
) -> List[Dict[str, Any]]:
    """Closed / historical USDT-M positions (read-only; for per-coin realized PnL).

    GET /api/v2/mix/position/history-position — does not place or cancel orders.
    """
    out: List[Dict[str, Any]] = []
    id_less: Optional[str] = None
    lim = max(1, min(100, int(limit or 100)))
    pages_n = max(1, min(10, int(pages or 1)))
    for _ in range(pages_n):
        params: Dict[str, Any] = {
            "productType": _PRODUCT_TYPE,
            "limit": str(lim),
        }
        if id_less:
            params["idLessThan"] = str(id_less)
        data = _signed_request(
            "GET",
            "/api/v2/mix/position/history-position",
            params=params,
        )
        rows: Any = data
        end_id = None
        if isinstance(data, dict):
            rows = data.get("list") if isinstance(data.get("list"), list) else []
            end_id = data.get("endId") or data.get("end_id")
        if not isinstance(rows, list) or not rows:
            break
        for row in rows:
            if isinstance(row, dict):
                out.append(row)
        if not end_id or str(end_id) == str(id_less or ""):
            break
        if len(rows) < lim:
            break
        id_less = str(end_id)
    return out


def aggregate_coin_realized_pnl(
    rows: List[Dict[str, Any]] | None,
) -> Dict[str, Dict[str, Any]]:
    """Sum historical position PnL by base coin (BTC from BTCUSDT).

    Prefer netProfit (pnl + funding + fees); fall back to pnl.
    """
    by_coin: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "").upper()
        if not sym:
            continue
        coin = sym[:-4] if sym.endswith("USDT") else sym
        if not coin:
            continue
        slot = by_coin.get(coin)
        if slot is None:
            slot = {
                "coin": coin,
                "realized": 0.0,
                "closes": 0,
                "last_ts": None,
                "last_ms": 0,
            }
            by_coin[coin] = slot
        pnl = None
        for key in ("netProfit", "net_profit", "pnl", "achievedProfits"):
            if row.get(key) is None or row.get(key) == "":
                continue
            try:
                pnl = float(row.get(key))
                break
            except (TypeError, ValueError):
                continue
        if pnl is not None:
            slot["realized"] = round(float(slot["realized"]) + pnl, 6)
        slot["closes"] = int(slot["closes"]) + 1
        ms = 0
        for key in ("uTime", "utime", "cTime", "ctime"):
            raw = row.get(key)
            if raw is None or raw == "":
                continue
            try:
                ms = int(float(raw))
                break
            except (TypeError, ValueError):
                continue
        if ms > int(slot.get("last_ms") or 0):
            slot["last_ms"] = ms
            try:
                from datetime import datetime, timezone

                slot["last_ts"] = datetime.fromtimestamp(
                    ms / 1000.0, tz=timezone.utc
                ).isoformat()
            except Exception:
                slot["last_ts"] = str(raw)
    for slot in by_coin.values():
        slot.pop("last_ms", None)
        slot["realized"] = round(float(slot.get("realized") or 0), 4)
    return by_coin


def fetch_all_position_rows() -> List[Dict[str, Any]]:
    """Raw non-flat USDT-M position rows (for desk overlay)."""
    try:
        rows = _signed_request(
            "GET",
            "/api/v2/mix/position/all-position",
            params={"productType": _PRODUCT_TYPE, "marginCoin": _MARGIN_COIN},
        )
    except RuntimeError as exc:
        logger.warning("[vnpy] bitget all-position failed: %s", exc)
        return []
    return rows if isinstance(rows, list) else []


def fetch_all_signed_positions() -> Dict[str, float]:
    """All non-zero one-way signed sizes on the active Bitget account."""
    out: Dict[str, float] = {}
    for row in fetch_all_position_rows():
        if not isinstance(row, dict):
            continue
        sym = norm_symbol(str(row.get("symbol") or ""))
        if not sym:
            continue
        total = float(row.get("total") or row.get("available") or 0.0)
        if abs(total) < 1e-12:
            continue
        side = str(row.get("holdSide") or "").lower()
        signed = total if side != "short" else -total
        out[sym] = signed
    return out


def fetch_position_snapshots(symbols: List[str]) -> Dict[str, Dict[str, float]]:
    want = {norm_symbol(s) for s in symbols}
    out: Dict[str, Dict[str, float]] = {}
    try:
        rows = _signed_request(
            "GET",
            "/api/v2/mix/position/all-position",
            params={"productType": _PRODUCT_TYPE, "marginCoin": _MARGIN_COIN},
        )
    except RuntimeError as exc:
        logger.warning("[vnpy] bitget position list failed: %s", exc)
        return out
    if not isinstance(rows, list):
        return out
    for row in rows:
        sym = norm_symbol(str(row.get("symbol") or ""))
        if sym not in want:
            continue
        total = float(row.get("total") or row.get("available") or 0.0)
        if abs(total) < 1e-12:
            continue
        side = str(row.get("holdSide") or "").lower()
        signed = total if side != "short" else -total
        out[sym] = {
            "amount": signed,
            "entry": float(row.get("openPriceAvg") or row.get("averageOpenPrice") or 0.0),
        }
    return out


def fetch_position_amounts(symbols: List[str]) -> Dict[str, float]:
    return {sym: float(snap.get("amount") or 0.0) for sym, snap in fetch_position_snapshots(symbols).items()}


def fetch_signed_position(symbol: str) -> float:
    """One-way signed base size for a single symbol (long +, short -)."""
    sym = norm_symbol(symbol)
    snaps = fetch_position_snapshots([sym])
    return float((snaps.get(sym) or {}).get("amount") or 0.0)


# Order-detail "no such clientOid" — must return None so place_market_order proceeds.
# Bitget wording is "cannot be found" (not "not found"); code 40109 is the stable signal.
_ORDER_NOT_FOUND_CODES = frozenset({"40109", "40015", "43001"})
_ORDER_NOT_FOUND_MSG = (
    "not exist",
    "not found",
    "cannot be found",
    "does not exist",
    "no order",
)
_BITGET_CODE_RE = re.compile(r"['\"]code['\"]\s*:\s*['\"]?(\d+)", re.I)


def is_order_not_found_error(exc: BaseException | str) -> bool:
    """True when Bitget order-detail says this clientOid has no order yet."""
    msg = str(exc or "")
    msg_l = msg.lower()
    m = _BITGET_CODE_RE.search(msg)
    if m and m.group(1) in _ORDER_NOT_FOUND_CODES:
        return True
    if any(f"code={c}" in msg_l for c in _ORDER_NOT_FOUND_CODES):
        return True
    return any(k in msg_l for k in _ORDER_NOT_FOUND_MSG)


def get_order_by_client_oid(symbol: str, client_oid: str) -> Optional[Dict[str, Any]]:
    """Return order detail if clientOid already used; None if not found."""
    sym = norm_symbol(symbol)
    oid = str(client_oid or "").strip()
    if not oid:
        return None
    try:
        data = _signed_request(
            "GET",
            "/api/v2/mix/order/detail",
            params={
                "symbol": sym,
                "productType": _PRODUCT_TYPE,
                "clientOid": oid,
            },
        )
    except RuntimeError as exc:
        if is_order_not_found_error(exc):
            return None
        # Ambiguous: do not place duplicate blindly — surface error to caller
        raise
    if not data:
        return None
    if isinstance(data, dict):
        # Some responses wrap empty
        if not data.get("clientOid") and not data.get("orderId") and not data.get("orderid"):
            return None
        return data
    return None


def place_market_order(
    *,
    symbol: str,
    side: str,
    size: float,
    client_oid: str,
    reduce_only: bool = False,
    leverage: Optional[int] = None,
) -> Dict[str, Any]:
    """Place USDT-M market order (same REST path as vnpy Bitget gateway).

    side: buy|sell (one-way mode)
    size: base-coin quantity
    client_oid: idempotent id (Bitget typically ≤64; keep ≤32)
    """
    sym = norm_symbol(symbol)
    side_l = str(side or "").strip().lower()
    if side_l not in ("buy", "sell"):
        raise ValueError(f"invalid side: {side}")
    qty = float(size)
    if qty <= 0:
        raise ValueError("size must be > 0")
    oid = str(client_oid or "").strip()
    if not oid:
        raise ValueError("client_oid required")
    if len(oid) > 32:
        oid = oid[:32]

    existing = get_order_by_client_oid(sym, oid)
    if existing:
        return {"deduped": True, "order": existing, "clientOid": oid, "symbol": sym}

    if leverage is not None and int(leverage) > 0 and not reduce_only:
        # Opens must use the leader leverage — never silently keep account default.
        set_symbol_leverage(sym, int(leverage))

    # Size string: trim trailing zeros; Bitget rejects excess precision per symbol.
    size_s = f"{qty:.8f}".rstrip("0").rstrip(".")
    if not size_s or size_s == "0":
        raise ValueError(f"size rounds to zero: {qty}")

    body = {
        "symbol": sym,
        "productType": _PRODUCT_TYPE,
        "marginCoin": _MARGIN_COIN,
        "marginMode": "crossed",
        "side": side_l,
        "orderType": "market",
        "size": size_s,
        "clientOid": oid,
        "reduceOnly": "YES" if reduce_only else "NO",
    }
    # One retry on transient transport/5xx — same clientOid stays idempotent.
    last_exc: Exception | None = None
    data = None
    for attempt in range(2):
        try:
            data = _signed_request("POST", "/api/v2/mix/order/place-order", body=body)
            last_exc = None
            break
        except Exception as exc:
            last_exc = exc
            msg = str(exc).lower()
            transient = any(
                k in msg
                for k in (
                    "timeout",
                    "timed out",
                    "temporarily",
                    "502",
                    "503",
                    "504",
                    "connection",
                    "reset by peer",
                )
            )
            if attempt == 0 and transient:
                logger.warning(
                    "[vnpy] bitget place retry %s oid=%s: %s", sym, oid, exc
                )
                time.sleep(0.4)
                # Another worker may have filled the same oid meanwhile.
                again = get_order_by_client_oid(sym, oid)
                if again:
                    return {
                        "deduped": True,
                        "order": again,
                        "clientOid": oid,
                        "symbol": sym,
                        "retried": True,
                    }
                continue
            raise
    if last_exc is not None:
        raise last_exc
    logger.info(
        "[vnpy] bitget place %s %s size=%s reduceOnly=%s oid=%s -> %s",
        sym,
        side_l,
        size_s,
        body["reduceOnly"],
        oid,
        data,
    )
    return {"deduped": False, "order": data, "clientOid": oid, "symbol": sym, "size": size_s, "side": side_l}


def _lane_leverage(cfg) -> int:
    lev = float(getattr(cfg, "live_leverage", 0.0) or 0.0)
    if lev > 0:
        return int(lev)
    return 5


def ensure_pool_leverage(symbols: List[str], cfg) -> None:
    ensure_one_way_mode()
    lev = _lane_leverage(cfg)
    for raw in symbols:
        set_symbol_leverage(norm_symbol(raw), lev)
