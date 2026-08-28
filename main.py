"""Client exec — consume HL mirror events and trade on Bitget."""

from __future__ import annotations

import logging
import os
import sys
from contextlib import asynccontextmanager

from env_loader import load_env_oi

load_env_oi()

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.concurrency import run_in_threadpool

from utils.rate_limit import MinIntervalGuard

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)
_catch_up_cooldown = MinIntervalGuard("HL_CATCH_UP_COOLDOWN_SEC", 60.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting next-k-client-exec...")
    try:
        from hl_exec.consumer import exec_consumer

        exec_consumer.start()
    except Exception as exc:
        logger.warning("exec consumer startup skipped: %s", exc)
    yield
    try:
        from hl_exec.consumer import exec_consumer

        exec_consumer.stop()
    except Exception as exc:
        logger.warning("exec consumer shutdown skipped: %s", exc)
    logger.info("Shutting down next-k-client-exec...")


app = FastAPI(
    title="Next K Client Exec",
    description="Bitget mirror executor for HL copy events.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    from utils.hl_bitget_executor import status

    return {"ok": True, "service": "next-k-client-exec", "live": status()}


@app.get("/live/status")
def live_status_route():
    from utils.hl_bitget_executor import status

    return status()


@app.get("/live/desk")
def live_desk_route():
    """映仓台卡片：本 KYC 的 Bitget 权益/仓位（overlay）。"""
    from utils.hl_bitget_executor import desk_seat_snapshot, status

    seat = desk_seat_snapshot()
    return {
        "ok": True,
        "client_id": seat.get("id"),
        "bot": seat,
        "live": status(),
    }


@app.post("/live/catch-up")
async def post_hl_bitget_catch_up(
    bot_id: str = Query(..., description="live seat id, e.g. bot_c"),
    coins: str = Query(..., description="comma-separated HL coins, e.g. xyz:GOOGL"),
    refresh: bool = Query(True, description="refresh leader snapshot from ingest before sizing"),
):
    from utils.hl_bitget_executor import catch_up_orphan_coins

    allowed, wait = _catch_up_cooldown.check_allow()
    if not allowed:
        raise HTTPException(status_code=429, detail=f"catch_up cooldown, retry in {wait:.0f}s")

    coin_list = [c.strip() for c in str(coins or "").split(",") if c.strip()]
    if not coin_list:
        raise HTTPException(status_code=400, detail="coins_required")

    out = await run_in_threadpool(
        lambda: catch_up_orphan_coins(str(bot_id or "").strip(), coin_list, refresh_target=bool(refresh))
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or "catch_up_failed")
    _catch_up_cooldown.mark_used()
    return out


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
