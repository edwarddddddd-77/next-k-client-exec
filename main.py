"""Client exec — consume HL mirror events and trade on Bitget."""

from __future__ import annotations

import logging
import os
import sys
from contextlib import asynccontextmanager

from env_loader import load_env_oi

load_env_oi()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


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


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
