# next-k-client-exec

Bitget mirror executor. Consumes HL mirror events from Redis and places orders per client.

## Env

```
REDIS_URL=redis://...
HL_EVENT_TRANSPORT=redis
HL_BITGET_LIVE=1
HL_BITGET_DRY_RUN=0
HL_BITGET_ENABLE_BOTS=bot_c
HL_BITGET_BOT_IDS=bot_c
HL_BITGET_ALLOW_COINS=BTC,ETH,...
BITGET_API_KEY=...
BITGET_API_SECRET=...
BITGET_PASSPHRASE=...
HL_EXEC_JITTER_MS=500-2500
DATA_DIR=/app/data
```

Deploy **one service per client KYC** with its own Static Outbound IP and Bitget keys.

## Run

```
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

## Architecture

```
Redis hl:mirror:rows → hl_bitget_executor → Bitget REST
Redis hl:bot:state  → bot_state_store (sizing)
```
