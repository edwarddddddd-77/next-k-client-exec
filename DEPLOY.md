# Railway 部署 — client-exec

完整步骤见同目录 sibling 仓库 **next-k-hl-event/DEPLOY.md**。

本服务职责：订阅 Redis `mirror_batch` → 整本 paper + rows → Bitget 执行。

**切流前**务必在 next-k-api 设 `HL_COPY_ENABLED=0` 且 `HL_BITGET_LIVE=0`，避免双下单。
