"""加载 next-k-api 目录下的 `.env.oi`（文件中的项覆盖已有环境变量，与代码默认一致）。"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def load_env_oi(base_dir: Optional[Path] = None) -> Optional[Path]:
    root = base_dir or Path(__file__).resolve().parent
    path = root / ".env.oi"
    if not path.is_file():
        return None
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                # Strip inline comment (whitespace + # and everything after)
                v = v.split(" #")[0].split("\t#")[0].strip()
                os.environ[k.strip()] = v
    return path
