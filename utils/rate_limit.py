"""进程内最小间隔限流（防 OI 刷新等重型接口被刷）。"""

from __future__ import annotations

import os
import threading
import time


class MinIntervalGuard:
    """两次 mark_used 之间至少间隔 min_sec 秒；min_sec=0 表示不限制。"""

    def __init__(self, env_key: str, default_sec: float) -> None:
        raw = os.getenv(env_key, str(default_sec))
        try:
            self.min_sec = max(0.0, float(str(raw).strip() or default_sec))
        except ValueError:
            self.min_sec = max(0.0, float(default_sec))
        self._env_key = env_key
        self._last_mark: float = 0.0
        self._lock = threading.Lock()

    def check_allow(self) -> tuple[bool, float]:
        """是否允许本次操作；不允许时返回 (False, 建议等待秒数)。"""
        if self.min_sec <= 0:
            return True, 0.0
        now = time.monotonic()
        with self._lock:
            if self._last_mark > 0 and (now - self._last_mark) < self.min_sec:
                return False, self.min_sec - (now - self._last_mark)
            return True, 0.0

    def mark_used(self) -> None:
        with self._lock:
            self._last_mark = time.monotonic()
