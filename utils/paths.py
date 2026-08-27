from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def resolve_data_dir() -> Path:
    raw = (os.getenv("DATA_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser()
    for candidate in (Path("/app/data"), Path("/data")):
        if candidate.is_dir():
            return candidate
    return PROJECT_ROOT
