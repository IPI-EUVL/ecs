# src/ipi_ecs/logging/tui/config.py
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from platformdirs import user_config_dir
from ipi_ecs.logging.viewer import QueryOptions

APP = "ipi-ecs"

def _path() -> Path:
    p = Path(user_config_dir(APP)) / "log_tui.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def load_state() -> dict[str, Any]:
    p = _path()
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))

def save_state(archive: str, opts: QueryOptions) -> None:
    p = _path()
    data = {"archive": archive, "filters": asdict(opts)}
    p.write_text(json.dumps(data, indent=2), encoding="utf-8")
