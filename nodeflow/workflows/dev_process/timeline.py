"""Append-only timeline.jsonl writer."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


def timeline_path(artifact_root: str) -> Path:
    return Path(artifact_root) / "timeline.jsonl"


def append_event(artifact_root: str, run_id: str, event: str, **fields: Any) -> None:
    row: Dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "event": event,
    }
    row.update(fields)
    path = timeline_path(artifact_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")
