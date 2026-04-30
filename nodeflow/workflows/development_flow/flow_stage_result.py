from __future__ import annotations

from typing import Any, Dict


def extract_stage_checkpoint_path(stage_result: Dict[str, Any]) -> str | None:
    arts = stage_result.get("artifacts")
    if not isinstance(arts, list):
        return None
    for a in reversed(arts):
        if isinstance(a, dict) and a.get("kind") == "checkpoint":
            p = a.get("path")
            if isinstance(p, str):
                return p
    return None
