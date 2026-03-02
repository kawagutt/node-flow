"""
NodeFlow v1.4.4 — ロード＋実行の入口。execution 層。
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

from .loader import load_pipeline


def load_and_kick_pipeline(
    workspace_dir: str,
    pipeline_path: str,
    initial_inputs: Dict[str, Any] | None = None,
    params: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Load root pipeline from YAML and execute. Returns output dict (or {})."""
    if not os.path.isabs(pipeline_path):
        pipeline_path = str(Path(workspace_dir) / pipeline_path)
    root = load_pipeline(workspace_dir, pipeline_path)
    return root.execute(initial_inputs or {}, params or {})
