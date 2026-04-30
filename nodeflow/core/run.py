"""
NodeFlow — load + execute entry (core).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure

from .loader import load_pipeline


def _resolve_pipeline_path(workspace_dir: str, pipeline_path: str) -> str:
    """Prefer workspace-relative path; fall back to cwd-relative if the file exists there."""
    p = Path(pipeline_path)
    if p.is_absolute():
        return str(p)
    under_ws = Path(workspace_dir) / pipeline_path
    if under_ws.is_file():
        return str(under_ws.resolve())
    if p.is_file():
        return str(p.resolve())
    return str(under_ws)


def load_and_kick_pipeline(
    workspace_dir: str,
    pipeline_path: str,
    initial_inputs: Dict[str, Any] | None = None,
    params: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Load root pipeline from YAML and execute.

    Top-level execution is fail-fast: non-``done`` root status raises
    ``NodeExecutionFailure`` so CLI callers cannot silently treat fatal/limit as success.
    """
    pipeline_path = _resolve_pipeline_path(workspace_dir, pipeline_path)
    root = load_pipeline(workspace_dir, pipeline_path)
    effective_params = dict(params or {})
    effective_params["_workspace_dir"] = str(Path(workspace_dir).resolve())
    result = root.execute(initial_inputs or {}, effective_params)
    status = root.read_status()
    if status != "done":
        err = root.read_error()
        if err is not None:
            reason = f"pipeline ended with status={status}: {err}"
        else:
            reason = f"pipeline ended with status={status}"
        raise NodeExecutionFailure(reason)
    return result
