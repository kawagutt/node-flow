from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure


def as_path(base: Path, raw: str | None) -> Path | None:
    if not raw:
        return None
    p = Path(raw)
    return p if p.is_absolute() else (base / p).resolve()


def resolve_git_toplevel(path: Path) -> Path:
    cp = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode != 0:
        err = (cp.stderr or cp.stdout or "").strip() or "not a git repository"
        raise NodeExecutionFailure(f"not a git repository: {path}: {err}")
    raw = (cp.stdout or "").strip()
    if not raw:
        raise NodeExecutionFailure(f"git rev-parse --show-toplevel returned empty for {path}")
    return Path(raw).resolve()


def require_same_source_repo(input_repo_root: Path, run_context: Dict[str, Any]) -> Path:
    saved = run_context.get("source_repo_root")
    if not isinstance(saved, str) or not saved.strip():
        raise NodeExecutionFailure("run_context.source_repo_root is required")
    saved_root = resolve_git_toplevel(Path(saved).resolve())
    input_root = resolve_git_toplevel(input_repo_root.resolve())
    if input_root != saved_root:
        raise NodeExecutionFailure(
            "repo_root does not match checkpoint source_repo_root: "
            f"input={input_root}, checkpoint={saved_root}"
        )
    return saved_root
