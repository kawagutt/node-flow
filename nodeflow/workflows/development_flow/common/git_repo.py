"""Git repository path helpers for development_flow."""

from __future__ import annotations

import subprocess
from pathlib import Path

from nodeflow.core.base_node import NodeExecutionFailure


def resolve_git_toplevel(path: Path) -> Path:
    """Resolve the git working tree top-level directory for ``path`` (may be a subdirectory)."""
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
