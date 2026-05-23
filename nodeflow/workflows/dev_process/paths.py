"""Path helpers for dev-process runs."""

from __future__ import annotations

import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from nodeflow.core.base_node import NodeExecutionFailure


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


def abs_path(path: str | Path) -> str:
    return str(Path(path).resolve())


def slugify(text: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", text.lower())
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized or "dev-process"


def next_run_index(runs_dir: Path) -> int:
    max_index = 0
    if runs_dir.exists():
        for child in runs_dir.iterdir():
            if not child.is_dir():
                continue
            m = re.match(r"^(\d+)_", child.name)
            if m:
                max_index = max(max_index, int(m.group(1)))
    return max_index + 1


_RUN_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def validate_run_id(run_id: str) -> str:
    """Reject path separators and other unsafe characters in user-supplied run_id."""
    if not run_id or not _RUN_ID_RE.fullmatch(run_id):
        raise NodeExecutionFailure(
            f"run_id {run_id!r} contains unsafe characters; " "allowed: [A-Za-z0-9_.-]+"
        )
    return run_id


def allocate_run_dir(
    repo_root: Path,
    *,
    task_prompt: str,
    run_id: str,
    artifact_root_dir: str = ".nodeflow/runs",
) -> tuple[str, str]:
    """Return (artifact_root absolute, run_dir_name)."""
    validate_run_id(run_id)
    runs_base = Path(artifact_root_dir)
    if not runs_base.is_absolute():
        runs_base = (repo_root / runs_base).resolve()
    runs_base.mkdir(parents=True, exist_ok=True)
    idx = next_run_index(runs_base)
    yyyymmdd = datetime.now(timezone.utc).strftime("%Y%m%d")
    slug = slugify(task_prompt.splitlines()[0] if task_prompt.strip() else run_id)
    short_hash = run_id[-6:] if len(run_id) >= 6 else run_id
    run_dir_name = f"{idx:03d}_{yyyymmdd}_{slug}_{short_hash}"
    artifact_root = runs_base / run_dir_name
    artifact_root.mkdir(parents=True, exist_ok=True)
    for sub in ("spec_plan", "implement", "review", "summary", "checkpoints", "evidence"):
        (artifact_root / sub).mkdir(parents=True, exist_ok=True)
    return abs_path(artifact_root), run_dir_name


def checkpoint_path_under_artifact_root(artifact_root: str, filename: str) -> Path:
    """Resolve a flow checkpoint path strictly under ``artifact_root/checkpoints/``."""
    if not filename or filename in (".", ".."):
        raise NodeExecutionFailure(f"invalid checkpoint filename: {filename!r}")
    if "/" in filename or "\\" in filename or Path(filename).name != filename:
        raise NodeExecutionFailure(f"checkpoint filename must be a bare name: {filename!r}")
    root = Path(artifact_root).resolve()
    checkpoints = (root / "checkpoints").resolve()
    cp = (checkpoints / filename).resolve()
    try:
        cp.relative_to(checkpoints)
    except ValueError as e:
        raise NodeExecutionFailure(f"checkpoint path escapes checkpoints/: {cp}") from e
    return cp


def assert_path_under_run_dir(run_dir: str, path: str) -> None:
    root = Path(run_dir).resolve()
    target = Path(path).resolve()
    try:
        target.relative_to(root)
    except ValueError as e:
        raise NodeExecutionFailure(f"path escapes run artifact_root: {target}") from e


def git_head_revision(repo_root: Path) -> str:
    cp = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode != 0:
        raise NodeExecutionFailure(f"git rev-parse HEAD failed in {repo_root}")
    return (cp.stdout or "").strip()


def git_current_branch(repo_root: Path) -> str:
    cp = subprocess.run(
        ["git", "-C", str(repo_root), "branch", "--show-current"],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode != 0:
        raise NodeExecutionFailure(f"git branch --show-current failed in {repo_root}")
    return (cp.stdout or "").strip() or "HEAD"


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
