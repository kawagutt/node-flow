"""Validate source repository readiness for development_flow."""

from __future__ import annotations

import subprocess
from fnmatch import fnmatch
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PythonActionNode


def _resolve_git_toplevel(path: Path) -> Path:
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


def _default_ignored_dirty_prefixes() -> List[str]:
    return [".nodeflow/"]


def _is_ignored_path(path: str, ignored_prefixes: List[str]) -> bool:
    return any(path.startswith(prefix) for prefix in ignored_prefixes)


def _parse_porcelain_v1_line(line: str) -> Dict[str, str]:
    xy = line[:2] if len(line) >= 2 else ""
    path_part = line[3:].strip() if len(line) >= 3 else line.strip()
    if path_part.startswith('"') and path_part.endswith('"'):
        path_part = path_part[1:-1]
    if " -> " in path_part:
        old_path, new_path = path_part.split(" -> ", 1)
        old_path = old_path.strip().strip('"')
        new_path = new_path.strip().strip('"')
        return {"xy": xy, "path": new_path, "old_path": old_path, "new_path": new_path}
    return {"xy": xy, "path": path_part, "old_path": path_part, "new_path": path_part}


def _status_violates_start_policy(
    status_text: str,
    *,
    ignored_prefixes: List[str],
    fail_on_tracked_changes: bool,
    fail_on_untracked: bool,
    allowed_untracked_prefixes: List[str],
    blocked_untracked_globs: List[str],
) -> bool:
    for raw in status_text.splitlines():
        line = raw.rstrip("\r\n")
        if not line:
            continue
        parsed = _parse_porcelain_v1_line(line)
        xy = parsed["xy"]
        path = parsed["path"]
        old_path = parsed["old_path"]
        new_path = parsed["new_path"]
        if _is_ignored_path(old_path, ignored_prefixes) and _is_ignored_path(
            new_path, ignored_prefixes
        ):
            continue

        is_untracked = xy == "??"
        if is_untracked:
            if any(fnmatch(path, pattern) for pattern in blocked_untracked_globs):
                return True
            if any(path.startswith(prefix) for prefix in allowed_untracked_prefixes):
                continue
            if fail_on_untracked:
                return True
            continue

        if fail_on_tracked_changes:
            return True
    return False


def _run_git(cwd: Path, argv: List[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *argv],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )


class CheckSourceWorkspaceNode(PythonActionNode):
    role = "check_source_workspace"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        raw_source_repo_root = inputs.get("source_repo_root")
        if not isinstance(raw_source_repo_root, str) or not raw_source_repo_root.strip():
            raise NodeExecutionFailure("source_repo_root is required")
        source_repo_root = Path(raw_source_repo_root).resolve()
        if not source_repo_root.exists():
            raise NodeExecutionFailure(f"source_repo_root does not exist: {source_repo_root}")

        cp = _run_git(source_repo_root, ["rev-parse", "--is-inside-work-tree"])
        if cp.returncode != 0:
            raise NodeExecutionFailure(
                f"source_repo_root is not a git repository: {source_repo_root}"
            )

        source_repo_root = _resolve_git_toplevel(source_repo_root)

        status_cp = _run_git(source_repo_root, ["status", "--porcelain"])
        if status_cp.returncode != 0:
            err = (status_cp.stderr or status_cp.stdout or "").strip() or "git status failed"
            raise NodeExecutionFailure(err)
        ignored_prefixes = params.get("ignored_dirty_prefixes")
        if isinstance(ignored_prefixes, list):
            prefixes = [str(x) for x in ignored_prefixes if isinstance(x, str)]
        else:
            prefixes = _default_ignored_dirty_prefixes()
        fail_on_tracked_changes = bool(params.get("fail_on_tracked_changes", True))
        fail_on_untracked = bool(params.get("fail_on_untracked", False))
        allowed_untracked_prefixes_raw = params.get("allowed_untracked_prefixes")
        if isinstance(allowed_untracked_prefixes_raw, list):
            allowed_untracked_prefixes = [
                str(x) for x in allowed_untracked_prefixes_raw if isinstance(x, str)
            ]
        else:
            allowed_untracked_prefixes = []
        blocked_untracked_globs_raw = params.get("blocked_untracked_globs")
        if isinstance(blocked_untracked_globs_raw, list):
            blocked_untracked_globs = [
                str(x) for x in blocked_untracked_globs_raw if isinstance(x, str)
            ]
        else:
            blocked_untracked_globs = []
        if _status_violates_start_policy(
            (status_cp.stdout or ""),
            ignored_prefixes=prefixes,
            fail_on_tracked_changes=fail_on_tracked_changes,
            fail_on_untracked=fail_on_untracked,
            allowed_untracked_prefixes=allowed_untracked_prefixes,
            blocked_untracked_globs=blocked_untracked_globs,
        ):
            raise NodeExecutionFailure(
                "source repository is dirty; commit/stash changes before starting development_flow"
            )

        branch_cp = _run_git(source_repo_root, ["branch", "--show-current"])
        if branch_cp.returncode != 0:
            err = (branch_cp.stderr or branch_cp.stdout or "").strip() or "failed to resolve branch"
            raise NodeExecutionFailure(err)
        current_branch = (branch_cp.stdout or "").strip()
        if not current_branch:
            raise NodeExecutionFailure("detached HEAD is not supported for development_flow start")

        rev_cp = _run_git(source_repo_root, ["rev-parse", "HEAD"])
        if rev_cp.returncode != 0:
            err = (
                rev_cp.stderr or rev_cp.stdout or ""
            ).strip() or "failed to resolve base revision"
            raise NodeExecutionFailure(err)
        base_revision = (rev_cp.stdout or "").strip()
        if not base_revision:
            raise NodeExecutionFailure("failed to resolve base revision")

        return {
            "source_workspace_check": {
                "source_repo_root": str(source_repo_root),
                "current_branch": current_branch,
                "base_revision": base_revision,
                "clean": True,
            }
        }
