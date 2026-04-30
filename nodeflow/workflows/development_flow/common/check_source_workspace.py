"""Validate source repository readiness for development_flow."""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.workflows.development_flow.common.git_repo import resolve_git_toplevel
from nodeflow.workflows.development_flow.common.git_status import (
    default_ignored_dirty_prefixes,
    status_violates_start_policy,
)


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

        source_repo_root = resolve_git_toplevel(source_repo_root)

        status_cp = _run_git(source_repo_root, ["status", "--porcelain"])
        if status_cp.returncode != 0:
            err = (status_cp.stderr or status_cp.stdout or "").strip() or "git status failed"
            raise NodeExecutionFailure(err)
        ignored_prefixes = params.get("ignored_dirty_prefixes")
        if isinstance(ignored_prefixes, list):
            prefixes = [str(x) for x in ignored_prefixes if isinstance(x, str)]
        else:
            prefixes = default_ignored_dirty_prefixes()
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
        if status_violates_start_policy(
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
