"""Prepare/reuse workspace for development_flow execution."""

from __future__ import annotations

import subprocess
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


def _status_has_non_ignored_changes(status_text: str, ignored_prefixes: List[str]) -> bool:
    for raw in status_text.splitlines():
        line = raw.rstrip("\r\n")
        if not line:
            continue
        parsed = _parse_porcelain_v1_line(line)
        old_path = parsed["old_path"]
        new_path = parsed["new_path"]
        if old_path != new_path:
            if _is_ignored_path(old_path, ignored_prefixes) and _is_ignored_path(
                new_path, ignored_prefixes
            ):
                continue
            return True
        if _is_ignored_path(parsed["path"], ignored_prefixes):
            continue
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


class PrepareWorkspaceNode(PythonActionNode):
    role = "prepare_workspace"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        raw_source_repo_root = inputs.get("source_repo_root")
        if not isinstance(raw_source_repo_root, str) or not raw_source_repo_root.strip():
            raise NodeExecutionFailure("source_repo_root is required")
        source_repo_root = Path(raw_source_repo_root.strip()).resolve()
        if not source_repo_root.exists():
            raise NodeExecutionFailure(f"source_repo_root does not exist: {source_repo_root}")
        run_context = (
            inputs.get("run_context") if isinstance(inputs.get("run_context"), dict) else {}
        )
        existing_workspace = (
            inputs.get("workspace_context")
            if isinstance(inputs.get("workspace_context"), dict)
            else None
        )
        strategy = str(params.get("strategy") or "current_repo").strip() or "current_repo"

        cp = _run_git(source_repo_root, ["rev-parse", "--is-inside-work-tree"])
        if cp.returncode != 0:
            raise NodeExecutionFailure(
                f"source_repo_root is not a git repository: {source_repo_root}"
            )
        source_repo_root = _resolve_git_toplevel(source_repo_root)

        if strategy != "current_repo":
            raise NodeExecutionFailure(f"unsupported prepare_workspace.strategy: {strategy}")

        planned_branch_name = str(run_context.get("planned_branch_name") or "").strip()
        if not planned_branch_name:
            raise NodeExecutionFailure("prepare_workspace requires run_context.planned_branch_name")
        cp_ref = _run_git(source_repo_root, ["check-ref-format", "--branch", planned_branch_name])
        if cp_ref.returncode != 0:
            raise NodeExecutionFailure(f"invalid planned_branch_name: {planned_branch_name}")
        source_base_revision = str(run_context.get("source_base_revision") or "").strip()
        if not source_base_revision:
            raise NodeExecutionFailure("run_context.source_base_revision is required")
        source_current_branch = str(run_context.get("source_current_branch") or "").strip()
        if not source_current_branch:
            raise NodeExecutionFailure("run_context.source_current_branch is required")
        run_source_repo_root_raw = str(run_context.get("source_repo_root") or "").strip()
        if not run_source_repo_root_raw:
            raise NodeExecutionFailure("run_context.source_repo_root is required")
        run_source_repo_root = _resolve_git_toplevel(Path(run_source_repo_root_raw).resolve())
        if run_source_repo_root != source_repo_root:
            raise NodeExecutionFailure(
                "source_repo_root does not match run_context.source_repo_root"
            )

        # Fresh workspace prep requires a clean source tree. Reuse (rework_implementation) skips
        # this so current_repo can rework with an in-tree working tree dirty vs base_revision.
        if existing_workspace is None:
            cp_status = _run_git(source_repo_root, ["status", "--porcelain"])
            if cp_status.returncode != 0:
                err = (cp_status.stderr or cp_status.stdout or "").strip() or "git status failed"
                raise NodeExecutionFailure(err)
            ignored_prefixes = params.get("ignored_dirty_prefixes")
            if isinstance(ignored_prefixes, list):
                prefixes = [str(x) for x in ignored_prefixes if isinstance(x, str)]
            else:
                prefixes = _default_ignored_dirty_prefixes()
            if _status_has_non_ignored_changes((cp_status.stdout or ""), prefixes):
                raise NodeExecutionFailure(
                    "source repository is dirty; commit/stash changes before starting development_flow"
                )

        if existing_workspace:
            prev_current_branch = existing_workspace.get("current_branch")
            prev_root = existing_workspace.get("workspace_root")
            if not isinstance(prev_root, str) or Path(prev_root).resolve() != source_repo_root:
                raise NodeExecutionFailure(
                    "existing workspace_context.workspace_root does not match source_repo_root"
                )
            prev_source = existing_workspace.get("source_repo_root")
            if not isinstance(prev_source, str) or Path(prev_source).resolve() != source_repo_root:
                raise NodeExecutionFailure(
                    "existing workspace_context.source_repo_root does not match source_repo_root"
                )
            prev_strategy = existing_workspace.get("strategy")
            if prev_strategy != "current_repo":
                raise NodeExecutionFailure(
                    f"existing workspace_context.strategy must be 'current_repo', got {prev_strategy!r}"
                )
            prev_planned = existing_workspace.get("planned_branch_name")
            if (
                isinstance(prev_planned, str)
                and prev_planned.strip()
                and prev_planned != planned_branch_name
            ):
                raise NodeExecutionFailure(
                    "existing workspace_context.planned_branch_name does not match run_context"
                )
            prev_base = existing_workspace.get("base_revision")
            if not isinstance(prev_base, str) or not prev_base.strip():
                raise NodeExecutionFailure("existing workspace_context.base_revision is invalid")
            if prev_base.strip() != source_base_revision:
                raise NodeExecutionFailure(
                    "workspace_context.base_revision must match run_context.source_base_revision"
                )
            # Reuse always keeps the original base_revision from workspace_context.
            # Resume-time base_ref is intentionally ignored.
            base_revision = prev_base.strip()
            check_base = _run_git(
                source_repo_root, ["rev-parse", "--verify", f"{base_revision}^{{commit}}"]
            )
            if check_base.returncode != 0:
                raise NodeExecutionFailure(f"base_revision is invalid: {base_revision}")
        else:
            prev_current_branch = None
            check_in = _run_git(
                source_repo_root, ["rev-parse", "--verify", f"{source_base_revision}^{{commit}}"]
            )
            if check_in.returncode != 0:
                raise NodeExecutionFailure(
                    "run_context.source_base_revision is not a valid commit in source_repo_root: "
                    f"{source_base_revision}"
                )
            base_revision = (check_in.stdout or "").strip()
            if not base_revision:
                raise NodeExecutionFailure(
                    "failed to resolve run_context.source_base_revision to commit"
                )
            head_cp = _run_git(source_repo_root, ["rev-parse", "HEAD"])
            if head_cp.returncode != 0:
                err = (head_cp.stderr or head_cp.stdout or "").strip() or "failed to resolve HEAD"
                raise NodeExecutionFailure(err)
            head_now = (head_cp.stdout or "").strip()
            if head_now != base_revision:
                raise NodeExecutionFailure(
                    "source repository HEAD changed since flow start; "
                    "restart development_flow or align HEAD with run_context.source_base_revision"
                )

        cp_branch = _run_git(source_repo_root, ["branch", "--show-current"])
        if cp_branch.returncode != 0:
            err = (cp_branch.stderr or cp_branch.stdout or "").strip() or "failed to resolve branch"
            raise NodeExecutionFailure(err)
        current_branch = (cp_branch.stdout or "").strip()
        if not current_branch:
            raise NodeExecutionFailure("detached HEAD is not supported for current_repo workspace")
        if not existing_workspace and current_branch != source_current_branch:
            raise NodeExecutionFailure(
                "source branch changed since flow start; "
                f"expected={source_current_branch}, current={current_branch}"
            )
        if existing_workspace:
            if not isinstance(prev_current_branch, str) or not prev_current_branch.strip():
                raise NodeExecutionFailure("existing workspace_context.current_branch is required")
            if current_branch != prev_current_branch:
                raise NodeExecutionFailure(
                    "current_repo workspace branch changed since previous checkpoint: "
                    f"previous={prev_current_branch}, current={current_branch}"
                )

        return {
            "workspace_context": {
                "strategy": "current_repo",
                "source_repo_root": str(source_repo_root),
                "workspace_root": str(source_repo_root),
                "current_branch": current_branch,
                "planned_branch_name": planned_branch_name,
                "base_revision": base_revision,
            }
        }
