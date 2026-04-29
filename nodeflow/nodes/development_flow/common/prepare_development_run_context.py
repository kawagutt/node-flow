"""Prepare run-scoped context for development_flow."""

from __future__ import annotations

import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.nodes.development_flow.common.git_repo import resolve_git_toplevel


class PrepareDevelopmentRunContextNode(PythonActionNode):
    role = "prepare_development_run_context"

    @staticmethod
    def _resolve_development_name(inputs: Dict[str, Any], params: MappingProxyType) -> str:
        explicit = str(inputs.get("development_name") or "").strip()
        if explicit:
            return explicit
        from_params = str(params.get("development_name") or "").strip()
        if from_params:
            return from_params
        task_prompt = str(inputs.get("task_prompt") or "")
        first_line = task_prompt.splitlines()[0].strip() if task_prompt.strip() else ""
        if first_line:
            return first_line
        return "development-flow"

    @staticmethod
    def _slugify(text: str) -> str:
        normalized = re.sub(r"[^a-z0-9]+", "-", text.lower())
        normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
        return normalized or "development-flow"

    @staticmethod
    def _next_index(runs_dir: Path) -> int:
        max_index = 0
        if runs_dir.exists():
            for child in runs_dir.iterdir():
                if not child.is_dir():
                    continue
                m = re.match(r"^(\d+)_", child.name)
                if not m:
                    continue
                max_index = max(max_index, int(m.group(1)))
        return max_index + 1

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        if "run_id" in params:
            raise NodeExecutionFailure(
                "prepare_development_run_context does not accept params.run_id; use input run_id"
            )
        source_workspace_check = (
            inputs.get("source_workspace_check")
            if isinstance(inputs.get("source_workspace_check"), dict)
            else {}
        )
        if not source_workspace_check:
            raise NodeExecutionFailure("source_workspace_check is required")
        source_repo_root_raw = source_workspace_check.get("source_repo_root")
        if not isinstance(source_repo_root_raw, str) or not source_repo_root_raw.strip():
            raise NodeExecutionFailure("source_workspace_check.source_repo_root is required")
        repo_root = Path(source_repo_root_raw).resolve()
        if not repo_root.exists():
            raise NodeExecutionFailure(f"repo_root does not exist: {repo_root}")
        repo_root = resolve_git_toplevel(repo_root)
        if repo_root != Path(source_repo_root_raw).resolve():
            raise NodeExecutionFailure(
                "source_workspace_check.source_repo_root must be git top-level"
            )
        source_base_revision = source_workspace_check.get("base_revision")
        if not isinstance(source_base_revision, str) or not source_base_revision.strip():
            raise NodeExecutionFailure("source_workspace_check.base_revision is required")
        source_current_branch = source_workspace_check.get("current_branch")
        if not isinstance(source_current_branch, str) or not source_current_branch.strip():
            raise NodeExecutionFailure("source_workspace_check.current_branch is required")
        if source_workspace_check.get("clean") is not True:
            raise NodeExecutionFailure("source_workspace_check.clean must be true")
        run_id = str(
            inputs.get("run_id") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        )
        development_name = self._resolve_development_name(inputs, params)
        run_slug = self._slugify(development_name)

        artifact_root_dir_raw = str(params.get("artifact_root_dir") or ".nodeflow/runs")
        artifact_root_dir = Path(artifact_root_dir_raw)
        if not artifact_root_dir.is_absolute():
            artifact_root_dir = (repo_root / artifact_root_dir).resolve()

        run_index = self._next_index(artifact_root_dir)
        yyyymmdd = datetime.now(timezone.utc).strftime("%Y%m%d")
        run_dir_format = str(params.get("run_dir_format") or "{index:03d}_{yyyymmdd}_{slug}")
        try:
            base_run_dir_name = run_dir_format.format(
                index=run_index, yyyymmdd=yyyymmdd, slug=run_slug
            )
        except (KeyError, ValueError) as e:
            raise NodeExecutionFailure(f"invalid run_dir_format: {run_dir_format!r}") from e

        run_dir_name = base_run_dir_name
        suffix = 2
        while (artifact_root_dir / run_dir_name).exists():
            run_dir_name = f"{base_run_dir_name}-{suffix}"
            suffix += 1

        planned_branch_name = str(inputs.get("planned_branch_name") or "").strip()
        if not planned_branch_name:
            prefix = str(params.get("branch_prefix") or "feat/nodeflow").strip().rstrip("/")
            if not prefix:
                raise NodeExecutionFailure("branch_prefix must not be empty")
            planned_branch_name = f"{prefix}/{run_index:03d}-{run_slug}"

        cp = subprocess.run(
            ["git", "check-ref-format", "--branch", planned_branch_name],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=False,
        )
        if cp.returncode != 0:
            raise NodeExecutionFailure(f"invalid planned_branch_name: {planned_branch_name}")

        artifact_root_path = (artifact_root_dir / run_dir_name).resolve()
        artifact_root_path.mkdir(parents=True, exist_ok=False)

        return {
            "run_context": {
                "run_id": run_id,
                "run_index": run_index,
                "development_name": development_name,
                "run_slug": run_slug,
                "run_dir_name": run_dir_name,
                "planned_branch_name": planned_branch_name,
                "artifact_root": str(artifact_root_path),
                "source_repo_root": str(repo_root),
                "source_base_revision": source_base_revision.strip(),
                "source_current_branch": source_current_branch.strip(),
            }
        }
