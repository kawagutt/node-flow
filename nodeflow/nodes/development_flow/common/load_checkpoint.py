"""Load approved spec/plan for implementation and review (single file or legacy two-file)."""

from __future__ import annotations

import json
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PythonActionNode


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, indent=2)


def _resolve_under_repo(path: Path, repo_root: Path) -> Path:
    if path.is_absolute():
        return path
    return (repo_root / path).resolve()


class LoadCheckpointNode(PythonActionNode):
    role = "load_checkpoint"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        repo_root = Path(str(inputs.get("repo_root") or ".")).resolve()

        ck_path = inputs.get("approved_checkpoint_path")
        if isinstance(ck_path, str) and ck_path.strip():
            path = _resolve_under_repo(Path(ck_path.strip()), repo_root)
            if not path.exists():
                raise NodeExecutionFailure(f"approved_checkpoint_path not found: {path}")
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise NodeExecutionFailure("approved checkpoint JSON must be an object")
            if "spec" not in data or "plan" not in data:
                raise NodeExecutionFailure(
                    "approved checkpoint must contain top-level string (or object) keys "
                    "'spec' and 'plan' (see nodeflow/nodes/development_flow/README.md)"
                )
            spec_text = _as_text(data["spec"])
            plan_text = _as_text(data["plan"])
            return {
                "approved_spec_plan": {
                    "checkpoint_path": str(path),
                    "spec_path": str(path),
                    "plan_path": str(path),
                    "spec": data["spec"],
                    "plan": data["plan"],
                },
                "codex_task_prompt": {
                    "text": (
                        "Implement only the following approved SPEC and PLAN. "
                        "Do not expand scope beyond them.\n\n"
                        f"## SPEC\n{spec_text}\n\n"
                        f"## PLAN\n{plan_text}\n"
                    )
                },
            }

        spec_path_raw = inputs.get("approved_spec_path")
        plan_path_raw = inputs.get("approved_plan_path")
        if not isinstance(spec_path_raw, str) or not isinstance(plan_path_raw, str):
            raise NodeExecutionFailure(
                "Provide approved_checkpoint_path (preferred) or approved_spec_path and approved_plan_path"
            )

        spec_path = _resolve_under_repo(Path(spec_path_raw), repo_root)
        plan_path = _resolve_under_repo(Path(plan_path_raw), repo_root)
        if not spec_path.exists() or not plan_path.exists():
            raise NodeExecutionFailure("approved spec/plan checkpoint file not found")

        spec_obj = json.loads(spec_path.read_text(encoding="utf-8"))
        plan_obj = json.loads(plan_path.read_text(encoding="utf-8"))
        spec_text = _as_text(spec_obj)
        plan_text = _as_text(plan_obj)

        return {
            "approved_spec_plan": {
                "checkpoint_path": None,
                "spec_path": str(spec_path),
                "plan_path": str(plan_path),
                "spec": spec_obj,
                "plan": plan_obj,
            },
            "codex_task_prompt": {
                "text": (
                    "Implement only the following approved SPEC and PLAN.\n\n"
                    f"## SPEC\n{spec_text}\n\n"
                    f"## PLAN\n{plan_text}\n"
                )
            },
        }
