"""Load approved spec/plan for implementation and review (single-file checkpoint)."""

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
        rework_context = inputs.get("rework_context")
        rework_text = ""
        if isinstance(rework_context, str) and rework_context.strip():
            rework_text = rework_context.strip()
        elif isinstance(rework_context, dict):
            rework_text = json.dumps(rework_context, ensure_ascii=False, indent=2)
        rework_block = f"## Rework context\n{rework_text}\n" if rework_text else ""

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
                    "'spec' and 'plan' (see nodeflow/workflows/development_flow/README.md)"
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
                        f"## PLAN\n{plan_text}\n\n"
                        f"{rework_block}"
                    )
                },
            }

        raise NodeExecutionFailure("approved_checkpoint_path is required")
