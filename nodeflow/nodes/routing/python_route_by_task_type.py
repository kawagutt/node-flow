"""PythonRouteByTaskTypeNode — deterministic routing by task metadata (no LLM)."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode


class PythonRouteByTaskTypeNode(PythonActionNode):
    role = "route_by_task_type"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        task_type = str(inputs.get("task_type", "unknown"))
        needs_repo_write = bool(inputs.get("needs_repo_write", False))
        needs_shell = bool(inputs.get("needs_shell", False))
        cost_tier = inputs.get("cost_tier")
        preferred = str(inputs.get("preferred_executor", "") or "")
        forbid = str(inputs.get("forbid_executor", "") or "")

        executor = "claude_code"
        next_node = "review_dispatch"
        reason_parts = [f"task_type={task_type!r}"]

        if task_type == "implement" or needs_repo_write or needs_shell:
            executor = "codex"
            next_node = "implement_dispatch"
            reason_parts.append("needs implementation path")

        if preferred and preferred != forbid:
            if preferred in ("codex", "claude_code"):
                executor = preferred
                next_node = (
                    "implement_dispatch" if preferred == "codex" else "review_dispatch"
                )
                reason_parts.append(f"preferred_executor={preferred!r}")

        if forbid and executor == forbid:
            executor = "claude_code" if forbid == "codex" else "codex"
            next_node = (
                "review_dispatch" if executor == "claude_code" else "implement_dispatch"
            )
            reason_parts.append("adjusted to respect forbid_executor")

        reason = "; ".join(reason_parts)
        if cost_tier is not None:
            reason += f"; cost_tier={cost_tier!r}"

        return {
            "route": {
                "executor": executor,
                "reason": reason,
                "next_node": next_node,
            }
        }
