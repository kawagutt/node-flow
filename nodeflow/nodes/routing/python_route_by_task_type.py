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
        recommended_pipe_type = "review_with_claude"
        reason_parts = [f"task_type={task_type!r}"]

        if task_type == "implement" or needs_repo_write or needs_shell:
            executor = "codex"
            recommended_pipe_type = "implement_with_codex"
            reason_parts.append("needs implementation path")

        if preferred and preferred != forbid:
            if preferred in ("codex", "claude_code"):
                executor = preferred
                recommended_pipe_type = (
                    "implement_with_codex" if preferred == "codex" else "review_with_claude"
                )
                reason_parts.append(f"preferred_executor={preferred!r}")

        if forbid and executor == forbid:
            executor = "claude_code" if forbid == "codex" else "codex"
            recommended_pipe_type = (
                "review_with_claude" if executor == "claude_code" else "implement_with_codex"
            )
            reason_parts.append("adjusted to respect forbid_executor")

        reason = "; ".join(reason_parts)
        if cost_tier is not None:
            reason += f"; cost_tier={cost_tier!r}"

        return {
            "route": {
                "executor": executor,
                "reason": reason,
                "recommended_pipe_type": recommended_pipe_type,
            }
        }
