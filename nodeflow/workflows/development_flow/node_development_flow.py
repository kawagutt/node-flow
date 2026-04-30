"""Development flow top-level action router."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import (
    BaseNode,
    ExecutionContext,
    NodeExecutionFailure,
    NodeExecutionLimit,
    domain_ports_from_observation,
)
from nodeflow.core.node_kinds import PipeNode


class DevelopmentFlowPipeNode(PipeNode):
    """Action router for start/revise_spec/approve/rework/merge."""

    def __init__(self) -> None:
        super().__init__()
        from nodeflow.workflows.development_flow.approve import ApprovePipeNode
        from nodeflow.workflows.development_flow.merge import MergePipeNode
        from nodeflow.workflows.development_flow.revise_spec import ReviseSpecPipeNode
        from nodeflow.workflows.development_flow.rework import ReworkPipeNode
        from nodeflow.workflows.development_flow.start import StartPipeNode

        self._start = StartPipeNode()
        self._revise_spec = ReviseSpecPipeNode()
        self._approve = ApprovePipeNode()
        self._rework = ReworkPipeNode()
        self._merge = MergePipeNode()

    def _run_child(
        self,
        *,
        child_name: str,
        child: BaseNode,
        inputs: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        child.reset_status()
        out = child.execute(inputs, params)
        out = domain_ports_from_observation(out)
        status = child.read_status()
        if status == "fatal":
            raise NodeExecutionFailure(f"{child_name} fatal: {child.read_error()}")
        if status == "limit":
            raise NodeExecutionLimit(f"{child_name} limit")
        if status != "done":
            raise NodeExecutionFailure(f"{child_name} unexpected status: {status}")
        return out

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        raw_action = inputs.get("action")
        if not isinstance(raw_action, str) or not raw_action.strip():
            raise NodeExecutionFailure("action is required")
        action = raw_action.strip()

        child_params = dict(params) if params else {}
        child_inputs = dict(inputs)
        child_inputs.pop("action", None)
        action_to_child: Dict[str, BaseNode] = {
            "start": self._start,
            "revise_spec": self._revise_spec,
            "approve": self._approve,
            "rework": self._rework,
            "merge": self._merge,
        }
        child = action_to_child.get(action)
        if child is None:
            raise NodeExecutionFailure(f"unsupported action: {raw_action}")
        return self._run_child(
            child_name=f"development_flow.{action}",
            child=child,
            inputs=child_inputs,
            params=child_params,
        )
