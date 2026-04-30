"""development_flow revise_spec action node."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import (
    ExecutionContext,
    NodeExecutionFailure,
    NodeExecutionLimit,
    domain_ports_from_observation,
)
from nodeflow.core.node_kinds import PipeNode
from nodeflow.workflows.development_flow.node_development_flow import (
    _DevelopmentFlowLegacyPipeNode,
)


class ReviseSpecPipeNode(PipeNode):
    """Run development_flow revise_spec action."""

    def __init__(self) -> None:
        super().__init__()

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        legacy = _DevelopmentFlowLegacyPipeNode()
        merged_inputs = dict(inputs)
        merged_inputs["action"] = "revise_spec"
        out = legacy.execute(merged_inputs, dict(params) if params else {})
        out = domain_ports_from_observation(out)
        status = legacy.read_status()
        if status == "fatal":
            raise NodeExecutionFailure(str(legacy.read_error()))
        if status == "limit":
            raise NodeExecutionLimit("development_flow revise_spec limit")
        if status != "done":
            raise NodeExecutionFailure(f"development_flow revise_spec unexpected status: {status}")
        return out
