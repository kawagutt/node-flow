"""Generic PipeNode — v1.7 container built from an executable :class:`~nodeflow.core.pipe_spec.PipeSpec`."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict, Optional

from nodeflow.core.base_node import (
    BaseNode,
    ExecutionContext,
    NodeExecutionFailure,
    NodeExecutionLimit,
)
from nodeflow.core.pipe_runtime import reset_child_nodes_for_pipe_execution
from nodeflow.core.pipe_spec import PipeSpec
from nodeflow.core.runner import Runner

__all__ = ["PipeNode"]


class PipeNode(BaseNode):
    """Generic composite: fixed child graph from ``spec``; internal :class:`~nodeflow.core.runner.Runner`.

    Workflow-specific subclasses are not supported (doc §7).
    """

    ALLOW_AS_CHILD = True

    def __init__(self, spec: PipeSpec) -> None:
        super().__init__()
        self._spec = spec
        self._last_pipe_spec: PipeSpec | None = None

    def reset_status(self) -> None:
        super().reset_status()
        self._pipe_run_snapshot = None

    def _resolved_node_params(
        self,
        spec: PipeSpec,
        raw_params: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        """Shallow-copy each child's ``NodeSpec.params`` and propagate ``_workspace_dir`` when present."""
        resolved: dict[str, dict[str, Any]] = {
            nid: dict(ns.params) for nid, ns in spec.nodes.items()
        }
        workspace_dir = raw_params.get("_workspace_dir")
        if isinstance(workspace_dir, str):
            for node_params in resolved.values():
                node_params.setdefault("_workspace_dir", workspace_dir)
        return resolved

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        self._pipe_run_snapshot = None
        spec = self._spec
        self._last_pipe_spec = spec
        raw_params = dict(params) if not isinstance(params, MappingProxyType) else dict(params)
        pipe_inputs = dict(inputs)
        nodes_map = {nid: ns.node for nid, ns in spec.nodes.items()}
        reset_child_nodes_for_pipe_execution(nodes_map)
        resolved_node_params = self._resolved_node_params(spec, raw_params)

        runner = Runner.from_pipe_spec(
            spec, pipe_inputs=pipe_inputs, node_params=resolved_node_params
        )
        while True:
            progressed = runner.step()
            statuses = [nodes_map[nid].read_status() for nid in spec.graph_node_order]
            if "fatal" in statuses:
                raise NodeExecutionFailure("child fatal")
            if "limit" in statuses:
                raise NodeExecutionLimit("child limit")
            if runner.all_pipe_outputs_filled():
                break
            if not progressed:
                break

        all_filled = runner.all_pipe_outputs_filled()
        self._pipe_run_snapshot = {
            "all_outputs_filled": all_filled,
            "nodes_map": nodes_map,
            "graph_node_order": tuple(spec.graph_node_order),
        }
        if all_filled:
            return runner.filled_pipe_outputs()
        return {}

    def _status_after_run(self, result: Dict[str, Any]) -> str:  # noqa: ARG002
        snap = getattr(self, "_pipe_run_snapshot", None)
        if snap is None:
            return super()._status_after_run(result)
        if snap["all_outputs_filled"]:
            self._status = "idle"
            self._refresh_status_from_output_occupancy()
            return self._status
        nodes_map: Dict[str, BaseNode] = snap["nodes_map"]
        order: tuple[str, ...] = snap["graph_node_order"]
        statuses = [nodes_map[nid].read_status() for nid in order]
        if any(s == "executing" for s in statuses):
            return "executing"
        self._status = "idle"
        return "idle"

    def read_error(self) -> Optional[Exception]:
        ps: PipeSpec | None = getattr(self, "_last_pipe_spec", None)
        if ps is None:
            ps = self._spec
        for node in (ns.node for ns in ps.nodes.values()):
            e = node.read_error()
            if e is not None:
                return e
        if self._status == "fatal" and self._error is not None:
            return self._error
        return None
