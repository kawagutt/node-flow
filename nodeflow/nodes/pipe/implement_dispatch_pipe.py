"""ImplementDispatchPipeNode — Part V §8.6 (route → Codex exec → summarize)."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import (
    BaseNode,
    ExecutionContext,
    NodeExecutionFailure,
    NodeExecutionLimit,
)
from nodeflow.core.runner import Runner
from nodeflow.nodes.action.exec.codex_exec import CodexExecNode
from nodeflow.nodes.action.routing.python_route_by_task_type import (
    PythonRouteByTaskTypeNode,
)
from nodeflow.nodes.action.transform.python_summarize_result import (
    PythonSummarizeResultNode,
)
from nodeflow.nodes.base.pipe import PipeNode
from nodeflow.nodes.pipe.serial_pipe import _reset_children_for_run


class ImplementDispatchPipeNode(PipeNode):
    """Fixed subgraph for implement path."""

    def __init__(self) -> None:
        super().__init__()
        self._graph_node_order = ["route", "exec", "summarize"]
        self._nodes: Dict[str, BaseNode] = {
            "route": PythonRouteByTaskTypeNode(),
            "exec": CodexExecNode(),
            "summarize": PythonSummarizeResultNode(),
        }
        self._node_input_bindings = {
            "route": {"task_type": ("inputs", "task_type")},
            "exec": {"prompt": ("inputs", "task_prompt")},
            "summarize": {"execution_result": ("node", "exec", "execution_result")},
        }

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        _reset_children_for_run(self._nodes)
        pipe_params = dict(params) if params else {}
        pipe_inputs = dict(inputs) if inputs else {}

        resolved_node_params = {
            "route": dict(pipe_params.get("route") or {}),
            "exec": dict(pipe_params.get("codex_exec") or {}),
            "summarize": dict(pipe_params.get("python_summarize_result") or {}),
        }
        latest_output: Dict[str, Dict[str, Any]] = {}
        runner = Runner(
            graph_node_order=self._graph_node_order,
            nodes=self._nodes,
            node_params=resolved_node_params,
            node_input_bindings=self._node_input_bindings,
            pipeline_inputs=pipe_inputs,
            pipeline_params=pipe_params,
            latest_output=latest_output,
        )

        while True:
            progressed = runner.step()
            statuses = [self._nodes[nid].read_status() for nid in self._graph_node_order]
            if "fatal" in statuses:
                raise NodeExecutionFailure("child fatal")
            if "limit" in statuses:
                raise NodeExecutionLimit("child limit")

            if self._nodes["summarize"].read_status() == "done":
                break
            if not progressed:
                raise NodeExecutionFailure("invalid execution state")

        out: Dict[str, Any] = {}
        if "route" in latest_output:
            out["route"] = latest_output["route"].get("route", {})
        if "summarize" in latest_output:
            out["summary"] = latest_output["summarize"].get("summary", {})
        if "exec" in latest_output:
            out["execution_result"] = latest_output["exec"].get("execution_result", {})
        return out
