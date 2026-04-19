"""ReviewDispatchPipeNode — route → Claude exec → summarize (reusable subgraph).

Graph wiring (child ids, bindings, param keys) lives in this class; the same
structure could later be moved to external config without changing exec
contract semantics.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict, Tuple

from nodeflow.core.base_node import (
    BaseNode,
    ExecutionContext,
    NodeExecutionFailure,
    NodeExecutionLimit,
)
from nodeflow.core.node_kinds import PipeNode, reset_children_for_graph
from nodeflow.core.runner import Runner
from nodeflow.nodes.exec.claude_code_exec import ClaudeCodeExecNode
from nodeflow.nodes.routing.python_route_by_task_type import PythonRouteByTaskTypeNode
from nodeflow.nodes.summarize.python_summarize_result import PythonSummarizeResultNode


class ReviewDispatchPipeNode(PipeNode):
    """Fixed subgraph; dispatch decisions stay inside node graph, not Runner."""

    def __init__(self) -> None:
        super().__init__()
        self._graph_node_order = ["route", "exec", "summarize"]
        self._nodes: Dict[str, BaseNode] = {
            "route": PythonRouteByTaskTypeNode(),
            "exec": ClaudeCodeExecNode(),
            "summarize": PythonSummarizeResultNode(),
        }
        self._node_input_bindings: Dict[str, Dict[str, Tuple[str, ...]]] = {
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
        reset_children_for_graph(self._nodes)
        pipe_params = dict(params) if params else {}
        pipe_inputs = dict(inputs) if inputs else {}

        resolved_node_params = {
            "route": dict(pipe_params.get("route") or {}),
            "exec": dict(pipe_params.get("claude_code_exec") or {}),
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

            final_node = self._nodes["summarize"]
            if final_node.read_status() == "done":
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
