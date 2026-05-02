"""ImplementWithCodexPipeNode — Codex exec -> summarize fixed provider pipe."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import (
    BaseNode,
    ExecutionContext,
    NodeExecutionFailure,
    NodeExecutionLimit,
)
from nodeflow.core.node_kinds import PipeNode
from nodeflow.legacy.runner import Runner
from nodeflow.legacy.runner_frame import reset_children_for_graph
from nodeflow.nodes.exec.codex_exec import CodexExecNode
from nodeflow.nodes.summarize.python_summarize_result import PythonSummarizeResultNode


class ImplementWithCodexPipeNode(PipeNode):
    """Fixed implement flow without dynamic routing semantics."""

    def __init__(self) -> None:
        super().__init__()
        self._graph_node_order = ["exec", "summarize"]
        self._nodes: Dict[str, BaseNode] = {
            "exec": CodexExecNode(),
            "summarize": PythonSummarizeResultNode(),
        }
        self._node_input_bindings = {
            "exec": {"prompt": ("inputs", "task_prompt"), "task_type": ("inputs", "task_type")},
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
        if not isinstance(pipe_inputs.get("task_prompt"), str):
            raise NodeExecutionFailure("inputs.task_prompt is required")
        if not isinstance(pipe_inputs.get("task_type"), str):
            raise NodeExecutionFailure("inputs.task_type is required")
        workspace_dir = pipe_params.get("_workspace_dir")

        resolved_node_params = {
            "exec": dict(pipe_params.get("codex_exec") or {}),
            "summarize": dict(pipe_params.get("python_summarize_result") or {}),
        }
        if isinstance(workspace_dir, str):
            resolved_node_params["exec"].setdefault("_workspace_dir", workspace_dir)
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
        if "summarize" in latest_output:
            out["summary"] = latest_output["summarize"].get("summary", {})
        if "exec" in latest_output:
            out["execution_result"] = latest_output["exec"].get("execution_result", {})
        return out
