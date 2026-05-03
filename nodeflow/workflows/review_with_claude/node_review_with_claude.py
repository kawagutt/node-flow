"""ReviewWithClaudePipeNode — Claude exec → summarize (v1.6 PipeSpec + core Runner)."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PipeNode
from nodeflow.core.pipe_spec import NodeSpec, PipeDeclaration, PipeSpec
from nodeflow.core.source_ref import SourceRef
from nodeflow.nodes.exec.claude_code_exec import ClaudeCodeExecNode
from nodeflow.nodes.summarize.python_summarize_result import PythonSummarizeResultNode


class ReviewWithClaudePipeNode(PipeNode):
    """Fixed review flow without dynamic routing semantics."""

    def __init__(self) -> None:
        super().__init__()
        self._exec = ClaudeCodeExecNode()
        self._summarize = PythonSummarizeResultNode()
        self._compose_params_holder: dict[str, Any] | None = None

    def pipe_spec(self) -> PipeSpec:
        holder = self._compose_params_holder
        exec_p = dict(holder.get("exec") or {}) if holder else {}
        sum_p = dict(holder.get("summarize") or {}) if holder else {}
        return PipeSpec(
            graph_node_order=("exec", "summarize"),
            pipe=PipeDeclaration(
                input_ports=frozenset({"task_prompt", "task_type"}),
                output_sources={
                    "summary": SourceRef(kind="node", node_id="summarize", port_name="summary"),
                    "execution_output": SourceRef(
                        kind="node", node_id="summarize", port_name="execution_output"
                    ),
                },
            ),
            nodes={
                "exec": NodeSpec(
                    node_id="exec",
                    node=self._exec,
                    input_sources={
                        "prompt": SourceRef(kind="input", port_name="task_prompt"),
                        "task_type": SourceRef(kind="input", port_name="task_type"),
                    },
                    output_ports=frozenset({"execution_output"}),
                    params=exec_p,
                ),
                "summarize": NodeSpec(
                    node_id="summarize",
                    node=self._summarize,
                    input_sources={
                        "execution_output": SourceRef(
                            kind="node", node_id="exec", port_name="execution_output"
                        ),
                    },
                    output_ports=frozenset({"summary", "execution_output"}),
                    params=sum_p,
                ),
            },
        )

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        if not isinstance(inputs.get("task_prompt"), str):
            raise NodeExecutionFailure("inputs.task_prompt is required")
        if not isinstance(inputs.get("task_type"), str):
            raise NodeExecutionFailure("inputs.task_type is required")
        normalized = dict(inputs)
        normalized["task_prompt"] = {"text": normalized["task_prompt"]}
        normalized["task_type"] = {"value": normalized["task_type"]}
        raw_params = dict(params) if not isinstance(params, MappingProxyType) else dict(params)
        exec_p = dict(raw_params.get("claude_code_exec") or {})
        ws = raw_params.get("_workspace_dir")
        if isinstance(ws, str):
            exec_p.setdefault("_workspace_dir", ws)
        self._compose_params_holder = {
            "exec": exec_p,
            "summarize": dict(raw_params.get("python_summarize_result") or {}),
        }
        try:
            return super().run(normalized, params, context)
        finally:
            self._compose_params_holder = None
