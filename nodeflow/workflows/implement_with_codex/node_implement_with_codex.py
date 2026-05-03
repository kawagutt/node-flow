"""ImplementWithCodexPipeNode — Codex exec → summarize (v1.6 PipeSpec + core Runner)."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PipeNode
from nodeflow.core.pipe_spec import NodeSpec, PipeDeclaration, PipeSpec
from nodeflow.core.source_ref import SourceRef
from nodeflow.nodes.exec.codex_exec import CodexExecNode
from nodeflow.nodes.summarize.python_summarize_result import PythonSummarizeResultNode
from nodeflow.workflows.fixed_provider_cli_ports import (
    optional_child_params,
    validate_task_prompt_task_type_ports,
)


class ImplementWithCodexPipeNode(PipeNode):
    """Fixed implement flow without dynamic routing semantics.

    Public inputs follow v1.6 dict-only port payloads (see Runner delivery): ``task_prompt`` is
    ``{\"text\": <str>}`` and ``task_type`` is ``{\"value\": <str>}`` — the shapes expected by
    :class:`~nodeflow.nodes.exec.codex_exec.CodexExecNode` on its ``prompt`` / ``task_type`` ports.

    Pipe-level ``params`` may include ``codex_exec`` and ``python_summarize_result`` dicts merged
    into the corresponding child nodes' params (via :meth:`_resolved_node_params`).
    """

    def __init__(self) -> None:
        super().__init__()
        self._exec = CodexExecNode()
        self._summarize = PythonSummarizeResultNode()

    def pipe_spec(self) -> PipeSpec:
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
                    params={},
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
                    params={},
                ),
            },
        )

    def _resolved_node_params(
        self, spec: PipeSpec, raw_params: dict[str, Any]
    ) -> dict[str, dict[str, Any]]:
        resolved = super()._resolved_node_params(spec, raw_params)
        resolved["exec"] = {
            **resolved["exec"],
            **optional_child_params(raw_params, "codex_exec"),
        }
        resolved["summarize"] = {
            **resolved["summarize"],
            **optional_child_params(raw_params, "python_summarize_result"),
        }
        return resolved

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        validate_task_prompt_task_type_ports(inputs)
        return super().run(inputs, params, context)
