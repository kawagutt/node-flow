"""
NodeFlow v1.41 — PipelineNode (StructuralNode). Graph 直列 1-shot 実行。
"""

from __future__ import annotations

import re
from types import MappingProxyType
from typing import Any, Dict, List, Tuple

from nodeflow.core.base_node import (
    BaseNode,
    ExecutionContext,
    NodeExecutionFailure,
    NodeExecutionLimit,
    StructuralNode,
)
from nodeflow.core.runner import Runner

InputBinding = Tuple[str, ...]
_REF_PATTERN = re.compile(r"\$\{([^}.]+)\.([^}]+)\}")


class InvalidStateError(Exception):
    """Raised when resume() is called and status is not pause (v1.5)."""


def _resolve_params_dict(
    params_def: Dict[str, Any],
    pipeline_params: Dict[str, Any],
    pipeline_inputs: Dict[str, Any],
) -> Dict[str, Any]:
    """${params.x} と ${inputs.x} を再帰的に解決。Runner に渡す前に PipelineNode が呼ぶ。"""
    if not params_def:
        return {}
    resolved: Dict[str, Any] = {}
    for k, v in params_def.items():
        if isinstance(v, str) and v.startswith("${") and v.endswith("}"):
            m = _REF_PATTERN.fullmatch(v.strip())
            if m:
                source, key = m.group(1), m.group(2)
                if source == "params" and key in pipeline_params:
                    resolved[k] = pipeline_params[key]
                elif source == "inputs" and key in pipeline_inputs:
                    resolved[k] = pipeline_inputs[key]
                else:
                    resolved[k] = v
            else:
                resolved[k] = v
        elif isinstance(v, dict):
            resolved[k] = _resolve_params_dict(v, pipeline_params, pipeline_inputs)
        else:
            resolved[k] = v
    return resolved


class PipelineNode(StructuralNode):
    """
    Graph を直列 1-shot 実行する StructuralNode。
    Runner を run の冒頭で毎回 new する。停止条件は毎ループで (1)fatal (2)limit (3)final done (4)no progress の順でチェック。
    """

    def __init__(
        self,
        graph_node_order: List[str],
        nodes: Dict[str, BaseNode],
        node_input_bindings: Dict[str, Dict[str, InputBinding]],
        node_param_definitions: Dict[str, Dict[str, Any]],
        final_id: str,
    ) -> None:
        super().__init__()
        self._graph_node_order = graph_node_order
        self._nodes = nodes
        self._node_input_bindings = node_input_bindings
        self._node_param_definitions = node_param_definitions
        self._final_id = final_id

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        pipeline_params = dict(params) if params else {}
        pipeline_inputs = dict(inputs) if inputs else {}

        # run 冒頭で node_params を解決し、Runner を毎回 new する
        resolved_node_params = {
            nid: _resolve_params_dict(
                self._node_param_definitions.get(nid, {}),
                pipeline_params,
                pipeline_inputs,
            )
            for nid in self._graph_node_order
        }
        latest_output: Dict[str, Dict[str, Any]] = {}
        runner = Runner(
            graph_node_order=self._graph_node_order,
            nodes=self._nodes,
            node_params=resolved_node_params,
            node_input_bindings=self._node_input_bindings,
            pipeline_inputs=pipeline_inputs,
            pipeline_params=pipeline_params,
            latest_output=latest_output,
        )

        while True:
            progressed = runner.step()

            # 毎ループで (1)〜(4) をすべてチェック。progressed の値に関わらず。
            statuses = [
                self._nodes[nid].read_status() for nid in self._graph_node_order
            ]
            if "fatal" in statuses:
                raise NodeExecutionFailure("child fatal")
            if "limit" in statuses:
                raise NodeExecutionLimit("child limit")

            final_node = self._nodes.get(self._final_id)
            if final_node is not None and final_node.read_status() == "done":
                break

            if not progressed:
                raise NodeExecutionFailure("invalid execution state")

        return latest_output.get(self._final_id, {})

    def read_error(self) -> Any:
        """子ノードの fatal 原因を集約。"""
        out: List[Exception] = []
        for node in self._nodes.values():
            e = node.read_error()
            if e is not None:
                out.append(e)
        if self._status == "fatal" and self._error is not None:
            out.append(self._error)
        return out
