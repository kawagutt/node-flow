"""
SerialPipeNode — YAML-driven serial graph (Part V §8, registry type `compose`).
"""

from __future__ import annotations

import re
from types import MappingProxyType
from typing import Any, Dict, List, Optional, Tuple

from nodeflow.core.base_node import (
    BaseNode,
    ExecutionContext,
    NodeExecutionFailure,
    NodeExecutionLimit,
)
from nodeflow.core.runner import Runner
from nodeflow.nodes.base.pipe import PipeNode

InputBinding = Tuple[str, ...]
_REF_PATTERN = re.compile(r"\$\{([^}.]+)\.([^}]+)\}")


def _resolve_params_dict(
    params_def: Dict[str, Any],
    pipe_params: Dict[str, Any],
    pipe_inputs: Dict[str, Any],
) -> Dict[str, Any]:
    """Resolve ${params.x} and ${inputs.x} before Runner.step."""
    if not params_def:
        return {}
    resolved: Dict[str, Any] = {}
    for k, v in params_def.items():
        if isinstance(v, str) and v.startswith("${") and v.endswith("}"):
            m = _REF_PATTERN.fullmatch(v.strip())
            if m:
                source, key = m.group(1), m.group(2)
                if source == "params" and key in pipe_params:
                    resolved[k] = pipe_params[key]
                elif source == "inputs" and key in pipe_inputs:
                    resolved[k] = pipe_inputs[key]
                else:
                    resolved[k] = v
            else:
                resolved[k] = v
        elif isinstance(v, dict):
            resolved[k] = _resolve_params_dict(v, pipe_params, pipe_inputs)
        else:
            resolved[k] = v
    return resolved


def _reset_children_for_run(nodes: Dict[str, BaseNode]) -> None:
    """Allow repeated root execute on the same graph instance."""
    for node in nodes.values():
        st = node.read_status()
        if st == "executing":
            raise RuntimeError("child node stuck in executing")
        if st != "ready":
            node.reset_status()


class SerialPipeNode(PipeNode):
    """
    One-shot serial execution using Runner inside ``run()`` only.

    **read_error**: Unlike aggregating every child error into a list, this class
    returns the **first** non-none child ``read_error()`` (then the pipe's own fatal
    error if any). That keeps the surface small and deterministic; callers that
    need full diagnostics should inspect child nodes directly.
    """

    # YAML `compose` root: disallow nesting another compose in the same graph (loader check).
    ALLOW_AS_CHILD = False

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
        _reset_children_for_run(self._nodes)
        pipe_params = dict(params) if params else {}
        pipe_inputs = dict(inputs) if inputs else {}

        resolved_node_params = {
            nid: _resolve_params_dict(
                self._node_param_definitions.get(nid, {}),
                pipe_params,
                pipe_inputs,
            )
            for nid in self._graph_node_order
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

            final_node = self._nodes.get(self._final_id)
            if final_node is not None and final_node.read_status() == "done":
                break

            if not progressed:
                raise NodeExecutionFailure("invalid execution state")

        return latest_output.get(self._final_id, {})

    def read_error(self) -> Optional[Exception]:
        for node in self._nodes.values():
            e = node.read_error()
            if e is not None:
                return e
        if self._status == "fatal" and self._error is not None:
            return self._error
        return None
