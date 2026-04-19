"""PipeNode — child graph wiring and orchestration."""

from __future__ import annotations

import re
from types import MappingProxyType
from typing import Any, Dict, List, Optional, Tuple

from nodeflow.core.base_node import (
    BaseNode,
    ExecutionContext,
    NodeExecutionFailure,
    NodeExecutionLimit,
    domain_ports_from_observation,
)
from nodeflow.core.runner import Runner

InputBinding = Tuple[str, ...]
_REF_PATTERN = re.compile(r"\$\{([^}.]+)\.([^}]+)\}")


def reset_children_for_graph(nodes: Dict[str, BaseNode]) -> None:
    """Reset child node status so the same graph instance can be executed again."""
    for node in nodes.values():
        st = node.read_status()
        if st == "executing":
            raise RuntimeError("child node stuck in executing")
        if st != "ready":
            node.reset_status()


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


class PipeNode(BaseNode):
    """Composite: connects children; domain semantics live in child ActionNodes.

    Instances built by the loader (``graph_node_order``, ``final_id``, bindings)
    are the minimal linear-graph implementation. Subclasses that override
    ``run()`` should keep wiring, child execution, and exposing **child domain
    ports** only—avoid routing or transform logic here (PipeNode contract in
    ``doc/nodeflow_spec.md``).
    """

    ALLOW_AS_CHILD = True

    def __init__(
        self,
        *,
        graph_node_order: List[str] | None = None,
        nodes: Dict[str, BaseNode] | None = None,
        node_input_bindings: Dict[str, Dict[str, InputBinding]] | None = None,
        node_param_definitions: Dict[str, Dict[str, Any]] | None = None,
        final_id: str | None = None,
    ) -> None:
        super().__init__()
        self._graph_node_order = graph_node_order
        self._nodes = nodes if nodes is not None else {}
        self._node_input_bindings = node_input_bindings or {}
        self._node_param_definitions = node_param_definitions or {}
        self._final_id = final_id

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        if self._final_id is None or not self._graph_node_order:
            raise NotImplementedError(
                "PipeNode without loader graph config must be subclassed with a custom run()"
            )
        reset_children_for_graph(self._nodes)
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

        final_obs = latest_output.get(self._final_id, {})
        return domain_ports_from_observation(final_obs)

    def read_error(self) -> Optional[Exception]:
        if not self._nodes:
            return super().read_error()
        for node in self._nodes.values():
            e = node.read_error()
            if e is not None:
                return e
        if self._status == "fatal" and self._error is not None:
            return self._error
        return None
