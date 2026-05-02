"""Legacy runner frame for pre-v1.6 PipeNode path."""

from __future__ import annotations

import re
from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import (
    BaseNode,
    NodeExecutionFailure,
    NodeExecutionLimit,
    domain_ports_from_observation,
)
from nodeflow.core.graph_spec import GraphSpec
from nodeflow.legacy.runner import Runner

_REF_PATTERN = re.compile(r"\$\{([^}.]+)\.([^}]+)\}")


def reset_children_for_graph(nodes: Dict[str, BaseNode]) -> None:
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


class RunnerFrame:
    def __init__(
        self,
        spec: GraphSpec,
        pipe_inputs: Dict[str, Any] | None = None,
        pipe_params: MappingProxyType | Dict[str, Any] | None = None,
    ) -> None:
        self.spec = spec
        self.pipe_inputs = dict(pipe_inputs or {})
        self.pipe_params = dict(pipe_params or {})
        self.latest_output: Dict[str, Dict[str, Any]] = {}

    def _resolve_node_params(self) -> Dict[str, Dict[str, Any]]:
        resolved_node_params = {
            nid: _resolve_params_dict(
                self.spec.params.get(nid, {}),
                self.pipe_params,
                self.pipe_inputs,
            )
            for nid in self.spec.order
        }
        workspace_dir = self.pipe_params.get("_workspace_dir")
        if isinstance(workspace_dir, str):
            for node_params in resolved_node_params.values():
                node_params.setdefault("_workspace_dir", workspace_dir)
        return resolved_node_params

    def run(self) -> Dict[str, Any]:
        reset_children_for_graph(self.spec.nodes)
        runner = Runner(
            graph_node_order=self.spec.order,
            nodes=self.spec.nodes,
            node_params=self._resolve_node_params(),
            node_input_bindings=self.spec.bindings,
            pipeline_inputs=self.pipe_inputs,
            pipeline_params=self.pipe_params,
            latest_output=self.latest_output,
        )
        while True:
            progressed = runner.step()
            statuses = [self.spec.nodes[nid].read_status() for nid in self.spec.order]
            if "fatal" in statuses:
                raise NodeExecutionFailure("child fatal")
            if "limit" in statuses:
                raise NodeExecutionLimit("child limit")
            final_node = self.spec.nodes.get(self.spec.final)
            if final_node is not None and final_node.read_status() == "done":
                break
            if not progressed:
                raise NodeExecutionFailure("invalid execution state")
        final_obs = self.latest_output.get(self.spec.final, {})
        return domain_ports_from_observation(final_obs)
