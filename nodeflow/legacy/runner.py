"""Legacy runner kept for pre-v1.6 workflows."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from nodeflow.core.base_node import BaseNode

InputBinding = Tuple[str, ...]


class Runner:
    def __init__(
        self,
        graph_node_order: List[str],
        nodes: Dict[str, BaseNode],
        node_params: Dict[str, Dict[str, Any]],
        node_input_bindings: Dict[str, Dict[str, InputBinding]],
        pipeline_inputs: Dict[str, Any],
        pipeline_params: Dict[str, Any],
        latest_output: Dict[str, Dict[str, Any]],
    ):
        self.graph_node_order = graph_node_order
        self.nodes = nodes
        self.node_params = node_params
        self.node_input_bindings = node_input_bindings
        self.pipeline_inputs = pipeline_inputs
        self.pipeline_params = pipeline_params
        self.latest_output = latest_output

    def _resolve_inputs(self, node_id: str) -> Dict[str, Any] | None:
        bindings = self.node_input_bindings.get(node_id)
        if not bindings:
            return {}
        resolved: Dict[str, Any] = {}
        for port, binding in bindings.items():
            if not binding:
                return None
            if binding[0] == "node":
                if len(binding) not in (3, 4):
                    return None
                _, src_node, src_port = binding[0], binding[1], binding[2]
                src_out = self.latest_output.get(src_node)
                if src_out is None:
                    return None
                if src_port not in src_out:
                    return None
                val = src_out[src_port]
                if len(binding) == 4:
                    inner = binding[3]
                    for key in inner.split("."):
                        if not isinstance(val, dict) or key not in val:
                            return None
                        val = val[key]
                resolved[port] = val
            elif binding[0] == "inputs":
                if len(binding) < 2:
                    return None
                src_port = binding[1]
                if src_port not in self.pipeline_inputs:
                    return None
                resolved[port] = self.pipeline_inputs[src_port]
            elif binding[0] == "params":
                if len(binding) < 2:
                    return None
                param_name = binding[1]
                if param_name not in self.pipeline_params:
                    return None
                resolved[port] = self.pipeline_params[param_name]
            else:
                return None
        return resolved

    def _get_params(self, node_id: str) -> Dict[str, Any]:
        return self.node_params.get(node_id, {})

    def step(self) -> bool:
        for node_id in self.graph_node_order:
            node = self.nodes.get(node_id)
            if node is None:
                continue
            if node.read_status() != "ready":
                continue
            inputs = self._resolve_inputs(node_id)
            if inputs is None:
                continue
            output = node.execute(inputs, self._get_params(node_id))
            if output != {}:
                self.latest_output[node_id] = output
            return True
        return False
