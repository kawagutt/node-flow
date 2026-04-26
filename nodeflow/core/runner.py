"""
NodeFlow — Runner (dumb graph stepping; see doc/nodeflow_spec.md §3.1). No loader dependency.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from .base_node import BaseNode

# node_input_bindings のタプル形式: ("node", src_node_id, src_port) | ("inputs", port) | ("params", param_name)
InputBinding = Tuple[str, ...]


class Runner:
    """
    実行可能なノードを graph_node_order 順に 1 つ execute する。
    例外は raise しない。output != {} のときのみ latest_output を更新。
    """

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
        """
        バインディングを解決。未解決が 1 つでもあれば None を返す。
        タプル先頭: "node" → latest_output[src_node][src_port], "inputs" → pipeline_inputs[port], "params" → pipeline_params[name]。
        """
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
        """解決済み node_params を返す。Runner はテンプレートを解釈しない。"""
        return self.node_params.get(node_id, {})

    def step(self) -> bool:
        """
        実行可能なノードを 1 つ見つけて execute する。
        status == "ready" かつ _resolve_inputs が None でないときのみ実行。
        実行した場合 True、それ以外 False。例外は raise しない。
        """
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
