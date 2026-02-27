"""
NodeFlow v1.2 — Runner (internal to StructuralNode) and pipeline kick entry point.
"""

from __future__ import annotations

from typing import Any, Dict, List

from .loader import (
    UNRESOLVED,
    get_required_input_ports,
    load_node_class,
    load_node_pipeline,
    resolve_inputs as resolve_inputs_impl,
    resolve_params,
)
from .node import BaseNode


class Runner:
    """
    Internal runner for StructuralNode (§7). Manages inputs resolution, executability (ready only),
    execute_node, save_output (skips {}), get_latest_output.
    """

    def __init__(
        self,
        workspace_dir: str,
        graph: Dict[str, Any],
        pipeline_inputs: Dict[str, Any],
        pipeline_params: Dict[str, Any],
        latest_outputs: Dict[str, Dict[str, Any]],
        node_instances: Dict[str, BaseNode],
    ):
        self.workspace_dir = workspace_dir
        self.graph = graph
        self.nodes_list: List[Dict[str, Any]] = graph.get("nodes") or []
        self.final_id: str = graph.get("final") or ""
        self.pipeline_inputs = pipeline_inputs
        self.pipeline_params = pipeline_params
        self.latest_outputs = latest_outputs
        self.node_instances = node_instances

    def resolve_inputs(self, node_id: str) -> Dict[str, Any]:
        """Resolve bindings for node_id. Unresolved refs → value UNRESOLVED (§3.1)."""
        node_def = self._node_def(node_id)
        if not node_def:
            return {}
        bindings = node_def.get("inputs") or {}
        return resolve_inputs_impl(
            bindings,
            self.latest_outputs,
            self.pipeline_inputs,
            self.pipeline_params,
        )

    def is_executable(self, node_id: str) -> bool:
        """True iff status is ready or done and all required input ports are resolved (§7.1, §7.3)."""
        node = self.node_instances.get(node_id)
        if node is None:
            return False
        st = node.read_status()
        if st not in ("ready", "done"):
            return False
        resolved = self.resolve_inputs(node_id)
        node_def = self._node_def(node_id)
        node_type = (node_def or {}).get("type") or ""
        required = get_required_input_ports(self.workspace_dir, node_type)
        for port in required:
            if port not in resolved or resolved[port] is UNRESOLVED:
                return False
        return True

    def execute_node(self, node_id: str) -> Dict[str, Any]:
        """Call node.execute(inputs, params). Returns dict (possibly {})."""
        node = self.node_instances.get(node_id)
        if node is None:
            return {}
        resolved = self.resolve_inputs(node_id)
        # Replace UNRESOLVED with missing key so node doesn't see sentinel (required already checked)
        inputs = {k: v for k, v in resolved.items() if v is not UNRESOLVED}
        node_def = self._node_def(node_id)
        params_def = (node_def or {}).get("params") or {}
        params = resolve_params(
            params_def,
            self.pipeline_params,
            self.latest_outputs,
            self.pipeline_inputs,
        )
        return node.execute(inputs, params)

    def save_output(self, node_id: str, output: Dict[str, Any]) -> None:
        """Update latest_outputs only if output is not {} (§3.3)."""
        if output != {}:
            self.latest_outputs[node_id] = output

    def get_latest_output(self, node_id: str) -> Dict[str, Any] | None:
        """Return latest output for node_id or None."""
        return self.latest_outputs.get(node_id)

    def _node_def(self, node_id: str) -> Dict[str, Any] | None:
        for n in self.nodes_list:
            if n.get("id") == node_id:
                return n
        return None

    def step(self) -> bool:
        """Execute one runnable node (graph.nodes order). Returns True if any node was executed."""
        for nd in self.nodes_list:
            nid = nd.get("id")
            if not nid:
                continue
            if not self.is_executable(nid):
                continue
            out = self.execute_node(nid)
            self.save_output(nid, out)
            return True
        return False


def build_runner(
    workspace_dir: str,
    graph: Dict[str, Any],
    pipeline_inputs: Dict[str, Any],
    pipeline_params: Dict[str, Any],
    latest_outputs: Dict[str, Dict[str, Any]],
) -> tuple[Runner, Dict[str, BaseNode]]:
    """Build Runner and node_instances (reused in Execution Scope)."""
    nodes_list = graph.get("nodes") or []
    node_instances: Dict[str, BaseNode] = {}
    for nd in nodes_list:
        nid = nd.get("id")
        ntype = nd.get("type")
        if not nid or not ntype:
            continue
        cls = load_node_class(workspace_dir, ntype)
        if cls is not None:
            node_instances[nid] = cls()
    runner = Runner(
        workspace_dir=workspace_dir,
        graph=graph,
        pipeline_inputs=pipeline_inputs,
        pipeline_params=pipeline_params,
        latest_outputs=latest_outputs,
        node_instances=node_instances,
    )
    return runner, node_instances


def load_and_kick_pipeline(
    workspace_dir: str,
    pipeline_path: str,
    initial_inputs: Dict[str, Any] | None = None,
    params: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Load root pipeline from node_pipeline.yaml and execute. Returns output dict (or {})."""
    from .pipeline_node import PipelineNode

    data = load_node_pipeline(pipeline_path)
    pipeline_params = params or data.get("params") or {}
    root = PipelineNode(workspace_dir=workspace_dir, pipeline_data=data)
    return root.execute(initial_inputs or {}, pipeline_params)
