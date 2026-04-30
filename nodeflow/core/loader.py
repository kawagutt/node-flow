"""
NodeFlow — pipeline YAML parse and graph assembly.

YAML vocabulary (fixed):
- version: \"1.5\"
- graph.nodes[].id, type, inputs, params
- graph.final: id of the terminal node
- Root graph is assembled internally as ``PipeNode`` (not a public YAML ``type``).

Allowed node type strings (registry keys for ``graph.nodes[].type``):
  python_route_by_task_type, python_summarize_result,
  codex_exec, claude_code_exec, kimi_exec, qwen_exec,
  review_with_claude, implement_with_codex,
  workflows.development_flow.spec_plan,
  workflows.development_flow.implement,
  workflows.development_flow.review,
  workflows.development_flow
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Set, Tuple

import nodeflow.nodes  # noqa: F401 — built-in registration
from nodeflow.core.base_node import BaseNode
from nodeflow.core.graph_spec import GraphSpec, InputBinding
from nodeflow.core.node_kinds import PipeNode
from nodeflow.core.registry import registry

from .config import load_yaml

SUPPORTED_VERSION = "1.5"


class VersionMismatchError(Exception):
    """Raised when YAML version is missing or does not match."""


class _GraphPipeNode(PipeNode):
    """Root pipeline graph: stable GraphSpec for default run() / read_error()."""

    def __init__(self, spec: GraphSpec) -> None:
        super().__init__()
        self._spec = spec

    def graph(self) -> GraphSpec:
        return self._spec


_REF_PATTERN = re.compile(r"\$\{([^}.]+)\.([^}]+)\}")
_REF_PATTERN_DEEP = re.compile(r"\$\{([^}.]+)\.([^}.]+)\.([^}]+)\}")


def _ref_to_binding(ref: Any) -> InputBinding | None:
    if not isinstance(ref, str):
        return None
    s = ref.strip()
    m_deep = _REF_PATTERN_DEEP.fullmatch(s)
    if m_deep:
        src, port, inner = m_deep.group(1), m_deep.group(2), m_deep.group(3)
        if src in ("inputs", "params"):
            return None
        return ("node", src, port, inner)
    m = _REF_PATTERN.fullmatch(s)
    if not m:
        return None
    source, rest = m.group(1), m.group(2)
    if source == "inputs":
        if "." in rest:
            return None
        return ("inputs", rest)
    if source == "params":
        if "." in rest:
            return None
        return ("params", rest)
    parts = rest.split(".", 1)
    port = parts[0]
    inner = parts[1] if len(parts) == 2 else None
    if inner is not None:
        return ("node", source, port, inner)
    return ("node", source, port)


def _build_node_input_bindings(
    nodes_list: List[Dict[str, Any]],
    node_ids: Set[str],
) -> Dict[str, Dict[str, InputBinding]]:
    out: Dict[str, Dict[str, InputBinding]] = {}
    seen_node_ids: Set[str] = set()
    for idx, nd in enumerate(nodes_list):
        nid = nd["id"]
        bindings_raw = nd.get("inputs", {})
        if not isinstance(bindings_raw, dict):
            raise ValueError(f"graph.nodes[{idx}].inputs must be a dict")
        resolved: Dict[str, InputBinding] = {}
        for port, ref in bindings_raw.items():
            if not isinstance(port, str) or not port:
                raise ValueError(f"graph.nodes[{idx}].inputs has invalid port key")
            b = _ref_to_binding(ref)
            if b is None:
                if (
                    isinstance(ref, str)
                    and ref.strip().startswith("${")
                    and ref.strip().endswith("}")
                ):
                    raise ValueError(
                        f"graph.nodes[{idx}].inputs.{port} has invalid reference syntax: {ref!r}"
                    )
                raise ValueError(
                    f"graph.nodes[{idx}].inputs.{port} must be a reference string like "
                    "'${inputs.x}' or '${node.port}'"
                )
            if b[0] == "node" and b[1] not in node_ids:
                raise ValueError(
                    f"graph.nodes[{idx}].inputs.{port} references unknown node id: {b[1]!r}"
                )
            if b[0] == "node" and b[1] not in seen_node_ids:
                raise ValueError(
                    f"graph.nodes[{idx}].inputs.{port} references node {b[1]!r} "
                    "before it is available"
                )
            resolved[port] = b
        out[nid] = resolved
        seen_node_ids.add(nid)
    return out


def _node_class_for_type(node_type: str):
    return registry.resolve(node_type)


def _validate_pipeline_root(data: Any, file_path: str) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError(f"pipeline YAML root must be a mapping: {file_path}")
    return data


def _validate_graph_shape(data: Dict[str, Any], file_path: str) -> Tuple[List[Any], str]:
    graph = data.get("graph")
    if not isinstance(graph, dict):
        raise ValueError(f"graph must be a mapping: {file_path}")
    nodes_list = graph.get("nodes")
    if not isinstance(nodes_list, list) or not nodes_list:
        raise ValueError(f"graph.nodes must be a non-empty list: {file_path}")
    final_id = graph.get("final")
    if not isinstance(final_id, str) or not final_id:
        raise ValueError(f"graph.final must be a non-empty string id: {file_path}")
    return nodes_list, final_id


def load_pipeline(workspace_dir: str, file_path: str) -> BaseNode:
    """
    Load pipeline YAML and return a root ``PipeNode`` wrapping the graph (not via registry).
    """
    data = load_yaml(file_path)
    if not data:
        raise ValueError(f"Empty or missing pipeline: {file_path}")
    data = _validate_pipeline_root(data, file_path)
    version = data.get("version")
    if version is None:
        raise VersionMismatchError(
            f"Unsupported version: missing. Engine supports: {SUPPORTED_VERSION}"
        )
    if version != SUPPORTED_VERSION:
        raise VersionMismatchError(
            f"Unsupported version: {version!r}. Engine supports: {SUPPORTED_VERSION}"
        )
    nodes_list, final_id = _validate_graph_shape(data, file_path)

    graph_node_order: List[str] = []
    nodes: Dict[str, BaseNode] = {}
    node_param_definitions: Dict[str, Dict[str, Any]] = {}
    node_ids: Set[str] = set()

    for idx, nd in enumerate(nodes_list):
        if not isinstance(nd, dict):
            raise ValueError(f"graph.nodes[{idx}] must be a mapping")
        nid = nd.get("id")
        ntype = nd.get("type")
        if not isinstance(nid, str) or not nid:
            raise ValueError(f"graph.nodes[{idx}].id is required and must be a non-empty string")
        if not isinstance(ntype, str) or not ntype:
            raise ValueError(f"graph.nodes[{idx}].type is required and must be a non-empty string")
        if nid in node_ids:
            raise ValueError(f"graph.nodes has duplicate id: {nid!r}")
        node_ids.add(nid)
        cls = _node_class_for_type(ntype)
        # Composable in YAML graphs by default; opt-out for types that must not appear
        # as graph nodes (aligns with composite PipeNode children in doc/nodeflow_spec.md).
        if not getattr(cls, "ALLOW_AS_CHILD", True):
            raise ValueError("This node type is not allowed as a child in graph.nodes")
        graph_node_order.append(nid)
        nodes[nid] = cls()
        raw_params = nd.get("params", {})
        if not isinstance(raw_params, dict):
            raise ValueError(f"graph.nodes[{idx}].params must be a dict")
        node_param_definitions[nid] = raw_params

    if final_id not in nodes:
        raise ValueError(f"graph.final references unknown node id: {final_id!r}")

    node_input_bindings = _build_node_input_bindings(nodes_list, node_ids)

    spec = GraphSpec(
        nodes=nodes,
        order=graph_node_order,
        bindings=node_input_bindings,
        params=node_param_definitions,
        final=final_id,
    )
    return _GraphPipeNode(spec)


def load_node_pipeline(file_path: str) -> Dict[str, Any]:
    """Load pipeline YAML (raw). Version and top-level graph shape are validated.

    This helper validates only root/shape constraints; executable node-level
    validation (id/type uniqueness, refs, final existence) is done in
    ``load_pipeline()``.
    """
    data = load_yaml(file_path)
    if not data:
        raise ValueError(f"Empty or missing pipeline: {file_path}")
    data = _validate_pipeline_root(data, file_path)
    version = data.get("version")
    if version is None:
        raise VersionMismatchError(
            f"Unsupported version: missing. Engine supports: {SUPPORTED_VERSION}"
        )
    if version != SUPPORTED_VERSION:
        raise VersionMismatchError(
            f"Unsupported version: {version!r}. Engine supports: {SUPPORTED_VERSION}"
        )
    _validate_graph_shape(data, file_path)
    return data
