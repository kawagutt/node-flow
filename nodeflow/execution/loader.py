"""
NodeFlow v1.5 — pipeline.yaml parse and graph assembly.

YAML vocabulary (fixed):
- version: \"1.5\"
- graph.nodes[].id, type, inputs, params
- graph.final: id of the terminal node
- Root graph is always loaded as compose (SerialPipeNode) wrapping listed nodes.

Allowed node type strings (registry keys):
  compose, python_route_by_task_type, python_summarize_result,
  codex_exec, claude_code_exec, kimi_exec, qwen_exec,
  review_dispatch, implement_dispatch
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

import nodeflow.nodes  # noqa: F401 — built-in registration
from nodeflow.core.base_node import BaseNode
from nodeflow.core.registry import registry

from .config import load_yaml

SUPPORTED_VERSION = "1.5"


class VersionMismatchError(Exception):
    """Raised when YAML version is missing or does not match."""


_REF_PATTERN = re.compile(r"\$\{([^}.]+)\.([^}]+)\}")
_REF_PATTERN_DEEP = re.compile(r"\$\{([^}.]+)\.([^}.]+)\.([^}]+)\}")

InputBinding = Tuple[str, ...]


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
) -> Dict[str, Dict[str, InputBinding]]:
    out: Dict[str, Dict[str, InputBinding]] = {}
    for nd in nodes_list:
        nid = nd.get("id")
        if not nid:
            continue
        bindings_raw = nd.get("inputs") or {}
        resolved: Dict[str, InputBinding] = {}
        for port, ref in bindings_raw.items():
            b = _ref_to_binding(ref)
            if b is not None:
                resolved[port] = b
        out[nid] = resolved
    return out


def _node_class_for_type(node_type: str):
    return registry.resolve(node_type)


def load_pipeline(workspace_dir: str, file_path: str) -> BaseNode:
    """
    Load pipeline YAML and return a SerialPipeNode (compose) root wrapping the graph.
    """
    data = load_yaml(file_path)
    if not data:
        raise ValueError(f"Empty or missing pipeline: {file_path}")
    version = data.get("version")
    if version is None:
        raise VersionMismatchError(
            f"Unsupported version: missing. Engine supports: {SUPPORTED_VERSION}"
        )
    if version != SUPPORTED_VERSION:
        raise VersionMismatchError(
            f"Unsupported version: {version!r}. Engine supports: {SUPPORTED_VERSION}"
        )
    graph = data.get("graph") or {}
    nodes_list = graph.get("nodes") or []
    final_id = graph.get("final") or ""
    if not nodes_list or not final_id:
        raise ValueError(f"graph.nodes and graph.final required: {file_path}")

    graph_node_order: List[str] = []
    nodes: Dict[str, BaseNode] = {}
    node_param_definitions: Dict[str, Dict[str, Any]] = {}

    for nd in nodes_list:
        nid = nd.get("id")
        ntype = nd.get("type")
        if not nid or not ntype:
            continue
        cls = _node_class_for_type(ntype)
        if not getattr(cls, "ALLOW_AS_CHILD", True):
            raise ValueError("Nested compose inside graph is not supported")
        graph_node_order.append(nid)
        nodes[nid] = cls()
        raw_params = nd.get("params") or {}
        node_param_definitions[nid] = raw_params

    node_input_bindings = _build_node_input_bindings(nodes_list)
    compose_cls = registry.resolve("compose")

    return compose_cls(
        graph_node_order=graph_node_order,
        nodes=nodes,
        node_input_bindings=node_input_bindings,
        node_param_definitions=node_param_definitions,
        final_id=final_id,
    )


def load_node_pipeline(file_path: str) -> Dict[str, Any]:
    """Load pipeline YAML (raw). Version must be 1.5."""
    data = load_yaml(file_path)
    if not data:
        raise ValueError(f"Empty or missing pipeline: {file_path}")
    version = data.get("version")
    if version is None:
        raise VersionMismatchError(
            f"Unsupported version: missing. Engine supports: {SUPPORTED_VERSION}"
        )
    if version != SUPPORTED_VERSION:
        raise VersionMismatchError(
            f"Unsupported version: {version!r}. Engine supports: {SUPPORTED_VERSION}"
        )
    graph = data.get("graph") or {}
    if "nodes" not in graph or "final" not in graph:
        raise ValueError(f"graph.nodes and graph.final required: {file_path}")
    return data
