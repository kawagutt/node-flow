"""
NodeFlow v1.41 — pipeline.yaml の parse、Node 組み立て、node_input_bindings のタプル形生成。
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .config import load_yaml
from .node import BaseNode
from .nodes import LLMNode, OpenRouterNode, PythonScriptNode
from .pipeline_node import PipelineNode

SUPPORTED_VERSION = "1.4"


class VersionMismatchError(Exception):
    """Raised when YAML version is missing or does not match."""


_REF_PATTERN = re.compile(r"\$\{([^}.]+)\.([^}]+)\}")
_REF_PATTERN_DEEP = re.compile(r"\$\{([^}.]+)\.([^}.]+)\.([^}]+)\}")

# node_input_bindings のタプル形式
InputBinding = Tuple[str, ...]


def _ref_to_binding(ref: Any) -> InputBinding | None:
    """${source.key} または ${source.port.inner} をタプルに変換。inner はドット区切り可。"""
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
            return None  # inputs への inner 参照は未サポート
        return ("inputs", rest)
    if source == "params":
        if "." in rest:
            return None  # params への inner 参照は未サポート
        return ("params", rest)
    # node 参照: rest は "port" または "port.inner" の形 → 最初の . で分割して 4-tuple に
    parts = rest.split(".", 1)
    port = parts[0]
    inner = parts[1] if len(parts) == 2 else None
    if inner is not None:
        return ("node", source, port, inner)
    return ("node", source, port)


def _build_node_input_bindings(
    nodes_list: List[Dict[str, Any]],
) -> Dict[str, Dict[str, InputBinding]]:
    """各ノードの inputs を §3.2.1 のタプル形に変換。"""
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
    """type 文字列から Node クラスを返す。loop は NotImplementedError。"""
    if node_type == "python_script":
        return PythonScriptNode
    if node_type == "llm":
        return LLMNode
    if node_type == "openrouter":
        return OpenRouterNode
    if node_type == "pipeline":
        return PipelineNode  # ネスト時は load_pipeline を再帰的に使う
    if node_type == "loop":
        raise NotImplementedError("LoopNode は本版では未サポートです")
    return None


def load_pipeline(workspace_dir: str, file_path: str) -> PipelineNode:
    """
    pipeline.yaml を読み、PipelineNode を組み立てて返す。
    version は "1.4" であること。script パスは python_script ノードで workspace 相対に解決する。
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
        if cls is None:
            raise ValueError(f"Unknown node type: {ntype!r}")
        if cls is PipelineNode:
            raise ValueError("Nested pipeline not supported in this version")
        graph_node_order.append(nid)
        nodes[nid] = cls()
        raw_params = nd.get("params") or {}
        # python_script の script を workspace 相対で絶対パスに
        if ntype == "python_script" and "script" in raw_params:
            script = raw_params["script"]
            if script and not os.path.isabs(script):
                raw_params = {**raw_params, "script": str(Path(workspace_dir) / script)}
        node_param_definitions[nid] = raw_params

    node_input_bindings = _build_node_input_bindings(nodes_list)

    return PipelineNode(
        graph_node_order=graph_node_order,
        nodes=nodes,
        node_input_bindings=node_input_bindings,
        node_param_definitions=node_param_definitions,
        final_id=final_id,
    )


def load_node_pipeline(file_path: str) -> Dict[str, Any]:
    """Load pipeline YAML (raw). Version must be 1.4. For backward compat / tests."""
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
