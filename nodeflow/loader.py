"""
NodeFlow v1.2 — node_pipeline.yaml, node.yaml, node class loading and input resolution.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from typing import Any, Dict, Optional, Type

from .config import load_yaml
from .node import BaseNode


# §11.4: Engine supported version (exact match)
SUPPORTED_VERSION = "1.2"


class VersionMismatchError(Exception):
    """§11.4: Raised when YAML version is missing or does not match engine supported version."""


# Sentinel: required port is present but unresolved → node not executable (§3.1)
UNRESOLVED = object()


def load_node_pipeline(file_path: str) -> Dict[str, Any]:
    """Load node_pipeline.yaml. §11.4: version required and must match. Expects graph.nodes, graph.final."""
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
    if "nodes" not in graph:
        raise ValueError(f"graph.nodes required: {file_path}")
    if "final" not in graph:
        raise ValueError(f"graph.final required: {file_path}")
    return data


def load_node_yaml(workspace_dir: str, node_type: str) -> Dict[str, Any]:
    """Load node.yaml for a node type. §11.4: version required and must match. required defaults to true (§7.1)."""
    path = Path(workspace_dir) / "nodes" / node_type / "node.yaml"
    if not path.exists():
        return {}
    data = load_yaml(str(path))
    if not data:
        return {}
    version = data.get("version")
    if version is None:
        raise VersionMismatchError(
            f"Unsupported version: missing in {path}. Engine supports: {SUPPORTED_VERSION}"
        )
    if version != SUPPORTED_VERSION:
        raise VersionMismatchError(
            f"Unsupported version: {version!r} in {path}. Engine supports: {SUPPORTED_VERSION}"
        )
    return data


def load_node_class(workspace_dir: str, node_type: str) -> Optional[Type[BaseNode]]:
    """Load Node class from nodes/<node_type>/node.py. Returns BaseNode subclass or None."""
    node_path = Path(workspace_dir) / "nodes" / node_type / "node.py"
    if not node_path.exists():
        return None
    spec = importlib.util.spec_from_file_location(
        f"nodes.{node_type}.node", str(node_path)
    )
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for name in dir(mod):
        obj = getattr(mod, name)
        if isinstance(obj, type) and issubclass(obj, BaseNode) and obj is not BaseNode:
            return obj
    return None


# Pattern: ${node_id.port}, ${inputs.port}, ${params.param_name}
_REF_PATTERN = re.compile(r"\$\{([^}.]+)\.([^}]+)\}")


def resolve_inputs(
    bindings: Dict[str, Any],
    latest_outputs: Dict[str, Dict[str, Any]],
    pipeline_inputs: Dict[str, Any],
    pipeline_params: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Resolve input bindings to values. Sources: latest_outputs (node_id.port), pipeline_inputs (inputs.port), pipeline_params (params.param_name).
    Undefined refs: do not raise; leave value as UNRESOLVED (§3.1). Caller treats node as not executable.
    """
    resolved: Dict[str, Any] = {}
    for port, ref in (bindings or {}).items():
        if not isinstance(ref, str):
            resolved[port] = ref
            continue
        m = _REF_PATTERN.fullmatch(ref.strip())
        if not m:
            resolved[port] = ref
            continue
        source, key = m.group(1), m.group(2)
        if source == "inputs":
            if key in pipeline_inputs:
                resolved[port] = pipeline_inputs[key]
            else:
                resolved[port] = UNRESOLVED
        elif source == "params":
            if key in pipeline_params:
                resolved[port] = pipeline_params[key]
            else:
                resolved[port] = UNRESOLVED
        else:
            # node_id.port
            if source in latest_outputs:
                out = latest_outputs[source]
                if isinstance(out, dict) and key in out:
                    resolved[port] = out[key]
                else:
                    resolved[port] = UNRESOLVED
            else:
                resolved[port] = UNRESOLVED
    return resolved


def resolve_params(
    params_def: Dict[str, Any],
    pipeline_params: Dict[str, Any],
    latest_outputs: Dict[str, Dict[str, Any]],
    pipeline_inputs: Dict[str, Any],
) -> Dict[str, Any]:
    """Resolve params that may contain ${params.xxx} etc. Same sources as inputs."""
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
            resolved[k] = resolve_params(
                v, pipeline_params, latest_outputs, pipeline_inputs
            )
        else:
            resolved[k] = v
    return resolved


def get_required_input_ports(workspace_dir: str, node_type: str) -> set:
    """Return set of input port names that are required (default true). §7.1."""
    schema = load_node_yaml(workspace_dir, node_type)
    inputs = schema.get("inputs") or {}
    required = set()
    for port, port_schema in inputs.items():
        if isinstance(port_schema, dict):
            if port_schema.get("required", True):
                required.add(port)
        else:
            required.add(port)
    return required
