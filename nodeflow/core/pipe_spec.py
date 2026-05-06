"""Executable PipeSpec (registry-resolved) and Phase A structural validation."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from nodeflow.core.base_node import RESERVED_TOP_LEVEL_FROM_RUN, BaseNode
from nodeflow.core.source_ref import SourceRef


class PipeSpecValidationError(ValueError):
    """Executable PipeSpec failed Phase A validation."""


_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def _reserved_port(port_name: str) -> bool:
    return port_name in RESERVED_TOP_LEVEL_FROM_RUN


def _validate_graph_node_id(name: str) -> None:
    """Graph node keys must be ``[A-Za-z][A-Za-z0-9_]*`` and not ``input`` / ``output``."""
    if not _NAME_RE.fullmatch(name):
        raise PipeSpecValidationError(f"invalid node_id {name!r}: must match [A-Za-z][A-Za-z0-9_]*")
    if name in ("input", "output"):
        raise PipeSpecValidationError(
            f"invalid node_id {name!r}: names 'input' and 'output' are reserved for wiring"
        )


def _validate_port_name(name: str) -> None:
    """Port names must match the same token pattern and must not be observation reserved."""
    if not _NAME_RE.fullmatch(name):
        raise PipeSpecValidationError(
            f"invalid port name {name!r}: must match [A-Za-z][A-Za-z0-9_]*"
        )
    if _reserved_port(name):
        raise PipeSpecValidationError(f"port name must not be reserved: {name!r}")


@dataclass
class NodeSpec:
    """One child node with declared wiring (executable)."""

    node_id: str
    node: BaseNode
    input_sources: dict[str, SourceRef]
    output_ports: frozenset[str]
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class PipeDeclaration:
    """Pipe-level ports and how pipe outputs are bound (node outputs only)."""

    input_ports: frozenset[str]
    output_sources: dict[str, SourceRef]


@dataclass
class PipeSpec:
    """In-memory executable graph: resolved BaseNode instances + Phase A–valid wiring."""

    graph_node_order: tuple[str, ...]
    pipe: PipeDeclaration
    nodes: Mapping[str, NodeSpec]


def _validate_source_ref(ref: object, where: str) -> SourceRef:
    if not isinstance(ref, SourceRef):
        raise PipeSpecValidationError(f"{where} must be SourceRef")
    _validate_port_name(ref.port_name)
    return ref


def _normalize_source_key(source: SourceRef) -> tuple[str, str | None, str]:
    return (source.kind, source.node_id, source.port_name)


def validate_executable_pipe_spec(spec: PipeSpec) -> None:
    """Validate registry-resolved executable PipeSpec (Phase A only; no JSON / type strings)."""
    if not spec.nodes:
        raise PipeSpecValidationError("PipeSpec.nodes must not be empty")
    for nid in spec.nodes:
        _validate_graph_node_id(nid)
    node_ids = frozenset(spec.nodes.keys())
    for nid, ns in spec.nodes.items():
        if ns.node_id != nid:
            raise PipeSpecValidationError(
                f"NodeSpec.node_id {ns.node_id!r} does not match nodes key {nid!r}"
            )
        if not isinstance(ns.node, BaseNode):
            raise PipeSpecValidationError(f"NodeSpec.node for {nid!r} must be BaseNode instance")
        if not isinstance(ns.input_sources, dict):
            raise PipeSpecValidationError(f"NodeSpec.input_sources for {nid!r} must be dict")
        if not isinstance(ns.output_ports, frozenset):
            raise PipeSpecValidationError(f"NodeSpec.output_ports for {nid!r} must be frozenset")
        if not isinstance(ns.params, dict):
            raise PipeSpecValidationError(f"NodeSpec.params for {nid!r} must be dict")
    order = tuple(spec.graph_node_order)
    for step_id in order:
        _validate_graph_node_id(step_id)
    if len(order) != len(node_ids) or frozenset(order) != node_ids:
        raise PipeSpecValidationError(
            "graph_node_order must be a permutation of nodes keys without duplicates "
            f"(order={order!r}, keys={sorted(node_ids)!r})"
        )
    for port in spec.pipe.input_ports:
        _validate_port_name(port)
    if not spec.pipe.output_sources:
        raise PipeSpecValidationError("pipe.output_sources must not be empty")
    for port in spec.pipe.output_sources:
        _validate_port_name(port)
    source_to_target: dict[tuple[str, str | None, str], str] = {}

    def _register_fanout_target(src: SourceRef, target_label: str) -> None:
        sk = _normalize_source_key(src)
        if sk in source_to_target:
            raise PipeSpecValidationError(
                "fan-out rejected: same source wired to multiple delivery targets "
                f"{source_to_target[sk]!r} and {target_label!r}"
            )
        source_to_target[sk] = target_label

    for out_name, src in spec.pipe.output_sources.items():
        src = _validate_source_ref(src, f"pipe.output_sources[{out_name!r}]")
        if src.kind != "node":
            raise PipeSpecValidationError(
                f"pipe.output_sources[{out_name!r}] must use kind='node' only, got {src.kind!r}"
            )
        if not src.node_id or src.node_id not in node_ids:
            raise PipeSpecValidationError(
                f"pipe.output_sources[{out_name!r}] references unknown node_id {src.node_id!r}"
            )
        out_ns = spec.nodes[src.node_id]
        if src.port_name not in out_ns.output_ports:
            raise PipeSpecValidationError(
                f"pipe.output_sources[{out_name!r}] references port {src.port_name!r} "
                f"not in output_ports of node {src.node_id!r}"
            )
        _register_fanout_target(src, f"pipe.output:{out_name}")

    for target_nid, ns in spec.nodes.items():
        for op in ns.output_ports:
            _validate_port_name(op)
        for target_port, src in ns.input_sources.items():
            _validate_port_name(target_port)
            src = _validate_source_ref(src, f"node {target_nid!r} input {target_port!r}")
            if src.kind == "input":
                if src.node_id is not None:
                    raise PipeSpecValidationError(
                        f"node {target_nid!r} input {target_port!r}: input source must have "
                        "node_id=None"
                    )
                if src.port_name not in spec.pipe.input_ports:
                    raise PipeSpecValidationError(
                        f"node {target_nid!r} input {target_port!r}: unknown pipe input port "
                        f"{src.port_name!r}"
                    )
            elif src.kind == "node":
                if not src.node_id or src.node_id not in node_ids:
                    raise PipeSpecValidationError(
                        f"node {target_nid!r} input {target_port!r}: unknown source node "
                        f"{src.node_id!r}"
                    )
                src_ns = spec.nodes[src.node_id]
                if src.port_name not in src_ns.output_ports:
                    raise PipeSpecValidationError(
                        f"node {target_nid!r} input {target_port!r}: source node {src.node_id!r} "
                        f"has no declared output port {src.port_name!r}"
                    )
            else:
                raise PipeSpecValidationError(f"unsupported SourceRef.kind {src.kind!r}")
            _register_fanout_target(src, f"node:{target_nid}.{target_port}")

    if _cycle_among_nodes(spec):
        raise PipeSpecValidationError("graph contains a cycle (node-to-node edges)")


def _cycle_among_nodes(spec: PipeSpec) -> bool:
    edges: dict[str, list[str]] = {nid: [] for nid in spec.nodes}
    for target_nid, ns in spec.nodes.items():
        for src in ns.input_sources.values():
            if src.kind == "node" and src.node_id:
                edges[src.node_id].append(target_nid)

    visited: set[str] = set()
    stack: set[str] = set()

    def dfs(u: str) -> bool:
        visited.add(u)
        stack.add(u)
        for v in edges.get(u, []):
            if v not in visited:
                if dfs(v):
                    return True
            elif v in stack:
                return True
        stack.remove(u)
        return False

    for nid in spec.nodes:
        if nid not in visited:
            if dfs(nid):
                return True
    return False
