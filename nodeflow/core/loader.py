"""v1.6 JSON PipeSpec loading — **JSON only**.

There is no YAML parse path in this module. ``load_pipeline`` keeps the legacy name used when
YAML 1.5 existed, but today it **only reads v1.6 PipeSpec ``*.json``** (§16.3 in
``doc/nodeflow_spec.md``). Paths ending in ``*.yaml`` / ``*.yml`` raise
``NotImplementedError`` explicitly (removed product path).

Use :func:`load_pipe_spec_from_json_object` / :func:`load_pipe_spec_from_json_bytes` for in-memory /
bytes input. Executable results are validated with
:class:`~nodeflow.core.pipe_spec.validate_executable_pipe_spec`.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

from nodeflow.core.base_node import RESERVED_TOP_LEVEL_FROM_RUN, BaseNode
from nodeflow.core.pipe_spec import (
    NodeSpec,
    PipeDeclaration,
    PipeSpec,
    PipeSpecValidationError,
    validate_executable_pipe_spec,
)
from nodeflow.core.registry import NodeRegistry, UnknownNodeTypeError
from nodeflow.core.registry import registry as default_registry
from nodeflow.core.source_ref import SourceRef

_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


class PipeSpecLoadError(ValueError):
    """JSON PipeSpec failed structural checks before or during registry normalization."""


def _reject_reserved_port(where: str, name: str) -> None:
    if name in RESERVED_TOP_LEVEL_FROM_RUN:
        raise PipeSpecLoadError(f"{where}: observation port names are not allowed: {name!r}")


def _parse_wire_source(where: str, raw: Any) -> str:
    if not isinstance(raw, str):
        raise PipeSpecLoadError(f"{where}: source must be a string, got {type(raw).__name__}")
    s = raw.strip()
    if not s:
        raise PipeSpecLoadError(f"{where}: source must not be empty")
    return s


def _source_ref_from_wire(where: str, wire: str) -> SourceRef:
    """External ``input.<pipe_in>`` or ``<node_id>.<output_port>`` wire string → SourceRef."""
    wire = _parse_wire_source(where, wire)
    if wire.startswith("input."):
        pn = wire[len("input.") :]
        if not pn:
            raise PipeSpecLoadError(f"{where}: invalid input source wire {wire!r}")
        _reject_reserved_port(where, pn)
        if not _NAME_RE.fullmatch(pn):
            raise PipeSpecLoadError(
                f"{where}: pipe input port fragment {pn!r} must match [A-Za-z][A-Za-z0-9_]*"
            )
        return SourceRef(kind="input", port_name=pn, node_id=None)
    dot = wire.find(".")
    if dot <= 0 or dot >= len(wire) - 1:
        raise PipeSpecLoadError(f"{where}: invalid wire {wire!r}; expected '<node>.<port>'")
    node_id = wire[:dot]
    port_name = wire[dot + 1 :]
    _reject_reserved_port(where, port_name)
    if not _NAME_RE.fullmatch(node_id):
        raise PipeSpecLoadError(f"{where}: node id in {wire!r} must match [A-Za-z][A-Za-z0-9_]*")
    if not _NAME_RE.fullmatch(port_name):
        raise PipeSpecLoadError(
            f"{where}: output port in {wire!r} must match [A-Za-z][A-Za-z0-9_]*"
        )
    return SourceRef(kind="node", node_id=node_id, port_name=port_name)


def _ensure_only_keys(where: str, obj: Mapping[str, Any], allowed: frozenset[str]) -> None:
    extras = frozenset(obj.keys()) - allowed
    if extras:
        sorted_ex = sorted(extras)
        raise PipeSpecLoadError(f"{where}: unknown keys {sorted_ex!r}")


def _topological_order_for_pipe_spec(
    node_ids_ordered: tuple[str, ...], node_specs_temp: Mapping[str, Any]
) -> tuple[str, ...]:
    """Compute ``graph_node_order`` for downstream :class:`~nodeflow.core.pipe_spec.PipeSpec`.

    JSON key order resolves ties among zero-indegree nodes. Cycles among node-to-node
    edges prevent a completion order here and are rejected (Phase A also rejects cycles).
    """

    nid_set = frozenset(node_ids_ordered)
    index = {nid: i for i, nid in enumerate(node_ids_ordered)}
    indeg = {nid: 0 for nid in node_ids_ordered}
    successors: dict[str, list[str]] = {nid: [] for nid in node_ids_ordered}
    for target in node_ids_ordered:
        wired = node_specs_temp[target]["input_refs"]
        for src in wired.values():
            if src.kind == "node" and src.node_id in nid_set:
                u, v = src.node_id, target
                successors[u].append(v)
                indeg[v] += 1
    heads = sorted((nid for nid in node_ids_ordered if indeg[nid] == 0), key=index.__getitem__)
    out: list[str] = []
    while heads:
        u = heads.pop(0)
        out.append(u)
        for v in successors[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                heads.append(v)
        heads.sort(key=index.__getitem__)
    if len(out) != len(node_ids_ordered):
        raise PipeSpecLoadError(
            "graph contains a cycle (node-to-node edges) or malformed wiring before validator"
        )
    return tuple(out)


def load_pipe_spec_from_json_object(
    data: Mapping[str, Any],
    *,
    reg: NodeRegistry | None = None,
) -> PipeSpec:
    """Parse validated v1.6 JSON object → executable PipeSpec (registry-resolve + Phase A)."""
    if not isinstance(data, Mapping):
        raise PipeSpecLoadError(f"PipeSpec JSON root must be an object, got {type(data).__name__}")

    try:
        _ensure_only_keys("root", data, frozenset({"pipe", "nodes"}))
    except PipeSpecLoadError:
        raise
    pipe_blob = data.get("pipe")
    nodes_blob = data.get("nodes")
    if not isinstance(pipe_blob, dict):
        raise PipeSpecLoadError(f"root.pipe must be an object, got {type(pipe_blob).__name__}")
    if not isinstance(nodes_blob, dict):
        raise PipeSpecLoadError(f"root.nodes must be an object, got {type(nodes_blob).__name__}")

    try:
        _ensure_only_keys("pipe", pipe_blob, frozenset({"input_ports", "output_ports"}))
    except PipeSpecLoadError:
        raise
    input_ports_js = pipe_blob.get("input_ports")
    output_ports_js = pipe_blob.get("output_ports")
    if not isinstance(input_ports_js, list):
        raise PipeSpecLoadError(
            f"pipe.input_ports must be an array, got {type(input_ports_js).__name__}"
        )
    pipe_input_list: list[str] = []
    seen_in: set[str] = set()
    for idx, raw in enumerate(input_ports_js):
        if not isinstance(raw, str):
            raise PipeSpecLoadError(
                f"pipe.input_ports[{idx}] must be a string, got {type(raw).__name__}"
            )
        pn = raw.strip()
        if not pn or pn != raw:
            raise PipeSpecLoadError(f"pipe.input_ports[{idx}] must be a bare port token")
        _reject_reserved_port(f"pipe.input_ports[{idx}]", pn)
        if not _NAME_RE.fullmatch(pn):
            raise PipeSpecLoadError(
                f"pipe.input_ports[{idx}] {pn!r} must match [A-Za-z][A-Za-z0-9_]*"
            )
        if pn in seen_in:
            raise PipeSpecLoadError(f"pipe.input_ports: duplicate port {pn!r}")
        seen_in.add(pn)
        pipe_input_list.append(pn)

    if not isinstance(output_ports_js, dict):
        raise PipeSpecLoadError(
            f"pipe.output_ports must be an object, got {type(output_ports_js).__name__}"
        )
    pipe_out_wires: dict[str, SourceRef] = {}
    seen_out_ports: set[str] = set()
    for raw_key, raw_val in output_ports_js.items():
        where = f"pipe.output_ports[{raw_key!r}]"
        if not isinstance(raw_key, str):
            raise PipeSpecLoadError("pipe.output_ports keys must be strings")
        pn = raw_key.strip()
        if not pn or pn != raw_key:
            raise PipeSpecLoadError(
                f"pipe.output_ports: invalid output port key {raw_key!r}: "
                "must be a bare token without surrounding whitespace"
            )
        _reject_reserved_port(where, pn)
        if not _NAME_RE.fullmatch(pn):
            raise PipeSpecLoadError(
                f"pipe.output_ports: invalid output port key {raw_key!r}: "
                "must match [A-Za-z][A-Za-z0-9_]*"
            )
        if pn in seen_out_ports:
            raise PipeSpecLoadError(f"pipe.output_ports: duplicate key {pn!r}")
        seen_out_ports.add(pn)
        ref = _source_ref_from_wire(where, raw_val)
        pipe_out_wires[pn] = ref

    if not nodes_blob:
        raise PipeSpecLoadError("nodes must not be empty")

    resolved_reg = reg if reg is not None else default_registry
    nodes_out: dict[str, NodeSpec] = {}
    prespec: dict[str, dict[str, Any]] = {}

    for node_key, blob in nodes_blob.items():
        if not isinstance(node_key, str):
            raise PipeSpecLoadError(f"nodes keys must be strings, got {type(node_key).__name__}")
        nid = node_key.strip()
        if not nid or nid != node_key or not _NAME_RE.fullmatch(nid):
            raise PipeSpecLoadError(
                f"invalid node id key {node_key!r}: must match [A-Za-z][A-Za-z0-9_]* bare token"
            )
        if nid in ("input", "output"):
            raise PipeSpecLoadError(f"reserved node id {nid!r}")
        if nid in prespec:
            raise PipeSpecLoadError(f"duplicate node id {nid!r}")
        where_base = f"nodes[{nid!r}]"

        if not isinstance(blob, dict):
            raise PipeSpecLoadError(
                f"{where_base}: value must be an object, got {type(blob).__name__}"
            )
        try:
            _ensure_only_keys(
                where_base, blob, frozenset({"type", "params", "input_ports", "output_ports"})
            )
        except PipeSpecLoadError:
            raise
        type_raw = blob.get("type")
        params_raw = blob.get("params")
        in_ports_raw = blob.get("input_ports")
        outs_raw = blob.get("output_ports")
        if not isinstance(type_raw, str) or not type_raw.strip() or type_raw != type_raw.strip():
            raise PipeSpecLoadError(f"{where_base}.type must be a non-empty bare string")
        type_name = type_raw.strip()

        if not isinstance(params_raw, dict):
            raise PipeSpecLoadError(f"{where_base}.params must be an object (use {{}} when empty)")

        try:
            node_cls = resolved_reg.resolve(type_name)
        except UnknownNodeTypeError as e:
            raise PipeSpecLoadError(str(e)) from e
        if not isinstance(node_cls, type) or not issubclass(node_cls, BaseNode):
            raise PipeSpecLoadError(
                f"{where_base}: registry resolved non-BaseNode class for type {type_name!r}"
            )

        try:
            instance = node_cls()
        except TypeError as e:
            raise PipeSpecLoadError(
                f"{where_base}: node type {type_name!r} must be constructible with no arguments; "
                "JSON params are stored in NodeSpec.params and passed by Runner at execution time"
            ) from e
        if not isinstance(in_ports_raw, dict):
            raise PipeSpecLoadError(
                f"{where_base}.input_ports must be an object, got {type(in_ports_raw).__name__}"
            )
        input_refs: dict[str, SourceRef] = {}
        seen_tgt: set[str] = set()
        for ipt_key, ipt_val in in_ports_raw.items():
            ipt_where = f"{where_base}.input_ports[{ipt_key!r}]"
            if not isinstance(ipt_key, str):
                raise PipeSpecLoadError(f"{ipt_where}: keys must be strings")
            ipt = ipt_key.strip()
            if not ipt or ipt != ipt_key:
                raise PipeSpecLoadError(
                    f"{ipt_where}: port key must be a bare token without spaces"
                )
            _reject_reserved_port(ipt_where, ipt)
            if not _NAME_RE.fullmatch(ipt):
                raise PipeSpecLoadError(
                    f"{ipt_where}: invalid port key {ipt_key!r} (pattern [A-Za-z][A-Za-z0-9_]*)"
                )
            if ipt in seen_tgt:
                raise PipeSpecLoadError(f"{where_base}: duplicate input port {ipt!r}")
            seen_tgt.add(ipt)
            src_ref = _source_ref_from_wire(ipt_where, ipt_val)
            if src_ref.kind == "input" and src_ref.port_name not in frozenset(pipe_input_list):
                raise PipeSpecLoadError(
                    f"{ipt_where}: refers to unknown pipe input port {src_ref.port_name!r}"
                )
            input_refs[ipt] = src_ref

        if not isinstance(outs_raw, list):
            raise PipeSpecLoadError(
                f"{where_base}.output_ports must be an array, got {type(outs_raw).__name__}"
            )
        out_names: list[str] = []
        seen_op: set[str] = set()
        for opi, elt in enumerate(outs_raw):
            if not isinstance(elt, str):
                raise PipeSpecLoadError(f"{where_base}.output_ports[{opi}] must be a string")
            op = elt.strip()
            if not op or op != elt:
                raise PipeSpecLoadError(f"{where_base}.output_ports[{opi}] must be a bare token")
            op_where = f"{where_base}.output_ports[{opi}]"
            _reject_reserved_port(op_where, op)
            if not _NAME_RE.fullmatch(op):
                raise PipeSpecLoadError(f"{op_where} {op!r} must match [A-Za-z][A-Za-z0-9_]*")
            if op in seen_op:
                raise PipeSpecLoadError(f"{where_base}.output_ports: duplicate {op!r}")
            seen_op.add(op)
            out_names.append(op)

        prespec[nid] = {"input_refs": input_refs, "output_names": out_names, "instance": instance}
        nodes_out[nid] = NodeSpec(
            node_id=nid,
            node=instance,
            input_sources=input_refs,
            output_ports=frozenset(out_names),
            params=dict(params_raw),
        )

    node_ids_ordered = tuple(nodes_blob.keys())
    order = _topological_order_for_pipe_spec(node_ids_ordered, prespec)

    for out_name, src in pipe_out_wires.items():
        if src.kind == "input":
            raise PipeSpecLoadError(
                f"pipe.output_ports[{out_name!r}]: must not wire from "
                f"input.* (pass-through forbidden); use a child node"
            )
        if not src.node_id or src.node_id not in nodes_out:
            raise PipeSpecLoadError(
                f"pipe.output_ports[{out_name!r}]: unknown node_id {src.node_id!r}"
            )
        if src.port_name not in prespec[src.node_id]["output_names"]:
            raise PipeSpecLoadError(
                f"pipe.output_ports[{out_name!r}]: source port {src.port_name!r} "
                f"not declared on node {src.node_id!r}"
            )

    for nid, ns in nodes_out.items():
        for target_port, src in ns.input_sources.items():
            if src.kind == "node":
                if not src.node_id or src.node_id not in nodes_out:
                    raise PipeSpecLoadError(
                        f"node {nid!r} input {target_port!r}: unknown source node {src.node_id!r}"
                    )
                if src.port_name not in prespec[src.node_id]["output_names"]:
                    raise PipeSpecLoadError(
                        f"node {nid!r} input {target_port!r}: source port {src.port_name!r} "
                        f"not declared on node {src.node_id!r}"
                    )

    spec = PipeSpec(
        graph_node_order=order,
        pipe=PipeDeclaration(
            input_ports=frozenset(pipe_input_list),
            output_sources=pipe_out_wires,
        ),
        nodes=nodes_out,
    )
    try:
        validate_executable_pipe_spec(spec)
    except PipeSpecValidationError as e:
        raise PipeSpecLoadError(str(e)) from e
    return spec


def load_pipe_spec_from_json_bytes(
    raw: bytes,
    *,
    reg: NodeRegistry | None = None,
) -> PipeSpec:
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise PipeSpecLoadError(f"invalid UTF-8 or JSON: {e}") from e
    if not isinstance(data, dict):
        raise PipeSpecLoadError("JSON document root must be an object")
    return load_pipe_spec_from_json_object(data, reg=reg)


def load_pipeline(
    workspace_dir: str,
    file_path: str,
    *,
    reg: NodeRegistry | None = None,
) -> PipeSpec:
    """Load **JSON only**: v1.6 PipeSpec from ``file_path`` (relative to ``workspace_dir`` if needed).

    Compatibility name retained from pre–v1.6 YAML loaders; behaviour is strictly ``*.json`` →
    :func:`load_pipe_spec_from_json_bytes`. ``*.yaml`` / ``*.yml`` raise ``NotImplementedError``.
    Pass ``reg`` to resolve ``type`` strings against a registry other than the module default.
    """
    root = Path(workspace_dir)
    path = Path(file_path)
    if not path.is_absolute():
        path = root / path
    path = path.resolve()
    suffix = path.suffix.lower()
    if suffix in (".yaml", ".yml"):
        raise NotImplementedError(
            "YAML 1.5 pipeline loading was removed from nodeflow.core. "
            "Use a v1.6 PipeSpec JSON file (.json) with load_pipeline, or "
            "PipeNode + pipe_spec() for in-memory graphs."
        )
    if suffix != ".json":
        raise PipeSpecLoadError(
            f"unsupported pipeline file type {suffix!r}; expected .json (v1.6 PipeSpec)"
        )
    try:
        raw = path.read_bytes()
    except OSError as e:
        raise PipeSpecLoadError(f"cannot read {path}: {e}") from e
    return load_pipe_spec_from_json_bytes(raw, reg=reg)


def load_node_pipeline(_file_path: str) -> dict:
    """Removed: use :func:`load_pipe_spec_from_json_object` / full PipeSpec graphs."""
    raise NotImplementedError(
        "Per-node YAML pipeline snippets were removed. Use v1.6 PipeSpec JSON or pipe_spec()."
    )
