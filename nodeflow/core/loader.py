"""PipeSpec v1.7 JSON loading — **JSON only** (``doc/nodeflow_spec.md`` §10).

``nodes[*].path`` resolves relative to ``workspace_dir``. YAML pipeline paths raise
``NotImplementedError``. v1.6-style PipeSpec documents are rejected.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

from nodeflow.core.base_node import RESERVED_TOP_LEVEL_FROM_RUN, BaseNode
from nodeflow.core.node_kinds.pipe_node import PipeNode
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
    """``input.<pipe_in>`` or ``<node_id>.<output_port>`` wire string → SourceRef."""
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


def _bare_token(where: str, raw_key: str, *, label: str) -> str:
    if not isinstance(raw_key, str):
        raise PipeSpecLoadError(f"{where}: {label} keys must be strings")
    tok = raw_key.strip()
    if not tok or tok != raw_key:
        raise PipeSpecLoadError(f"{where}: {label} key must be a bare token without spaces")
    return tok


def _parse_port_list(where: str, raw: Any, *, field: str) -> list[str]:
    if not isinstance(raw, list):
        raise PipeSpecLoadError(f"{where}.{field} must be an array, got {type(raw).__name__}")
    out: list[str] = []
    seen: set[str] = set()
    for idx, elt in enumerate(raw):
        if not isinstance(elt, str):
            raise PipeSpecLoadError(f"{where}.{field}[{idx}] must be a string")
        pn = elt.strip()
        if not pn or pn != elt:
            raise PipeSpecLoadError(f"{where}.{field}[{idx}] must be a bare token")
        pw = f"{where}.{field}[{idx}]"
        _reject_reserved_port(pw, pn)
        if not _NAME_RE.fullmatch(pn):
            raise PipeSpecLoadError(f"{pw} {pn!r} must match [A-Za-z][A-Za-z0-9_]*")
        if pn in seen:
            raise PipeSpecLoadError(f"{where}.{field}: duplicate port {pn!r}")
        seen.add(pn)
        out.append(pn)
    return out


def _load_child_definition(
    workspace: Path,
    path_str: str,
    reg: NodeRegistry,
) -> tuple[BaseNode, frozenset[str], frozenset[str], dict[str, Any]]:
    if not isinstance(path_str, str) or not path_str.strip():
        raise PipeSpecLoadError("nodes[*].path must be a non-empty string")
    path = (workspace / path_str.strip()).resolve()
    if not path.is_file():
        raise PipeSpecLoadError(f"definition file not found: {path}")
    try:
        blob = json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as e:
        raise PipeSpecLoadError(f"cannot read JSON {path}: {e}") from e
    if not isinstance(blob, Mapping):
        raise PipeSpecLoadError(f"{path}: root must be an object")
    kind = blob.get("kind")
    version = blob.get("version")

    if kind == "pipe" and version == "1.7":
        _ensure_only_keys(str(path), blob, frozenset({"kind", "version", "pipe", "nodes"}))
        inner = _build_v17_executable_pipe(blob, workspace, reg)
        return (
            PipeNode(inner),
            inner.pipe.input_ports,
            frozenset(inner.pipe.output_sources.keys()),
            {},
        )

    if kind == "node" and version == "1.7":
        _ensure_only_keys(
            str(path),
            blob,
            frozenset({"kind", "version", "type", "input_ports", "output_ports", "default_config"}),
        )
        type_raw = blob.get("type")
        if not isinstance(type_raw, str) or not type_raw.strip() or type_raw.strip() != type_raw:
            raise PipeSpecLoadError(f"{path}: type must be a non-empty bare string")
        type_name = type_raw.strip()
        defaults_any = blob.get("default_config", {})
        if defaults_any is None:
            defaults_any = {}
        if not isinstance(defaults_any, dict):
            raise PipeSpecLoadError(f"{path}: default_config must be an object when present")
        in_list = _parse_port_list(str(path), blob.get("input_ports"), field="input_ports")
        out_list = _parse_port_list(str(path), blob.get("output_ports"), field="output_ports")

        try:
            node_cls = reg.resolve(type_name)
        except UnknownNodeTypeError as e:
            raise PipeSpecLoadError(str(e)) from e
        if not isinstance(node_cls, type) or not issubclass(node_cls, BaseNode):
            raise PipeSpecLoadError(
                f"{path}: registry resolved non-BaseNode class for {type_name!r}"
            )

        try:
            instance = node_cls()
        except TypeError as e:
            raise PipeSpecLoadError(
                f"{path}: node type {type_name!r} must be constructible with no arguments"
            ) from e

        return instance, frozenset(in_list), frozenset(out_list), dict(defaults_any)

    raise PipeSpecLoadError(
        f"{path}: unsupported definition (expected kind 'node'|'pipe' with version '1.7')"
    )


def _build_v17_executable_pipe(
    data: Mapping[str, Any], workspace: Path, reg: NodeRegistry
) -> PipeSpec:
    _ensure_only_keys("root", data, frozenset({"kind", "version", "pipe", "nodes"}))
    if data.get("kind") != "pipe" or data.get("version") != "1.7":
        raise PipeSpecLoadError("pipe document must declare kind='pipe' and version='1.7'")

    pipe_blob = data["pipe"]
    if not isinstance(pipe_blob, dict):
        raise PipeSpecLoadError(f"pipe must be an object, got {type(pipe_blob).__name__}")
    _ensure_only_keys("pipe", pipe_blob, frozenset({"outputs"}))
    outputs_raw = pipe_blob.get("outputs")
    if not isinstance(outputs_raw, dict):
        raise PipeSpecLoadError(f"pipe.outputs must be an object, got {type(outputs_raw).__name__}")
    if len(outputs_raw) == 0:
        raise PipeSpecLoadError("pipe.outputs must not be empty")

    pipe_out_wires: dict[str, SourceRef] = {}
    seen_out: set[str] = set()
    for raw_key, raw_val in outputs_raw.items():
        pn = _bare_token("pipe.outputs", raw_key, label="output")
        _reject_reserved_port(f"pipe.outputs[{raw_key!r}]", pn)
        if not _NAME_RE.fullmatch(pn):
            raise PipeSpecLoadError(f"pipe.outputs: invalid output port key {raw_key!r}")
        if pn in seen_out:
            raise PipeSpecLoadError(f"pipe.outputs: duplicate key {pn!r}")
        seen_out.add(pn)
        ref = _source_ref_from_wire(f"pipe.outputs[{raw_key!r}]", raw_val)
        pipe_out_wires[pn] = ref

    nodes_list = data.get("nodes")
    if not isinstance(nodes_list, list) or not nodes_list:
        raise PipeSpecLoadError("nodes must be a non-empty array")

    derived_pipe_inputs: set[str] = set()
    nodes_out: dict[str, NodeSpec] = {}
    graph_order: list[str] = []
    seen_ids: set[str] = set()

    for idx, entry in enumerate(nodes_list):
        loc = f"nodes[{idx}]"
        if not isinstance(entry, dict):
            raise PipeSpecLoadError(f"{loc} must be an object")
        _ensure_only_keys(loc, entry, frozenset({"id", "path", "inputs", "config"}))

        nid_raw = entry.get("id")
        if not isinstance(nid_raw, str):
            raise PipeSpecLoadError(f"{loc}.id must be a string")
        nid = nid_raw.strip()
        if not nid or nid != nid_raw or not _NAME_RE.fullmatch(nid):
            raise PipeSpecLoadError(f"{loc}.id must match [A-Za-z][A-Za-z0-9_]* bare token")
        if nid in ("input", "output"):
            raise PipeSpecLoadError(f"{loc}.id must not be reserved ({nid!r})")
        if nid in seen_ids:
            raise PipeSpecLoadError(f"{loc}: duplicate node id {nid!r}")
        seen_ids.add(nid)

        path_str = entry.get("path")
        if not isinstance(path_str, str):
            raise PipeSpecLoadError(f"{loc}.path must be a string")

        inputs_raw = entry.get("inputs")
        if not isinstance(inputs_raw, dict):
            raise PipeSpecLoadError(f"{loc}.inputs must be an object")

        cfg_overlay = entry.get("config")
        if cfg_overlay is not None and not isinstance(cfg_overlay, dict):
            raise PipeSpecLoadError(f"{loc}.config must be an object when present")

        instance, accepted_in, static_out, defaults = _load_child_definition(
            workspace, path_str, reg
        )

        input_refs: dict[str, SourceRef] = {}
        seen_tgt: set[str] = set()
        for ipt_key, ipt_val in inputs_raw.items():
            ipt = _bare_token(f"{loc}.inputs", ipt_key, label="input")
            _reject_reserved_port(f"{loc}.inputs[{ipt_key!r}]", ipt)
            if not _NAME_RE.fullmatch(ipt):
                raise PipeSpecLoadError(f"{loc}.inputs[{ipt_key!r}] invalid port name")
            if ipt in seen_tgt:
                raise PipeSpecLoadError(f"{loc}.inputs: duplicate input port {ipt!r}")
            seen_tgt.add(ipt)
            if ipt not in accepted_in:
                raise PipeSpecLoadError(
                    f"{loc}.inputs[{ipt_key!r}]: port {ipt!r} not accepted "
                    f"(expected one of {sorted(accepted_in)!r})"
                )
            src_ref = _source_ref_from_wire(f"{loc}.inputs[{ipt_key!r}]", ipt_val)
            if src_ref.kind == "input":
                derived_pipe_inputs.add(src_ref.port_name)
            input_refs[ipt] = src_ref

        merged_params = dict(defaults)
        merged_params.update(cfg_overlay or {})

        nodes_out[nid] = NodeSpec(
            node_id=nid,
            node=instance,
            input_sources=input_refs,
            output_ports=static_out,
            params=merged_params,
        )
        graph_order.append(nid)

    for out_name, src in pipe_out_wires.items():
        if src.kind == "input":
            raise PipeSpecLoadError(
                f"pipe.outputs[{out_name!r}]: must not wire from input.* (pass-through forbidden)"
            )
        if not src.node_id or src.node_id not in nodes_out:
            raise PipeSpecLoadError(f"pipe.outputs[{out_name!r}]: unknown node id {src.node_id!r}")
        if src.port_name not in nodes_out[src.node_id].output_ports:
            raise PipeSpecLoadError(
                f"pipe.outputs[{out_name!r}]: source port {src.port_name!r} "
                f"not declared on node {src.node_id!r}"
            )

    spec = PipeSpec(
        graph_node_order=tuple(graph_order),
        pipe=PipeDeclaration(
            input_ports=frozenset(derived_pipe_inputs),
            output_sources=pipe_out_wires,
        ),
        nodes=nodes_out,
    )
    try:
        validate_executable_pipe_spec(spec)
    except PipeSpecValidationError as e:
        raise PipeSpecLoadError(str(e)) from e
    return spec


def load_pipe_spec_from_json_object(
    data: Mapping[str, Any],
    *,
    reg: NodeRegistry | None = None,
    workspace_dir: str | Path | None = None,
) -> PipeSpec:
    """Parse a v1.7 PipeSpec JSON object → executable :class:`~nodeflow.core.pipe_spec.PipeSpec`.

    ``workspace_dir`` is used to resolve ``nodes[*].path`` (default: current working directory).
    """
    if not isinstance(data, Mapping):
        raise PipeSpecLoadError(f"PipeSpec JSON root must be an object, got {type(data).__name__}")

    if data.get("kind") != "pipe" or data.get("version") != "1.7":
        raise PipeSpecLoadError(
            "unsupported PipeSpec document: require root kind='pipe' and version='1.7' "
            "(older formats are rejected)"
        )

    ws = Path(workspace_dir).resolve() if workspace_dir is not None else Path.cwd().resolve()
    resolved_reg = reg if reg is not None else default_registry
    return _build_v17_executable_pipe(data, ws, resolved_reg)


def load_pipe_spec_from_json_bytes(
    raw: bytes,
    *,
    reg: NodeRegistry | None = None,
    workspace_dir: str | Path | None = None,
) -> PipeSpec:
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise PipeSpecLoadError(f"invalid UTF-8 or JSON: {e}") from e
    if not isinstance(data, dict):
        raise PipeSpecLoadError("JSON document root must be an object")
    return load_pipe_spec_from_json_object(data, reg=reg, workspace_dir=workspace_dir)


def load_pipeline(
    workspace_dir: str,
    file_path: str,
    *,
    reg: NodeRegistry | None = None,
) -> PipeSpec:
    """Load a v1.7 ``*.json`` PipeSpec from disk (paths inside JSON resolve against ``workspace_dir``)."""
    root = Path(workspace_dir)
    path = Path(file_path)
    if not path.is_absolute():
        path = root / path
    path = path.resolve()
    suffix = path.suffix.lower()
    if suffix in (".yaml", ".yml"):
        raise NotImplementedError(
            "YAML pipeline loading was removed. Use a v1.7 PipeSpec JSON file (.json) with "
            "load_pipeline."
        )
    if suffix != ".json":
        raise PipeSpecLoadError(
            f"unsupported pipeline file type {suffix!r}; expected .json (v1.7 PipeSpec)"
        )
    try:
        raw = path.read_bytes()
    except OSError as e:
        raise PipeSpecLoadError(f"cannot read {path}: {e}") from e
    return load_pipe_spec_from_json_bytes(raw, reg=reg, workspace_dir=root.resolve())


def load_node_pipeline(_file_path: str) -> dict:
    """Removed: use :func:`load_pipe_spec_from_json_object` / full PipeSpec graphs."""
    raise NotImplementedError(
        "Per-node YAML pipeline snippets were removed. Use v1.7 PipeSpec JSON."
    )
