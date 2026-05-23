"""v1.7 JSON PipeSpec loader — structural rejects, workspace-relative paths, validator hook."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from types import MappingProxyType

import pytest

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.loader import (
    PipeSpecLoadError,
    load_pipe_spec_from_json_bytes,
    load_pipe_spec_from_json_object,
    load_pipeline,
)
from nodeflow.core.pipe_spec import validate_executable_pipe_spec
from nodeflow.core.registry import NodeRegistry, registry


class _LoaderStubPass(BaseNode):
    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        return {"out": {}}


class _LoaderStubJoin(BaseNode):
    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        return {"out": {}}


class _NeedsCtorArg(BaseNode):
    def __init__(self, required: object):  # noqa: ARG002
        super().__init__()

    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        return {"out": {}}


def _stub_pass_blob() -> dict[str, object]:
    return {
        "kind": "node",
        "version": "1.7",
        "type": "loader_test_pass",
        "input_ports": ["in"],
        "output_ports": ["out"],
    }


def _stub_join_blob() -> dict[str, object]:
    return {
        "kind": "node",
        "version": "1.7",
        "type": "loader_test_join",
        "input_ports": ["from_a", "from_b"],
        "output_ports": ["out"],
    }


def _single_node_pipe(*, nid: str = "a", wire: str = "input.x") -> dict[str, object]:
    return {
        "kind": "pipe",
        "version": "1.7",
        "pipe": {"outputs": {f"out{nid}": f"{nid}.out"}},
        "nodes": [{"id": nid, "path": "stub.json", "inputs": {"in": wire}}],
    }


@pytest.fixture
def ws(tmp_path: Path) -> Path:
    d = tmp_path / "ws"
    d.mkdir()
    (d / "stub.json").write_text(json.dumps(_stub_pass_blob()), encoding="utf-8")
    (d / "join.json").write_text(json.dumps(_stub_join_blob()), encoding="utf-8")
    return d


@pytest.fixture(autouse=True)
def _register_loader_stubs() -> Iterator[None]:
    registry.register("loader_test_pass", _LoaderStubPass, override=True)
    registry.register("loader_test_join", _LoaderStubJoin, override=True)
    try:
        yield
    finally:
        registry.unregister("loader_test_pass")
        registry.unregister("loader_test_join")


def test_unknown_root_key_raises(ws: Path) -> None:
    data = dict(_single_node_pipe())
    data["meta"] = 1
    with pytest.raises(PipeSpecLoadError, match=r"unknown keys"):
        load_pipe_spec_from_json_object(data, workspace_dir=ws)


def test_invalid_json_raises() -> None:
    with pytest.raises(PipeSpecLoadError, match="JSON"):
        load_pipe_spec_from_json_bytes(b'{"kind": ')


def test_empty_nodes_raises(ws: Path) -> None:
    data = {
        "kind": "pipe",
        "version": "1.7",
        "pipe": {"outputs": {"o": "a.out"}},
        "nodes": [],
    }
    with pytest.raises(PipeSpecLoadError, match="non-empty array"):
        load_pipe_spec_from_json_object(data, workspace_dir=ws)


def test_unsupported_root_kind_raises(ws: Path) -> None:
    data = dict(_single_node_pipe())
    data["kind"] = "graph"
    with pytest.raises(PipeSpecLoadError, match=r"unsupported PipeSpec document"):
        load_pipe_spec_from_json_object(data, workspace_dir=ws)


def test_duplicate_node_id_raises(ws: Path) -> None:
    data = {
        "kind": "pipe",
        "version": "1.7",
        "pipe": {"outputs": {"o": "b.out"}},
        "nodes": [
            {"id": "a", "path": "stub.json", "inputs": {"in": "input.x"}},
            {"id": "a", "path": "stub.json", "inputs": {"in": "input.y"}},
        ],
    }
    with pytest.raises(PipeSpecLoadError, match=r"duplicate node id"):
        load_pipe_spec_from_json_object(data, workspace_dir=ws)


def test_pipe_output_pass_through_input_rejected(ws: Path) -> None:
    data = dict(_single_node_pipe())
    data["pipe"] = {"outputs": {"direct": "input.x"}}
    with pytest.raises(PipeSpecLoadError, match=r"pass-through forbidden"):
        load_pipe_spec_from_json_object(data, workspace_dir=ws)


def test_unknown_node_type_raises(ws: Path) -> None:
    iso = NodeRegistry()
    data = dict(_single_node_pipe())
    with pytest.raises(PipeSpecLoadError, match="Unknown node type"):
        load_pipe_spec_from_json_object(data, reg=iso, workspace_dir=ws)


def test_node_ctor_must_be_no_arg_raises(ws: Path) -> None:
    iso = NodeRegistry()
    iso.register("loader_needs_ctor", _NeedsCtorArg)
    (ws / "bad.json").write_text(
        json.dumps(
            {
                "kind": "node",
                "version": "1.7",
                "type": "loader_needs_ctor",
                "input_ports": ["in"],
                "output_ports": ["out"],
            }
        ),
        encoding="utf-8",
    )
    data = {
        "kind": "pipe",
        "version": "1.7",
        "pipe": {"outputs": {"o": "n.out"}},
        "nodes": [{"id": "n", "path": "bad.json", "inputs": {"in": "input.x"}}],
    }
    with pytest.raises(PipeSpecLoadError, match="constructible with no arguments"):
        load_pipe_spec_from_json_object(data, reg=iso, workspace_dir=ws)


def test_pipe_output_unknown_source_port_raises(ws: Path) -> None:
    data = dict(_single_node_pipe())
    data["pipe"] = {"outputs": {"o": "a.missing"}}
    with pytest.raises(PipeSpecLoadError, match="not declared"):
        load_pipe_spec_from_json_object(data, workspace_dir=ws)


def test_node_input_unknown_source_node_raises(ws: Path) -> None:
    data = dict(_single_node_pipe())
    data["nodes"][0]["inputs"]["in"] = "missing.out"
    with pytest.raises(PipeSpecLoadError, match="unknown source node"):
        load_pipe_spec_from_json_object(data, workspace_dir=ws)


def test_loader_accepts_fan_out_two_consumers_same_pipe_input(ws: Path) -> None:
    data = {
        "kind": "pipe",
        "version": "1.7",
        "pipe": {"outputs": {"o": "right.out"}},
        "nodes": [
            {"id": "left", "path": "stub.json", "inputs": {"in": "input.x"}},
            {"id": "right", "path": "stub.json", "inputs": {"in": "input.x"}},
        ],
    }
    spec = load_pipe_spec_from_json_object(data, workspace_dir=ws)
    assert spec.pipe.input_ports == frozenset({"x"})


def test_graph_node_order_follows_nodes_array_order(ws: Path) -> None:
    data = {
        "kind": "pipe",
        "version": "1.7",
        "pipe": {"outputs": {"o": "c.out"}},
        "nodes": [
            {"id": "b", "path": "stub.json", "inputs": {"in": "input.x"}},
            {"id": "a", "path": "stub.json", "inputs": {"in": "input.y"}},
            {
                "id": "c",
                "path": "join.json",
                "inputs": {"from_a": "a.out", "from_b": "b.out"},
            },
        ],
    }
    spec = load_pipe_spec_from_json_object(data, workspace_dir=ws)
    assert spec.graph_node_order == ("b", "a", "c")


def test_entry_config_must_be_object(ws: Path) -> None:
    data = dict(_single_node_pipe())
    data["nodes"][0]["config"] = []
    with pytest.raises(PipeSpecLoadError, match=r"config must be an object"):
        load_pipe_spec_from_json_object(data, workspace_dir=ws)


def test_loader_rejects_cycle_before_running(ws: Path) -> None:
    data = {
        "kind": "pipe",
        "version": "1.7",
        "pipe": {"outputs": {"o": "b.out"}},
        "nodes": [
            {"id": "a", "path": "stub.json", "inputs": {"in": "b.out"}},
            {"id": "b", "path": "stub.json", "inputs": {"in": "a.out"}},
        ],
    }
    wire = json.dumps(data)
    with pytest.raises(PipeSpecLoadError, match=r"cycle"):
        load_pipe_spec_from_json_bytes(wire.encode(), workspace_dir=ws)


def test_normalized_spec_passes_standalone_validator(ws: Path) -> None:
    spec = load_pipe_spec_from_json_object(dict(_single_node_pipe()), workspace_dir=ws)
    validate_executable_pipe_spec(spec)


def test_load_pipeline_yaml_still_removed(tmp_path: Path) -> None:
    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text('version: "1.5"\ngraph:\n  nodes: []\n')
    with pytest.raises(NotImplementedError, match="YAML"):
        load_pipeline(str(tmp_path), str(yaml_path))


def test_load_pipeline_json_file_roundtrip(ws: Path) -> None:
    iso = NodeRegistry()
    iso.register("loader_test_pass", _LoaderStubPass)

    g = ws / "g.json"
    g.write_text(json.dumps(dict(_single_node_pipe())), encoding="utf-8")

    spec = load_pipeline(str(ws), "g.json", reg=iso)
    assert spec.pipe.input_ports == frozenset({"x"})
    assert tuple(spec.nodes.keys()) == spec.graph_node_order


def test_reserved_observation_output_port_name_rejected(ws: Path) -> None:
    (ws / "bad_ports.json").write_text(
        json.dumps(
            {
                **_stub_pass_blob(),
                "output_ports": ["_state"],
            }
        ),
        encoding="utf-8",
    )
    data = {
        "kind": "pipe",
        "version": "1.7",
        "pipe": {"outputs": {"o": "n.out"}},
        "nodes": [{"id": "n", "path": "bad_ports.json", "inputs": {"in": "input.x"}}],
    }
    with pytest.raises(PipeSpecLoadError, match=r"observation"):
        load_pipe_spec_from_json_object(data, workspace_dir=ws)


def test_pipe_output_must_not_wire_observation_port(ws: Path) -> None:
    data = dict(_single_node_pipe())
    data["pipe"] = {"outputs": {"o": "a._runtime"}}
    with pytest.raises(PipeSpecLoadError, match=r"observation"):
        load_pipe_spec_from_json_object(data, workspace_dir=ws)
