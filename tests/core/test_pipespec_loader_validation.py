"""v1.6 JSON PipeSpec loader — structural rejects, normalization, Phase A validator hook."""

from __future__ import annotations

import json
from collections.abc import Iterator
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


class _NeedsCtorArg(BaseNode):
    def __init__(self, required: object):  # noqa: ARG002
        super().__init__()

    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        return {"out": {}}


def _minimal_json(*, nid: str = "a") -> dict:
    return {
        "pipe": {
            "input_ports": ["x"],
            "output_ports": {f"out{nid}": f"{nid}.out"},
        },
        "nodes": {
            nid: {
                "type": "loader_test_pass",
                "params": {},
                "input_ports": {"in": "input.x"},
                "output_ports": ["out"],
            },
        },
    }


@pytest.fixture(autouse=True)
def _register_loader_test_pass() -> Iterator[None]:
    registry.register("loader_test_pass", _LoaderStubPass, override=True)
    try:
        yield
    finally:
        registry.unregister("loader_test_pass")


def test_unknown_root_key_raises() -> None:
    data = _minimal_json()
    data["meta"] = 1
    with pytest.raises(PipeSpecLoadError, match=r"unknown keys"):
        load_pipe_spec_from_json_object(data)


def test_invalid_json_raises() -> None:
    with pytest.raises(PipeSpecLoadError, match="JSON"):
        load_pipe_spec_from_json_bytes(b'{"pipe": ')


def test_empty_nodes_raises() -> None:
    data = {
        "pipe": {"input_ports": ["x"], "output_ports": {"o": "a.out"}},
        "nodes": {},
    }
    with pytest.raises(PipeSpecLoadError, match="must not be empty"):
        load_pipe_spec_from_json_object(data)


def test_duplicate_pipe_input_raises() -> None:
    data = _minimal_json()
    data["pipe"]["input_ports"] = ["x", "x"]
    with pytest.raises(PipeSpecLoadError, match=r"duplicate"):
        load_pipe_spec_from_json_object(data)


def test_pipe_output_pass_through_input_rejected_early() -> None:
    data = _minimal_json()
    data["pipe"]["output_ports"] = {"direct": "input.x"}
    with pytest.raises(PipeSpecLoadError, match=r"pass-through forbidden"):
        load_pipe_spec_from_json_object(data)


def test_unknown_node_type_raises() -> None:
    iso = NodeRegistry()
    data = _minimal_json()
    with pytest.raises(PipeSpecLoadError, match="Unknown node type"):
        load_pipe_spec_from_json_object(data, reg=iso)


def test_node_ctor_must_be_no_arg_raises() -> None:
    iso = NodeRegistry()
    iso.register("loader_needs_ctor", _NeedsCtorArg)
    data = _minimal_json()
    data["nodes"]["a"]["type"] = "loader_needs_ctor"
    with pytest.raises(PipeSpecLoadError, match="constructible with no arguments"):
        load_pipe_spec_from_json_object(data, reg=iso)


def test_pipe_output_unknown_source_port_raises() -> None:
    data = _minimal_json()
    data["pipe"]["output_ports"] = {"o": "a.missing"}
    with pytest.raises(PipeSpecLoadError, match="not declared"):
        load_pipe_spec_from_json_object(data)


def test_node_input_unknown_source_node_raises() -> None:
    data = _minimal_json()
    data["nodes"]["a"]["input_ports"] = {"in": "missing.out"}
    with pytest.raises(PipeSpecLoadError, match="unknown source node"):
        load_pipe_spec_from_json_object(data)


def test_loader_uses_phase_a_validator_for_fanout_reject() -> None:
    data = {
        "pipe": {
            "input_ports": ["x"],
            "output_ports": {"o": "b.out"},
        },
        "nodes": {
            "a": {
                "type": "loader_test_pass",
                "params": {},
                "input_ports": {"in": "input.x"},
                "output_ports": ["out"],
            },
            "b": {
                "type": "loader_test_pass",
                "params": {},
                "input_ports": {"in": "input.x"},
                "output_ports": ["out"],
            },
        },
    }
    with pytest.raises(PipeSpecLoadError, match="fan-out|source"):
        load_pipe_spec_from_json_object(data)


def test_graph_node_order_is_topological_with_json_order_tiebreak() -> None:
    """Independent nodes ordered by JSON key order among zero-indegree; dependents last.

    Two distinct pipe inputs avoid fan-out from one source to multiple node inputs.
    """
    data = {
        "pipe": {
            "input_ports": ["x", "y"],
            "output_ports": {"o": "c.out"},
        },
        "nodes": {
            "b": {
                "type": "loader_test_pass",
                "params": {},
                "input_ports": {"in": "input.x"},
                "output_ports": ["out"],
            },
            "a": {
                "type": "loader_test_pass",
                "params": {},
                "input_ports": {"in": "input.y"},
                "output_ports": ["out"],
            },
            "c": {
                "type": "loader_test_pass",
                "params": {},
                "input_ports": {"from_a": "a.out", "from_b": "b.out"},
                "output_ports": ["out"],
            },
        },
    }
    spec = load_pipe_spec_from_json_object(data)
    assert spec.graph_node_order == ("b", "a", "c")


def test_params_must_be_object() -> None:
    data = _minimal_json()
    data["nodes"]["a"]["params"] = []
    with pytest.raises(PipeSpecLoadError, match=r"params must be an object"):
        load_pipe_spec_from_json_object(data)


def test_loader_rejects_topo_cycle_before_validator() -> None:
    wire = json.dumps(
        {
            "pipe": {
                "input_ports": [],
                "output_ports": {"o": "b.out"},
            },
            "nodes": {
                "a": {
                    "type": "loader_test_pass",
                    "params": {},
                    "input_ports": {"in": "b.out"},
                    "output_ports": ["out"],
                },
                "b": {
                    "type": "loader_test_pass",
                    "params": {},
                    "input_ports": {"in": "a.out"},
                    "output_ports": ["out"],
                },
            },
        }
    )
    with pytest.raises(PipeSpecLoadError, match=r"cycle"):
        load_pipe_spec_from_json_bytes(wire.encode())


def test_normalized_spec_passes_standalone_validator() -> None:
    spec = load_pipe_spec_from_json_object(_minimal_json())
    validate_executable_pipe_spec(spec)


def test_load_pipeline_yaml_still_removed(tmp_path) -> None:
    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text('version: "1.5"\ngraph:\n  nodes: []\n')
    with pytest.raises(NotImplementedError, match="YAML 1.5"):
        load_pipeline(str(tmp_path), str(yaml_path))


def test_load_pipeline_json_file_roundtrip(tmp_path) -> None:
    iso = NodeRegistry()
    iso.register("loader_test_pass", _LoaderStubPass)

    p = tmp_path / "g.json"
    p.write_text(json.dumps(_minimal_json()), encoding="utf-8")

    spec = load_pipeline(str(tmp_path), str(p.name), reg=iso)
    assert spec.pipe.input_ports == frozenset({"x"})
    assert set(spec.nodes.keys()) == set(spec.graph_node_order)


def test_reserved_observation_output_port_name_rejected() -> None:
    data = _minimal_json()
    data["nodes"]["a"]["output_ports"] = ["_state"]
    with pytest.raises(PipeSpecLoadError, match=r"observation"):
        load_pipe_spec_from_json_object(data)


def test_pipe_output_must_not_wire_observation_port() -> None:
    """Wiring ``a._runtime`` is rejected — observation ports cannot be sourced."""
    data = _minimal_json()
    data["pipe"]["output_ports"] = {"o": "a._runtime"}
    with pytest.raises(PipeSpecLoadError, match=r"observation"):
        load_pipe_spec_from_json_object(data)
