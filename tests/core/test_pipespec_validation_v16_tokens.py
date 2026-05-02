"""v1.6 PipeSpec: graph node id / port name token rules and graph_node_order uniqueness."""

from __future__ import annotations

from types import MappingProxyType

import pytest

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.pipe_spec import (
    NodeSpec,
    PipeDeclaration,
    PipeSpec,
    PipeSpecValidationError,
    validate_executable_pipe_spec,
)
from nodeflow.core.source_ref import SourceRef


class _Pass(BaseNode):
    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        return {"out": {}}


def _minimal_linear() -> tuple[BaseNode, BaseNode, PipeSpec]:
    a, b = _Pass(), _Pass()
    spec = PipeSpec(
        graph_node_order=("a", "b"),
        pipe=PipeDeclaration(
            input_ports=frozenset({"x"}),
            output_sources={"r": SourceRef(kind="node", node_id="b", port_name="out")},
        ),
        nodes={
            "a": NodeSpec(
                node_id="a",
                node=a,
                input_sources={"in": SourceRef(kind="input", port_name="x")},
                output_ports=frozenset({"out"}),
            ),
            "b": NodeSpec(
                node_id="b",
                node=b,
                input_sources={"in": SourceRef(kind="node", node_id="a", port_name="out")},
                output_ports=frozenset({"out"}),
            ),
        },
    )
    return a, b, spec


def test_validate_rejects_invalid_graph_node_id_key() -> None:
    _, _, base = _minimal_linear()
    n = base.nodes["a"].node
    bad = PipeSpec(
        graph_node_order=("0bad", "b"),
        pipe=base.pipe,
        nodes={
            "0bad": NodeSpec(
                node_id="0bad",
                node=n,
                input_sources={"in": SourceRef(kind="input", port_name="x")},
                output_ports=frozenset({"out"}),
            ),
            "b": base.nodes["b"],
        },
    )
    with pytest.raises(PipeSpecValidationError, match="invalid node_id"):
        validate_executable_pipe_spec(bad)


def test_validate_rejects_node_id_input_output_keywords() -> None:
    _, _, base = _minimal_linear()
    n = base.nodes["a"].node
    bad = PipeSpec(
        graph_node_order=("input", "b"),
        pipe=base.pipe,
        nodes={
            "input": NodeSpec(
                node_id="input",
                node=n,
                input_sources={"in": SourceRef(kind="input", port_name="x")},
                output_ports=frozenset({"out"}),
            ),
            "b": base.nodes["b"],
        },
    )
    with pytest.raises(PipeSpecValidationError, match="reserved for wiring"):
        validate_executable_pipe_spec(bad)


def test_validate_rejects_invalid_port_name_in_pipe_inputs() -> None:
    a, b, _ = _minimal_linear()
    bad = PipeSpec(
        graph_node_order=("a", "b"),
        pipe=PipeDeclaration(
            input_ports=frozenset({"x", "9bad"}),
            output_sources={"r": SourceRef(kind="node", node_id="b", port_name="out")},
        ),
        nodes={
            "a": NodeSpec(
                node_id="a",
                node=a,
                input_sources={"in": SourceRef(kind="input", port_name="x")},
                output_ports=frozenset({"out"}),
            ),
            "b": NodeSpec(
                node_id="b",
                node=b,
                input_sources={"in": SourceRef(kind="node", node_id="a", port_name="out")},
                output_ports=frozenset({"out"}),
            ),
        },
    )
    with pytest.raises(PipeSpecValidationError, match="invalid port name"):
        validate_executable_pipe_spec(bad)


def test_validate_rejects_duplicate_graph_node_order() -> None:
    a = _Pass()
    bad = PipeSpec(
        graph_node_order=("a", "a"),
        pipe=PipeDeclaration(
            input_ports=frozenset({"x"}),
            output_sources={"r": SourceRef(kind="node", node_id="a", port_name="out")},
        ),
        nodes={
            "a": NodeSpec(
                node_id="a",
                node=a,
                input_sources={"in": SourceRef(kind="input", port_name="x")},
                output_ports=frozenset({"out"}),
            ),
        },
    )
    with pytest.raises(PipeSpecValidationError, match="permutation"):
        validate_executable_pipe_spec(bad)
