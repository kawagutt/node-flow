from __future__ import annotations

from types import MappingProxyType

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.pipe_spec import (
    NodeSpec,
    PipeDeclaration,
    PipeSpec,
    validate_executable_pipe_spec,
)
from nodeflow.core.source_ref import SourceRef


class _PassNode(BaseNode):
    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        return {"out": {}}


def _minimal_linear_spec() -> PipeSpec:
    a = _PassNode()
    b = _PassNode()
    return PipeSpec(
        graph_node_order=("a", "b"),
        pipe=PipeDeclaration(
            input_ports=frozenset({"request"}),
            output_sources={"pout": SourceRef(kind="node", node_id="b", port_name="out")},
        ),
        nodes={
            "a": NodeSpec(
                node_id="a",
                node=a,
                input_sources={"in": SourceRef(kind="input", port_name="request")},
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


def test_validate_accepts_minimal_executable_spec() -> None:
    validate_executable_pipe_spec(_minimal_linear_spec())


def test_validate_accepts_fan_out_same_source_to_two_inputs() -> None:
    src = _PassNode()
    left = _PassNode()
    right = _PassNode()
    wire = SourceRef(kind="node", node_id="src", port_name="out")
    spec = PipeSpec(
        graph_node_order=("src", "left", "right"),
        pipe=PipeDeclaration(
            input_ports=frozenset({"request"}),
            output_sources={
                "pout": SourceRef(kind="node", node_id="right", port_name="out"),
            },
        ),
        nodes={
            "src": NodeSpec(
                node_id="src",
                node=src,
                input_sources={"in": SourceRef(kind="input", port_name="request")},
                output_ports=frozenset({"out"}),
            ),
            "left": NodeSpec(
                node_id="left",
                node=left,
                input_sources={"x": wire},
                output_ports=frozenset({"out"}),
            ),
            "right": NodeSpec(
                node_id="right",
                node=right,
                input_sources={"y": wire},
                output_ports=frozenset({"out"}),
            ),
        },
    )
    validate_executable_pipe_spec(spec)
