"""PipeNode must not become ``done`` when pipe output buffers are only partially filled."""

from __future__ import annotations

from types import MappingProxyType

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.node_kinds.pipe_node import PipeNode
from nodeflow.core.pipe_spec import NodeSpec, PipeDeclaration, PipeSpec
from nodeflow.core.source_ref import SourceRef


class _Pass(BaseNode):
    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        if "in" not in inputs:
            return {}
        return {"out": dict(inputs["in"])}


def _dual_spec() -> PipeSpec:
    a = _Pass()
    b = _Pass()
    return PipeSpec(
        graph_node_order=("a", "b"),
        pipe=PipeDeclaration(
            input_ports=frozenset({"x", "y"}),
            output_sources={
                "ra": SourceRef(kind="node", node_id="a", port_name="out"),
                "rb": SourceRef(kind="node", node_id="b", port_name="out"),
            },
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
                input_sources={"in": SourceRef(kind="input", port_name="y")},
                output_ports=frozenset({"out"}),
            ),
        },
    )


def test_pipe_node_not_done_when_pipe_outputs_incomplete() -> None:
    pipe = PipeNode(_dual_spec())
    out = pipe.execute({"x": {"v": 1}}, {})
    assert pipe.read_status() == "idle"
    assert "ra" not in out and "rb" not in out
    assert out["_state"]["value"] == "idle"


def test_pipe_node_done_when_all_pipe_outputs_filled() -> None:
    pipe = PipeNode(_dual_spec())
    out = pipe.execute({"x": {"v": 1}, "y": {"v": 2}}, {})
    assert pipe.read_status() == "done"
    assert out["ra"] == {"v": 1}
    assert out["rb"] == {"v": 2}
