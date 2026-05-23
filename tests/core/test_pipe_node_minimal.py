"""Minimal generic PipeNode: pipe outputs completion."""

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


def _linear_spec() -> PipeSpec:
    a = _Pass()
    b = _Pass()
    return PipeSpec(
        graph_node_order=("a", "b"),
        pipe=PipeDeclaration(
            input_ports=frozenset({"x"}),
            output_sources={"result": SourceRef(kind="node", node_id="b", port_name="out")},
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


def test_pipe_node_completes_on_pipe_outputs_not_final() -> None:
    pipe = PipeNode(_linear_spec())
    out = pipe.execute({"x": {"v": 3}}, {})
    assert pipe.read_status() == "done"
    assert out["result"] == {"v": 3}
    assert "_state" in out
