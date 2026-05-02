"""Zero-declared-input nodes must not execute again after outputs are consumed (v1.6 single-run)."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.pipe_spec import NodeSpec, PipeDeclaration, PipeSpec
from nodeflow.core.runner import Runner
from nodeflow.core.source_ref import SourceRef


class _Seed(BaseNode):
    def __init__(self) -> None:
        super().__init__()
        self.run_count = 0

    def run(self, inputs: dict[str, Any], params: MappingProxyType, context: ExecutionContext):
        self.run_count += 1
        return {"out": {"n": self.run_count}}


class _Passthrough(BaseNode):
    def run(self, inputs: dict[str, Any], params: MappingProxyType, context: ExecutionContext):
        if "in" not in inputs:
            return {}
        return {"result": dict(inputs["in"])}


def test_zero_input_child_not_reexecuted_after_downstream_consumes_output() -> None:
    a = _Seed()
    b = _Passthrough()
    spec = PipeSpec(
        graph_node_order=("a", "b"),
        pipe=PipeDeclaration(
            input_ports=frozenset(),
            output_sources={"pout": SourceRef(kind="node", node_id="b", port_name="result")},
        ),
        nodes={
            "a": NodeSpec(
                node_id="a",
                node=a,
                input_sources={},
                output_ports=frozenset({"out"}),
            ),
            "b": NodeSpec(
                node_id="b",
                node=b,
                input_sources={"in": SourceRef(kind="node", node_id="a", port_name="out")},
                output_ports=frozenset({"result"}),
            ),
        },
    )
    runner = Runner.from_pipe_spec(spec, pipe_inputs={})
    while not runner.all_pipe_outputs_filled():
        assert runner.step() is True
    assert a.run_count == 1
    assert runner.filled_pipe_outputs()["pout"]["n"] == 1
