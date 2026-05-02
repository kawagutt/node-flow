"""``Runner.step()`` must not count as progress when no delivery and no execution occurs."""

from __future__ import annotations

from types import MappingProxyType

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.runner import Runner
from nodeflow.core.source_ref import SourceRef


class _TwoIn(BaseNode):
    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        if "a" not in inputs or "b" not in inputs:
            return {}
        return {"out": {"ok": True}}


def test_runner_second_step_false_when_waiting_second_input() -> None:
    n = _TwoIn()
    runner = Runner(
        graph_node_order=["n"],
        nodes={"n": n},
        node_params={"n": {}},
        node_input_sources={
            "n": {
                "a": SourceRef(kind="input", port_name="p1"),
                "b": SourceRef(kind="input", port_name="p2"),
            },
        },
        pipe_inputs={"p1": {"x": 1}},
    )
    assert runner.step() is True
    assert n.read_status() == "ready"
    assert runner.step() is False
