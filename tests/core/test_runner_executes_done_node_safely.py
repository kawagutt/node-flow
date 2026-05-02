from __future__ import annotations

from types import MappingProxyType

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.runner import Runner, SourceRef


class _OneShotNode(BaseNode):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        self.calls += 1
        return {"out": {"value": inputs.get("x", "missing")}}


def test_runner_calls_done_node_without_duplicate_execution():
    node = _OneShotNode()
    runner = Runner(
        graph_node_order=["n1"],
        nodes={"n1": node},
        node_params={"n1": {}},
        node_input_sources={"n1": {"x": SourceRef(kind="input", port_name="x")}},
        pipe_inputs={"x": {"value": "first"}},
    )

    progressed1 = runner.step()
    assert progressed1 is True
    assert node.calls == 1
    assert node.get_output_snapshot()["out"]["value"] == {"value": "first"}

    progressed2 = runner.step()
    assert progressed2 is False
    assert node.calls == 1
    assert node.get_output_snapshot()["out"]["value"] == {"value": "first"}
    assert node.read_status() == "done"
