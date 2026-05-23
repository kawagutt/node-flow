from __future__ import annotations

from types import MappingProxyType

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.runner import Runner


class _CountNode(BaseNode):
    def __init__(self, initial_status: str = "idle") -> None:
        super().__init__()
        self._status = initial_status
        self.execute_calls = 0
        self.calls = 0

    def execute(self, inputs, params):
        self.execute_calls += 1
        return super().execute(inputs, params)

    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        self.calls += 1
        return {"out": {"calls": self.calls}}


def test_runner_calls_execute_on_every_scanned_node_each_round():
    idle_node = _CountNode("idle")
    done_node = _CountNode("done")
    executing_node = _CountNode("executing")
    limit_node = _CountNode("limit")
    fatal_node = _CountNode("fatal")
    runner = Runner(
        graph_node_order=["idle_node", "done_node", "executing_node", "limit_node", "fatal_node"],
        nodes={
            "idle_node": idle_node,
            "done_node": done_node,
            "executing_node": executing_node,
            "limit_node": limit_node,
            "fatal_node": fatal_node,
        },
        node_params={},
        node_input_sources={},
        pipe_inputs={},
    )

    assert runner.step() is True
    assert idle_node.execute_calls == 1
    assert done_node.execute_calls == 1
    assert executing_node.execute_calls == 1
    assert limit_node.execute_calls == 1
    assert fatal_node.execute_calls == 1

    assert idle_node.calls == 1
    assert done_node.calls == 0
    assert executing_node.calls == 0
    assert limit_node.calls == 0
    assert fatal_node.calls == 0

    assert executing_node.read_status() == "executing"
    assert limit_node.read_status() == "limit"
    assert fatal_node.read_status() == "fatal"
