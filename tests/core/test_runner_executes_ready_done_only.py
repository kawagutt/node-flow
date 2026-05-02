from __future__ import annotations

from types import MappingProxyType

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.runner import Runner


class _CountNode(BaseNode):
    def __init__(self, initial_status: str = "ready") -> None:
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


def test_runner_executes_ready_done_only_and_keeps_other_states():
    ready = _CountNode("ready")
    done = _CountNode("done")
    executing = _CountNode("executing")
    limit = _CountNode("limit")
    fatal = _CountNode("fatal")
    runner = Runner(
        graph_node_order=["ready", "done", "executing", "limit", "fatal"],
        nodes={
            "ready": ready,
            "done": done,
            "executing": executing,
            "limit": limit,
            "fatal": fatal,
        },
        node_params={},
        node_input_sources={},
        pipe_inputs={},
    )

    assert runner.step() is True
    assert ready.execute_calls == 1
    assert done.execute_calls == 1
    assert ready.calls == 1
    assert done.calls == 0
    assert executing.calls == 0
    assert limit.calls == 0
    assert fatal.calls == 0
    assert executing.read_status() == "executing"
    assert limit.read_status() == "limit"
    assert fatal.read_status() == "fatal"
