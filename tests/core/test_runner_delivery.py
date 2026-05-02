from __future__ import annotations

from types import MappingProxyType

import pytest

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.runner import Runner
from nodeflow.core.source_ref import SourceRef


class _ForwardNode(BaseNode):
    def __init__(self) -> None:
        super().__init__()
        self.last_observation = None

    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        if "input" not in inputs:
            return {}
        return {"result": {"value": inputs["input"]["value"]}}

    def execute(self, inputs, params):
        out = super().execute(inputs, params)
        self.last_observation = out
        return out


def test_runner_delivery_uses_occupancy_and_clears_source_only():
    a = _ForwardNode()
    b = _ForwardNode()
    runner = Runner(
        graph_node_order=["a", "b"],
        nodes={"a": a, "b": b},
        node_params={"a": {}, "b": {}},
        node_input_sources={
            "a": {"input": SourceRef(kind="input", port_name="request")},
            "b": {"input": SourceRef(kind="node", node_id="a", port_name="result")},
        },
        pipe_inputs={"request": {"value": 7}},
    )

    assert runner.step() is True
    assert runner.pipe_input_occupancy["request"] is False
    assert a.is_output_filled("result") is True
    assert b.is_input_filled("input") is False
    first_observation = a.last_observation

    assert runner.step() is True
    assert a.is_output_filled("result") is False
    assert b.is_output_filled("result") is True
    assert first_observation["result"] == {"value": 7}


def test_runner_skips_pipe_delivery_when_input_port_already_occupied():
    """Runner must not deliver into a filled port (no shadow buffer bypass)."""
    b = _ForwardNode()
    b.set_input("input", {"value": 999})
    b._status = "executing"
    runner = Runner(
        graph_node_order=["b"],
        nodes={"b": b},
        node_params={"b": {}},
        node_input_sources={"b": {"input": SourceRef(kind="input", port_name="request")}},
        pipe_inputs={"request": {"value": 1}},
    )
    runner.step()
    assert b.get_input_snapshot(filled_only=False)["input"] == {"value": 999}


def test_runner_does_not_deliver_when_target_already_filled():
    a = _ForwardNode()
    b = _ForwardNode()
    b.set_input("input", {"value": 999})
    b._status = "executing"
    runner = Runner(
        graph_node_order=["a", "b"],
        nodes={"a": a, "b": b},
        node_params={"a": {}, "b": {}},
        node_input_sources={
            "a": {"input": SourceRef(kind="input", port_name="request")},
            "b": {"input": SourceRef(kind="node", node_id="a", port_name="result")},
        },
        pipe_inputs={"request": {"value": 1}},
    )

    runner.step()
    before = b.get_input_snapshot(filled_only=False)["input"]
    runner.step()
    after = b.get_input_snapshot(filled_only=False)["input"]
    assert before == {"value": 999}
    assert after == {"value": 999}


def test_runner_rejects_non_dict_pipe_input_payload():
    """Core Runner does not coerce payloads; dict-only v1.6 contract."""

    class _N(BaseNode):
        def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
            return {"out": {}}

    n = _N()
    runner = Runner(
        graph_node_order=["n"],
        nodes={"n": n},
        node_params={"n": {}},
        node_input_sources={"n": {"a": SourceRef(kind="input", port_name="p1")}},
        pipe_inputs={"p1": None},
    )
    with pytest.raises(TypeError, match="requires dict"):
        runner.step()
