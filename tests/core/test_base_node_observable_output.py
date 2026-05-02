from __future__ import annotations

from types import MappingProxyType

import pytest

from nodeflow.core.base_node import BaseNode, ExecutionContext


class _EmitNode(BaseNode):
    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        return {"result": {"value": inputs.get("x", 0)}}


def test_execute_always_contains_observation_fields():
    node = _EmitNode()
    out = node.execute({"x": 1}, {})
    assert out["_state"]["value"] == "done"
    assert out["_usage"] == {}
    assert "result" in out
    assert "revision" in out["_runtime"]["ports"]["result"]


def test_done_node_second_execute_is_safe_noop():
    node = _EmitNode()
    out1 = node.execute({"x": 1}, {})
    out2 = node.execute({"x": 999}, {})
    assert out1["result"] == {"value": 1}
    assert out2["result"] == {"value": 1}
    assert out2["_state"]["value"] == "done"
    assert (
        out2["_runtime"]["ports"]["result"]["revision"]
        == out1["_runtime"]["ports"]["result"]["revision"]
    )


def test_clear_output_occupancy_does_not_mutate_snapshot():
    node = _EmitNode()
    node.execute({"x": 42}, {})
    snap_before = node.get_output_snapshot(filled_only=False)
    node.clear_output_occupancy("result")
    snap_after = node.get_output_snapshot(filled_only=False)
    assert snap_before == {"result": {"value": 42}}
    assert snap_after == {"result": {"value": 42}}
    assert node.get_output_snapshot() == {}
    assert node.is_output_filled("result") is False
    assert node.read_status() == "ready"


def test_set_input_requires_dict_payload():
    node = _EmitNode()
    with pytest.raises(TypeError, match="payload must be dict"):
        node.set_input("request", "not-a-dict")


def test_set_input_copies_top_level_dict_from_caller():
    node = _EmitNode()
    outer = {"value": 1}
    node.set_input("x", outer)
    outer["value"] = 999
    assert node.get_input_snapshot()["x"]["value"] == 1


def test_input_port_api_snapshot_and_clear():
    node = _EmitNode()
    node.set_input("request", {"value": 1})
    assert node.is_input_filled("request") is True
    assert node.get_input_snapshot() == {"request": {"value": 1}}
    node.clear_input_occupancy("request")
    assert node.is_input_filled("request") is False
    assert node.get_input_snapshot() == {}
    assert node.get_input_snapshot(filled_only=False) == {"request": {"value": 1}}
