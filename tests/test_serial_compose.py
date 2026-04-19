"""SerialPipeNode (compose) — linear graph execution."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import BaseNode, ExecutionContext, NodeExecutionLimit
from nodeflow.nodes.pipe.serial_pipe import SerialPipeNode


class EchoNode(BaseNode):
    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        return {"result": {"value": str(inputs.get("x", "")) + ":a"}}


class EchoBNode(BaseNode):
    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        data = inputs.get("data", {})
        val = data.get("value", "") if isinstance(data, dict) else data
        return {"result": {"out": str(val) + ":b"}}


def test_compose_two_nodes():
    nodes = {"a": EchoNode(), "b": EchoBNode()}
    graph_node_order = ["a", "b"]
    node_input_bindings = {
        "a": {"x": ("inputs", "x")},
        "b": {"data": ("node", "a", "result")},
    }
    node_param_definitions = {"a": {}, "b": {}}
    pipe = SerialPipeNode(
        graph_node_order=graph_node_order,
        nodes=nodes,
        node_input_bindings=node_input_bindings,
        node_param_definitions=node_param_definitions,
        final_id="b",
    )
    out = pipe.execute({"x": "in"}, {})
    assert pipe.read_status() == "done"
    assert nodes["a"].read_status() == "done"
    assert nodes["b"].read_status() == "done"
    assert "result" in out
    assert out["result"]["out"] == "in:a:b"


def test_compose_fatal_propagates():
    class FailingNode(BaseNode):
        def run(self, inputs, params, context):
            raise RuntimeError("a failed")

    nodes = {"a": FailingNode(), "b": EchoBNode()}
    pipe = SerialPipeNode(
        graph_node_order=["a", "b"],
        nodes=nodes,
        node_input_bindings={
            "a": {"x": ("inputs", "x")},
            "b": {"data": ("node", "a", "result")},
        },
        node_param_definitions={"a": {}, "b": {}},
        final_id="b",
    )
    out = pipe.execute({"x": 1}, {})
    assert pipe.read_status() == "fatal"
    assert out == {}
    assert nodes["a"].read_status() == "fatal"


def test_compose_limit_propagates():
    class LimitOnceNode(BaseNode):
        def run(self, inputs, params, context):
            raise NodeExecutionLimit()

    nodes = {"a": LimitOnceNode(), "b": EchoBNode()}
    pipe = SerialPipeNode(
        graph_node_order=["a", "b"],
        nodes=nodes,
        node_input_bindings={
            "a": {"x": ("inputs", "x")},
            "b": {"data": ("node", "a", "result")},
        },
        node_param_definitions={"a": {}, "b": {}},
        final_id="b",
    )
    out = pipe.execute({"x": 1}, {})
    assert nodes["a"].read_status() == "limit"
    assert pipe.read_status() == "limit"
    assert out == {}


def test_compose_max_calls_on_root():
    nodes = {"a": EchoNode(), "b": EchoBNode()}
    pipe = SerialPipeNode(
        graph_node_order=["a", "b"],
        nodes=nodes,
        node_input_bindings={
            "a": {"x": ("inputs", "x")},
            "b": {"data": ("node", "a", "result")},
        },
        node_param_definitions={"a": {}, "b": {}},
        final_id="b",
    )
    params = {"limit": {"max_calls": 1}}
    out1 = pipe.execute({"x": "1"}, params)
    assert pipe.read_status() == "done"
    assert "result" in out1
    pipe.reset_status()
    out2 = pipe.execute({"x": "2"}, params)
    assert pipe.read_status() == "limit"
    assert out2 == {}
