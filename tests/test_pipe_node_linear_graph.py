"""PipeNode — linear graph execution (tests use GraphSpec + _TestPipeNode; loader uses _GraphPipeNode)."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import BaseNode, ExecutionContext, NodeExecutionLimit
from nodeflow.core.graph_spec import GraphSpec
from nodeflow.core.node_kinds import PipeNode


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


class _TestPipeNode(PipeNode):
    def __init__(self, spec: GraphSpec) -> None:
        super().__init__()
        self._spec = spec

    def graph(self) -> GraphSpec:
        return self._spec


def _make_pipe(nodes, order, bindings, params_def, final_id):
    return _TestPipeNode(
        GraphSpec(
            nodes=nodes,
            order=order,
            bindings=bindings,
            params=params_def,
            final=final_id,
        )
    )


def test_pipe_two_nodes():
    nodes = {"a": EchoNode(), "b": EchoBNode()}
    graph_node_order = ["a", "b"]
    node_input_bindings = {
        "a": {"x": ("inputs", "x")},
        "b": {"data": ("node", "a", "result")},
    }
    node_param_definitions = {"a": {}, "b": {}}
    pipe = _make_pipe(nodes, graph_node_order, node_input_bindings, node_param_definitions, "b")
    out = pipe.execute({"x": "in"}, {})
    assert pipe.read_status() == "done"
    assert nodes["a"].read_status() == "done"
    assert nodes["b"].read_status() == "done"
    assert "result" in out
    assert out["result"]["out"] == "in:a:b"


def test_pipe_fatal_propagates():
    class FailingNode(BaseNode):
        def run(self, inputs, params, context):
            raise RuntimeError("a failed")

    nodes = {"a": FailingNode(), "b": EchoBNode()}
    pipe = _make_pipe(
        nodes,
        ["a", "b"],
        {
            "a": {"x": ("inputs", "x")},
            "b": {"data": ("node", "a", "result")},
        },
        {"a": {}, "b": {}},
        "b",
    )
    out = pipe.execute({"x": 1}, {})
    assert pipe.read_status() == "fatal"
    assert out["_state"]["value"] == "fatal"
    assert nodes["a"].read_status() == "fatal"


def test_pipe_limit_propagates():
    class LimitOnceNode(BaseNode):
        def run(self, inputs, params, context):
            raise NodeExecutionLimit()

    nodes = {"a": LimitOnceNode(), "b": EchoBNode()}
    pipe = _make_pipe(
        nodes,
        ["a", "b"],
        {
            "a": {"x": ("inputs", "x")},
            "b": {"data": ("node", "a", "result")},
        },
        {"a": {}, "b": {}},
        "b",
    )
    out = pipe.execute({"x": 1}, {})
    assert nodes["a"].read_status() == "limit"
    assert pipe.read_status() == "limit"
    assert out["_state"]["value"] == "limit"


def test_pipe_max_calls_on_root():
    nodes = {"a": EchoNode(), "b": EchoBNode()}
    pipe = _make_pipe(
        nodes,
        ["a", "b"],
        {
            "a": {"x": ("inputs", "x")},
            "b": {"data": ("node", "a", "result")},
        },
        {"a": {}, "b": {}},
        "b",
    )
    params = {"limit": {"max_calls": 1}}
    out1 = pipe.execute({"x": "1"}, params)
    assert pipe.read_status() == "done"
    assert "result" in out1
    pipe.reset_status()
    out2 = pipe.execute({"x": "2"}, params)
    assert pipe.read_status() == "limit"
    assert out2["_state"]["value"] == "limit"
