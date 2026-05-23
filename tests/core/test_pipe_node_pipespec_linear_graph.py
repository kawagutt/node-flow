"""Linear child graph via generic ``PipeNode`` + executable ``PipeSpec``."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.node_kinds.pipe_node import PipeNode
from nodeflow.core.pipe_spec import NodeSpec, PipeDeclaration, PipeSpec
from nodeflow.core.source_ref import SourceRef


class EchoNode(BaseNode):
    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        if "x" not in inputs:
            return {}
        x = inputs["x"]
        text = x.get("text", "") if isinstance(x, dict) else ""
        return {"result": {"value": str(text) + ":a"}}


class EchoBNode(BaseNode):
    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        if "data" not in inputs:
            return {}
        data = inputs["data"]
        val = data.get("value", "") if isinstance(data, dict) else data
        return {"result": {"out": str(val) + ":b"}}


def _spec(nodes: Dict[str, BaseNode]) -> PipeSpec:
    a = nodes["a"]
    b = nodes["b"]
    return PipeSpec(
        graph_node_order=("a", "b"),
        pipe=PipeDeclaration(
            input_ports=frozenset({"x"}),
            output_sources={"result": SourceRef(kind="node", node_id="b", port_name="result")},
        ),
        nodes={
            "a": NodeSpec(
                node_id="a",
                node=a,
                input_sources={"x": SourceRef(kind="input", port_name="x")},
                output_ports=frozenset({"result"}),
            ),
            "b": NodeSpec(
                node_id="b",
                node=b,
                input_sources={"data": SourceRef(kind="node", node_id="a", port_name="result")},
                output_ports=frozenset({"result"}),
            ),
        },
    )


def _make_pipe(nodes: Dict[str, BaseNode]) -> PipeNode:
    return PipeNode(_spec(nodes))


def test_pipe_two_nodes() -> None:
    nodes = {"a": EchoNode(), "b": EchoBNode()}
    pipe = _make_pipe(nodes)
    out = pipe.execute({"x": {"text": "in"}}, {})
    assert pipe.read_status() == "done"
    assert nodes["a"].read_status() == "done"
    assert nodes["b"].read_status() == "done"
    assert out["result"]["out"] == "in:a:b"


def test_pipe_fatal_propagates() -> None:
    class FailingNode(BaseNode):
        def run(self, inputs, params, context):
            raise RuntimeError("a failed")

    nodes = {"a": FailingNode(), "b": EchoBNode()}
    pipe = _make_pipe(nodes)
    out = pipe.execute({"x": {"text": "1"}}, {})
    assert pipe.read_status() == "fatal"
    assert out["_state"]["value"] == "fatal"
    assert nodes["a"].read_status() == "fatal"


def test_pipe_limit_propagates() -> None:
    from nodeflow.core.base_node import NodeExecutionLimit

    class LimitOnceNode(BaseNode):
        def run(self, inputs, params, context):
            raise NodeExecutionLimit()

    nodes = {"a": LimitOnceNode(), "b": EchoBNode()}
    pipe = _make_pipe(nodes)
    out = pipe.execute({"x": {"text": "1"}}, {})
    assert nodes["a"].read_status() == "limit"
    assert pipe.read_status() == "limit"
    assert out["_state"]["value"] == "limit"


def test_pipe_max_calls_on_root() -> None:
    nodes = {"a": EchoNode(), "b": EchoBNode()}
    pipe = _make_pipe(nodes)
    params = {"limit": {"max_calls": 1}}
    out1 = pipe.execute({"x": {"text": "1"}}, params)
    assert pipe.read_status() == "done"
    assert "result" in out1
    pipe.reset_status()
    out2 = pipe.execute({"x": {"text": "2"}}, params)
    assert pipe.read_status() == "limit"
    assert out2["_state"]["value"] == "limit"
