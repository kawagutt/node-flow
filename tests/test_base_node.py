"""
Tier 1: BaseNode 単体テスト。
"""

import pytest
from types import MappingProxyType

from nodeflow.node import (
    BaseNode,
    ExecutionContext,
    NodeExecutionFailure,
    NodeExecutionLimit,
    _attach_revision,
)


class DummyNode(BaseNode):
    """run を実装するテスト用ノード。port は dict なので _meta.revision が付く。"""

    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        return {"out": {"value": inputs.get("x", "")}}


class FailingNode(BaseNode):
    def run(self, inputs, params, context):
        raise ValueError("fail")


class LimitNode(BaseNode):
    def run(self, inputs, params, context):
        raise NodeExecutionLimit()


class FailureNode(BaseNode):
    def run(self, inputs, params, context):
        raise NodeExecutionFailure("reason")


def test_execute_happy_path():
    node = DummyNode()
    out = node.execute({"x": "hello"}, {})
    assert node.read_status() == "done"
    assert "out" in out
    assert out["out"]["value"] == "hello"
    assert "_meta" in out["out"]
    assert "revision" in out["out"]["_meta"]


def test_run_raises_fatal():
    node = FailingNode()
    out = node.execute({}, {})
    assert node.read_status() == "fatal"
    assert out == {}
    assert node.read_error() is not None
    assert "fail" in str(node.read_error())


def test_max_calls_limit():
    node = DummyNode()
    params = {"limit": {"max_calls": 2}}
    node.execute({"x": "1"}, params)
    assert node.read_status() == "done"
    node.reset_status()
    node.execute({"x": "2"}, params)
    assert node.read_status() == "done"
    node.reset_status()
    out3 = node.execute({"x": "3"}, params)
    assert node.read_status() == "limit"
    assert out3 == {}


def test_reset_limit_state_and_reset_status():
    node = DummyNode()
    params = {"limit": {"max_calls": 1}}
    node.execute({"x": "1"}, params)
    assert node.read_status() == "done"
    node.reset_status()
    node.execute({"x": "2"}, params)
    assert node.read_status() == "limit"
    node.reset_limit_state("calls")
    node.reset_status()
    out = node.execute({"x": "3"}, params)
    assert node.read_status() == "done"
    assert out.get("out", {}).get("value") == "3"


def test_execute_when_not_ready_raises():
    node = DummyNode()
    node.execute({"x": "1"}, {})
    assert node.read_status() == "done"
    with pytest.raises(RuntimeError, match="status is not ready"):
        node.execute({"x": "2"}, {})


def test_reset_status_while_executing_raises():
    class SlowNode(BaseNode):
        def run(self, inputs, params, context):
            self.reset_status()  # executing 中なので RuntimeError
            return {"ok": True}

    node = SlowNode()
    out = node.execute({}, {})
    assert node.read_status() == "fatal"
    assert out == {}
    assert isinstance(node.read_error(), RuntimeError)
    assert "cannot reset while executing" in str(node.read_error())


def test_attach_revision_skips_reserved_keys():
    output = {"_meta": {"x": 1}, "_usage": {"y": 2}, "result": {"value": 1}}
    _attach_revision(output)
    assert "_meta" in output and "revision" not in output["_meta"]
    assert (
        "result" in output
        and "_meta" in output["result"]
        and "revision" in output["result"]["_meta"]
    )


def test_attach_revision_promotes_scalar_to_dict():
    """scalar port は execute 内で {"value": x, "_meta": {"revision": ...}} に昇格する。"""
    class ScalarNode(BaseNode):
        def run(self, inputs, params, context):
            return {"out": 42}

    node = ScalarNode()
    out = node.execute({}, {})
    assert isinstance(out["out"], dict)
    assert out["out"]["value"] == 42
    assert "revision" in out["out"]["_meta"]


def test_node_execution_limit_sets_limit():
    node = LimitNode()
    out = node.execute({}, {})
    assert node.read_status() == "limit"
    assert out == {}


def test_node_execution_failure_sets_fatal():
    node = FailureNode()
    out = node.execute({}, {})
    assert node.read_status() == "fatal"
    assert out == {}
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert node.read_error().reason == "reason"


def test_run_returns_usage_removed():
    """_usage が除去され、dict port には revision が付くことを確認。"""

    class UsageNode(BaseNode):
        def run(self, inputs, params, context):
            return {"out": {"value": 1}, "_usage": {"total_tokens": 10}}

    node = UsageNode()
    out = node.execute({}, {})
    assert "_usage" not in out
    assert "out" in out
    assert out["out"]["value"] == 1
    assert "_meta" in out["out"] and "revision" in out["out"]["_meta"]
