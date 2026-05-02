"""
Tier 1: BaseNode 単体テスト。
"""

from types import MappingProxyType

from nodeflow.core.base_node import (
    BaseNode,
    ExecutionContext,
    NodeExecutionFailure,
    NodeExecutionLimit,
    _attach_runtime,
)


class DummyNode(BaseNode):
    """run を実装するテスト用ノード。revision は _runtime['ports'] に付く。"""

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
    assert out["_state"]["value"] == "done"
    assert "_runtime" in out
    assert "revision" in out["_runtime"]["ports"]["out"]
    assert out["_usage"] == {}


def test_run_raises_fatal():
    node = FailingNode()
    out = node.execute({}, {})
    assert node.read_status() == "fatal"
    assert out["_state"]["value"] == "fatal"
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
    assert out3["_state"]["value"] == "limit"


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
    out1 = node.execute({"x": "1"}, {})
    assert node.read_status() == "done"
    out2 = node.execute({"x": "2"}, {})
    assert out2["out"] == out1["out"]
    assert out2["_state"]["value"] == "done"


def test_reset_status_while_executing_raises():
    class SlowNode(BaseNode):
        def run(self, inputs, params, context):
            self.reset_status()  # executing 中なので RuntimeError
            return {"ok": True}

    node = SlowNode()
    out = node.execute({}, {})
    assert node.read_status() == "fatal"
    assert out["_state"]["value"] == "fatal"
    assert isinstance(node.read_error(), RuntimeError)
    assert "cannot reset while executing" in str(node.read_error())


def test_attach_runtime_skips_reserved_keys():
    output = {"_usage": {"y": 2}, "result": {"value": 1}}
    _attach_runtime(output)
    assert "_runtime" in output
    assert "ports" in output["_runtime"]
    assert "revision" in output["_runtime"]["ports"]["result"]


def test_attach_runtime_rejects_scalar_port_payload():
    """Port payload must be dict; execute returns fatal observation."""

    class ScalarNode(BaseNode):
        def run(self, inputs, params, context):
            return {"out": 42}

    node = ScalarNode()
    out = node.execute({}, {})
    assert node.read_status() == "fatal"
    assert out["_state"]["value"] == "fatal"
    assert "payload must be dict" in str(node.read_error())


def test_node_execution_limit_sets_limit():
    node = LimitNode()
    out = node.execute({}, {})
    assert node.read_status() == "limit"
    assert out["_state"]["value"] == "limit"


def test_node_execution_failure_sets_fatal():
    node = FailureNode()
    out = node.execute({}, {})
    assert node.read_status() == "fatal"
    assert out["_state"]["value"] == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert node.read_error().reason == "reason"


def test_run_returns_usage_removed():
    """_usage が観測出力に入り、_runtime に revision が付くことを確認。"""

    class UsageNode(BaseNode):
        def run(self, inputs, params, context):
            return {"out": {"value": 1}, "_usage": {"total_tokens": 10}}

    node = UsageNode()
    out = node.execute({}, {})
    assert out["_usage"]["total_tokens"] == 10
    assert "out" in out
    assert out["out"]["value"] == 1
    assert "revision" in out["_runtime"]["ports"]["out"]


def test_run_must_not_return_runtime():
    class BadNode(BaseNode):
        def run(self, inputs, params, context):
            return {"out": {"x": 1}, "_runtime": {"ports": {}}}

    node = BadNode()
    out = node.execute({}, {})
    assert node.read_status() == "fatal"
    assert out["_state"]["value"] == "fatal"
    assert isinstance(node.read_error(), ValueError)
    assert "run() must not return _runtime" in str(node.read_error())


def test_run_must_not_return_state():
    class BadNode(BaseNode):
        def run(self, inputs, params, context):
            return {"out": {"x": 1}, "_state": {"value": "done"}}

    node = BadNode()
    out = node.execute({}, {})
    assert node.read_status() == "fatal"
    assert out["_state"]["value"] == "fatal"
    assert isinstance(node.read_error(), ValueError)
    assert "run() must not return _state" in str(node.read_error())
