"""reset_child_nodes_for_pipe_execution clears ready children too (stale port state)."""

from __future__ import annotations

from types import MappingProxyType

import pytest

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.pipe_runtime import reset_child_nodes_for_pipe_execution


class _Idle(BaseNode):
    def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
        return {}


def test_reset_clears_input_on_ready_child() -> None:
    n = _Idle()
    n.set_input("x", {"k": 1})
    assert n.get_input_snapshot()
    reset_child_nodes_for_pipe_execution({"n": n})
    assert n.get_input_snapshot() == {}
    assert n.read_status() == "ready"


def test_reset_raises_when_child_executing() -> None:
    n = _Idle()
    n._status = "executing"
    with pytest.raises(RuntimeError, match="stuck in executing"):
        reset_child_nodes_for_pipe_execution({"n": n})
