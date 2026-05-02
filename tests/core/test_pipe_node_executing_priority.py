"""PipeNode §10.4-style status: child ``executing`` maps to composite ``executing``."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.node_kinds.pipe_node import PipeNode
from nodeflow.core.pipe_spec import PipeSpec


class _ChildExec(BaseNode):
    def read_status(self) -> str:
        return "executing"


class _PipeSnapshot(PipeNode):
    """Synthetic snapshot (no real Runner) to exercise :meth:`PipeNode._status_after_run`."""

    def pipe_spec(self) -> PipeSpec:
        raise NotImplementedError

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        self._pipe_run_snapshot = {
            "all_outputs_filled": False,
            "nodes_map": {"c": _ChildExec()},
            "graph_node_order": ("c",),
        }
        return {}


def test_pipe_node_status_executing_when_child_executing() -> None:
    p = _PipeSnapshot()
    p.execute({}, {})
    assert p.read_status() == "executing"


def test_pipe_node_status_ready_when_incomplete_and_children_not_executing() -> None:
    class _ChildReady(BaseNode):
        def read_status(self) -> str:
            return "ready"

    class _PipeReady(_PipeSnapshot):
        def run(
            self,
            inputs: Dict[str, Any],
            params: MappingProxyType,
            context: ExecutionContext,
        ) -> Dict[str, Any]:
            self._pipe_run_snapshot = {
                "all_outputs_filled": False,
                "nodes_map": {"c": _ChildReady()},
                "graph_node_order": ("c",),
            }
            return {}

    p = _PipeReady()
    p.execute({}, {})
    assert p.read_status() == "ready"
