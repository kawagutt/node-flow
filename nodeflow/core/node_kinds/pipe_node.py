"""PipeNode — child graph declaration; RunnerFrame runs the graph."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict, Optional

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.graph_spec import GraphSpec
from nodeflow.legacy.runner_frame import RunnerFrame

__all__ = ["PipeNode"]


class PipeNode(BaseNode):
    """Composite: declares a child GraphSpec; RunnerFrame executes it.

    Subclasses that override ``run()`` for imperative orchestration need not
    implement ``graph()``; ``read_error()`` falls back to ``BaseNode`` in that case.

    Subclasses that use the default ``run()`` must implement ``graph()`` and should
    return a stable ``GraphSpec`` (same child instances across calls) so
    ``read_error()`` can inspect child errors.
    """

    ALLOW_AS_CHILD = True

    def graph(self) -> GraphSpec:
        raise NotImplementedError

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        frame = RunnerFrame(self.graph(), inputs, params)
        return frame.run()

    def read_error(self) -> Optional[Exception]:
        try:
            nodes = self.graph().nodes
        except NotImplementedError:
            return super().read_error()
        for node in nodes.values():
            e = node.read_error()
            if e is not None:
                return e
        if self._status == "fatal" and self._error is not None:
            return self._error
        return None
