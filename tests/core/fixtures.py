"""Test-only helpers for smoke and loader integration (do not wire into builtins)."""

from __future__ import annotations

from copy import deepcopy
from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode


class SmokeCopyNode(PythonActionNode):
    """1:1 copy of the ``in`` dict port to ``out`` — avoids production Copy nodes in Phase 7 smoke."""

    role = "nf_test_copy"

    def run(
        self,
        inputs: Dict[str, Any],
        _params: MappingProxyType,
        _context: ExecutionContext,
    ) -> Dict[str, Any]:
        if "in" not in inputs:
            return {}
        pl = inputs["in"]
        if not isinstance(pl, dict):
            raise TypeError("SmokeCopyNode expects dict payload on port 'in'")
        return {"out": deepcopy(pl)}
