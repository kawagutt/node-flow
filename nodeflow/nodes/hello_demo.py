"""Tiny demo ActionNode for ``examples/pipes/hello.json`` (v1.6 PipeSpec smoke / samples)."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode


class HelloDemoNode(PythonActionNode):
    role = "hello_demo"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        _context: ExecutionContext,
    ) -> Dict[str, Any]:
        # Runner does not infer required wiring; undeclivered ports omit from snapshot.
        if "tick" not in inputs:
            return {}
        message = str(params.get("message") or "Hello, World!")
        return {"message": {"data": message}}
