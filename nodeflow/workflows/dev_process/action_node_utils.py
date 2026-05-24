"""Helpers for running child ActionNodes from dev_process wrappers."""

from __future__ import annotations

from typing import Any, Mapping

from nodeflow.core.base_node import NodeExecutionFailure, NodeExecutionLimit


def execute_or_raise(
    node: Any,
    inputs: Mapping[str, Any],
    params: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run a child ActionNode and propagate fatal/limit instead of returning empty outputs."""
    out = node.execute(dict(inputs), dict(params or {}))
    status = node.read_status()
    if status == "fatal":
        err = node.read_error()
        if isinstance(err, NodeExecutionFailure):
            raise err
        if err is not None:
            raise NodeExecutionFailure(str(err))
        raise NodeExecutionFailure(f"{node.__class__.__name__} entered fatal state")
    if status == "limit":
        err = node.read_error()
        raise NodeExecutionLimit(str(err) if err else f"{node.__class__.__name__} limit")
    if status != "done":
        raise NodeExecutionFailure(f"{node.__class__.__name__} unexpected status: {status}")
    if not isinstance(out, dict):
        raise NodeExecutionFailure(f"{node.__class__.__name__} returned non-dict output")
    return out
