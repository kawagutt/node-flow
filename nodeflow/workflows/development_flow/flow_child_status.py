from __future__ import annotations

from nodeflow.core.base_node import BaseNode, NodeExecutionFailure, NodeExecutionLimit


def raise_if_child_not_done(*, child_name: str, child: BaseNode) -> None:
    status = child.read_status()
    if status == "fatal":
        raise NodeExecutionFailure(f"{child_name} fatal: {child.read_error()}")
    if status == "limit":
        raise NodeExecutionLimit(f"{child_name} limit")
    if status != "done":
        raise NodeExecutionFailure(f"{child_name} unexpected status: {status}")
