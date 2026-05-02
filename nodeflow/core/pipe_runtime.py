"""Helpers for pipe / subgraph execution (v1.6)."""

from __future__ import annotations

from typing import Any


def reset_child_nodes_for_pipe_execution(nodes: dict[str, Any]) -> None:
    """Reset child node runtime state before running a pipe or workflow subgraph."""
    for node in nodes.values():
        if node.read_status() == "executing":
            raise RuntimeError("child node stuck in executing")
        node.reset_status()
