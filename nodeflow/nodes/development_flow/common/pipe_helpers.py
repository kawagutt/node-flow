"""Small helpers shared by development_flow stage PipeNode run loops."""

from __future__ import annotations

from typing import Dict, List

from nodeflow.core.base_node import BaseNode, NodeExecutionFailure


def raise_child_fatal_if_any(
    *,
    graph_node_order: List[str],
    nodes: Dict[str, BaseNode],
    prefix: str = "child fatal",
) -> None:
    fatal_children = [nid for nid in graph_node_order if nodes[nid].read_status() == "fatal"]
    if fatal_children:
        raise NodeExecutionFailure(f"{prefix}: {fatal_children}")
