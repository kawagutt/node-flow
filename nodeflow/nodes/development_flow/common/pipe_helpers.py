"""Small helpers shared by development_flow stage PipeNode run loops."""

from __future__ import annotations

from typing import Dict, List

from nodeflow.core.base_node import BaseNode, NodeExecutionFailure, NodeExecutionLimit
from nodeflow.core.runner import Runner


def raise_child_fatal_if_any(
    *,
    graph_node_order: List[str],
    nodes: Dict[str, BaseNode],
    prefix: str = "child fatal",
) -> None:
    fatal_children = [nid for nid in graph_node_order if nodes[nid].read_status() == "fatal"]
    if fatal_children:
        raise NodeExecutionFailure(f"{prefix}: {fatal_children}")


def run_until_node_done(
    *,
    runner: Runner,
    graph_node_order: List[str],
    nodes: Dict[str, BaseNode],
    done_node_id: str,
) -> None:
    while True:
        progressed = runner.step()
        statuses = [nodes[nid].read_status() for nid in graph_node_order]
        raise_child_fatal_if_any(graph_node_order=graph_node_order, nodes=nodes)
        if "limit" in statuses:
            raise NodeExecutionLimit("child limit")
        if nodes[done_node_id].read_status() == "done":
            return
        if not progressed:
            raise NodeExecutionFailure("invalid execution state")
