"""Resolve argv/worker/model for a node from exec_policy_snapshot."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from nodeflow.workflows.dev_process.exec_policy import default_argv_for_worker, default_node_entries


def _valid_argv(argv: Any) -> bool:
    return isinstance(argv, list) and bool(argv) and all(isinstance(x, str) for x in argv)


def _snapshot(body: Dict[str, Any]) -> Dict[str, Any]:
    dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
    snap = dp.get("exec_policy_snapshot")
    if isinstance(snap, dict) and snap.get("nodes"):
        return snap
    return {
        "default_worker": str(dp.get("exec_worker_kind") or "codex"),
        "default_model": dp.get("exec_model"),
        "default_argv": dp.get("exec_argv"),
        "nodes": default_node_entries(),
    }


def resolve_node_exec(body: Dict[str, Any], node_name: str) -> Tuple[str, Optional[str], List[str]]:
    snap = _snapshot(body)
    nodes = snap.get("nodes") if isinstance(snap.get("nodes"), dict) else {}
    entry = nodes.get(node_name) if isinstance(nodes.get(node_name), dict) else {}
    worker = str(entry.get("worker") or snap.get("default_worker") or "codex")
    model = entry.get("model") or snap.get("default_model")
    argv = entry.get("argv")
    if not _valid_argv(argv):
        argv = snap.get("default_argv")
    if not _valid_argv(argv):
        argv = default_argv_for_worker(worker)
    return worker, model if isinstance(model, str) else None, list(argv)
