"""Resolve argv/worker/model for a job from exec_policy_snapshot."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from nodeflow.workflows.dev_process.exec_policy import default_argv_for_job, default_job_entries


def _valid_argv(argv: Any) -> bool:
    return isinstance(argv, list) and bool(argv) and all(isinstance(x, str) for x in argv)


def _snapshot(body: Dict[str, Any]) -> Dict[str, Any]:
    dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
    snap = dp.get("exec_policy_snapshot")
    if isinstance(snap, dict) and snap.get("jobs"):
        return snap
    return {
        "default_worker": str(dp.get("exec_worker_kind") or "codex"),
        "default_model": dp.get("exec_model"),
        "default_argv": dp.get("exec_argv"),
        "jobs": default_job_entries(),
    }


def resolve_job(body: Dict[str, Any], job_key: str) -> Tuple[str, Optional[str], List[str]]:
    snap = _snapshot(body)
    jobs = snap.get("jobs") if isinstance(snap.get("jobs"), dict) else {}
    entry = jobs.get(job_key) if isinstance(jobs.get(job_key), dict) else {}
    worker = str(entry.get("worker") or snap.get("default_worker") or "codex")
    model = entry.get("model") or snap.get("default_model")
    argv = entry.get("argv")
    if not _valid_argv(argv):
        argv = snap.get("default_argv")
    if not _valid_argv(argv):
        argv = default_argv_for_job(job_key)
    return worker, model if isinstance(model, str) else None, list(argv)
