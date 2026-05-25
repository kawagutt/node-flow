"""Exec policy snapshot for dev-process (P10: job argv/worker defaults; not jobs[] SOT yet)."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

from nodeflow.workflows.dev_process.hermetic_argv import (
    implement_argv,
    plan_argv,
    plan_review_argv,
    review_argv,
    spec_argv,
    spec_review_argv,
)

POLICY_SCHEMA = "dev_process.exec_policy.v1"

JOB_KEYS = (
    "write_spec",
    "spec_review",
    "write_plan",
    "plan_review",
    "write_implementation",
    "write_tests",
    "run_tests",
    "review_diff",
    "review_tests",
    "review_spec",
    "review_wide",
    "review_spec_revision",
)


def default_argv_for_job(job_key: str) -> List[str]:
    """Hermetic argv used when neither job entry nor default_argv supplies argv."""
    if job_key == "write_spec":
        return spec_argv()
    if job_key == "spec_review":
        return spec_review_argv()
    if job_key == "write_plan":
        return plan_argv()
    if job_key == "plan_review":
        return plan_review_argv()
    if job_key in ("write_implementation", "write_tests", "run_tests"):
        return implement_argv()
    if job_key.startswith("review_"):
        return review_argv()
    return implement_argv()


def default_job_entries() -> Dict[str, Dict[str, Any]]:
    """Worker defaults only — argv resolved via default_argv then default_argv_for_job."""
    return {key: {"worker": "codex"} for key in JOB_KEYS}


def build_exec_policy_snapshot(
    *,
    exec_worker_kind: str = "codex",
    exec_argv: Optional[list[str]] = None,
    exec_model: Optional[str] = None,
) -> Dict[str, Any]:
    jobs = default_job_entries()
    snapshot: Dict[str, Any] = {
        "schema": POLICY_SCHEMA,
        "default_worker": exec_worker_kind,
        "jobs": deepcopy(jobs),
    }
    if exec_model:
        snapshot["default_model"] = exec_model
    if exec_argv is not None:
        snapshot["default_argv"] = list(exec_argv)
    return snapshot


def apply_snapshot_to_body(body: Dict[str, Any], snapshot: Dict[str, Any]) -> None:
    dp = body.setdefault("dev_process", {})
    dp["exec_policy_snapshot"] = snapshot
