"""Compact stage references for phase_results (checkpoint-safe pointers)."""

from __future__ import annotations

from typing import Any, Dict

# Keys never stored in phase_results — large blobs belong in artifact files only.
_DROP_KEYS = frozenset(
    {
        "execution_output",
        "diff_result",
        "review_result",
        "stage_result",
        "stdout",
        "stderr",
        "ruff_stdout",
        "ruff_stderr",
        "prompt",
        "argv",
    }
)


def compact_stage_ref(stage: Dict[str, Any] | None) -> Dict[str, Any]:
    """Shrink a stage result dict to checkpoint-safe pointers and summaries."""
    if not stage:
        return {}
    ref: Dict[str, Any] = {}
    if stage.get("status"):
        ref["status"] = stage["status"]
    for key in (
        "decision",
        "lint_fix",
        "merge_ready",
        "phase_count",
        "parse_attempts",
        "plan_sha256",
        "plan_version",
        "continuation",
        "plan_version_deferred",
        "plan_version_status",
        "accepted_plan_version",
        "ruff_exit_code",
        "reason",
        "warning",
    ):
        if key in stage:
            ref[key] = stage[key]

    for path_key in (
        "summary_artifact",
        "plan_artifact",
        "plan_json_path",
        "spec_artifact",
        "stage_checkpoint_path",
        "aggregate_path",
        "continuation_json_path",
        "versioned_spec_path",
        "versioned_path",
    ):
        if stage.get(path_key):
            ref[path_key] = stage[path_key]

    ev = stage.get("evidence_paths")
    if isinstance(ev, list) and ev:
        ref["evidence_paths"] = [str(p) for p in ev if p]

    log_paths = stage.get("log_paths")
    if isinstance(log_paths, list) and log_paths:
        ref["log_paths"] = [str(p) for p in log_paths if p]

    agg = stage.get("aggregate")
    if isinstance(agg, dict):
        ref["aggregate"] = {
            k: agg[k]
            for k in ("decision", "ok", "blocking_count", "stage", "spec_version", "plan_version")
            if k in agg
        }

    rr = stage.get("review_result")
    if isinstance(rr, dict):
        blocking = rr.get("blocking_findings") or []
        ref["review_summary"] = {
            "decision": rr.get("decision"),
            "blocking_count": len(blocking) if isinstance(blocking, list) else 0,
        }

    for key in ("reviewed_branch_head", "reviewed_branch_name"):
        if stage.get(key):
            ref[key] = stage[key]

    fixed_files = stage.get("fixed_files")
    if isinstance(fixed_files, list) and fixed_files:
        ref["fixed_files_count"] = len(fixed_files)

    return ref


def compact_phase_stages(
    *,
    implementation: Dict[str, Any] | None = None,
    test_implementation: Dict[str, Any] | None = None,
    lint_fix: Dict[str, Any] | None = None,
    run_tests: Dict[str, Any] | None = None,
    review: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Build compact ``phase_results[phase_id]['stage_refs']`` map."""
    return {
        "implementation": compact_stage_ref(implementation),
        "test_implementation": compact_stage_ref(test_implementation),
        "lint_fix": compact_stage_ref(lint_fix),
        "run_tests": compact_stage_ref(run_tests),
        "review": compact_stage_ref(review),
    }
