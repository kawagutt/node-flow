"""Phase-based sequential execution loop for dev-process."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Optional

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.paths import git_head_revision
from nodeflow.workflows.dev_process.phase_git import (
    phase_commit,
)
from nodeflow.workflows.dev_process.plan_phases import PlanData

MAX_LOOP_RETRIES = 5


def _plan_data_from_json(raw: Dict[str, Any], *, raw_text: str = "") -> PlanData:
    from nodeflow.workflows.dev_process.plan_phases import PlanPhase

    phases = []
    for p in raw.get("phases", []):
        phases.append(
            PlanPhase(
                index=p["index"],
                id=p["id"],
                title=p.get("title", ""),
                goal=p.get("goal", ""),
                scope_include=p.get("scope_include", []),
                scope_exclude=p.get("scope_exclude", []),
                test_plan=p.get("test_plan", []),
                review_targets=p.get("review_targets", []),
                review_agents=p.get("review_agents", []),
                review_checklist=p.get("review_checklist", []),
                acceptance_criteria=p.get("acceptance_criteria", []),
                contract_sha256=p.get("contract_sha256", ""),
                source_heading=p.get("source_heading", ""),
            )
        )
    return PlanData(
        phases=phases,
        raw_text=raw_text,
        plan_sha256=raw.get("plan_sha256", ""),
    )


def _validate_plan_md_json_consistency(
    plan_data: PlanData,
    plan_text: str,
    raw: Dict[str, Any],
) -> None:
    """Ensure plan.md and plan.json describe the same executable plan."""
    if not plan_text.strip():
        return

    expected_sha = str(raw.get("plan_sha256") or plan_data.plan_sha256 or "")
    if expected_sha:
        actual_sha = hashlib.sha256(plan_text.encode()).hexdigest()
        if actual_sha != expected_sha:
            raise NodeExecutionFailure("plan.md / plan.json drift detected: plan_sha256 mismatch")

    from nodeflow.workflows.dev_process.plan_phases import PlanParseError, parse_new_plan

    try:
        parsed = parse_new_plan(plan_text)
    except PlanParseError as exc:
        raise NodeExecutionFailure(
            f"plan.md / plan.json drift detected: cannot parse plan.md ({exc})"
        ) from exc

    if len(parsed.phases) != len(plan_data.phases):
        raise NodeExecutionFailure(
            f"plan.md / plan.json drift detected: phase count mismatch "
            f"(md={len(parsed.phases)}, json={len(plan_data.phases)})"
        )

    for p_json, p_md in zip(plan_data.phases, parsed.phases, strict=True):
        if p_json.id != p_md.id:
            raise NodeExecutionFailure(
                f"plan.md / plan.json drift detected: phase id mismatch at index {p_json.index} "
                f"(json={p_json.id!r}, md={p_md.id!r})"
            )
        if p_json.contract_sha256 != p_md.contract_sha256:
            raise NodeExecutionFailure(
                f"plan.md / plan.json drift detected: contract_sha256 mismatch for {p_json.id}"
            )


def load_plan_data(artifact_root: str) -> PlanData:
    """Load plan.json from artifact root and reconstruct PlanData."""
    plan_json_path = Path(artifact_root) / "plan" / "plan.json"
    if not plan_json_path.exists():
        raise NodeExecutionFailure(f"plan.json not found: {plan_json_path}")
    raw = json.loads(plan_json_path.read_text(encoding="utf-8"))
    plan_text = ""
    plan_md = Path(artifact_root) / "plan" / "plan.md"
    if plan_md.exists():
        plan_text = plan_md.read_text(encoding="utf-8")
    plan_data = _plan_data_from_json(raw, raw_text=plan_text)
    from nodeflow.workflows.dev_process.plan_phases import assert_strict_phase_plan

    assert_strict_phase_plan(plan_data, plan_text=plan_text)
    _validate_plan_md_json_consistency(plan_data, plan_text, raw)
    return plan_data


def continuation_plan_from_merged(plan_data: PlanData, completed_count: int) -> PlanData:
    """Extract continuation-only phases from an already-merged executable plan."""
    if completed_count > len(plan_data.phases):
        raise NodeExecutionFailure(
            f"completed_count {completed_count} exceeds merged plan phase count "
            f"{len(plan_data.phases)}"
        )
    phases = plan_data.phases[completed_count:]
    if not phases:
        raise NodeExecutionFailure("merged plan has no continuation phases to apply")
    return PlanData(phases=phases, raw_text="", plan_sha256=plan_data.plan_sha256)


def load_plan_data_from_json(plan_json_path: str | Path) -> PlanData:
    """Load PlanData from an explicit plan JSON path (e.g. continuation artifact)."""
    path = Path(plan_json_path)
    if not path.exists():
        raise NodeExecutionFailure(f"plan JSON not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    plan_text = ""
    md_path = path.with_suffix(".md")
    if md_path.exists():
        plan_text = md_path.read_text(encoding="utf-8")
    plan_data = _plan_data_from_json(raw, raw_text=plan_text)
    from nodeflow.workflows.dev_process.plan_phases import assert_strict_phase_plan

    assert_strict_phase_plan(plan_data, plan_text=plan_text)
    _validate_plan_md_json_consistency(plan_data, plan_text, raw)
    return plan_data


def compute_max_auto_steps(total_phases: int) -> int:
    """Dynamically compute _MAX_AUTO_STEPS based on phase count."""
    return max(30, total_phases * (MAX_LOOP_RETRIES + 1) * 6 + 20)


def init_phase_state(
    dp: Dict[str, Any],
    plan_data: PlanData,
) -> None:
    """Initialize phase tracking fields in dev_process state."""
    dp["total_phases"] = len(plan_data.phases)
    dp["phase_index"] = 0
    dp["current_phase_id"] = plan_data.phases[0].id if plan_data.phases else ""
    dp["plan_sha256"] = plan_data.plan_sha256

    results: Dict[str, Any] = {}
    for phase in plan_data.phases:
        results[phase.id] = {
            "status": "pending",
            "contract_sha256": phase.contract_sha256,
            "title": phase.title,
        }
    dp["phase_results"] = results
    dp.setdefault("recovery_refs", [])


def get_current_phase_context(
    dp: Dict[str, Any],
    plan_data: PlanData,
) -> Optional[Dict[str, Any]]:
    """Get the current phase's context for stage execution.

    Returns None if all phases are completed.
    """
    idx = dp.get("phase_index", 0)
    if idx >= len(plan_data.phases):
        return None
    phase = plan_data.phases[idx]
    return {
        "phase_id": phase.id,
        "phase_index": idx,
        "phase_title": phase.title,
        "phase_goal": phase.goal,
        "phase_scope_include": phase.scope_include,
        "phase_scope_exclude": phase.scope_exclude,
        "phase_test_plan": phase.test_plan,
        "phase_review_targets": phase.review_targets,
        "phase_review_agents": phase.review_agents,
        "phase_review_checklist": phase.review_checklist,
        "phase_acceptance_criteria": phase.acceptance_criteria,
        "total_phases": len(plan_data.phases),
    }


def record_phase_start(
    dp: Dict[str, Any],
    repo_root: Path,
) -> str:
    """Record phase_start_git_ref for current phase. Returns the ref."""
    from nodeflow.workflows.dev_process.artifact_versions import stamp_phase_artifact_versions

    phase_id = dp.get("current_phase_id", "")
    ref = git_head_revision(repo_root)
    results = dp.setdefault("phase_results", {})
    pr = results.setdefault(phase_id, {})
    pr["phase_start_git_ref"] = ref
    pr["status"] = "in_progress"
    stamp_phase_artifact_versions(dp, phase_id)
    return ref


def complete_phase(
    dp: Dict[str, Any],
    repo_root: Path,
    *,
    artifact_roots: list[str],
) -> Dict[str, Any]:
    """Commit current phase and advance index. Returns commit info."""
    phase_id = dp.get("current_phase_id", "")
    results = dp.get("phase_results", {})
    pr = results.get(phase_id, {})
    title = pr.get("title", "")
    expected_branch = dp.get("task_branch", {}).get("name", "")

    commit_info = phase_commit(
        repo_root,
        phase_id=phase_id,
        phase_title=title,
        artifact_roots=artifact_roots,
        expected_branch=expected_branch,
    )

    pr["phase_commit"] = commit_info["phase_commit"]
    pr["actual_commit_created"] = commit_info["actual_commit_created"]
    pr["committed_paths"] = list(commit_info.get("committed_paths") or [])
    pr["status"] = "completed"
    pr["implementation"] = "completed"
    pr["test"] = "completed"
    pr["review"] = "passed"

    reset_loop_counter(dp, f"{phase_id}_implementation_rework")
    reset_loop_counter(dp, f"{phase_id}_test_rework")

    idx = dp.get("phase_index", 0) + 1
    dp["phase_index"] = idx
    if idx < dp.get("total_phases", 0):
        next_id = f"phase_{idx:03d}"
        dp["current_phase_id"] = next_id
    else:
        dp["current_phase_id"] = ""

    return commit_info


def all_phases_completed(dp: Dict[str, Any]) -> bool:
    """Check if all phases are completed."""
    idx = dp.get("phase_index", 0)
    total = dp.get("total_phases", 0)
    return idx >= total


def increment_loop_counter(dp: Dict[str, Any], loop_key: str) -> int:
    """Increment and return the retry counter for the given loop key.

    Loop keys: ``phase_NNN_implementation_rework``, ``phase_NNN_test_rework``,
    ``plan_revision``, ``final_review_rework``.
    """
    counters = dp.setdefault("loop_counters", {})
    count = counters.get(loop_key, 0) + 1
    counters[loop_key] = count
    return count


def check_loop_limit(dp: Dict[str, Any], loop_key: str) -> None:
    """Raise ``NodeExecutionFailure`` if the loop counter exceeds MAX_LOOP_RETRIES."""
    counters = dp.get("loop_counters") or {}
    count = counters.get(loop_key, 0)
    if count >= MAX_LOOP_RETRIES:
        raise NodeExecutionFailure(
            f"Loop retry limit reached for {loop_key!r}: " f"{count} >= {MAX_LOOP_RETRIES}"
        )


PHASE_RESULT_RUN_KEYS: tuple[str, ...] = (
    "phase_commit",
    "actual_commit_created",
    "implementation",
    "test",
    "review",
    "phase_start_git_ref",
    "spec_version",
    "plan_version",
    "stages",
    "stage_refs",
    "lint_fix",
    "run_tests",
    "reviewed_branch_head",
    "reviewed_branch_name",
    "committed_paths",
)


def clear_phase_run_state(pr: Dict[str, Any]) -> None:
    """Remove per-phase implementation/review artifacts from checkpoint state."""
    for key in PHASE_RESULT_RUN_KEYS:
        pr.pop(key, None)


def reset_loop_counter(dp: Dict[str, Any], loop_key: str) -> None:
    """Remove a loop counter after a successful completion."""
    counters = dp.get("loop_counters")
    if isinstance(counters, dict):
        counters.pop(loop_key, None)


def invalidate_phases_from(dp: Dict[str, Any], from_index: int) -> None:
    """Invalidate phase results and loop counters from `from_index` onward."""
    results = dp.get("phase_results", {})
    total = dp.get("total_phases", 0)
    for i in range(from_index, total):
        pid = f"phase_{i:03d}"
        if pid in results:
            results[pid]["status"] = "pending"
            clear_phase_run_state(results[pid])
        reset_loop_counter(dp, f"{pid}_implementation_rework")
        reset_loop_counter(dp, f"{pid}_test_rework")
    dp["phase_index"] = from_index
    if from_index < total:
        dp["current_phase_id"] = f"phase_{from_index:03d}"
    else:
        dp["current_phase_id"] = ""
