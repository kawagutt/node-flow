"""Contract check: ensure completed phases are not modified by plan rework."""

from __future__ import annotations

import hashlib
from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.phase_loop import reset_loop_counter
from nodeflow.workflows.dev_process.plan_phases import PlanData


def validate_rework_contracts(
    new_plan: PlanData,
    dp: Dict[str, Any],
) -> None:
    """Validate that completed phases have not been modified.

    Raises NodeExecutionFailure if any completed phase is missing,
    reordered, or has a different contract_sha256.
    """
    results = dp.get("phase_results", {})
    completed_count = count_completed_prefix(results, dp.get("total_phases", 0))

    if completed_count == 0:
        return

    if len(new_plan.phases) < completed_count:
        raise NodeExecutionFailure(
            f"Plan rework removed completed phases: "
            f"new plan has {len(new_plan.phases)} phases but {completed_count} are completed"
        )

    for i in range(completed_count):
        expected_id = f"phase_{i:03d}"
        pr = results.get(expected_id, {})
        expected_sha = pr.get("contract_sha256", "")

        if i >= len(new_plan.phases):
            raise NodeExecutionFailure(f"Plan rework removed completed phase {expected_id}")

        new_phase = new_plan.phases[i]

        if new_phase.id != expected_id:
            raise NodeExecutionFailure(
                f"Plan rework reordered completed phase: "
                f"expected {expected_id} at index {i}, got {new_phase.id}"
            )

        if expected_sha and new_phase.contract_sha256 != expected_sha:
            raise NodeExecutionFailure(
                f"Plan rework changed contract of completed phase {expected_id}: "
                f"expected {expected_sha[:16]}..., got {new_phase.contract_sha256[:16]}..."
            )


def count_completed_prefix(results: Dict[str, Any], total_phases: int) -> int:
    """Count consecutive completed phases from index 0."""
    count = 0
    for i in range(total_phases):
        pid = f"phase_{i:03d}"
        pr = results.get(pid, {})
        if pr.get("status") == "completed":
            count += 1
        else:
            break
    return count


def get_completed_phase_info(dp: Dict[str, Any]) -> list[Dict[str, Any]]:
    """Extract completed phase info for prompt injection."""
    results = dp.get("phase_results", {})
    total = dp.get("total_phases", 0)
    completed: list[Dict[str, Any]] = []
    for i in range(total):
        pid = f"phase_{i:03d}"
        pr = results.get(pid, {})
        if pr.get("status") == "completed":
            completed.append(
                {
                    "id": pid,
                    "title": pr.get("title", ""),
                    "contract_sha256": pr.get("contract_sha256", ""),
                }
            )
        else:
            break
    return completed


def apply_rework_plan_update(
    new_plan: PlanData,
    dp: Dict[str, Any],
) -> None:
    """After contract validation, update phase_results for current and later phases.

    Preserves completed phases, invalidates current and later.
    """

    results = dp.get("phase_results", {})
    completed_count = count_completed_prefix(results, dp.get("total_phases", 0))

    dp["total_phases"] = len(new_plan.phases)
    dp["plan_sha256"] = new_plan.plan_sha256

    for phase in new_plan.phases[completed_count:]:
        results.setdefault(phase.id, {})
        results[phase.id]["status"] = "pending"
        results[phase.id]["contract_sha256"] = phase.contract_sha256
        results[phase.id]["title"] = phase.title
        from nodeflow.workflows.dev_process.phase_loop import clear_phase_run_state

        clear_phase_run_state(results[phase.id])

    stale_ids = set()
    for pid in list(results.keys()):
        idx_match = pid.replace("phase_", "")
        try:
            idx = int(idx_match)
        except ValueError:
            continue
        if idx >= len(new_plan.phases):
            stale_ids.add(pid)
    for pid in stale_ids:
        del results[pid]

    for phase in new_plan.phases[completed_count:]:
        reset_loop_counter(dp, f"{phase.id}_implementation_rework")
        reset_loop_counter(dp, f"{phase.id}_test_rework")
    for pid in stale_ids:
        reset_loop_counter(dp, f"{pid}_implementation_rework")
        reset_loop_counter(dp, f"{pid}_test_rework")

    dp.pop("final_rework_required", None)
    dp.pop("final_synthesis", None)

    dp["phase_results"] = results
    dp["phase_index"] = completed_count
    if completed_count < len(new_plan.phases):
        dp["current_phase_id"] = f"phase_{completed_count:03d}"
    else:
        dp["current_phase_id"] = ""


def enter_continuation_planning_mode(
    dp: Dict[str, Any],
    *,
    findings: list[Dict[str, Any]],
    completed_count: int,
) -> None:
    """Enter continuation planning; pin accepted plan version for merge retries."""
    from nodeflow.workflows.dev_process.artifact_versions import (
        ensure_continuation_base_plan_version,
    )

    ensure_continuation_base_plan_version(dp)
    dp["planning_mode"] = "continuation_from_head"
    dp["continuation_findings"] = list(findings)
    dp["continuation_start_phase"] = f"phase_{completed_count:03d}"


def merge_continuation_plan(
    existing_plan: PlanData,
    continuation_plan: PlanData,
    *,
    completed_count: int,
) -> PlanData:
    """Build executable plan = completed original phases + continuation phases."""
    merged_phases = list(existing_plan.phases[:completed_count]) + list(continuation_plan.phases)
    raw_text = (
        existing_plan.raw_text.rstrip()
        + "\n\n---\n\n"
        + "## Continuation plan\n\n"
        + continuation_plan.raw_text.strip()
    )
    plan_sha = hashlib.sha256(raw_text.encode()).hexdigest()
    return PlanData(phases=merged_phases, raw_text=raw_text, plan_sha256=plan_sha)


def apply_continuation_plan_update(
    continuation_plan: PlanData,
    dp: Dict[str, Any],
) -> None:
    """Append continuation phases after all completed phases.

    Unlike ``apply_rework_plan_update``, this does NOT invalidate or reset
    any existing phases.  It only appends new phases starting from
    ``completed_count``.  No git reset is performed — implementation
    continues from the current HEAD.
    """
    results = dp.get("phase_results", {})
    completed_count = count_completed_prefix(results, dp.get("total_phases", 0))

    for offset, phase in enumerate(continuation_plan.phases):
        expected_id = f"phase_{completed_count + offset:03d}"
        if phase.id != expected_id:
            raise NodeExecutionFailure(
                f"continuation phase id mismatch: expected {expected_id}, got {phase.id}"
            )
        existing = results.get(expected_id, {})
        if existing.get("status") == "completed":
            raise NodeExecutionFailure(
                f"continuation plan must not modify completed phase {expected_id}"
            )
        results.setdefault(phase.id, {})
        results[phase.id]["status"] = "pending"
        results[phase.id]["contract_sha256"] = phase.contract_sha256
        results[phase.id]["title"] = phase.title

    new_total = completed_count + len(continuation_plan.phases)
    dp["total_phases"] = new_total
    dp["phase_results"] = results
    dp["phase_index"] = completed_count
    dp["current_phase_id"] = f"phase_{completed_count:03d}" if continuation_plan.phases else ""
    dp["continuation_start_phase"] = f"phase_{completed_count:03d}"

    continuation_count = dp.get("continuation_count", 0) + 1
    dp["continuation_count"] = continuation_count

    dp.pop("final_rework_required", None)
    dp.pop("final_synthesis", None)
