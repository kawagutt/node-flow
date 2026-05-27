"""Phase rewind: reset branch to a target phase after final review failure."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.paths import git_head_revision
from nodeflow.workflows.dev_process.phase_git import reset_to_ref
from nodeflow.workflows.dev_process.phase_loop import invalidate_phases_from


def rewind_to_phase(
    dp: Dict[str, Any],
    repo_root: Path,
    *,
    target_phase: str,
    owner: str,
) -> Dict[str, Any]:
    """Rewind branch to target_phase start, invalidating it and all later phases.

    Returns dict with recovery_ref, target_phase, owner, skip_implementation.

    v1 behaviour: ``skip_implementation`` is always ``False`` regardless of
    ``owner``.  Rewind resets to ``phase_start_git_ref`` which removes the
    implementation diff, so re-running the implementation stage is required.
    Test-only rework (``owner=test``, skip impl, re-run tests only) requires
    saving ``post_implementation_git_ref`` per phase — future work.
    """
    results = dp.get("phase_results", {})
    total = dp.get("total_phases", 0)

    target_index = _phase_index(target_phase)
    if target_index is None or target_index >= total:
        raise NodeExecutionFailure(
            f"Invalid target_phase {target_phase!r} for rewind (total_phases={total})"
        )

    pr = results.get(target_phase, {})
    start_ref = pr.get("phase_start_git_ref")
    if not start_ref:
        raise NodeExecutionFailure(
            f"Cannot rewind to {target_phase}: no phase_start_git_ref recorded"
        )

    recovery_ref = git_head_revision(repo_root)
    recovery_refs = dp.setdefault("recovery_refs", [])
    recovery_refs.append(
        {
            "reason": "final_review_rewind",
            "target_phase": target_phase,
            "owner": owner,
            "ref": recovery_ref,
            "reset_to_ref": start_ref,
        }
    )

    expected_branch = dp.get("task_branch", {}).get("name", "")
    reset_to_ref(repo_root, start_ref, expected_branch=expected_branch)

    invalidate_phases_from(dp, target_index)

    return {
        "recovery_ref": recovery_ref,
        "target_phase": target_phase,
        "target_index": target_index,
        "owner": owner,
        "skip_implementation": False,
        "reset_to_ref": start_ref,
    }


def _phase_index(phase_id: str) -> int | None:
    """Extract index from phase_id like 'phase_002'."""
    if not phase_id.startswith("phase_"):
        return None
    try:
        return int(phase_id[6:])
    except ValueError:
        return None
