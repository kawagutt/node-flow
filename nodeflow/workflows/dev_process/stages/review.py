"""review stage — multi-reviewer orchestration (coordinator path until PR4 subpipe)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.node_runner import (
    clear_review_argv_override,
    review_argv_override_from_body,
)
from nodeflow.workflows.dev_process.paths import assert_path_under_run_dir
from nodeflow.workflows.dev_process.reuse import aggregate_reviews, write_stage_checkpoint
from nodeflow.workflows.dev_process.review_config import (
    FINAL_REVIEW_AGENTS,
    KNOWN_FINAL_REVIEW_TARGETS,
    KNOWN_PHASE_REVIEW_TARGETS,
    KNOWN_REVIEW_AGENTS,
    KNOWN_REVIEW_TARGETS,
    review_node_name,
)
from nodeflow.workflows.dev_process.review_presets import normalize_preset, reviewer_keys_for_preset
from nodeflow.workflows.dev_process.stages.review_agent import run_one_review_agent_stage


def run_review_stage(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    base_revision: str,
    approved_spec: str,
    approved_plan: str,
    diff_result: Dict[str, Any],
    test_result: Dict[str, Any],
    review_depth_preset: str = "standard",
    body: Dict[str, Any],
    review_targets: list[str] | None = None,
    review_agents: list[str] | None = None,
    review_checklist: list[str] | None = None,
    review_acceptance_criteria: list[str] | None = None,
    lint_result: Dict[str, Any] | None = None,
    review_scope: str = "",
) -> Dict[str, Any]:
    """Run all active review agents then aggregate (coordinator-only; leaf nodes use ``run_one_review_agent_stage``)."""
    preset = normalize_preset(review_depth_preset)

    if review_targets:
        target_set = set(review_targets)
        if review_scope == "phase":
            scope_allowed = KNOWN_PHASE_REVIEW_TARGETS
        elif review_scope == "final":
            scope_allowed = KNOWN_FINAL_REVIEW_TARGETS
        else:
            scope_allowed = KNOWN_REVIEW_TARGETS
        unknown_targets = target_set - scope_allowed
        if unknown_targets:
            raise NodeExecutionFailure(
                f"Unknown review targets for scope {review_scope or 'default'!r}: "
                f"{sorted(unknown_targets)}; allowed: {sorted(scope_allowed)}"
            )

    if review_scope == "final":
        active_agents = list(FINAL_REVIEW_AGENTS)
    elif review_agents:
        unknown = set(review_agents) - KNOWN_REVIEW_AGENTS
        if unknown:
            raise NodeExecutionFailure(
                f"Unknown review agents: {sorted(unknown)}; "
                f"allowed: {sorted(KNOWN_REVIEW_AGENTS)}"
            )
        active_agents = list(review_agents)
    else:
        active_agents = list(reviewer_keys_for_preset(preset))
    active_node_names = [review_node_name(agent) for agent in active_agents]
    expected = set(active_node_names)
    review_inputs: Dict[str, Any] = {}
    evidence_paths: List[str] = []

    augmented_plan = approved_plan
    review_supplement: list[str] = []
    if review_targets:
        review_supplement.append(f"Review targets: {', '.join(review_targets)}")
    if review_checklist:
        review_supplement.append("Review checklist:")
        review_supplement.extend(f"- {c}" for c in review_checklist)
    if review_acceptance_criteria:
        review_supplement.append("Acceptance criteria:")
        review_supplement.extend(f"- {c}" for c in review_acceptance_criteria)
    if lint_result and lint_result.get("lint_fix") == "ruff_failed":
        review_supplement.append("LINT FAILURE (ruff): treat as blocking finding")
        for ep in lint_result.get("log_paths") or []:
            review_supplement.append(f"  lint log: {ep}")
    if review_supplement:
        augmented_plan = approved_plan + "\n\n---\n" + "\n".join(review_supplement)

    argv_override = review_argv_override_from_body(body)
    for agent_key, node_name in zip(active_agents, active_node_names, strict=True):
        er, ep = run_one_review_agent_stage(
            agent=agent_key,
            body=body,
            repo_root=repo_root,
            artifact_root=artifact_root,
            run_id=run_id,
            base_revision=base_revision,
            approved_spec=approved_spec,
            approved_plan=augmented_plan,
            diff_result=diff_result,
            test_result=test_result,
            review_depth_preset=preset,
            argv_override=argv_override,
        )
        review_inputs[node_name] = er
        evidence_paths.append(ep)

    actual = set(review_inputs)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise NodeExecutionFailure(
            f"review preset {preset!r} incomplete: missing={missing!r} extra={extra!r}"
        )

    review_result, checkpoint_request = aggregate_reviews(
        review_inputs=review_inputs,
        test_result=test_result,
        diff_result=diff_result,
        expected_review_keys=active_node_names,
    )

    stage_result = write_stage_checkpoint(
        request=checkpoint_request,
        checkpoint_dir=str(Path(artifact_root) / "review"),
        run_id=run_id,
        stage="review",
        repo_root=repo_root,
        extra_inputs={"review_result": review_result},
    )
    cp_path = None
    for art in stage_result.get("artifacts") or []:
        if isinstance(art, dict) and art.get("kind") == "checkpoint":
            cp_path = art.get("path")
            break
    if cp_path:
        assert_path_under_run_dir(artifact_root, cp_path)

    blocking: List[Any] = list(review_result.get("blocking_findings") or [])
    aggregate = {
        "blocking_count": len(blocking),
        "decision": review_result.get("decision"),
        "spec_revision_needed": review_result.get("spec_revision_needed"),
    }
    dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else None
    if dp is not None:
        from nodeflow.workflows.dev_process.artifact_versions import review_aggregate_metadata

        scope = review_scope or str(dp.get("review_scope", ""))
        aggregate.update(review_aggregate_metadata(dp, review_scope=scope))
    review_summary = Path(artifact_root) / "review" / "aggregate.json"
    review_summary.write_text(json.dumps(aggregate, indent=2), encoding="utf-8")

    merge_ready = not blocking and review_result.get("decision") == "merge_ok"

    clear_review_argv_override(body)

    return {
        "status": "completed",
        "stage_checkpoint_path": cp_path,
        "stage_result": stage_result,
        "review_result": review_result,
        "aggregate": aggregate,
        "merge_ready": merge_ready,
        "review_depth_preset": preset,
        "stale": False,
        "evidence_paths": evidence_paths,
    }
