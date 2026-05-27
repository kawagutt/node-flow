"""review stage — reviewers + aggregate."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import EXEC_TIMEOUT_SECONDS
from nodeflow.workflows.dev_process.evidence import record_exec_evidence
from nodeflow.workflows.dev_process.exec_policy import default_argv_for_worker
from nodeflow.workflows.dev_process.hermetic_argv import review_argv
from nodeflow.workflows.dev_process.paths import assert_path_under_run_dir
from nodeflow.workflows.dev_process.reuse import (
    aggregate_reviews,
    build_review_prompt,
    write_stage_checkpoint,
)
from nodeflow.workflows.dev_process.review_config import review_node_name
from nodeflow.workflows.dev_process.review_presets import normalize_preset, reviewer_keys_for_preset
from nodeflow.workflows.dev_process.review_prompt_limits import prompt_params_for_review_node
from nodeflow.workflows.dev_process.workers import ExecWorker, resolve_exec_worker, run_exec


def _run_one_reviewer(
    *,
    repo_root: Path,
    base_revision: str,
    diff_result: Dict[str, Any],
    test_result: Dict[str, Any],
    approved_spec: str,
    approved_plan: str,
    reviewer_key: str,
    preset: str,
    exec_argv: list[str],
    worker: ExecWorker,
) -> tuple[Dict[str, Any], str, str]:
    node_name = review_node_name(reviewer_key)
    text = build_review_prompt(
        node_name,
        repo_root=repo_root,
        base_revision=base_revision,
        diff_result=diff_result,
        test_result=test_result,
        approved_spec=approved_spec,
        approved_plan=approved_plan,
        prompt_params=prompt_params_for_review_node(preset, node_name),
    )
    cwd = str(repo_root)
    er = run_exec(worker, prompt=text, cwd=cwd, argv=exec_argv, timeout=EXEC_TIMEOUT_SECONDS)
    return er, text, cwd


def _run_one_reviewer_via_node(
    *,
    body: Dict[str, Any],
    repo_root: Path,
    base_revision: str,
    diff_result: Dict[str, Any],
    test_result: Dict[str, Any],
    approved_spec: str,
    approved_plan: str,
    reviewer_key: str,
    preset: str,
    run_id: str,
    artifact_root: str,
    argv_override: Optional[list[str]] = None,
) -> tuple[Dict[str, Any], str]:
    from nodeflow.workflows.dev_process.node_runner import run_node_exec

    node_name = review_node_name(reviewer_key)
    text = build_review_prompt(
        node_name,
        repo_root=repo_root,
        base_revision=base_revision,
        diff_result=diff_result,
        test_result=test_result,
        approved_spec=approved_spec,
        approved_plan=approved_plan,
        prompt_params=prompt_params_for_review_node(preset, node_name),
    )
    cwd = str(repo_root)
    execution_output, evidence_path, _rec = run_node_exec(
        body,
        node_name=node_name,
        stage="review",
        prompt=text,
        cwd=cwd,
        run_id=run_id,
        artifact_root=artifact_root,
        invoker_override=reviewer_key,
        argv_override=argv_override,
    )
    return execution_output, evidence_path


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
    exec_argv: list[str] | None = None,
    force_blocking: bool = False,
    review_depth_preset: str = "standard",
    exec_worker_kind: Optional[str] = None,
    body: Optional[Dict[str, Any]] = None,
    review_targets: list[str] | None = None,
    review_agents: list[str] | None = None,
    review_checklist: list[str] | None = None,
    review_acceptance_criteria: list[str] | None = None,
    lint_result: Dict[str, Any] | None = None,
    review_scope: str = "",
) -> Dict[str, Any]:
    from nodeflow.workflows.dev_process.review_config import (
        FINAL_REVIEW_AGENTS,
        KNOWN_FINAL_REVIEW_TARGETS,
        KNOWN_PHASE_REVIEW_TARGETS,
        KNOWN_REVIEW_AGENTS,
        KNOWN_REVIEW_TARGETS,
    )

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

    if body is not None:
        blocking_argv = review_argv(blocking=True) if force_blocking else None
        for agent_key, node_name in zip(active_agents, active_node_names, strict=True):
            er, ep = _run_one_reviewer_via_node(
                body=body,
                repo_root=repo_root,
                base_revision=base_revision,
                diff_result=diff_result,
                test_result=test_result,
                approved_spec=approved_spec,
                approved_plan=augmented_plan,
                reviewer_key=agent_key,
                preset=preset,
                run_id=run_id,
                artifact_root=artifact_root,
                argv_override=blocking_argv,
            )
            review_inputs[node_name] = er
            if not er.get("ok"):
                raise NodeExecutionFailure(f"review subprocess failed for {node_name}")
            evidence_paths.append(ep)
    else:
        worker = resolve_exec_worker(exec_worker_kind)
        argv = exec_argv if exec_argv is not None else default_argv_for_worker(worker.kind)
        for agent_key, node_name in zip(active_agents, active_node_names, strict=True):
            er, prompt_text, cwd = _run_one_reviewer(
                repo_root=repo_root,
                base_revision=base_revision,
                diff_result=diff_result,
                test_result=test_result,
                approved_spec=approved_spec,
                approved_plan=augmented_plan,
                reviewer_key=agent_key,
                preset=preset,
                exec_argv=argv,
                worker=worker,
            )
            review_inputs[node_name] = er
            if not er.get("ok"):
                raise NodeExecutionFailure(f"review subprocess failed for {node_name}")
            ep = record_exec_evidence(
                artifact_root=artifact_root,
                run_id=run_id,
                stage="review",
                invoker=node_name,
                execution_output=er,
                argv=argv,
                prompt=prompt_text,
                cwd=cwd,
            )
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
    dp = (body or {}).get("dev_process") if body else None
    if dp is not None:
        from nodeflow.workflows.dev_process.artifact_versions import review_aggregate_metadata

        scope = review_scope or str(dp.get("review_scope", ""))
        aggregate.update(review_aggregate_metadata(dp, review_scope=scope))
    review_summary = Path(artifact_root) / "review" / "aggregate.json"
    review_summary.write_text(json.dumps(aggregate, indent=2), encoding="utf-8")

    merge_ready = not blocking and review_result.get("decision") == "merge_ok"

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
