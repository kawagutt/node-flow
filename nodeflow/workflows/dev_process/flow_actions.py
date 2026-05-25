"""dev-process flow orchestration (v2 actions and run_flow)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process import timeline
from nodeflow.workflows.dev_process.argv_builder import resolve_job
from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint, write_flow_checkpoint
from nodeflow.workflows.dev_process.constants import (
    ACTION_APPROVE_FINAL,
    ACTION_APPROVE_SPEC,
    ACTION_CONTINUE_IMPLEMENTATION,
    ACTION_MERGE,
    ACTION_REJECT_FINAL,
    ACTION_REJECT_SPEC,
    ACTION_REQUEST_SPEC_REVISION,
    ACTION_REVISE_PLAN,
    ACTION_REVISE_SPEC,
    ACTION_REWORK,
    ACTION_START,
    EXEC_WORKER_CODEX,
    MERGE_POLICY_RECORD_ONLY,
    STATE_AWAITING_FINAL,
    STATE_AWAITING_IMPLEMENTATION,
    STATE_AWAITING_PLAN_REVISION,
    STATE_AWAITING_REWORK_DECISION,
    STATE_AWAITING_SPEC_HUMAN_GATE,
    STATE_AWAITING_SPEC_REVISION,
    STATE_FAILED,
    STATE_INITIALIZED,
    WORKSPACE_STRATEGY_CURRENT_REPO,
)
from nodeflow.workflows.dev_process.exec_policy import (
    apply_snapshot_to_body,
    build_exec_policy_snapshot,
)
from nodeflow.workflows.dev_process.flow_context import (
    _assert_resume_identity,
    _clear_git_worktree_on_revise,
    _combine_revision_context,
    _empty_stages,
    _exec_worker_kind,
    _fail_checkpoint,
    _finalize,
    _read_plan_text,
    _read_spec_text,
    _resolve_exec_argv,
    _revision_context_from_plan_review,
    _revision_context_from_spec_review,
    _run_context_for_prepare_workspace,
    _stored_exec_argv,
    _stored_exec_model,
    _workspace_repo_root,
    _workspace_strategy,
)
from nodeflow.workflows.dev_process.flow_merge import _handle_approve_final, _handle_merge
from nodeflow.workflows.dev_process.merge import (
    assert_merge_policy_allowed_at_start,
    record_reviewed_branch_snapshot,
    validate_merge_policy,
)
from nodeflow.workflows.dev_process.paths import (
    abs_path,
    allocate_run_dir,
    git_current_branch,
    git_head_revision,
    new_run_id,
    planned_branch_name_for_run,
    resolve_git_toplevel,
    validate_run_id,
)
from nodeflow.workflows.dev_process.reuse import check_source_workspace, prepare_workspace
from nodeflow.workflows.dev_process.stage_inputs import (
    build_rework_context,
    collect_revision_inputs,
    collect_rework_inputs,
    collect_spec_inputs,
    format_revision_context,
    load_stored_spec_inputs,
)
from nodeflow.workflows.dev_process.stages import (
    run_implementation_stage,
    run_plan_review_stage,
    run_plan_stage,
    run_review_stage,
    run_run_tests_stage,
    run_spec_review_stage,
    run_spec_stage,
    run_test_implementation_stage,
)
from nodeflow.workflows.dev_process.stale import clear_stage_stale, mark_stale
from nodeflow.workflows.dev_process.state_machine import assert_action_allowed
from nodeflow.workflows.dev_process.synthesis import assign_owners_to_findings, route_owner_to_state


def _job_argv(
    body: Dict[str, Any],
    job_key: str,
    exec_argv: Optional[list[str]],
) -> Optional[list[str]]:
    dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
    if isinstance(dp.get("exec_policy_snapshot"), dict):
        _, _, argv = resolve_job(body, job_key)
        return argv
    return exec_argv


def run_flow(
    *,
    action: str,
    repo_root: str,
    task_prompt: str = "",
    flow_checkpoint_path: Optional[str] = None,
    run_id: Optional[str] = None,
    run_spec_on_start: bool = True,
    human_comment_text: str = "",
    exec_argv: Optional[list[str]] = None,
    exec_model: Optional[str] = None,
    force_review_blocking: bool = False,
    workspace_strategy: Optional[str] = None,
    exec_worker_kind: Optional[str] = None,
    merge_policy: Optional[str] = None,
    interactive: bool = False,
    spec_inputs_provided: Optional[Dict[str, Any]] = None,
    revision_provided: Optional[Dict[str, Any]] = None,
    rework_provided: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if action == ACTION_START and flow_checkpoint_path:
        raise NodeExecutionFailure("start does not accept flow_checkpoint_path")

    if action == ACTION_START:
        argv = _resolve_exec_argv(exec_argv)
        provided = dict(spec_inputs_provided or {})
        if task_prompt.strip() and not provided.get("task_prompt"):
            provided["task_prompt"] = task_prompt.strip()
        return _handle_start(
            repo_root=repo_root,
            task_prompt=task_prompt,
            run_id=run_id,
            run_spec_on_start=run_spec_on_start,
            exec_argv=argv,
            exec_model=exec_model.strip()
            if isinstance(exec_model, str) and exec_model.strip()
            else None,
            workspace_strategy=workspace_strategy or WORKSPACE_STRATEGY_CURRENT_REPO,
            exec_worker_kind=exec_worker_kind or EXEC_WORKER_CODEX,
            merge_policy=merge_policy or MERGE_POLICY_RECORD_ONLY,
            interactive=interactive,
            spec_inputs_provided=provided,
        )

    if not flow_checkpoint_path:
        raise NodeExecutionFailure(f"action {action!r} requires flow_checkpoint_path")

    body = _load_and_validate_resume(
        flow_checkpoint_path,
        action=action,
        repo_root=repo_root,
        run_id=run_id,
        workspace_strategy=workspace_strategy,
        exec_worker_kind=exec_worker_kind,
        merge_policy=merge_policy,
        exec_argv=exec_argv,
        exec_model=exec_model,
    )
    run_context = body["run_context"]
    run_id = str(run_context.get("run_id") or "")
    argv = _resolve_exec_argv(exec_argv, body=body)
    state = str((body.get("flow_result") or {}).get("state") or "")
    assert_action_allowed(state, action)
    timeline.append_event(
        run_context["artifact_root"], run_id, "action_received", action=action, state=state
    )

    if action == ACTION_APPROVE_SPEC:
        return _handle_approve_spec(body, run_id=run_id, exec_argv=argv)
    if action in (ACTION_REVISE_SPEC, ACTION_REQUEST_SPEC_REVISION):
        rev_provided = dict(revision_provided or {})
        if action == ACTION_REQUEST_SPEC_REVISION:
            if human_comment_text.strip() and not rev_provided.get("revision_comment"):
                rev_provided["revision_comment"] = human_comment_text.strip()
        elif task_prompt.strip() and not rev_provided.get("revision_comment"):
            rev_provided["revision_comment"] = task_prompt.strip()
        return _handle_revise_spec(
            body,
            run_id=run_id,
            exec_argv=argv,
            interactive=interactive,
            revision_provided=rev_provided,
            use_human_comment=action == ACTION_REQUEST_SPEC_REVISION,
        )
    if action == ACTION_REVISE_PLAN:
        rev_provided = dict(revision_provided or {})
        if task_prompt.strip() and not rev_provided.get("revision_comment"):
            rev_provided["revision_comment"] = task_prompt.strip()
        return _handle_revise_plan(
            body,
            run_id=run_id,
            exec_argv=argv,
            interactive=interactive,
            revision_provided=rev_provided,
        )
    if action == ACTION_CONTINUE_IMPLEMENTATION:
        return _handle_continue_implementation(
            body,
            run_id=run_id,
            exec_argv=argv,
            force_review_blocking=force_review_blocking,
        )
    if action == ACTION_REWORK:
        rw_provided = dict(rework_provided or {})
        if human_comment_text.strip() and not rw_provided.get("rework_comment"):
            rw_provided["rework_comment"] = human_comment_text.strip()
        if not rw_provided.get("rework_comment") and not interactive:
            rw_provided.setdefault("rework_comment", "rework requested")
        return _handle_rework(
            body,
            run_id=run_id,
            exec_argv=argv,
            force_review_blocking=force_review_blocking,
            interactive=interactive,
            rework_provided=rw_provided,
        )
    if action == ACTION_MERGE:
        return _handle_merge(body, run_id=run_id)
    if action == ACTION_APPROVE_FINAL:
        return _handle_approve_final(body, run_id=run_id)
    if action in (ACTION_REJECT_SPEC, ACTION_REJECT_FINAL):
        return _handle_reject(
            body,
            run_id=run_id,
            action=action,
            human_comment_text=human_comment_text,
        )

    raise NodeExecutionFailure(f"unsupported action {action!r}")


def _load_and_validate_resume(
    flow_checkpoint_path: str,
    *,
    action: str,
    repo_root: str,
    run_id: Optional[str],
    workspace_strategy: Optional[str],
    exec_worker_kind: Optional[str],
    merge_policy: Optional[str],
    exec_argv: Optional[list[str]],
    exec_model: Optional[str],
) -> Dict[str, Any]:
    del action
    doc = load_flow_checkpoint(flow_checkpoint_path)
    body = dict(doc)
    run_context = dict(body.get("run_context") or {})
    if workspace_strategy is not None:
        requested = _workspace_strategy({"workspace_strategy": workspace_strategy})
        stored = _workspace_strategy(run_context)
        if requested != stored:
            raise NodeExecutionFailure(
                f"workspace_strategy mismatch on resume: checkpoint {stored!r} != request {requested!r}"
            )
    if exec_worker_kind is not None:
        stored_worker = _exec_worker_kind(body)
        if exec_worker_kind != stored_worker:
            raise NodeExecutionFailure(
                f"exec_worker_kind mismatch on resume: checkpoint {stored_worker!r} != request {exec_worker_kind!r}"
            )
    if merge_policy is not None:
        stored_mp = str(
            (body.get("dev_process") or {}).get("merge_policy") or MERGE_POLICY_RECORD_ONLY
        )
        if merge_policy != stored_mp:
            raise NodeExecutionFailure(
                f"merge_policy mismatch on resume: checkpoint {stored_mp!r} != request {merge_policy!r}"
            )
    stored_argv = _stored_exec_argv(body)
    if exec_argv is not None and stored_argv is not None and exec_argv != stored_argv:
        raise NodeExecutionFailure(
            f"exec_argv mismatch on resume: checkpoint {stored_argv!r} != request {exec_argv!r}"
        )
    if exec_model is not None:
        requested_model = exec_model.strip()
        stored_model = _stored_exec_model(body)
        if requested_model != stored_model:
            raise NodeExecutionFailure(
                f"exec_model mismatch on resume: checkpoint {stored_model!r} != request {requested_model!r}"
            )
    stored_run_id = str(run_context.get("run_id") or "")
    if run_id and run_id != stored_run_id:
        raise NodeExecutionFailure(
            f"run_id mismatch: input {run_id!r} != checkpoint {stored_run_id!r}"
        )
    incoming_identity: Dict[str, Any] = {"run_id": stored_run_id}
    if repo_root.strip():
        incoming_identity["repo_root"] = abs_path(resolve_git_toplevel(Path(repo_root)))
    _assert_resume_identity(run_context, incoming_identity)
    return body


def _handle_start(
    *,
    repo_root: str,
    task_prompt: str,
    run_id: Optional[str],
    run_spec_on_start: bool,
    exec_argv: Optional[list[str]],
    exec_model: Optional[str],
    workspace_strategy: str,
    exec_worker_kind: str,
    merge_policy: str,
    interactive: bool,
    spec_inputs_provided: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    repo = resolve_git_toplevel(Path(repo_root))
    swc = check_source_workspace(repo)
    rid = validate_run_id(run_id) if run_id else new_run_id()
    artifact_root, _run_dir_name = allocate_run_dir(repo, task_prompt=task_prompt, run_id=rid)
    head = git_head_revision(repo)
    branch = git_current_branch(repo)
    planned = planned_branch_name_for_run(rid)
    strategy = _workspace_strategy({"workspace_strategy": workspace_strategy})
    policy = validate_merge_policy(merge_policy)
    assert_merge_policy_allowed_at_start(
        merge_policy=policy,
        workspace_strategy=strategy,
        source_current_branch=branch,
    )

    run_context = {
        "run_id": rid,
        "repo_root": abs_path(repo),
        "artifact_root": artifact_root,
        "workspace_root": abs_path(repo),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_base_revision": head,
        "source_current_branch": branch,
        "workspace_strategy": strategy,
        "planned_branch_name": planned,
    }
    body: Dict[str, Any] = {
        "schema_version": "dev_process.flow.v2",
        "run_context": run_context,
        "flow_result": {
            "state": STATE_INITIALIZED,
            "ok": True,
            "allowed_actions": [],
            "next_action": None,
            "merge_ready": False,
        },
        "dev_process": {
            "review_depth_preset": "standard",
            "exec_worker_kind": exec_worker_kind,
            "merge_policy": policy,
            "workspace_attempt": 1,
            "human_gates": {"spec": "pending", "final": "not_reached"},
        },
        "stages": _empty_stages(),
        "stale": {},
        "task_prompt": task_prompt,
        "source_workspace_check": swc,
    }
    timeline.append_event(
        artifact_root, rid, "flow_started", state=STATE_INITIALIZED, action=ACTION_START
    )
    snapshot = build_exec_policy_snapshot(
        exec_worker_kind=exec_worker_kind,
        exec_argv=exec_argv,
        exec_model=exec_model,
    )
    apply_snapshot_to_body(body, snapshot)
    out = _finalize(body=body, run_id=rid, action=ACTION_START, state=STATE_INITIALIZED)

    if not run_spec_on_start:
        return out

    body = load_flow_checkpoint(out["flow_checkpoint_path"])
    return _run_spec_cycle(
        body,
        run_id=rid,
        task_prompt=task_prompt,
        exec_argv=exec_argv,
        action=ACTION_START,
        interactive=interactive,
        spec_inputs_provided=spec_inputs_provided or {},
    )


def _run_spec_cycle(
    body: Dict[str, Any],
    *,
    run_id: str,
    task_prompt: str,
    exec_argv: Optional[list[str]],
    action: str,
    revision_context: Optional[str] = None,
    interactive: bool = False,
    spec_inputs_provided: Optional[Dict[str, Any]] = None,
    use_human_comment: bool = False,
    revision_provided: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    run_context = body["run_context"]
    repo = Path(run_context["repo_root"])
    worker_kind = _exec_worker_kind(body)
    notes = ""
    materials: list[dict[str, Any]] | None = None
    previous_spec: str | None = None

    if action in (ACTION_REVISE_SPEC, ACTION_REQUEST_SPEC_REVISION):
        _clear_git_worktree_on_revise(body)
        resolved_task_prompt = str(body.get("task_prompt") or task_prompt or "")
        if revision_context is None:
            revision_context = _revision_context_from_spec_review(body)
        notes, materials = load_stored_spec_inputs(run_context["artifact_root"], repo)
        previous_spec = _read_spec_text(run_context["artifact_root"])
        mark_stale(body, upstream="spec")
    else:
        sp_inputs, materials, sp_input_path, sp_ref_path = collect_spec_inputs(
            artifact_root=run_context["artifact_root"],
            repo_root=repo,
            provided=spec_inputs_provided or {},
            interactive=interactive,
        )
        resolved_task_prompt = str(sp_inputs.get("task_prompt") or task_prompt or "")
        body["task_prompt"] = resolved_task_prompt
        notes = str(sp_inputs.get("notes") or "")
        if sp_input_path is not None:
            body.setdefault("stages", {}).setdefault("spec", {})["input_artifact"] = str(
                sp_input_path
            )

    timeline.append_event(run_context["artifact_root"], run_id, "writing_spec", stage="spec")
    try:
        sp = run_spec_stage(
            repo_root=repo,
            artifact_root=run_context["artifact_root"],
            run_id=run_id,
            task_prompt=resolved_task_prompt,
            base_revision=run_context["source_base_revision"],
            exec_argv=_job_argv(body, "write_spec", exec_argv),
            revision_context=revision_context,
            notes=notes or None,
            reference_materials=materials,
            previous_spec=previous_spec,
            exec_worker_kind=worker_kind,
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(body=body, run_id=run_id, action=action, reason=str(e))
        raise
    body["stages"]["spec"] = sp
    clear_stage_stale(body, "spec")

    spec_text = _read_spec_text(run_context["artifact_root"])
    timeline.append_event(
        run_context["artifact_root"], run_id, "reviewing_spec", stage="spec_review"
    )
    try:
        sr = run_spec_review_stage(
            repo_root=repo,
            artifact_root=run_context["artifact_root"],
            run_id=run_id,
            task_prompt=resolved_task_prompt,
            spec_text=spec_text,
            exec_argv=_job_argv(body, "spec_review", exec_argv),
            exec_worker_kind=worker_kind,
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(body=body, run_id=run_id, action=action, reason=str(e))
        raise
    body["stages"]["spec_review"] = sr
    clear_stage_stale(body, "spec_review")

    gates = body.setdefault("dev_process", {}).setdefault("human_gates", {})
    gates["spec"] = "pending"
    gates["final"] = "not_reached"

    if sr.get("decision") == "fail":
        return _finalize(
            body=body,
            run_id=run_id,
            action=action,
            state=STATE_AWAITING_SPEC_REVISION,
            merge_ready=False,
        )
    return _finalize(
        body=body,
        run_id=run_id,
        action=action,
        state=STATE_AWAITING_SPEC_HUMAN_GATE,
        merge_ready=False,
    )


def _run_plan_cycle(
    body: Dict[str, Any],
    *,
    run_id: str,
    exec_argv: Optional[list[str]],
    action: str,
    revision_context: Optional[str] = None,
    interactive: bool = False,
    revision_provided: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    run_context = body["run_context"]
    repo = Path(run_context["repo_root"])
    worker_kind = _exec_worker_kind(body)
    task_prompt = str(body.get("task_prompt") or "")
    spec_text = _read_spec_text(run_context["artifact_root"])

    previous_plan: str | None = None
    if action == ACTION_REVISE_PLAN:
        if revision_context is None:
            revision_context = _revision_context_from_plan_review(body)
        previous_plan = _read_plan_text(run_context["artifact_root"])

    timeline.append_event(run_context["artifact_root"], run_id, "writing_plan", stage="plan")
    try:
        pl = run_plan_stage(
            repo_root=repo,
            artifact_root=run_context["artifact_root"],
            run_id=run_id,
            task_prompt=task_prompt,
            approved_spec=spec_text,
            exec_argv=_job_argv(body, "write_plan", exec_argv),
            revision_context=revision_context,
            previous_plan=previous_plan,
            exec_worker_kind=worker_kind,
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(body=body, run_id=run_id, action=action, reason=str(e))
        raise
    body["stages"]["plan"] = pl
    clear_stage_stale(body, "plan")
    mark_stale(body, upstream="plan")

    plan_text = _read_plan_text(run_context["artifact_root"])
    timeline.append_event(
        run_context["artifact_root"], run_id, "reviewing_plan", stage="plan_review"
    )
    try:
        pr = run_plan_review_stage(
            repo_root=repo,
            artifact_root=run_context["artifact_root"],
            run_id=run_id,
            task_prompt=task_prompt,
            spec_text=spec_text,
            plan_text=plan_text,
            exec_argv=_job_argv(body, "plan_review", exec_argv),
            exec_worker_kind=worker_kind,
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(body=body, run_id=run_id, action=action, reason=str(e))
        raise
    body["stages"]["plan_review"] = pr
    clear_stage_stale(body, "plan_review")

    if pr.get("decision") == "fail":
        return _finalize(
            body=body,
            run_id=run_id,
            action=action,
            state=STATE_AWAITING_PLAN_REVISION,
            merge_ready=False,
        )
    gates = body.setdefault("dev_process", {}).setdefault("human_gates", {})
    gates["spec"] = "approved"
    return _finalize(
        body=body,
        run_id=run_id,
        action=action,
        state=STATE_AWAITING_IMPLEMENTATION,
        merge_ready=False,
    )


def _handle_approve_spec(
    body: Dict[str, Any],
    *,
    run_id: str,
    exec_argv: Optional[list[str]],
) -> Dict[str, Any]:
    gates = body.setdefault("dev_process", {}).setdefault("human_gates", {})
    gates["spec"] = "approved"
    return _run_plan_cycle(body, run_id=run_id, exec_argv=exec_argv, action=ACTION_APPROVE_SPEC)


def _handle_revise_spec(
    body: Dict[str, Any],
    *,
    run_id: str,
    exec_argv: Optional[list[str]],
    interactive: bool,
    revision_provided: Dict[str, Any],
    use_human_comment: bool,
) -> Dict[str, Any]:
    run_context = body["run_context"]
    repo = Path(run_context["repo_root"])
    findings = _revision_context_from_spec_review(body)
    comment = str(revision_provided.get("revision_comment") or "")
    materials: list[dict[str, Any]] | None = None
    if use_human_comment or comment.strip() or revision_provided.get("reference_paths") or interactive:
        rev_inputs, materials, rev_input_path, rev_ref_path = collect_revision_inputs(
            artifact_root=run_context["artifact_root"],
            repo_root=repo,
            provided=revision_provided,
            interactive=interactive,
            require_comment=use_human_comment,
        )
        comment = str(rev_inputs.get("revision_comment") or comment)
        body.setdefault("stages", {}).setdefault("revision", {})["input_artifact"] = str(
            rev_input_path
        )
        if rev_ref_path is not None:
            body["stages"]["revision"]["reference_materials_artifact"] = str(rev_ref_path)
    human_context = format_revision_context(comment, materials or [])
    revision_context = _combine_revision_context(findings, human_context)
    return _run_spec_cycle(
        body,
        run_id=run_id,
        task_prompt=str(body.get("task_prompt") or ""),
        exec_argv=exec_argv,
        action=ACTION_REVISE_SPEC if not use_human_comment else ACTION_REQUEST_SPEC_REVISION,
        revision_context=revision_context,
        interactive=interactive,
        use_human_comment=use_human_comment,
    )


def _handle_revise_plan(
    body: Dict[str, Any],
    *,
    run_id: str,
    exec_argv: Optional[list[str]],
    interactive: bool,
    revision_provided: Dict[str, Any],
) -> Dict[str, Any]:
    run_context = body["run_context"]
    repo = Path(run_context["repo_root"])
    findings = _revision_context_from_plan_review(body)
    comment = str(revision_provided.get("revision_comment") or "")
    materials: list[dict[str, Any]] | None = None
    if comment.strip() or revision_provided.get("reference_paths") or interactive:
        rev_inputs, materials, rev_input_path, rev_ref_path = collect_revision_inputs(
            artifact_root=run_context["artifact_root"],
            repo_root=repo,
            provided=revision_provided,
            interactive=interactive,
            require_comment=False,
        )
        comment = str(rev_inputs.get("revision_comment") or comment)
        body.setdefault("stages", {}).setdefault("revision", {})["input_artifact"] = str(
            rev_input_path
        )
        if rev_ref_path is not None:
            body["stages"]["revision"]["reference_materials_artifact"] = str(rev_ref_path)
    human_context = format_revision_context(comment, materials or [])
    revision_context = _combine_revision_context(findings, human_context)
    return _run_plan_cycle(
        body,
        run_id=run_id,
        exec_argv=exec_argv,
        action=ACTION_REVISE_PLAN,
        revision_context=revision_context,
    )


def _handle_continue_implementation(
    body: Dict[str, Any],
    *,
    run_id: str,
    exec_argv: Optional[list[str]],
    force_review_blocking: bool,
    rework_context: Optional[str] = None,
) -> Dict[str, Any]:
    run_context = body["run_context"]
    strategy = _workspace_strategy(run_context)
    existing_workspace = body.get("workspace_context")
    workspace_context = prepare_workspace(
        source_repo_root=run_context["repo_root"],
        run_context=_run_context_for_prepare_workspace(body),
        strategy=strategy,
        existing_workspace=existing_workspace if isinstance(existing_workspace, dict) else None,
    )
    body["workspace_context"] = workspace_context
    repo = _workspace_repo_root(body)
    spec_text = _read_spec_text(run_context["artifact_root"])
    plan_text = _read_plan_text(run_context["artifact_root"])
    task_prompt = str(body.get("task_prompt") or "")

    impl_argv = _job_argv(body, "write_implementation", exec_argv)
    tests_argv = _job_argv(body, "write_tests", exec_argv)
    timeline.append_event(
        run_context["artifact_root"], run_id, "implementing", stage="implementation"
    )
    try:
        impl = run_implementation_stage(
            repo_root=repo,
            artifact_root=run_context["artifact_root"],
            run_id=run_id,
            task_prompt=task_prompt,
            base_revision=workspace_context.get("base_revision")
            or run_context["source_base_revision"],
            approved_spec=spec_text,
            approved_plan=plan_text,
            exec_argv=impl_argv,
            rework_context=rework_context,
            exec_worker_kind=_exec_worker_kind(body),
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(
            body=body, run_id=run_id, action=ACTION_CONTINUE_IMPLEMENTATION, reason=str(e)
        )
        raise
    body["stages"]["implementation"] = impl
    clear_stage_stale(body, "implementation")
    mark_stale(body, upstream="implementation")

    timeline.append_event(
        run_context["artifact_root"], run_id, "writing_tests", stage="test_implementation"
    )
    try:
        test_impl = run_test_implementation_stage(
            repo_root=repo,
            artifact_root=run_context["artifact_root"],
            run_id=run_id,
            approved_spec=spec_text,
            approved_plan=plan_text,
            exec_argv=tests_argv,
            exec_worker_kind=_exec_worker_kind(body),
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(
            body=body, run_id=run_id, action=ACTION_CONTINUE_IMPLEMENTATION, reason=str(e)
        )
        raise
    body["stages"]["test_implementation"] = test_impl
    clear_stage_stale(body, "test_implementation")
    mark_stale(body, upstream="test_implementation")

    timeline.append_event(run_context["artifact_root"], run_id, "running_tests", stage="run_tests")
    try:
        run_tests_st = run_run_tests_stage(
            repo_root=repo,
            artifact_root=run_context["artifact_root"],
            run_id=run_id,
            diff_result=impl.get("diff_result") or {},
            execution_output=impl.get("execution_output") or {},
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(
            body=body, run_id=run_id, action=ACTION_CONTINUE_IMPLEMENTATION, reason=str(e)
        )
        raise
    body["stages"]["run_tests"] = run_tests_st
    clear_stage_stale(body, "run_tests")
    run_tests_ok = run_tests_st.get("status") == "completed"
    impl_bundle = {
        "status": run_tests_st.get("status", "completed"),
        "test_result": run_tests_st.get("test_result"),
        "diff_result": impl.get("diff_result"),
        "evidence_paths": list(impl.get("evidence_paths") or [])
        + list(test_impl.get("evidence_paths") or []),
    }

    timeline.append_event(run_context["artifact_root"], run_id, "reviewing_changes", stage="review")
    preset = str((body.get("dev_process") or {}).get("review_depth_preset") or "standard")
    try:
        rev = run_review_stage(
            repo_root=repo,
            artifact_root=run_context["artifact_root"],
            run_id=run_id,
            base_revision=workspace_context.get("base_revision")
            or run_context["source_base_revision"],
            approved_spec=spec_text,
            approved_plan=plan_text,
            diff_result=impl_bundle.get("diff_result") or {},
            test_result=impl_bundle.get("test_result") or {},
            exec_argv=exec_argv,
            force_blocking=force_review_blocking,
            review_depth_preset=preset,
            exec_worker_kind=_exec_worker_kind(body),
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(
            body=body, run_id=run_id, action=ACTION_CONTINUE_IMPLEMENTATION, reason=str(e)
        )
        raise

    review_result = rev.get("review_result") or {}
    blocking = list(review_result.get("blocking_findings") or [])
    blocking = assign_owners_to_findings(blocking)
    if isinstance(review_result, dict):
        review_result = dict(review_result)
        review_result["blocking_findings"] = blocking
        rev["review_result"] = review_result

    branch_name, branch_head = record_reviewed_branch_snapshot(body)
    rev["reviewed_branch_name"] = branch_name
    rev["reviewed_branch_head"] = branch_head
    body["stages"]["review"] = rev
    clear_stage_stale(body, "review")

    merge_ready = bool(rev.get("merge_ready")) and run_tests_ok
    gates = body.setdefault("dev_process", {}).setdefault("human_gates", {})

    if blocking:
        body["rework_owner"] = route_owner_to_state(blocking)
        gates["final"] = "not_reached"
        return _finalize(
            body=body,
            run_id=run_id,
            action=ACTION_CONTINUE_IMPLEMENTATION,
            state=STATE_AWAITING_REWORK_DECISION,
            merge_ready=False,
        )
    if not run_tests_ok:
        body["rework_owner"] = "test"
        gates["final"] = "not_reached"
        return _finalize(
            body=body,
            run_id=run_id,
            action=ACTION_CONTINUE_IMPLEMENTATION,
            state=STATE_AWAITING_REWORK_DECISION,
            merge_ready=False,
        )
    if merge_ready:
        gates["final"] = "pending"
        return _finalize(
            body=body,
            run_id=run_id,
            action=ACTION_CONTINUE_IMPLEMENTATION,
            state=STATE_AWAITING_FINAL,
            merge_ready=True,
        )
    gates["final"] = "not_reached"
    return _finalize(
        body=body,
        run_id=run_id,
        action=ACTION_CONTINUE_IMPLEMENTATION,
        state=STATE_AWAITING_REWORK_DECISION,
        merge_ready=False,
    )


def _handle_rework(
    body: Dict[str, Any],
    *,
    run_id: str,
    exec_argv: Optional[list[str]],
    force_review_blocking: bool,
    interactive: bool,
    rework_provided: Dict[str, Any],
) -> Dict[str, Any]:
    workspace_context = body.get("workspace_context")
    if not isinstance(workspace_context, dict):
        raise NodeExecutionFailure(
            "rework_implementation requires workspace_context from prior continue_implementation"
        )
    run_context = body["run_context"]
    review_st = body.get("stages", {}).get("review")
    rw_inputs, rw_input_path = collect_rework_inputs(
        artifact_root=run_context["artifact_root"],
        provided=rework_provided,
        interactive=interactive,
    )
    rework_context = build_rework_context(
        str(rw_inputs.get("rework_comment") or ""),
        review_st if isinstance(review_st, dict) else None,
    )
    body.setdefault("stages", {}).setdefault("rework", {})["input_artifact"] = str(rw_input_path)

    owner = str(body.get("rework_owner") or "implementation")
    if owner == "spec":
        mark_stale(body, upstream="spec")
        return _handle_revise_spec(
            body,
            run_id=run_id,
            exec_argv=exec_argv,
            interactive=interactive,
            revision_provided={"revision_comment": rework_context},
            use_human_comment=True,
        )
    if owner == "plan":
        mark_stale(body, upstream="plan")
        return _handle_revise_plan(
            body,
            run_id=run_id,
            exec_argv=exec_argv,
            interactive=interactive,
            revision_provided={"revision_comment": rework_context},
        )

    return _handle_continue_implementation(
        body,
        run_id=run_id,
        exec_argv=exec_argv,
        force_review_blocking=force_review_blocking,
        rework_context=rework_context,
    )


def _handle_reject(
    body: Dict[str, Any],
    *,
    run_id: str,
    action: str,
    human_comment_text: str,
) -> Dict[str, Any]:
    run_context = body["run_context"]
    gates = body.setdefault("dev_process", {}).setdefault("human_gates", {})
    if action == ACTION_REJECT_SPEC:
        gates["spec"] = "rejected"
    else:
        gates["final"] = "rejected"
    reason = human_comment_text.strip() or action
    fr = body.setdefault("flow_result", {})
    fr["state"] = STATE_FAILED
    fr["ok"] = False
    fr["allowed_actions"] = []
    fr["next_action"] = None
    fr["merge_ready"] = False
    path, doc = write_flow_checkpoint(
        artifact_root=run_context["artifact_root"],
        run_id=run_id,
        action=action,
        body=body,
    )
    timeline.append_event(
        run_context["artifact_root"],
        run_id,
        "flow_failed",
        state=STATE_FAILED,
        reason=reason,
        path=path,
    )
    return {
        "flow_checkpoint_path": path,
        "flow_result": doc["flow_result"],
        "run_context": doc["run_context"],
    }
