"""dev-process flow orchestration."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process import timeline
from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint, write_flow_checkpoint
from nodeflow.workflows.dev_process.constants import (
    ACTION_APPROVE_FINAL,
    ACTION_APPROVE_SPEC,
    ACTION_MERGE,
    ACTION_REJECT_FINAL,
    ACTION_REJECT_SPEC,
    ACTION_REVISE_SPEC,
    ACTION_REWORK,
    ACTION_START,
    EXEC_WORKER_CODEX,
    MERGE_POLICY_RECORD_ONLY,
    STATE_AWAITING_FINAL,
    STATE_AWAITING_REVIEW,
    STATE_AWAITING_SPEC,
    STATE_FAILED,
    STATE_INITIALIZED,
    STATE_MERGED,
    WORKSPACE_STRATEGY_CURRENT_REPO,
    WORKSPACE_STRATEGY_GIT_WORKTREE,
)
from nodeflow.workflows.dev_process.evidence import assert_expected_stage_evidence
from nodeflow.workflows.dev_process.merge import (
    assert_merge_policy_allowed_at_start,
    execute_merge_policy,
    record_reviewed_branch_snapshot,
    resolve_merge_policy,
    validate_merge_policy,
)
from nodeflow.workflows.dev_process.paths import (
    abs_path,
    allocate_run_dir,
    git_current_branch,
    git_head_revision,
    new_run_id,
    planned_branch_name_for_attempt,
    planned_branch_name_for_run,
    resolve_git_toplevel,
    validate_run_id,
    workspace_attempt_subdir,
)
from nodeflow.workflows.dev_process.reuse import (
    check_source_workspace,
    prepare_workspace,
    remove_git_worktree,
    run_context_for_df,
    write_development_summary,
)
from nodeflow.workflows.dev_process.stage_inputs import (
    build_rework_context,
    collect_revision_inputs,
    collect_rework_inputs,
    collect_spec_plan_inputs,
    format_revision_context,
)
from nodeflow.workflows.dev_process.stages import (
    run_implement_stage,
    run_review_stage,
    run_spec_plan_stage,
)
from nodeflow.workflows.dev_process.state_machine import (
    allowed_actions_for_state,
    assert_action_allowed,
    next_action_for_state,
)


def _workspace_attempt(body: Dict[str, Any]) -> int:
    dp = body.setdefault("dev_process", {})
    attempt = dp.get("workspace_attempt")
    if not isinstance(attempt, int) or attempt < 1:
        dp["workspace_attempt"] = 1
        return 1
    return attempt


def _increment_workspace_attempt(body: Dict[str, Any]) -> int:
    dp = body.setdefault("dev_process", {})
    n = _workspace_attempt(body) + 1
    dp["workspace_attempt"] = n
    return n


def _run_context_for_prepare_workspace(body: Dict[str, Any]) -> Dict[str, Any]:
    run_context = body["run_context"]
    attempt = _workspace_attempt(body)
    run_id = str(run_context.get("run_id") or "")
    branch = planned_branch_name_for_attempt(run_id, attempt)
    return run_context_for_df(run_context) | {
        "artifact_root": run_context["artifact_root"],
        "workspace_attempt": attempt,
        "worktree_subdirectory": workspace_attempt_subdir(attempt),
        "planned_branch_name": branch,
    }


def _clear_git_worktree_on_revise(body: Dict[str, Any]) -> None:
    run_context = body["run_context"]
    if _workspace_strategy(run_context) != WORKSPACE_STRATEGY_GIT_WORKTREE:
        body.pop("workspace_context", None)
        return
    wc = body.get("workspace_context")
    if isinstance(wc, dict):
        root = wc.get("workspace_root")
        if isinstance(root, str) and root.strip():
            remove_git_worktree(
                source_repo_root=run_context["repo_root"],
                artifact_root=run_context["artifact_root"],
                workspace_root=root,
            )
    body.pop("workspace_context", None)
    _increment_workspace_attempt(body)


def _stored_exec_argv(body: Dict[str, Any]) -> Optional[list[str]]:
    dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
    raw = dp.get("exec_argv")
    if raw is None:
        return None
    if not isinstance(raw, list) or not all(isinstance(x, str) for x in raw):
        return None
    return raw


def _resolve_exec_argv(
    exec_argv: Optional[list[str]],
    codex_argv: Optional[list[str]],
    *,
    body: Optional[Dict[str, Any]] = None,
) -> Optional[list[str]]:
    if exec_argv is not None:
        return exec_argv
    if codex_argv is not None:
        return codex_argv
    if body is not None:
        return _stored_exec_argv(body)
    return None


def _exec_worker_kind(body: Dict[str, Any]) -> str:
    dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
    return str(dp.get("exec_worker_kind") or "codex")


def _workspace_strategy(run_context: Dict[str, Any]) -> str:
    strategy = str(run_context.get("workspace_strategy") or WORKSPACE_STRATEGY_CURRENT_REPO).strip()
    if strategy not in (WORKSPACE_STRATEGY_CURRENT_REPO, WORKSPACE_STRATEGY_GIT_WORKTREE):
        raise NodeExecutionFailure(f"unsupported workspace_strategy: {strategy!r}")
    return strategy


def _workspace_repo_root(body: Dict[str, Any]) -> Path:
    wc = body.get("workspace_context")
    if isinstance(wc, dict):
        root = wc.get("workspace_root")
        if isinstance(root, str) and root.strip():
            return Path(root).resolve()
    return Path(body["run_context"]["repo_root"]).resolve()


def _empty_stages() -> Dict[str, Any]:
    return {
        "spec_plan": {"status": "pending"},
        "implement": {"status": "pending"},
        "review": {"status": "pending"},
    }


def _read_approved_text(artifact_root: str) -> Tuple[str, str]:
    spec_p = Path(artifact_root) / "spec_plan" / "spec.md"
    plan_p = Path(artifact_root) / "spec_plan" / "plan.md"
    return spec_p.read_text(encoding="utf-8"), plan_p.read_text(encoding="utf-8")


def _fail_checkpoint(
    *,
    body: Dict[str, Any],
    run_id: str,
    action: str,
    reason: str,
) -> None:
    fr = body.setdefault("flow_result", {})
    fr["state"] = STATE_FAILED
    fr["ok"] = False
    fr["allowed_actions"] = []
    fr["next_action"] = None
    fr["merge_ready"] = False
    artifact_root = body["run_context"]["artifact_root"]
    path, _ = write_flow_checkpoint(
        artifact_root=artifact_root, run_id=run_id, action=action, body=body
    )
    timeline.append_event(
        artifact_root, run_id, "flow_failed", state=STATE_FAILED, reason=reason, path=path
    )


def _resume_identity_fields(run_context: Dict[str, Any]) -> Dict[str, Any]:
    return {
        k: run_context.get(k)
        for k in (
            "run_id",
            "repo_root",
            "artifact_root",
            "workspace_root",
            "workspace_strategy",
            "planned_branch_name",
            "source_base_revision",
        )
    }


def _assert_resume_identity(stored: Dict[str, Any], incoming: Dict[str, Any]) -> None:
    """Compare only identity fields explicitly supplied on resume (repo_root, run_id)."""
    for key in incoming:
        if key not in _resume_identity_fields(stored):
            continue
        val = _resume_identity_fields(stored)[key]
        if incoming.get(key) != val:
            raise NodeExecutionFailure(
                f"resume identity mismatch on {key!r}: checkpoint {val!r} != request {incoming.get(key)!r}"
            )


def _review_spec_revision_needed(body: Dict[str, Any]) -> bool:
    rev = (body.get("stages") or {}).get("review") or {}
    if not isinstance(rev, dict):
        return False
    rr = rev.get("review_result") or {}
    agg = rev.get("aggregate") or {}
    if isinstance(rr, dict) and rr.get("spec_revision_needed"):
        return True
    if isinstance(agg, dict) and agg.get("spec_revision_needed"):
        return True
    return False


def _finalize(
    *,
    body: Dict[str, Any],
    run_id: str,
    action: str,
    state: str,
    merge_ready: bool = False,
) -> Dict[str, Any]:
    fr = body.setdefault("flow_result", {})
    fr["state"] = state
    fr["ok"] = True
    spec_rev = _review_spec_revision_needed(body) if state == STATE_AWAITING_REVIEW else False
    actions = allowed_actions_for_state(
        state,
        merge_ready=merge_ready if state == STATE_AWAITING_REVIEW else None,
        spec_revision_needed=spec_rev,
    )
    fr["allowed_actions"] = actions
    fr["next_action"] = next_action_for_state(actions)
    fr["merge_ready"] = merge_ready
    artifact_root = body["run_context"]["artifact_root"]
    path, doc = write_flow_checkpoint(
        artifact_root=artifact_root, run_id=run_id, action=action, body=body
    )
    timeline.append_event(
        artifact_root,
        run_id,
        "checkpoint_written",
        state=state,
        path=path,
        action=action,
    )
    out: Dict[str, Any] = {
        "flow_checkpoint_path": path,
        "flow_result": doc["flow_result"],
        "run_context": doc["run_context"],
    }
    wc = doc.get("workspace_context")
    if isinstance(wc, dict) and wc:
        out["workspace_context"] = wc
    if isinstance(doc.get("development_summary"), dict) and doc["development_summary"]:
        out["development_summary"] = doc["development_summary"]
    if isinstance(doc.get("merge_result"), dict) and doc["merge_result"]:
        out["merge_result"] = doc["merge_result"]
    return out


def run_flow(
    *,
    action: str,
    repo_root: str,
    task_prompt: str = "",
    flow_checkpoint_path: Optional[str] = None,
    run_id: Optional[str] = None,
    run_spec_plan_on_start: bool = True,
    human_comment_text: str = "",
    codex_argv: Optional[list[str]] = None,
    exec_argv: Optional[list[str]] = None,
    force_review_blocking: bool = False,
    workspace_strategy: Optional[str] = None,
    exec_worker_kind: Optional[str] = None,
    merge_policy: Optional[str] = None,
    interactive: bool = False,
    spec_plan_provided: Optional[Dict[str, Any]] = None,
    revision_provided: Optional[Dict[str, Any]] = None,
    rework_provided: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if action == ACTION_START and flow_checkpoint_path:
        raise NodeExecutionFailure("start does not accept flow_checkpoint_path")

    if action == ACTION_START:
        argv = _resolve_exec_argv(exec_argv, codex_argv)
        provided = dict(spec_plan_provided or {})
        if task_prompt.strip() and not provided.get("task_prompt"):
            provided["task_prompt"] = task_prompt.strip()
        return _handle_start(
            repo_root=repo_root,
            task_prompt=task_prompt,
            run_id=run_id,
            run_spec_plan_on_start=run_spec_plan_on_start,
            exec_argv=argv,
            workspace_strategy=workspace_strategy or WORKSPACE_STRATEGY_CURRENT_REPO,
            exec_worker_kind=exec_worker_kind or EXEC_WORKER_CODEX,
            merge_policy=merge_policy or MERGE_POLICY_RECORD_ONLY,
            interactive=interactive,
            spec_plan_provided=provided,
        )

    if not flow_checkpoint_path:
        raise NodeExecutionFailure(f"action {action!r} requires flow_checkpoint_path")

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
    requested_argv = exec_argv if exec_argv is not None else codex_argv
    if requested_argv is not None and stored_argv is not None and requested_argv != stored_argv:
        raise NodeExecutionFailure(
            f"exec_argv mismatch on resume: checkpoint {stored_argv!r} != request {requested_argv!r}"
        )
    argv = _resolve_exec_argv(exec_argv, codex_argv, body=body)
    stored_run_id = str(run_context.get("run_id") or "")
    if run_id and run_id != stored_run_id:
        raise NodeExecutionFailure(
            f"run_id mismatch: input {run_id!r} != checkpoint {stored_run_id!r}"
        )
    run_id = stored_run_id
    incoming_identity: Dict[str, Any] = {"run_id": run_id}
    if repo_root.strip():
        incoming_identity["repo_root"] = abs_path(resolve_git_toplevel(Path(repo_root)))
    _assert_resume_identity(run_context, incoming_identity)
    state = str((body.get("flow_result") or {}).get("state") or "")
    assert_action_allowed(state, action)
    timeline.append_event(
        run_context["artifact_root"], run_id, "action_received", action=action, state=state
    )

    if action == ACTION_APPROVE_SPEC:
        return _handle_approve_spec(
            body, run_id=run_id, exec_argv=argv, force_review_blocking=force_review_blocking
        )
    if action == ACTION_REVISE_SPEC:
        rev_provided = dict(revision_provided or {})
        if task_prompt.strip() and not rev_provided.get("revision_comment"):
            rev_provided["revision_comment"] = task_prompt.strip()
        if not rev_provided.get("revision_comment") and not interactive:
            rev_provided.setdefault("revision_comment", "revise spec")
        return _handle_revise_spec(
            body,
            run_id=run_id,
            exec_argv=argv,
            interactive=interactive,
            revision_provided=rev_provided,
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


def _handle_start(
    *,
    repo_root: str,
    task_prompt: str,
    run_id: Optional[str],
    run_spec_plan_on_start: bool,
    exec_argv: Optional[list[str]],
    workspace_strategy: str,
    exec_worker_kind: str,
    merge_policy: str,
    interactive: bool = False,
    spec_plan_provided: Optional[Dict[str, Any]] = None,
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
            **({"exec_argv": exec_argv} if exec_argv else {}),
        },
        "stages": _empty_stages(),
        "task_prompt": task_prompt,
        "source_workspace_check": swc,
    }
    timeline.append_event(
        artifact_root, rid, "flow_started", state=STATE_INITIALIZED, action=ACTION_START
    )
    out = _finalize(body=body, run_id=rid, action=ACTION_START, state=STATE_INITIALIZED)

    if not run_spec_plan_on_start:
        return out

    body = load_flow_checkpoint(out["flow_checkpoint_path"])
    return _run_spec_plan_and_finalize(
        body,
        run_id=rid,
        task_prompt=task_prompt,
        exec_argv=exec_argv,
        action=ACTION_START,
        interactive=interactive,
        spec_plan_provided=spec_plan_provided or {},
    )


def _attach_spec_plan_input_artifacts(
    stage_result: Dict[str, Any],
    *,
    input_artifact: Path,
    reference_materials_artifact: Optional[Path],
) -> None:
    stage_result["input_artifact"] = str(input_artifact)
    if reference_materials_artifact is not None:
        stage_result["reference_materials_artifact"] = str(reference_materials_artifact)


def _run_spec_plan_and_finalize(
    body: Dict[str, Any],
    *,
    run_id: str,
    task_prompt: str,
    exec_argv: Optional[list[str]],
    action: str,
    revision_context: Optional[str] = None,
    interactive: bool = False,
    spec_plan_provided: Optional[Dict[str, Any]] = None,
    revision_provided: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    run_context = body["run_context"]
    if action == ACTION_REVISE_SPEC:
        _clear_git_worktree_on_revise(body)
    repo = Path(run_context["repo_root"])
    worker_kind = _exec_worker_kind(body)

    notes = ""
    sp_materials: list[dict[str, Any]] | None = None
    sp_input_path: Path | None = None
    sp_ref_path: Path | None = None

    if action == ACTION_REVISE_SPEC:
        rev_inputs, rev_materials, rev_input_path, rev_ref_path = collect_revision_inputs(
            artifact_root=run_context["artifact_root"],
            repo_root=repo,
            provided=revision_provided or {},
            interactive=interactive,
        )
        revision_context = format_revision_context(
            str(rev_inputs.get("revision_comment") or ""),
            rev_materials,
        )
        body.setdefault("stages", {}).setdefault("revision", {})["input_artifact"] = str(
            rev_input_path
        )
        if rev_ref_path is not None:
            body["stages"]["revision"]["reference_materials_artifact"] = str(rev_ref_path)
        resolved_task_prompt = str(body.get("task_prompt") or task_prompt or "")
    else:
        sp_inputs, sp_materials, sp_input_path, sp_ref_path = collect_spec_plan_inputs(
            artifact_root=run_context["artifact_root"],
            repo_root=repo,
            provided=spec_plan_provided or {},
            interactive=interactive,
        )
        resolved_task_prompt = str(sp_inputs.get("task_prompt") or "")
        body["task_prompt"] = resolved_task_prompt
        notes = str(sp_inputs.get("notes") or "")

    timeline.append_event(run_context["artifact_root"], run_id, "stage_started", stage="spec_plan")
    try:
        sp = run_spec_plan_stage(
            repo_root=repo,
            artifact_root=run_context["artifact_root"],
            run_id=run_id,
            task_prompt=resolved_task_prompt,
            base_revision=run_context["source_base_revision"],
            exec_argv=exec_argv,
            revision_context=revision_context,
            notes=notes or None,
            reference_materials=sp_materials,
            exec_worker_kind=worker_kind,
        )
        if sp_input_path is not None:
            _attach_spec_plan_input_artifacts(
                sp,
                input_artifact=sp_input_path,
                reference_materials_artifact=sp_ref_path,
            )
    except NodeExecutionFailure as e:
        _fail_checkpoint(body=body, run_id=run_id, action=action, reason=str(e))
        raise
    body["stages"]["spec_plan"] = sp
    body["stages"]["implement"] = {"status": "pending"}
    body["stages"]["review"] = {"status": "pending"}
    timeline.append_event(
        run_context["artifact_root"],
        run_id,
        "stage_completed",
        stage="spec_plan",
        ok=True,
    )
    gates = body.setdefault("dev_process", {}).setdefault("human_gates", {})
    gates["spec"] = "pending"
    gates["final"] = "not_reached"
    return _finalize(
        body=body,
        run_id=run_id,
        action=action,
        state=STATE_AWAITING_SPEC,
        merge_ready=False,
    )


def _handle_approve_spec(
    body: Dict[str, Any],
    *,
    run_id: str,
    exec_argv: Optional[list[str]],
    force_review_blocking: bool,
    action: str = ACTION_APPROVE_SPEC,
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
    gates = body.setdefault("dev_process", {}).setdefault("human_gates", {})
    gates["spec"] = "approved"
    gates["final"] = "not_reached"
    spec_text, plan_text = _read_approved_text(run_context["artifact_root"])
    task_prompt = str(body.get("task_prompt") or "")

    timeline.append_event(run_context["artifact_root"], run_id, "stage_started", stage="implement")
    try:
        impl = run_implement_stage(
            repo_root=repo,
            artifact_root=run_context["artifact_root"],
            run_id=run_id,
            task_prompt=task_prompt,
            base_revision=workspace_context.get("base_revision")
            or run_context["source_base_revision"],
            approved_spec=spec_text,
            approved_plan=plan_text,
            exec_argv=exec_argv,
            exec_worker_kind=_exec_worker_kind(body),
            rework_context=rework_context,
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(body=body, run_id=run_id, action=action, reason=str(e))
        raise
    body["stages"]["implement"] = impl
    timeline.append_event(
        run_context["artifact_root"],
        run_id,
        "stage_completed",
        stage="implement",
        ok=impl.get("status") == "completed",
    )

    timeline.append_event(run_context["artifact_root"], run_id, "stage_started", stage="review")
    try:
        preset = str((body.get("dev_process") or {}).get("review_depth_preset") or "standard")
        rev = run_review_stage(
            repo_root=repo,
            artifact_root=run_context["artifact_root"],
            run_id=run_id,
            base_revision=workspace_context.get("base_revision")
            or run_context["source_base_revision"],
            approved_spec=spec_text,
            approved_plan=plan_text,
            diff_result=impl.get("diff_result") or {},
            test_result=impl.get("test_result") or {},
            exec_argv=exec_argv,
            force_blocking=force_review_blocking,
            review_depth_preset=preset,
            exec_worker_kind=_exec_worker_kind(body),
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(body=body, run_id=run_id, action=action, reason=str(e))
        raise
    branch_name, branch_head = record_reviewed_branch_snapshot(body)
    rev["reviewed_branch_name"] = branch_name
    rev["reviewed_branch_head"] = branch_head
    body["stages"]["review"] = rev
    timeline.append_event(
        run_context["artifact_root"],
        run_id,
        "stage_completed",
        stage="review",
        ok=True,
    )
    merge_ready = bool(rev.get("merge_ready"))
    if merge_ready:
        gates["final"] = "pending"
    else:
        gates["final"] = "not_reached"
    return _finalize(
        body=body,
        run_id=run_id,
        action=action,
        state=STATE_AWAITING_REVIEW,
        merge_ready=merge_ready,
    )


def _handle_revise_spec(
    body: Dict[str, Any],
    *,
    run_id: str,
    exec_argv: Optional[list[str]],
    interactive: bool = False,
    revision_provided: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return _run_spec_plan_and_finalize(
        body,
        run_id=run_id,
        task_prompt=str(body.get("task_prompt") or ""),
        exec_argv=exec_argv,
        action=ACTION_REVISE_SPEC,
        interactive=interactive,
        revision_provided=revision_provided or {},
    )


def _handle_rework(
    body: Dict[str, Any],
    *,
    run_id: str,
    exec_argv: Optional[list[str]],
    force_review_blocking: bool,
    interactive: bool = False,
    rework_provided: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    workspace_context = body.get("workspace_context")
    if not isinstance(workspace_context, dict):
        raise NodeExecutionFailure(
            "rework_implementation requires workspace_context from prior approve_spec"
        )
    run_context = body["run_context"]
    review_st = body.get("stages", {}).get("review")
    rw_inputs, rw_input_path = collect_rework_inputs(
        artifact_root=run_context["artifact_root"],
        provided=rework_provided or {},
        interactive=interactive,
    )
    rework_context = build_rework_context(
        str(rw_inputs.get("rework_comment") or ""),
        review_st if isinstance(review_st, dict) else None,
    )
    body_for_approve = dict(body)
    if isinstance(review_st, dict):
        review_st_copy = dict(review_st)
        review_st_copy["stale"] = True
        body_for_approve.setdefault("stages", {})["review"] = review_st_copy
    body_for_approve.setdefault("stages", {}).setdefault("rework", {})["input_artifact"] = str(
        rw_input_path
    )
    return _handle_approve_spec(
        body_for_approve,
        run_id=run_id,
        exec_argv=exec_argv,
        force_review_blocking=force_review_blocking,
        action=ACTION_REWORK,
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


def _merge_gate_ok(body: Dict[str, Any]) -> None:
    fr = body.get("flow_result") or {}
    stages = body.get("stages") or {}
    if fr.get("state") != STATE_AWAITING_FINAL:
        raise NodeExecutionFailure(f"merge requires state {STATE_AWAITING_FINAL!r}")
    if not fr.get("merge_ready"):
        raise NodeExecutionFailure("merge_ready is false")
    for name in ("spec_plan", "implement", "review"):
        st = stages.get(name) or {}
        if st.get("status") != "completed":
            raise NodeExecutionFailure(f"stages.{name}.status must be completed")
    agg = (stages.get("review") or {}).get("aggregate") or {}
    if agg.get("blocking_count", 1) != 0:
        raise NodeExecutionFailure("review has blocking findings")
    review_st = stages.get("review") or {}
    if review_st.get("stale"):
        raise NodeExecutionFailure("stages.review is stale; rework or re-run review before merge")


def _write_fallback_development_summary(
    body: Dict[str, Any],
    *,
    action: str,
    reason: str,
) -> Dict[str, Any]:
    run_context = body["run_context"]
    artifact_root = Path(run_context["artifact_root"])
    path = artifact_root / "summary" / f"{action}_development_summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    summary: Dict[str, Any] = {
        "status": "fallback",
        "reason": reason,
        "artifact_path": str(path.resolve()),
        "merge_result": body.get("merge_result"),
    }
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return summary


def _handle_merge(body: Dict[str, Any], *, run_id: str) -> Dict[str, Any]:
    run_context = body["run_context"]
    assert_expected_stage_evidence(body, run_id=run_id)
    _merge_gate_ok(body)
    policy = resolve_merge_policy(body)
    artifact_root = run_context["artifact_root"]
    timeline.append_event(
        artifact_root,
        run_id,
        "merge_attempted",
        merge_ready=True,
        merge_policy=policy,
    )
    try:
        merge_result = execute_merge_policy(body)
        body["merge_result"] = merge_result
    except NodeExecutionFailure as e:
        _fail_checkpoint(body=body, run_id=run_id, action=ACTION_MERGE, reason=str(e))
        raise
    try:
        summary = write_development_summary(
            body=body,
            action=ACTION_MERGE,
            merge_ready=True,
        )
        body["development_summary"] = summary
    except NodeExecutionFailure as e:
        timeline.append_event(
            artifact_root,
            run_id,
            "summary_failed",
            reason=str(e),
            merge_policy=policy,
        )
        summary = _write_fallback_development_summary(
            body,
            action=ACTION_MERGE,
            reason=str(e),
        )
        body["development_summary"] = summary
    return _finalize(
        body=body,
        run_id=run_id,
        action=ACTION_MERGE,
        state=STATE_MERGED,
        merge_ready=True,
    )


def _handle_approve_final(body: Dict[str, Any], *, run_id: str) -> Dict[str, Any]:
    assert_expected_stage_evidence(body, run_id=run_id)
    fr = body.get("flow_result") or {}
    if fr.get("state") != STATE_AWAITING_REVIEW:
        raise NodeExecutionFailure(
            f"approve_final requires state {STATE_AWAITING_REVIEW!r}, got {fr.get('state')!r}"
        )
    if not fr.get("merge_ready"):
        raise NodeExecutionFailure("approve_final requires merge_ready=true")
    body.setdefault("dev_process", {}).setdefault("human_gates", {})["final"] = "approved"
    return _finalize(
        body=body,
        run_id=run_id,
        action=ACTION_APPROVE_FINAL,
        state=STATE_AWAITING_FINAL,
        merge_ready=True,
    )
