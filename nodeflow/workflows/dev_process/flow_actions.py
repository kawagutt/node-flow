"""dev-process flow orchestration (v2 actions and run_flow)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import click as _click

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process import timeline
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
    STATE_AWAITING_MERGE,
    STATE_AWAITING_PLAN_REVISION,
    STATE_AWAITING_REWORK_DECISION,
    STATE_AWAITING_SPEC_HUMAN_GATE,
    STATE_AWAITING_SPEC_REVISION,
    STATE_FAILED,
    STATE_INITIALIZED,
    TERMINAL_STATES,
    WORKSPACE_STRATEGY_CURRENT_REPO,
)
from nodeflow.workflows.dev_process.constraints import (
    generate_constraints_audit,
)
from nodeflow.workflows.dev_process.exec_policy import (
    apply_snapshot_to_body,
    build_exec_policy_snapshot,
    load_exec_policy_file,
)
from nodeflow.workflows.dev_process.flow_context import (
    _assert_resume_identity,
    _clear_git_worktree_on_revise,
    _combine_revision_context,
    _empty_stages,
    _exec_worker_kind,
    _fail_checkpoint,
    _finalize,
    _phase_repo_root,
    _read_plan_text,
    _read_spec_text,
    _resolve_exec_argv,
    _revision_context_from_plan_review,
    _revision_context_from_spec_review,
    _stored_exec_argv,
    _stored_exec_model,
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
from nodeflow.workflows.dev_process.reuse import check_source_workspace
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


def _status(msg: str) -> None:
    """Print a progress message to stderr so the user knows what is happening."""
    _click.echo(f">> {msg}", err=True)


def _is_tests_ok(run_tests_st: Dict[str, Any]) -> bool:
    """Return whether ``run_tests`` reported success via ``test_result.ok``."""
    test_result = run_tests_st.get("test_result") or {}
    return test_result.get("ok") is True


_HUMAN_GATE_STATES = frozenset(
    {
        STATE_AWAITING_SPEC_HUMAN_GATE,
        STATE_AWAITING_FINAL,
        STATE_AWAITING_MERGE,
    }
)

_DEFAULT_MAX_AUTO_STEPS = 30

GateAction = Optional[tuple[str, Dict[str, Any]]]
GateHandler = Optional[Any]  # Callable[[str, Dict[str, Any]], GateAction]


def _effective_max_auto_steps(body: Dict[str, Any]) -> int:
    dp = body.get("dev_process", {})
    total = dp.get("total_phases")
    if isinstance(total, int) and total > 0:
        from nodeflow.workflows.dev_process.phase_loop import compute_max_auto_steps

        return compute_max_auto_steps(total)
    return _DEFAULT_MAX_AUTO_STEPS


def _dispatch_auto_action(
    body: Dict[str, Any],
    *,
    action: str,
    run_id: str,
    extra: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Dispatch a single action during auto-continue."""
    if action == ACTION_CONTINUE_IMPLEMENTATION:
        return _handle_continue_implementation(body, run_id=run_id, force_review_blocking=False)
    if action == ACTION_REVISE_PLAN:
        return _handle_revise_plan(
            body,
            run_id=run_id,
            interactive=False,
            revision_provided=extra.get("revision_provided", {}),
        )
    if action in (ACTION_REVISE_SPEC, ACTION_REQUEST_SPEC_REVISION):
        return _handle_revise_spec(
            body,
            run_id=run_id,
            interactive=False,
            revision_provided=extra.get("revision_provided", {}),
            use_human_comment=action == ACTION_REQUEST_SPEC_REVISION,
        )
    if action == ACTION_REWORK:
        dp = body.get("dev_process") or {}
        req = dp.get("final_rework_required")
        if isinstance(req, dict) and (
            req.get("target_phase_required") or req.get("decision_required")
        ):
            return None
        return _handle_rework(
            body,
            run_id=run_id,
            force_review_blocking=False,
            interactive=False,
            rework_provided=extra.get(
                "rework_provided", {"rework_comment": "auto-rework based on review findings"}
            ),
        )
    if action == ACTION_APPROVE_SPEC:
        return _handle_approve_spec(body, run_id=run_id)
    if action == ACTION_APPROVE_FINAL:
        return _handle_approve_final(body, run_id=run_id)
    if action == ACTION_MERGE:
        return _handle_merge(body, run_id=run_id)
    if action in (ACTION_REJECT_SPEC, ACTION_REJECT_FINAL):
        return _handle_reject(
            body,
            run_id=run_id,
            action=action,
            human_comment_text=extra.get("human_comment_text", ""),
        )
    return None


def _interactive_gate_handler(state: str, result: Dict[str, Any]) -> GateAction:
    """Prompt user at human gates. Returns ``(action, extra_kwargs)`` or *None* to stop."""
    rc = result.get("run_context") or {}
    artifact_root = rc.get("artifact_root", "")

    if state == STATE_AWAITING_SPEC_HUMAN_GATE:
        if artifact_root:
            _status(f"Spec: {Path(artifact_root) / 'spec' / 'spec.md'}")
        response = _click.prompt(
            ">> Approve spec? (Enter=approve, type comment to revise)",
            default="",
            show_default=False,
        ).strip()
        if not response:
            return (ACTION_APPROVE_SPEC, {})
        return (
            ACTION_REQUEST_SPEC_REVISION,
            {
                "revision_provided": {"revision_comment": response},
                "human_comment_text": response,
            },
        )

    if state == STATE_AWAITING_FINAL:
        if artifact_root:
            _status(f"Artifacts: {artifact_root}")
        response = _click.prompt(
            ">> Approve final? (Enter=approve, type comment to rework)",
            default="",
            show_default=False,
        ).strip()
        if not response:
            return (ACTION_APPROVE_FINAL, {})
        return (
            ACTION_REWORK,
            {
                "rework_provided": {"rework_comment": response},
            },
        )

    if state == STATE_AWAITING_MERGE:
        _status("Reached merge gate. Run `dev-process merge` explicitly to merge.")
        return None

    return None


def _auto_continue(
    result: Dict[str, Any],
    *,
    run_id: str,
    gate_handler: GateHandler = None,
) -> Dict[str, Any]:
    """Drive the flow forward until a stop-state or terminal state is reached.

    *gate_handler*: when provided, called at human-gate states to decide the
    next action.  When *None* (default), human gates are stop-states.
    """
    step = 0
    max_steps = _DEFAULT_MAX_AUTO_STEPS

    while step < max_steps:
        step += 1
        fr = result.get("flow_result") or {}
        state = str(fr.get("state") or "")

        if state in TERMINAL_STATES:
            break

        extra: Dict[str, Any] = {}

        if state in _HUMAN_GATE_STATES:
            if gate_handler is None:
                break
            gate_result = gate_handler(state, result)
            if gate_result is None:
                break
            next_action, extra = gate_result
        else:
            next_action = fr.get("next_action")
            if not next_action:
                break

        cp = result.get("flow_checkpoint_path")
        if not cp:
            break

        _status(f"Auto-continuing: {next_action}...")
        body = load_flow_checkpoint(cp)
        max_steps = max(max_steps, _effective_max_auto_steps(body))
        rid = str((body.get("run_context") or {}).get("run_id") or run_id)

        assert_action_allowed(state, next_action)
        timeline.append_event(
            body["run_context"]["artifact_root"],
            rid,
            "action_received",
            action=next_action,
            state=state,
        )

        dispatched = _dispatch_auto_action(body, action=next_action, run_id=rid, extra=extra)
        if dispatched is None:
            break
        result = dispatched
    else:
        _status(f"Stopped after {max_steps} auto-continue steps; check status.")

    return result


def _write_constraints_audit(body: Dict[str, Any]) -> str | None:
    """Write constraints_audit.md under artifact_root/agent_context/ (audit only).

    Shows global constraints separately from effective per-node constraints.
    NOT used as Codex AGENTS.md — per-node CODEX_HOME/AGENTS.md is generated
    on-demand by run_node_exec().
    """
    dp = body.get("dev_process")
    if not isinstance(dp, dict):
        return None
    snapshot = dp.get("exec_policy_snapshot")
    if not isinstance(snapshot, dict):
        return None

    content = generate_constraints_audit(snapshot)
    if "(none)" in content and "## Effective constraints by node" not in content:
        return None

    run_context = body.get("run_context", {})
    artifact_root = run_context.get("artifact_root")
    if not artifact_root:
        return None

    ctx_dir = Path(artifact_root) / "agent_context"
    ctx_dir.mkdir(parents=True, exist_ok=True)
    audit_path = ctx_dir / "constraints_audit.md"
    audit_path.write_text(content, encoding="utf-8")
    dp["constraints_audit_path"] = str(audit_path)
    return str(audit_path)


_EXEC_MODE_CHOICES = {
    "1": {
        "label": "Full auto (--sandbox workspace-write)",
        "argv": ["codex", "exec", "--sandbox", "workspace-write"],
    },
    "2": {
        "label": "Suggest mode (default Codex approval)",
        "argv": ["codex", "exec"],
    },
    "3": {
        "label": "Custom (enter argv manually)",
        "argv": None,
    },
}


def _prompt_exec_mode() -> list[str]:
    """Interactive prompt to select Codex execution mode."""
    import json as _json

    import click

    click.echo("\n--- Codex execution mode ---")
    for key, info in _EXEC_MODE_CHOICES.items():
        click.echo(f"  {key}) {info['label']}")
    choice = click.prompt(
        "Select execution mode",
        type=click.Choice(list(_EXEC_MODE_CHOICES.keys())),
        default="1",
    )
    selected = _EXEC_MODE_CHOICES[choice]
    if selected["argv"] is not None:
        return selected["argv"]
    raw = click.prompt(
        "Enter exec_argv as JSON array", default='["codex", "exec", "--sandbox", "workspace-write"]'
    )
    try:
        argv = _json.loads(raw)
        if isinstance(argv, list) and all(isinstance(x, str) for x in argv) and argv:
            return argv
    except (ValueError, TypeError):
        pass
    raise NodeExecutionFailure(f"invalid exec_argv: {raw!r}")


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
    exec_policy_path: Optional[str] = None,
    exec_policy_overrides: Optional[Dict[str, Any]] = None,
    force_review_blocking: bool = False,
    workspace_strategy: Optional[str] = None,
    exec_worker_kind: Optional[str] = None,
    merge_policy: Optional[str] = None,
    interactive: bool = False,
    spec_inputs_provided: Optional[Dict[str, Any]] = None,
    revision_provided: Optional[Dict[str, Any]] = None,
    rework_provided: Optional[Dict[str, Any]] = None,
    auto_continue: bool = True,
    prompt_at_gates: bool = False,
) -> Dict[str, Any]:
    if action == ACTION_START and flow_checkpoint_path:
        raise NodeExecutionFailure("start does not accept flow_checkpoint_path")
    if action != ACTION_START and (exec_policy_path or exec_policy_overrides):
        raise NodeExecutionFailure(
            "exec policy is start-only; resume uses the frozen exec_policy_snapshot from the checkpoint"
        )

    if action == ACTION_START:
        argv = _resolve_exec_argv(exec_argv)
        provided = dict(spec_inputs_provided or {})
        if task_prompt.strip() and not provided.get("task_prompt"):
            provided["task_prompt"] = task_prompt.strip()
        if exec_policy_path and exec_policy_overrides:
            raise NodeExecutionFailure(
                "exec_policy_path and inline exec_policy_overrides are mutually exclusive"
            )
        policy_overrides = exec_policy_overrides
        if exec_policy_path:
            policy_overrides = load_exec_policy_file(exec_policy_path)

        if argv is None and not policy_overrides and interactive:
            from nodeflow.workflows.dev_process.exec_policy import WORKER_DEFAULT_ARGV

            worker = exec_worker_kind or EXEC_WORKER_CODEX
            if not WORKER_DEFAULT_ARGV.get(worker):
                argv = _prompt_exec_mode()

        result = _handle_start(
            repo_root=repo_root,
            task_prompt=task_prompt,
            run_id=run_id,
            run_spec_on_start=run_spec_on_start,
            exec_argv=argv,
            exec_model=exec_model.strip()
            if isinstance(exec_model, str) and exec_model.strip()
            else None,
            exec_policy_overrides=policy_overrides,
            workspace_strategy=workspace_strategy or WORKSPACE_STRATEGY_CURRENT_REPO,
            exec_worker_kind=exec_worker_kind or EXEC_WORKER_CODEX,
            merge_policy=merge_policy or MERGE_POLICY_RECORD_ONLY,
            interactive=interactive,
            spec_inputs_provided=provided,
        )
        if auto_continue:
            _rid = str((result.get("run_context") or {}).get("run_id") or run_id or "")
            _gh = _interactive_gate_handler if prompt_at_gates else None
            result = _auto_continue(result, run_id=_rid, gate_handler=_gh)
        return result

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
    state = str((body.get("flow_result") or {}).get("state") or "")
    assert_action_allowed(state, action)
    timeline.append_event(
        run_context["artifact_root"], run_id, "action_received", action=action, state=state
    )

    if action == ACTION_APPROVE_SPEC:
        result = _handle_approve_spec(body, run_id=run_id)
    elif action in (ACTION_REVISE_SPEC, ACTION_REQUEST_SPEC_REVISION):
        rev_provided = dict(revision_provided or {})
        if action == ACTION_REQUEST_SPEC_REVISION:
            if human_comment_text.strip() and not rev_provided.get("revision_comment"):
                rev_provided["revision_comment"] = human_comment_text.strip()
        elif task_prompt.strip() and not rev_provided.get("revision_comment"):
            rev_provided["revision_comment"] = task_prompt.strip()
        result = _handle_revise_spec(
            body,
            run_id=run_id,
            interactive=interactive,
            revision_provided=rev_provided,
            use_human_comment=action == ACTION_REQUEST_SPEC_REVISION,
        )
    elif action == ACTION_REVISE_PLAN:
        rev_provided = dict(revision_provided or {})
        if task_prompt.strip() and not rev_provided.get("revision_comment"):
            rev_provided["revision_comment"] = task_prompt.strip()
        result = _handle_revise_plan(
            body,
            run_id=run_id,
            interactive=interactive,
            revision_provided=rev_provided,
        )
    elif action == ACTION_CONTINUE_IMPLEMENTATION:
        result = _handle_continue_implementation(
            body,
            run_id=run_id,
            force_review_blocking=force_review_blocking,
        )
    elif action == ACTION_REWORK:
        rw_provided = dict(rework_provided or {})
        if human_comment_text.strip() and not rw_provided.get("rework_comment"):
            rw_provided["rework_comment"] = human_comment_text.strip()
        if not rw_provided.get("rework_comment") and not interactive:
            rw_provided.setdefault("rework_comment", "rework requested")
        from_human_gate = bool(human_comment_text.strip()) or interactive
        result = _handle_rework(
            body,
            run_id=run_id,
            force_review_blocking=force_review_blocking,
            interactive=interactive,
            rework_provided=rw_provided,
            from_human_gate=from_human_gate,
        )
    elif action == ACTION_MERGE:
        result = _handle_merge(body, run_id=run_id)
    elif action == ACTION_APPROVE_FINAL:
        result = _handle_approve_final(body, run_id=run_id)
    elif action in (ACTION_REJECT_SPEC, ACTION_REJECT_FINAL):
        result = _handle_reject(
            body,
            run_id=run_id,
            action=action,
            human_comment_text=human_comment_text,
        )
    else:
        raise NodeExecutionFailure(f"unsupported action {action!r}")

    if auto_continue:
        _gh = _interactive_gate_handler if prompt_at_gates else None
        result = _auto_continue(result, run_id=run_id, gate_handler=_gh)
    return result


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
    exec_policy_overrides: Optional[Dict[str, Any]] = None,
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
        "node_runs": [],
        "stale": {},
        "task_prompt": task_prompt,
        "source_workspace_check": swc,
    }
    _status(f"Flow started (run_id: {rid})")
    timeline.append_event(
        artifact_root, rid, "flow_started", state=STATE_INITIALIZED, action=ACTION_START
    )
    snapshot = build_exec_policy_snapshot(
        exec_worker_kind=exec_worker_kind,
        exec_argv=exec_argv,
        exec_model=exec_model,
        exec_policy_overrides=exec_policy_overrides,
    )
    apply_snapshot_to_body(body, snapshot)
    _write_constraints_audit(body)
    out = _finalize(body=body, run_id=rid, action=ACTION_START, state=STATE_INITIALIZED)

    if not run_spec_on_start:
        return out

    body = load_flow_checkpoint(out["flow_checkpoint_path"])
    return _run_spec_cycle(
        body,
        run_id=rid,
        task_prompt=task_prompt,
        action=ACTION_START,
        interactive=interactive,
        spec_inputs_provided=spec_inputs_provided or {},
    )


def _run_spec_cycle(
    body: Dict[str, Any],
    *,
    run_id: str,
    task_prompt: str,
    action: str,
    revision_context: Optional[str] = None,
    interactive: bool = False,
    spec_inputs_provided: Optional[Dict[str, Any]] = None,
    use_human_comment: bool = False,
    revision_provided: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    run_context = body["run_context"]
    repo = Path(run_context["repo_root"])
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

    spec_epoch_bump = bool(body.get("dev_process", {}).pop("spec_rework_epoch_bump", False))

    _status("Writing spec...")
    timeline.append_event(run_context["artifact_root"], run_id, "writing_spec", stage="spec")
    body["spec_epoch_bump"] = spec_epoch_bump
    try:
        sp = run_spec_stage(
            repo_root=repo,
            artifact_root=run_context["artifact_root"],
            run_id=run_id,
            task_prompt=resolved_task_prompt,
            base_revision=run_context["source_base_revision"],
            revision_context=revision_context,
            notes=notes or None,
            reference_materials=materials,
            previous_spec=previous_spec,
            body=body,
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(body=body, run_id=run_id, action=action, reason=str(e))
        raise
    body["stages"]["spec"] = sp
    clear_stage_stale(body, "spec")

    spec_text = _read_spec_text(run_context["artifact_root"])
    _status("Reviewing spec...")
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
            body=body,
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
        dp = body.setdefault("dev_process", {})
        from nodeflow.workflows.dev_process.phase_loop import (
            check_loop_limit,
            increment_loop_counter,
        )

        check_loop_limit(dp, "spec_revision")
        increment_loop_counter(dp, "spec_revision")
        return _finalize(
            body=body,
            run_id=run_id,
            action=action,
            state=STATE_AWAITING_SPEC_REVISION,
            merge_ready=False,
        )
    from nodeflow.workflows.dev_process.phase_loop import reset_loop_counter as _rlc

    _rlc(body.setdefault("dev_process", {}), "spec_revision")
    return _finalize(
        body=body,
        run_id=run_id,
        action=action,
        state=STATE_AWAITING_SPEC_HUMAN_GATE,
        merge_ready=False,
    )


def _archive_failed_plan_attempt(
    artifact_root: str,
    previous_plan: str | None,
    *,
    plan_stage: Dict[str, Any] | None = None,
) -> None:
    """Move rejected plan to rework_attempts/ and restore previous accepted plan.

    Raises ``NodeExecutionFailure`` if ``plan.md`` was restored but ``plan.json``
    could not be regenerated from ``previous_plan`` (avoids md/json drift).
    """
    plan_dir = Path(artifact_root) / "plan"
    attempts_dir = plan_dir / "rework_attempts"
    if attempts_dir.exists():
        existing = list(attempts_dir.glob("attempt_*"))
        attempt_idx = len(existing) + 1
    else:
        attempt_idx = 1
    draft_dir = attempts_dir / f"attempt_{attempt_idx:03d}"
    draft_dir.mkdir(parents=True, exist_ok=True)
    plan_md = plan_dir / "plan.md"
    plan_json = plan_dir / "plan.json"
    if plan_md.exists():
        (draft_dir / "plan.md").write_text(plan_md.read_text(encoding="utf-8"), encoding="utf-8")
    if plan_json.exists():
        (draft_dir / "plan.json").write_text(
            plan_json.read_text(encoding="utf-8"), encoding="utf-8"
        )
    if plan_stage is not None:
        plan_stage["rework_attempt_archive"] = str(draft_dir)
    if previous_plan:
        plan_md.write_text(previous_plan, encoding="utf-8")
        from nodeflow.workflows.dev_process.plan_phases import parse_new_plan, save_plan_json

        try:
            old_plan_data = parse_new_plan(previous_plan)
            save_plan_json(old_plan_data, str(plan_dir))
        except Exception as exc:
            if plan_stage is not None:
                plan_stage["plan_restore_failed"] = True
                plan_stage["plan_restore_error"] = str(exc)
            raise NodeExecutionFailure(
                "Plan contract validation rejected the draft, but restoring the "
                f"previous plan.json from the accepted plan.md failed: {exc}"
            ) from exc


def _run_plan_cycle(
    body: Dict[str, Any],
    *,
    run_id: str,
    action: str,
    revision_context: Optional[str] = None,
    interactive: bool = False,
    revision_provided: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    run_context = body["run_context"]
    repo = Path(run_context["repo_root"])
    task_prompt = str(body.get("task_prompt") or "")
    spec_text = _read_spec_text(run_context["artifact_root"])

    dp = body.setdefault("dev_process", {})
    is_rework = action == ACTION_REVISE_PLAN
    is_continuation = dp.get("planning_mode") == "continuation_from_head"
    completed_phases_info: list[Dict[str, Any]] | None = None

    previous_plan: str | None = None
    if is_rework:
        if revision_context is None:
            revision_context = _revision_context_from_plan_review(body)
        previous_plan = _read_plan_text(run_context["artifact_root"])
        if dp.get("total_phases"):
            from nodeflow.workflows.dev_process.contract_check import get_completed_phase_info

            completed_phases_info = get_completed_phase_info(dp)

    continuation_findings: list[Dict[str, Any]] | None = None
    continuation_start_index = 0
    existing_plan = None
    existing_plan_text: str | None = None
    if is_continuation:
        continuation_findings = dp.get("continuation_findings", [])
        from nodeflow.workflows.dev_process.artifact_versions import (
            ensure_continuation_base_plan_version,
            load_versioned_plan,
        )
        from nodeflow.workflows.dev_process.contract_check import count_completed_prefix

        continuation_start_index = count_completed_prefix(
            dp.get("phase_results", {}), dp.get("total_phases", 0)
        )
        base_version = ensure_continuation_base_plan_version(dp)
        existing_plan = load_versioned_plan(run_context["artifact_root"], base_version)
        existing_plan_text = existing_plan.raw_text

    plan_repo = _phase_repo_root(body) if is_continuation else repo
    defer_plan_version = is_rework and not is_continuation and bool(dp.get("total_phases"))

    _status("Writing plan..." if not is_continuation else "Writing continuation plan...")
    timeline.append_event(run_context["artifact_root"], run_id, "writing_plan", stage="plan")
    try:
        pl = run_plan_stage(
            repo_root=plan_repo,
            artifact_root=run_context["artifact_root"],
            run_id=run_id,
            task_prompt=task_prompt,
            approved_spec=spec_text,
            revision_context=revision_context,
            previous_plan=previous_plan,
            body=body,
            completed_phases=completed_phases_info,
            continuation_findings=continuation_findings,
            continuation_start_index=continuation_start_index,
            existing_plan=existing_plan,
            existing_plan_text=existing_plan_text,
            defer_plan_version_commit=defer_plan_version,
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(body=body, run_id=run_id, action=action, reason=str(e))
        raise
    # Same dict as pl — later updates (e.g. plan_version, contract_validation_*) mutate in place.
    body.setdefault("stages", {})["plan"] = pl
    clear_stage_stale(body, "plan")
    mark_stale(body, upstream="plan")

    plan_stage = body.get("stages", {}).get("plan", {})
    plan_json_path = plan_stage.get("plan_json_path")
    _rework_contract_validated = False
    if is_rework and not is_continuation and dp.get("total_phases") and plan_json_path:
        from nodeflow.workflows.dev_process.contract_check import validate_rework_contracts
        from nodeflow.workflows.dev_process.phase_loop import load_plan_data

        _status("Validating phase contracts...")
        new_plan = load_plan_data(run_context["artifact_root"])
        try:
            validate_rework_contracts(new_plan, dp)
        except NodeExecutionFailure as e:
            plan_st = body.setdefault("stages", {}).setdefault("plan", {})
            plan_st["contract_validation_failed"] = True
            plan_st["contract_validation_error"] = str(e)
            try:
                _archive_failed_plan_attempt(
                    run_context["artifact_root"],
                    previous_plan,
                    plan_stage=plan_st,
                )
            except NodeExecutionFailure as restore_err:
                from nodeflow.workflows.dev_process.artifact_versions import (
                    clear_plan_draft_pending_contract_validation,
                )

                clear_plan_draft_pending_contract_validation(dp, plan_st)
                _fail_checkpoint(body=body, run_id=run_id, action=action, reason=str(restore_err))
                raise restore_err from e
            from nodeflow.workflows.dev_process.artifact_versions import (
                clear_plan_draft_pending_contract_validation,
            )

            clear_plan_draft_pending_contract_validation(dp, plan_st)
            _fail_checkpoint(body=body, run_id=run_id, action=action, reason=str(e))
            raise
        _rework_contract_validated = True
        if defer_plan_version and pl.get("plan_version_deferred"):
            from nodeflow.workflows.dev_process.artifact_versions import (
                clear_plan_draft_pending_contract_validation,
                commit_plan_version,
            )

            commit_plan_version(run_context["artifact_root"], new_plan, dp)
            pl["plan_version"] = dp.get("current_plan_version", "")
            clear_plan_draft_pending_contract_validation(dp, pl)

    plan_text = _read_plan_text(run_context["artifact_root"])
    _status("Reviewing plan...")
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
            body=body,
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(body=body, run_id=run_id, action=action, reason=str(e))
        raise
    body["stages"]["plan_review"] = pr
    clear_stage_stale(body, "plan_review")

    if pr.get("decision") == "fail":
        from nodeflow.workflows.dev_process.phase_loop import (
            check_loop_limit,
            increment_loop_counter,
        )

        if is_continuation:
            from nodeflow.workflows.dev_process.artifact_versions import (
                restore_plan_latest_from_version,
                restore_plan_version_pointer,
            )

            base_version = str(dp.get("continuation_base_plan_version") or "").strip()
            if base_version:
                restore_plan_latest_from_version(run_context["artifact_root"], base_version)
                restore_plan_version_pointer(dp, base_version)
                plan_st = body.setdefault("stages", {}).setdefault("plan", {})
                plan_st["continuation_draft_restored"] = True
                plan_st["restored_plan_version"] = base_version
                plan_st["plan_version"] = base_version

        check_loop_limit(dp, "plan_revision")
        increment_loop_counter(dp, "plan_revision")
        return _finalize(
            body=body,
            run_id=run_id,
            action=action,
            state=STATE_AWAITING_PLAN_REVISION,
            merge_ready=False,
        )
    gates = dp.setdefault("human_gates", {})
    gates["spec"] = "approved"
    from nodeflow.workflows.dev_process.phase_loop import reset_loop_counter as _rlc_plan

    _rlc_plan(dp, "plan_revision")

    if plan_json_path:
        from nodeflow.workflows.dev_process.phase_loop import (
            init_phase_state,
            load_plan_data,
        )

        plan_data = load_plan_data(run_context["artifact_root"])
        if is_continuation:
            from nodeflow.workflows.dev_process.contract_check import (
                apply_continuation_plan_update,
                count_completed_prefix,
            )
            from nodeflow.workflows.dev_process.phase_loop import continuation_plan_from_merged

            start_raw = plan_stage.get("continuation_start_index")
            if start_raw is not None:
                completed_count = int(start_raw)
            else:
                completed_count = count_completed_prefix(
                    dp.get("phase_results", {}), dp.get("total_phases", 0)
                )
            continuation_plan = continuation_plan_from_merged(plan_data, completed_count)
            apply_continuation_plan_update(continuation_plan, dp)
            dp["plan_sha256"] = plan_data.plan_sha256
            dp.pop("planning_mode", None)
            dp.pop("continuation_findings", None)
            dp.pop("continuation_base_plan_version", None)
        elif is_rework and _rework_contract_validated:
            from nodeflow.workflows.dev_process.contract_check import (
                apply_rework_plan_update,
            )

            apply_rework_plan_update(plan_data, dp)
        elif not is_rework:
            task_branch = dp.get("task_branch", {})
            if task_branch.get("created") and task_branch.get("base_ref"):
                from nodeflow.workflows.dev_process.phase_git import reset_to_ref

                repo_for_reset = Path(task_branch.get("worktree_path") or run_context["repo_root"])
                pre_reset_ref = git_head_revision(repo_for_reset)
                reset_to_ref(
                    repo_for_reset,
                    task_branch["base_ref"],
                    expected_branch=task_branch.get("name", ""),
                )
                recovery_refs = dp.setdefault("recovery_refs", [])
                recovery_refs.append(
                    {
                        "reason": "spec_rework_reinit",
                        "ref": pre_reset_ref,
                        "reset_to_ref": task_branch["base_ref"],
                    }
                )
            init_phase_state(dp, plan_data)
        dp["plan_json_path"] = plan_json_path

        if not dp.get("task_branch", {}).get("created"):
            from nodeflow.workflows.dev_process.phase_git import create_task_branch

            strategy = _workspace_strategy(run_context)
            task_branch = create_task_branch(
                Path(run_context["repo_root"]),
                run_id,
                workspace_strategy=strategy,
            )
            task_branch["base_branch"] = run_context.get("source_current_branch", "")
            dp["task_branch"] = task_branch
            if task_branch.get("worktree_path"):
                dp.setdefault("cleanup_targets", []).append(
                    {
                        "kind": "git_worktree",
                        "branch": task_branch["name"],
                        "worktree_path": task_branch["worktree_path"],
                        "worktree_root": task_branch.get("worktree_root", ""),
                        "run_id": run_id,
                    }
                )

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
) -> Dict[str, Any]:
    gates = body.setdefault("dev_process", {}).setdefault("human_gates", {})
    gates["spec"] = "approved"
    return _run_plan_cycle(body, run_id=run_id, action=ACTION_APPROVE_SPEC)


def _handle_revise_spec(
    body: Dict[str, Any],
    *,
    run_id: str,
    interactive: bool,
    revision_provided: Dict[str, Any],
    use_human_comment: bool,
) -> Dict[str, Any]:
    run_context = body["run_context"]
    repo = Path(run_context["repo_root"])
    findings = _revision_context_from_spec_review(body)
    comment = str(revision_provided.get("revision_comment") or "")
    materials: list[dict[str, Any]] | None = None
    if (
        use_human_comment
        or comment.strip()
        or revision_provided.get("reference_paths")
        or interactive
    ):
        _status("Collecting revision inputs...")
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
        action=ACTION_REVISE_SPEC if not use_human_comment else ACTION_REQUEST_SPEC_REVISION,
        revision_context=revision_context,
        interactive=interactive,
        use_human_comment=use_human_comment,
    )


def _handle_revise_plan(
    body: Dict[str, Any],
    *,
    run_id: str,
    interactive: bool,
    revision_provided: Dict[str, Any],
) -> Dict[str, Any]:
    run_context = body["run_context"]
    repo = Path(run_context["repo_root"])
    findings = _revision_context_from_plan_review(body)
    comment = str(revision_provided.get("revision_comment") or "")
    materials: list[dict[str, Any]] | None = None
    if comment.strip() or revision_provided.get("reference_paths") or interactive:
        _status("Collecting plan revision inputs...")
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
        action=ACTION_REVISE_PLAN,
        revision_context=revision_context,
    )


def _handle_continue_implementation(
    body: Dict[str, Any],
    *,
    run_id: str,
    force_review_blocking: bool,
    rework_context: Optional[str] = None,
    skip_implementation: bool = False,
) -> Dict[str, Any]:
    dp = body.setdefault("dev_process", {})
    total_phases = int(dp.get("total_phases") or 0)
    if total_phases <= 0:
        raise NodeExecutionFailure(
            "Phase-based plan is required before implementation. "
            "Complete plan review with a valid phase plan, use revise-plan, "
            "or restart the dev_process run."
        )

    return _handle_phase_implementation(
        body,
        run_id=run_id,
        force_review_blocking=force_review_blocking,
        rework_context=rework_context,
        skip_implementation=skip_implementation,
    )


def _handle_phase_implementation(
    body: Dict[str, Any],
    *,
    run_id: str,
    force_review_blocking: bool,
    rework_context: Optional[str] = None,
    skip_implementation: bool = False,
) -> Dict[str, Any]:
    """Phase-based implementation loop."""
    from nodeflow.workflows.dev_process.phase_loop import (
        get_current_phase_context,
        load_plan_data,
        record_phase_start,
    )

    run_context = body["run_context"]
    dp = body["dev_process"]
    plan_data = load_plan_data(run_context["artifact_root"])
    phase_ctx = get_current_phase_context(dp, plan_data)

    if phase_ctx is None:
        return _run_final_review(body, run_id=run_id)

    phase_id = phase_ctx["phase_id"]
    phase_idx = phase_ctx["phase_index"]
    _status(f"Phase {phase_idx + 1}/{phase_ctx['total_phases']}: {phase_ctx['phase_title']}")

    last_rewind = dp.pop("last_rewind", None)
    if last_rewind and last_rewind.get("skip_implementation"):
        skip_implementation = True

    results = dp.setdefault("phase_results", {})
    pr = results.setdefault(phase_id, {})

    repo = _phase_repo_root(body)
    task_branch = dp.get("task_branch") or {}
    expected_branch = task_branch.get("name", "")
    if expected_branch:
        from nodeflow.workflows.dev_process.phase_git import verify_on_task_branch

        verify_on_task_branch(repo, expected_branch)

    if not pr.get("phase_start_git_ref"):
        record_phase_start(dp, repo)

    return _run_single_phase(
        body,
        run_id=run_id,
        phase_ctx=phase_ctx,
        force_review_blocking=force_review_blocking,
        rework_context=rework_context,
        skip_implementation=skip_implementation,
    )


def _run_single_phase(
    body: Dict[str, Any],
    *,
    run_id: str,
    phase_ctx: Dict[str, Any],
    force_review_blocking: bool,
    rework_context: Optional[str] = None,
    skip_implementation: bool = False,
) -> Dict[str, Any]:
    """Run impl → test → run_tests → review → synthesis for a single phase."""
    from nodeflow.workflows.dev_process.phase_git import verify_on_task_branch
    from nodeflow.workflows.dev_process.phase_loop import (
        all_phases_completed,
        complete_phase,
    )

    run_context = body["run_context"]
    dp = body["dev_process"]
    repo = _phase_repo_root(body)
    task_branch = dp.get("task_branch") or {}

    expected_branch = task_branch.get("name", "")
    if expected_branch:
        verify_on_task_branch(repo, expected_branch)

    body["workspace_context"] = {
        "workspace_root": str(repo),
        "base_revision": task_branch.get("base_ref", ""),
        "current_branch": task_branch.get("name", ""),
        "strategy": _workspace_strategy(run_context),
    }
    spec_text = _read_spec_text(run_context["artifact_root"])
    plan_text = _read_plan_text(run_context["artifact_root"])
    task_prompt = str(body.get("task_prompt") or "")

    phase_id = phase_ctx["phase_id"]
    phase_artifact_root = str(Path(run_context["artifact_root"]) / "phases" / phase_id)
    Path(phase_artifact_root).mkdir(parents=True, exist_ok=True)

    phase_goal = phase_ctx["phase_goal"]
    phase_scope = phase_ctx["phase_scope_include"]
    phase_test_plan = phase_ctx["phase_test_plan"]

    phase_excluded = phase_ctx.get("phase_scope_exclude") or []
    phase_review_targets = phase_ctx.get("phase_review_targets") or []
    phase_review_agents = phase_ctx.get("phase_review_agents") or []
    phase_checklist = phase_ctx.get("phase_review_checklist") or []
    phase_criteria = phase_ctx.get("phase_acceptance_criteria") or []

    parts = [
        f"## Current Phase: {phase_ctx['phase_title']}",
        "",
        f"**Goal:** {phase_goal}",
        "",
        "**Scope:**",
        *[f"- {s}" for s in phase_scope],
        "",
    ]
    if phase_excluded:
        parts += ["**Excluded:**", *[f"- {s}" for s in phase_excluded], ""]
    parts += [
        "**Test plan:**",
        *[f"- {t}" for t in phase_test_plan],
        "",
        f"**Review targets:** {', '.join(phase_review_targets)}",
        f"**Review agents:** {', '.join(phase_review_agents)}",
        "",
        "**Review checklist:**",
        *[f"- {c}" for c in phase_checklist],
        "",
        "**Acceptance criteria:**",
        *[f"- {a}" for a in phase_criteria],
    ]
    phase_plan_text = "\n".join(parts)
    augmented_plan = plan_text + "\n\n---\n\n" + phase_plan_text

    phase_start_ref = dp.get("phase_results", {}).get(phase_id, {}).get("phase_start_git_ref", "")
    base_rev = (
        phase_start_ref
        or dp.get("task_branch", {}).get("base_ref")
        or run_context["source_base_revision"]
    )

    if not skip_implementation:
        _status(f"[{phase_id}] Implementing...")
        timeline.append_event(
            run_context["artifact_root"],
            run_id,
            "implementing",
            stage="implementation",
            phase=phase_id,
        )
        try:
            impl = run_implementation_stage(
                repo_root=repo,
                artifact_root=phase_artifact_root,
                run_id=run_id,
                task_prompt=task_prompt,
                base_revision=base_rev,
                approved_spec=spec_text,
                approved_plan=augmented_plan,
                rework_context=rework_context,
                body=body,
            )
        except NodeExecutionFailure as e:
            _fail_checkpoint(
                body=body, run_id=run_id, action=ACTION_CONTINUE_IMPLEMENTATION, reason=str(e)
            )
            raise
        impl["phase_id"] = phase_id
        body["stages"]["implementation"] = impl
        clear_stage_stale(body, "implementation")
        mark_stale(body, upstream="implementation")

    if skip_implementation:
        impl = body.get("stages", {}).get("implementation") or {}
        impl_phase = impl.get("phase_id", "")
        if impl_phase and impl_phase != phase_id:
            raise NodeExecutionFailure(
                f"skip_implementation: cached implementation is for {impl_phase!r}, "
                f"not current phase {phase_id!r}; cannot skip safely"
            )

    _status(f"[{phase_id}] Writing tests...")
    timeline.append_event(
        run_context["artifact_root"],
        run_id,
        "writing_tests",
        stage="test_implementation",
        phase=phase_id,
    )
    try:
        test_impl = run_test_implementation_stage(
            repo_root=repo,
            artifact_root=phase_artifact_root,
            run_id=run_id,
            approved_spec=spec_text,
            approved_plan=phase_plan_text,
            body=body,
            rework_context=rework_context,
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(
            body=body, run_id=run_id, action=ACTION_CONTINUE_IMPLEMENTATION, reason=str(e)
        )
        raise
    body["stages"]["test_implementation"] = test_impl
    clear_stage_stale(body, "test_implementation")
    mark_stale(body, upstream="test_implementation")

    from nodeflow.workflows.dev_process.phase_git import collect_phase_changed_paths
    from nodeflow.workflows.dev_process.stages.lint_fix import run_lint_fix_stage

    _status(f"[{phase_id}] Lint fix...")
    timeline.append_event(
        run_context["artifact_root"],
        run_id,
        "lint_fix",
        stage="lint_fix",
        phase=phase_id,
    )
    changed = collect_phase_changed_paths(
        repo, artifact_roots=[run_context["artifact_root"], phase_artifact_root]
    )
    lint_result = run_lint_fix_stage(
        repo_root=repo,
        changed_paths=changed,
        artifact_root=run_context["artifact_root"],
        phase_id=phase_id,
    )
    body["stages"]["lint_fix"] = lint_result

    _status(f"[{phase_id}] Running tests...")
    timeline.append_event(
        run_context["artifact_root"],
        run_id,
        "running_tests",
        stage="run_tests",
        phase=phase_id,
    )
    from nodeflow.workflows.dev_process.reuse import collect_diff

    pre_test_diff = collect_diff(repo_root=repo, base_revision=base_rev)

    try:
        run_tests_st = run_run_tests_stage(
            repo_root=repo,
            artifact_root=phase_artifact_root,
            run_id=run_id,
            diff_result=pre_test_diff,
            execution_output=impl.get("execution_output") or {},
        )
    except NodeExecutionFailure as e:
        _fail_checkpoint(
            body=body, run_id=run_id, action=ACTION_CONTINUE_IMPLEMENTATION, reason=str(e)
        )
        raise
    body["stages"]["run_tests"] = run_tests_st
    clear_stage_stale(body, "run_tests")
    run_tests_ok = _is_tests_ok(run_tests_st)

    phase_diff_result = collect_diff(repo_root=repo, base_revision=base_rev)

    lint_log_paths = list(lint_result.get("log_paths") or [])
    impl_bundle = {
        "status": run_tests_st.get("status", "completed"),
        "test_result": run_tests_st.get("test_result"),
        "diff_result": phase_diff_result,
        "lint_result": lint_result,
        "evidence_paths": list(impl.get("evidence_paths") or [])
        + list(test_impl.get("evidence_paths") or []),
        "lint_log_paths": lint_log_paths,
    }

    _status(f"[{phase_id}] Reviewing changes...")
    timeline.append_event(
        run_context["artifact_root"],
        run_id,
        "reviewing_changes",
        stage="review",
        phase=phase_id,
    )
    preset = str(dp.get("review_depth_preset") or "standard")
    try:
        rev = run_review_stage(
            repo_root=repo,
            artifact_root=phase_artifact_root,
            run_id=run_id,
            base_revision=base_rev,
            approved_spec=spec_text,
            approved_plan=augmented_plan,
            diff_result=impl_bundle.get("diff_result") or {},
            test_result=impl_bundle.get("test_result") or {},
            force_blocking=force_review_blocking,
            review_depth_preset=preset,
            body=body,
            review_targets=phase_review_targets or None,
            review_agents=phase_review_agents or None,
            review_checklist=phase_checklist or None,
            review_acceptance_criteria=phase_criteria or None,
            lint_result=lint_result,
            review_scope="phase",
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

    results = dp.get("phase_results", {})
    pr = results.setdefault(phase_id, {})
    from nodeflow.workflows.dev_process.phase_stage_refs import compact_phase_stages

    pr["stage_refs"] = compact_phase_stages(
        implementation=impl,
        test_implementation=test_impl,
        lint_fix=lint_result,
        run_tests=run_tests_st,
        review=rev,
    )

    gates = dp.setdefault("human_gates", {})

    lint_failed = lint_result.get("lint_fix") == "ruff_failed"

    if blocking or lint_failed:
        body["rework_owner"] = route_owner_to_state(blocking) if blocking else "implementation"
        dp["review_scope"] = "phase"
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
        dp["review_scope"] = "phase"
        gates["final"] = "not_reached"
        return _finalize(
            body=body,
            run_id=run_id,
            action=ACTION_CONTINUE_IMPLEMENTATION,
            state=STATE_AWAITING_REWORK_DECISION,
            merge_ready=False,
        )

    commit_info = complete_phase(
        dp,
        repo,
        artifact_roots=[run_context["artifact_root"], phase_artifact_root],
    )
    _status(f"[{phase_id}] Phase completed (commit: {commit_info['phase_commit'][:8]})")

    if all_phases_completed(dp):
        return _run_final_review(body, run_id=run_id)

    return _finalize(
        body=body,
        run_id=run_id,
        action=ACTION_CONTINUE_IMPLEMENTATION,
        state=STATE_AWAITING_IMPLEMENTATION,
        merge_ready=False,
    )


def _run_final_review(
    body: Dict[str, Any],
    *,
    run_id: str,
) -> Dict[str, Any]:
    """Run final review over the full diff (base_ref..HEAD).

    If no blocking findings: transition to awaiting_final_approval.
    If blocking: run final_synthesis to determine owner/target, then route.
    """
    from nodeflow.workflows.dev_process.final_review import (
        parse_final_synthesis,
        route_final_synthesis,
    )
    from nodeflow.workflows.dev_process.phase_rewind import rewind_to_phase

    run_context = body["run_context"]
    dp = body["dev_process"]
    repo = _phase_repo_root(body)
    spec_text = _read_spec_text(run_context["artifact_root"])
    plan_text = _read_plan_text(run_context["artifact_root"])

    task_branch = dp.get("task_branch", {})
    base_ref = task_branch.get("base_ref") or run_context["source_base_revision"]

    _status("Final review (full diff)...")
    timeline.append_event(
        run_context["artifact_root"], run_id, "final_review", stage="final_review"
    )

    final_artifact_root = str(Path(run_context["artifact_root"]) / "final_review")
    Path(final_artifact_root).mkdir(parents=True, exist_ok=True)

    from nodeflow.workflows.dev_process.reuse import collect_diff

    final_diff_result = collect_diff(repo_root=repo, base_revision=base_ref)

    preset = str(dp.get("review_depth_preset") or "standard")
    try:
        from nodeflow.workflows.dev_process.review_config import FINAL_REVIEW_AGENTS

        rev = run_review_stage(
            repo_root=repo,
            artifact_root=final_artifact_root,
            run_id=run_id,
            base_revision=base_ref,
            approved_spec=spec_text,
            approved_plan=plan_text,
            diff_result=final_diff_result,
            test_result={},
            force_blocking=False,
            review_depth_preset=preset,
            body=body,
            review_targets=["final_diff"],
            review_agents=list(FINAL_REVIEW_AGENTS),
            review_scope="final",
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
    from nodeflow.workflows.dev_process.paths import git_tree_hash

    rev["reviewed_tree"] = git_tree_hash(repo, branch_head)

    body["stages"]["final_review"] = rev
    gates = dp.setdefault("human_gates", {})

    if not blocking:
        from nodeflow.workflows.dev_process.phase_loop import reset_loop_counter

        reset_loop_counter(dp, "final_review_rework")
        gates["final"] = "pending"
        dp["review_scope"] = "final"
        return _finalize(
            body=body,
            run_id=run_id,
            action=ACTION_CONTINUE_IMPLEMENTATION,
            state=STATE_AWAITING_FINAL,
            merge_ready=True,
        )

    from nodeflow.workflows.dev_process.phase_loop import check_loop_limit, increment_loop_counter

    check_loop_limit(dp, "final_review_rework")
    increment_loop_counter(dp, "final_review_rework")

    _status("Final review found blocking findings; synthesizing routing...")
    synthesis_output = _build_final_synthesis(blocking, dp)

    if synthesis_output.get("target_phase_required") or synthesis_output.get("decision_required"):
        dp["review_scope"] = "final"
        dp["final_rework_required"] = {
            "owner": synthesis_output["owner"],
            "owners": synthesis_output.get("owners"),
            "target_phase_required": bool(synthesis_output.get("target_phase_required")),
            "decision_required": bool(synthesis_output.get("decision_required")),
            "findings": synthesis_output["findings"],
        }
        gates["final"] = "not_reached"
        body["rework_owner"] = synthesis_output["owner"]
        return _finalize(
            body=body,
            run_id=run_id,
            action=ACTION_CONTINUE_IMPLEMENTATION,
            state=STATE_AWAITING_REWORK_DECISION,
            merge_ready=False,
        )

    try:
        synthesis = parse_final_synthesis(synthesis_output)
    except NodeExecutionFailure as e:
        _fail_checkpoint(
            body=body, run_id=run_id, action=ACTION_CONTINUE_IMPLEMENTATION, reason=str(e)
        )
        raise

    routing = route_final_synthesis(synthesis, rewind_implemented=True)
    dp["final_synthesis"] = routing

    if routing["decision"] == "ok":
        gates["final"] = "pending"
        dp["review_scope"] = "final"
        return _finalize(
            body=body,
            run_id=run_id,
            action=ACTION_CONTINUE_IMPLEMENTATION,
            state=STATE_AWAITING_FINAL,
            merge_ready=True,
        )

    owner = routing["owner"]

    if owner == "plan":
        from nodeflow.workflows.dev_process.contract_check import count_completed_prefix

        completed_count = count_completed_prefix(
            dp.get("phase_results", {}), dp.get("total_phases", 0)
        )
        dp["review_scope"] = "final"
        from nodeflow.workflows.dev_process.contract_check import enter_continuation_planning_mode

        enter_continuation_planning_mode(
            dp,
            findings=list(blocking),
            completed_count=completed_count,
        )
        return _finalize(
            body=body,
            run_id=run_id,
            action=ACTION_CONTINUE_IMPLEMENTATION,
            state=STATE_AWAITING_PLAN_REVISION,
            merge_ready=False,
        )

    if owner == "spec":
        dp["review_scope"] = "final"
        dp["spec_rework_epoch_bump"] = True
        if dp.get("total_phases"):
            _rework_save_and_reset(body, prefer_task_branch_base=True)
        return _finalize(
            body=body,
            run_id=run_id,
            action=ACTION_CONTINUE_IMPLEMENTATION,
            state=STATE_AWAITING_SPEC_REVISION,
            merge_ready=False,
        )

    target_phase = routing.get("target_phase")
    if target_phase:
        _status(f"Rewinding to {target_phase}...")
        rewind_info = rewind_to_phase(
            dp,
            repo,
            target_phase=target_phase,
            owner=owner,
        )
        dp["last_rewind"] = rewind_info
        return _finalize(
            body=body,
            run_id=run_id,
            action=ACTION_CONTINUE_IMPLEMENTATION,
            state=STATE_AWAITING_IMPLEMENTATION,
            merge_ready=False,
        )

    gates["final"] = "not_reached"
    dp["review_scope"] = "final"
    body["rework_owner"] = owner
    return _finalize(
        body=body,
        run_id=run_id,
        action=ACTION_CONTINUE_IMPLEMENTATION,
        state=STATE_AWAITING_REWORK_DECISION,
        merge_ready=False,
    )


def _build_final_synthesis(
    blocking_findings: list[Dict[str, Any]],
    dp: Dict[str, Any],
) -> Dict[str, Any]:
    """Derive owner routing from blocking findings (v1: no LLM synthesis).

    v1 does NOT use LLM to generate a final_synthesis output.  Instead it
    derives the routing conservatively from per-finding ``owner`` tags
    produced by the review stage.

    - Single owner (plan/spec): auto-routes directly.
    - Single owner (impl/test): ``target_phase_required=True`` → human gate
      (the human must supply ``--target-phase`` via CLI).
    - Multiple owners: ``decision_required=True`` → human gate
      (the human must supply ``--owner`` and, for impl/test, ``--target-phase``).
    """
    from nodeflow.workflows.dev_process.final_review import VALID_OWNERS

    owners: set[str] = set()
    for f in blocking_findings:
        raw_owner = f.get("owner", "implementation")
        if raw_owner not in VALID_OWNERS:
            raw_owner = "implementation"
        owners.add(raw_owner)

    if len(owners) == 1:
        owner = next(iter(owners))
        if owner == "plan":
            return {
                "owner": "plan",
                "target_phase": None,
                "findings": blocking_findings,
            }
        if owner == "spec":
            return {
                "owner": "spec",
                "target_phase": None,
                "findings": blocking_findings,
            }
        return {
            "owner": owner,
            "target_phase": None,
            "target_phase_required": True,
            "findings": blocking_findings,
        }

    return {
        "owner": "mixed",
        "owners": sorted(owners),
        "target_phase": None,
        "decision_required": True,
        "target_phase_required": bool(owners & {"implementation", "test"}),
        "findings": blocking_findings,
    }


def _rework_save_and_reset(
    body: Dict[str, Any],
    *,
    prefer_task_branch_base: bool = False,
) -> None:
    """Save uncommitted diff and reset repo before plan/spec rework."""
    from nodeflow.workflows.dev_process.phase_git import (
        reset_to_ref,
        save_uncommitted_diff,
    )

    dp = body.get("dev_process") or {}
    run_context = body["run_context"]
    repo = _phase_repo_root(body)
    results = dp.get("phase_results", {})
    phase_id = dp.get("current_phase_id", "")

    start_ref: str | None = None
    if prefer_task_branch_base:
        start_ref = (dp.get("task_branch") or {}).get("base_ref") or None
    if not start_ref and phase_id:
        start_ref = results.get(phase_id, {}).get("phase_start_git_ref")
    if not start_ref and dp.get("total_phases", 0) > 0:
        start_ref = results.get("phase_000", {}).get("phase_start_git_ref")
    if not start_ref:
        start_ref = (dp.get("task_branch") or {}).get("base_ref")
    if not start_ref:
        return

    if not phase_id:
        total = dp.get("total_phases", 0)
        phase_id = f"phase_{total - 1:03d}" if total > 0 else "spec_rework"

    diff_info = save_uncommitted_diff(
        repo,
        artifact_root=run_context["artifact_root"],
        phase_id=phase_id,
        artifact_roots=[run_context["artifact_root"]],
    )
    dp.setdefault("rework_backup", {})[phase_id] = diff_info

    untracked_list_path = diff_info.get("untracked_list_path", "")
    clean_files: list[str] = []
    if untracked_list_path:
        txt = Path(untracked_list_path).read_text(encoding="utf-8").strip()
        clean_files = [f for f in txt.splitlines() if f.strip()]

    expected_branch = dp.get("task_branch", {}).get("name", "")
    reset_to_ref(repo, start_ref, clean_untracked=clean_files, expected_branch=expected_branch)


def _effective_rework_owner(
    owner: str,
    dp: Dict[str, Any],
    rework_provided: Dict[str, Any],
) -> str:
    """Resolve loop-counter owner before increment (final review may supply owner via CLI)."""
    final_req = dp.get("final_rework_required")
    if not isinstance(final_req, dict):
        return owner
    if final_req.get("decision_required"):
        resolved = str(rework_provided.get("owner") or "").strip()
        if resolved:
            return resolved
    req_owner = str(final_req.get("owner") or "").strip()
    if req_owner and req_owner not in ("mixed", ""):
        return req_owner
    return owner


def _validate_final_rework_inputs(
    dp: Dict[str, Any], rework_provided: Dict[str, Any]
) -> None:
    """Fail fast before loop counters increment when final rework inputs are incomplete."""
    final_req = dp.get("final_rework_required")
    if not isinstance(final_req, dict):
        return
    if final_req.get("decision_required"):
        resolved_owner = str(rework_provided.get("owner") or "").strip()
        if not resolved_owner:
            raise NodeExecutionFailure(
                "final_review rework with mixed owners requires explicit owner; "
                f"available owners: {final_req.get('owners')}"
            )
        if resolved_owner in ("implementation", "test"):
            if not str(rework_provided.get("target_phase") or "").strip():
                raise NodeExecutionFailure(
                    f"final_review rework with owner={resolved_owner!r} requires "
                    "explicit target_phase; provide target_phase like 'phase_001'"
                )
    elif final_req.get("target_phase_required"):
        if not str(rework_provided.get("target_phase") or "").strip():
            raise NodeExecutionFailure(
                "final_review rework requires explicit target_phase; "
                "provide target_phase like 'phase_001'"
            )


def _handle_rework(
    body: Dict[str, Any],
    *,
    run_id: str,
    force_review_blocking: bool,
    interactive: bool,
    rework_provided: Dict[str, Any],
    from_human_gate: bool = False,
) -> Dict[str, Any]:
    workspace_context = body.get("workspace_context")
    if not isinstance(workspace_context, dict):
        raise NodeExecutionFailure(
            "rework_implementation requires workspace_context from prior continue_implementation"
        )
    run_context = body["run_context"]
    review_st = body.get("stages", {}).get("review")
    _status("Collecting rework inputs...")
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

    from nodeflow.workflows.dev_process.phase_loop import check_loop_limit, increment_loop_counter

    owner = str(body.get("rework_owner") or "implementation")

    dp = body.get("dev_process") or {}
    _validate_final_rework_inputs(dp, rework_provided)
    effective_owner = _effective_rework_owner(owner, dp, rework_provided)

    phase_id = dp.get("current_phase_id", "")
    if not from_human_gate:
        if effective_owner == "plan":
            loop_key = "plan_revision"
        elif effective_owner == "spec":
            loop_key = "spec_revision"
        elif effective_owner == "test" and phase_id:
            loop_key = f"{phase_id}_test_rework"
        elif phase_id:
            loop_key = f"{phase_id}_implementation_rework"
        else:
            loop_key = "implementation_rework"
        check_loop_limit(dp, loop_key)
        increment_loop_counter(dp, loop_key)

    if effective_owner in ("spec", "plan") and dp.get("total_phases"):
        if dp.get("planning_mode") != "continuation_from_head":
            _rework_save_and_reset(
                body,
                prefer_task_branch_base=(effective_owner == "spec"),
            )

    if effective_owner == "spec":
        dp["spec_rework_epoch_bump"] = True
        dp.pop("final_rework_required", None)
        mark_stale(body, upstream="spec")
        return _handle_revise_spec(
            body,
            run_id=run_id,
            interactive=interactive,
            revision_provided={"revision_comment": rework_context},
            use_human_comment=True,
        )
    final_req_snapshot = dp.get("final_rework_required")
    final_findings = (
        list(final_req_snapshot.get("findings") or [])
        if isinstance(final_req_snapshot, dict)
        else []
    )

    if owner == "plan":
        from nodeflow.workflows.dev_process.contract_check import count_completed_prefix

        total = dp.get("total_phases", 0)
        completed_count = count_completed_prefix(dp.get("phase_results", {}), total)
        if total and completed_count == total:
            from nodeflow.workflows.dev_process.contract_check import (
                enter_continuation_planning_mode,
            )

            enter_continuation_planning_mode(
                dp,
                findings=final_findings or list(dp.get("continuation_findings", [])),
                completed_count=completed_count,
            )
        dp.pop("final_rework_required", None)
        mark_stale(body, upstream="plan")
        return _handle_revise_plan(
            body,
            run_id=run_id,
            interactive=interactive,
            revision_provided={"revision_comment": rework_context},
        )

    final_req = dp.get("final_rework_required")
    if isinstance(final_req, dict):
        from nodeflow.workflows.dev_process.phase_rewind import rewind_to_phase

        if final_req.get("decision_required"):
            resolved_owner = str(rework_provided.get("owner") or "").strip()
            if not resolved_owner:
                raise NodeExecutionFailure(
                    "final_review rework with mixed owners requires explicit owner; "
                    f"available owners: {final_req.get('owners')}"
                )
            if resolved_owner in ("spec", "plan"):
                from nodeflow.workflows.dev_process.contract_check import count_completed_prefix

                if resolved_owner == "plan":
                    total = dp.get("total_phases", 0)
                    completed_count = count_completed_prefix(dp.get("phase_results", {}), total)
                    if total and completed_count == total:
                        from nodeflow.workflows.dev_process.contract_check import (
                            enter_continuation_planning_mode,
                        )

                        enter_continuation_planning_mode(
                            dp,
                            findings=list(final_req.get("findings") or []),
                            completed_count=completed_count,
                        )
                dp.pop("final_rework_required", None)
                body["rework_owner"] = resolved_owner
                if resolved_owner == "spec":
                    if dp.get("total_phases"):
                        _rework_save_and_reset(body, prefer_task_branch_base=True)
                    dp["spec_rework_epoch_bump"] = True
                    mark_stale(body, upstream="spec")
                    return _handle_revise_spec(
                        body,
                        run_id=run_id,
                        interactive=interactive,
                        revision_provided={"revision_comment": rework_context},
                        use_human_comment=True,
                    )
                mark_stale(body, upstream="plan")
                return _handle_revise_plan(
                    body,
                    run_id=run_id,
                    interactive=interactive,
                    revision_provided={"revision_comment": rework_context},
                )
            if resolved_owner in ("implementation", "test"):
                target_phase = str(rework_provided.get("target_phase") or "").strip()
                if not target_phase:
                    raise NodeExecutionFailure(
                        f"final_review rework with owner={resolved_owner!r} requires "
                        "explicit target_phase; provide target_phase like 'phase_001'"
                    )
                rewind_info = rewind_to_phase(
                    dp,
                    _phase_repo_root(body),
                    target_phase=target_phase,
                    owner=resolved_owner,
                )
                dp["last_rewind"] = rewind_info
                dp.pop("final_rework_required", None)
                return _handle_continue_implementation(
                    body,
                    run_id=run_id,
                    force_review_blocking=force_review_blocking,
                    rework_context=rework_context,
                    skip_implementation=False,
                )
            raise NodeExecutionFailure(
                f"unsupported resolved owner {resolved_owner!r} for mixed final rework; "
                f"available: {final_req.get('owners')}"
            )

        if final_req.get("target_phase_required"):
            target_phase = str(rework_provided.get("target_phase") or "").strip()
            if not target_phase:
                raise NodeExecutionFailure(
                    "final_review rework requires explicit target_phase; "
                    "provide target_phase like 'phase_001'"
                )
            rw_owner = str(final_req.get("owner") or owner)
            rewind_info = rewind_to_phase(
                dp,
                _phase_repo_root(body),
                target_phase=target_phase,
                owner=rw_owner,
            )
            dp["last_rewind"] = rewind_info
            dp.pop("final_rework_required", None)

            return _handle_continue_implementation(
                body,
                run_id=run_id,
                force_review_blocking=force_review_blocking,
                rework_context=rework_context,
                skip_implementation=False,
            )

    total = dp.get("total_phases", 0)
    rewound = False
    if total and dp.get("phase_index", 0) >= total:
        from nodeflow.workflows.dev_process.phase_loop import invalidate_phases_from
        from nodeflow.workflows.dev_process.phase_rewind import rewind_to_phase

        last_idx = total - 1
        last_id = f"phase_{last_idx:03d}"
        results = dp.get("phase_results", {})
        start_ref = results.get(last_id, {}).get("phase_start_git_ref")
        if start_ref:
            rewind_info = rewind_to_phase(
                dp,
                _phase_repo_root(body),
                target_phase=last_id,
                owner=owner,
            )
            dp["last_rewind"] = rewind_info
            rewound = True
        else:
            invalidate_phases_from(dp, last_idx)

    skip_impl = False if rewound else (owner == "test")
    return _handle_continue_implementation(
        body,
        run_id=run_id,
        force_review_blocking=force_review_blocking,
        rework_context=rework_context,
        skip_implementation=skip_impl,
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
