"""Shared flow context helpers for dev-process."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process import timeline
from nodeflow.workflows.dev_process.checkpoint import write_flow_checkpoint
from nodeflow.workflows.dev_process.constants import (
    STATE_AWAITING_REWORK_DECISION,
    STATE_FAILED,
    V2_CHECKPOINT_STAGES,
    WORKSPACE_STRATEGY_CURRENT_REPO,
    WORKSPACE_STRATEGY_GIT_WORKTREE,
)
from nodeflow.workflows.dev_process.paths import (
    planned_branch_name_for_attempt,
    workspace_attempt_subdir,
)
from nodeflow.workflows.dev_process.reuse import remove_git_worktree, run_context_for_df
from nodeflow.workflows.dev_process.state_machine import (
    allowed_actions_for_state,
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
    """Drop an existing implementation worktree; bump attempt only when one was active."""
    wc = body.get("workspace_context")
    if not isinstance(wc, dict) or not wc:
        return
    run_context = body["run_context"]
    if _workspace_strategy(run_context) != WORKSPACE_STRATEGY_GIT_WORKTREE:
        body.pop("workspace_context", None)
        return
    root = wc.get("workspace_root")
    if isinstance(root, str) and root.strip():
        remove_git_worktree(
            source_repo_root=run_context["repo_root"],
            artifact_root=run_context["artifact_root"],
            workspace_root=root,
        )
    body.pop("workspace_context", None)
    _increment_workspace_attempt(body)


def _policy_snapshot(dp: Dict[str, Any]) -> Dict[str, Any]:
    snap = dp.get("exec_policy_snapshot")
    return snap if isinstance(snap, dict) else {}


def _stored_exec_model(body: Dict[str, Any]) -> Optional[str]:
    dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
    snap = _policy_snapshot(dp)
    raw = snap.get("default_model") or dp.get("exec_model")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return None


def _stored_exec_argv(body: Dict[str, Any]) -> Optional[list[str]]:
    dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
    snap = _policy_snapshot(dp)
    raw = snap.get("default_argv") if snap else dp.get("exec_argv")
    if raw is None:
        return None
    if not isinstance(raw, list) or not all(isinstance(x, str) for x in raw):
        return None
    return raw


def _resolve_exec_argv(
    exec_argv: Optional[list[str]],
    *,
    body: Optional[Dict[str, Any]] = None,
) -> Optional[list[str]]:
    if exec_argv is not None:
        return exec_argv
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
    return {name: {"status": "pending"} for name in V2_CHECKPOINT_STAGES}


def _read_spec_text(artifact_root: str) -> str:
    return (Path(artifact_root) / "spec" / "spec.md").read_text(encoding="utf-8")


def _read_plan_text(artifact_root: str) -> str:
    return (Path(artifact_root) / "plan" / "plan.md").read_text(encoding="utf-8")


def _read_approved_text(artifact_root: str) -> Tuple[str, str]:
    return _read_spec_text(artifact_root), _read_plan_text(artifact_root)


def _combine_revision_context(findings_context: str, human_context: str) -> str:
    parts: list[str] = []
    if findings_context.strip():
        parts.append(findings_context.strip())
    if human_context.strip():
        parts.append(human_context.strip())
    return "\n\n".join(parts)


def _revision_context_from_spec_review(body: Dict[str, Any]) -> str:
    sr = (body.get("stages") or {}).get("spec_review") or {}
    agg = sr.get("aggregate") if isinstance(sr, dict) else {}
    if not isinstance(agg, dict):
        return ""
    findings = agg.get("blocking_findings") or []
    lines = ["## Spec review findings"]
    for f in findings:
        if isinstance(f, dict):
            lines.append(f"- {f.get('summary', f)}")
    return "\n".join(lines)


def _revision_context_from_plan_review(body: Dict[str, Any]) -> str:
    pr = (body.get("stages") or {}).get("plan_review") or {}
    agg = pr.get("aggregate") if isinstance(pr, dict) else {}
    if not isinstance(agg, dict):
        return ""
    findings = agg.get("blocking_findings") or []
    lines = ["## Plan review findings"]
    for f in findings:
        if isinstance(f, dict):
            lines.append(f"- {f.get('summary', f)}")
    return "\n".join(lines)


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
    spec_rev = (
        _review_spec_revision_needed(body) if state == STATE_AWAITING_REWORK_DECISION else False
    )
    actions = allowed_actions_for_state(
        state,
        merge_ready=merge_ready if state == STATE_AWAITING_REWORK_DECISION else None,
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
