"""DevelopmentFlowPipeNode — orchestrate stage pipes via checkpoint/resume actions."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import (
    BaseNode,
    ExecutionContext,
    NodeExecutionFailure,
    NodeExecutionLimit,
)
from nodeflow.core.node_kinds import PipeNode
from nodeflow.workflows.development_flow.common.check_source_workspace import (
    CheckSourceWorkspaceNode,
)
from nodeflow.workflows.development_flow.common.git_repo import resolve_git_toplevel
from nodeflow.workflows.development_flow.common.prepare_development_run_context import (
    PrepareDevelopmentRunContextNode,
)
from nodeflow.workflows.development_flow.common.prepare_workspace import PrepareWorkspaceNode
from nodeflow.workflows.development_flow.common.write_development_summary import (
    WriteDevelopmentSummaryNode,
)
from nodeflow.workflows.development_flow.implement import ImplementPipeNode
from nodeflow.workflows.development_flow.profiles import apply_profiles_to_pipe_params
from nodeflow.workflows.development_flow.review import ReviewPipeNode
from nodeflow.workflows.development_flow.spec_plan import SpecPlanPipeNode
from nodeflow.workflows.development_flow.state_machine import (
    require_state,
    review_allowed_actions,
    validate_merge_gate,
)


def _as_path(base: Path, raw: str | None) -> Path | None:
    if not raw:
        return None
    p = Path(raw)
    return p if p.is_absolute() else (base / p).resolve()


def _read_json_required(path: Path, *, label: str) -> Dict[str, Any]:
    if not path.exists():
        raise NodeExecutionFailure(f"{label} not found: {path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise NodeExecutionFailure(f"{label} is not valid JSON: {path}") from e
    except OSError as e:
        raise NodeExecutionFailure(f"{label} could not be read: {path}") from e
    if not isinstance(obj, dict):
        raise NodeExecutionFailure(f"{label} must be a JSON object: {path}")
    return obj


def _require_same_source_repo(input_repo_root: Path, run_context: Dict[str, Any]) -> Path:
    saved = run_context.get("source_repo_root")
    if not isinstance(saved, str) or not saved.strip():
        raise NodeExecutionFailure("run_context.source_repo_root is required")
    saved_root = resolve_git_toplevel(Path(saved).resolve())
    input_root = resolve_git_toplevel(input_repo_root.resolve())
    if input_root != saved_root:
        raise NodeExecutionFailure(
            "repo_root does not match checkpoint source_repo_root: "
            f"input={input_root}, checkpoint={saved_root}"
        )
    return saved_root


def _extract_stage_checkpoint_path(stage_result: Dict[str, Any]) -> str | None:
    arts = stage_result.get("artifacts")
    if not isinstance(arts, list):
        return None
    for a in reversed(arts):
        if isinstance(a, dict) and a.get("kind") == "checkpoint":
            p = a.get("path")
            if isinstance(p, str):
                return p
    return None


class DevelopmentFlowPipeNode(PipeNode):
    """Top-level orchestration: start/approve/rework/revise_spec/merge/force_merge."""

    def __init__(self) -> None:
        super().__init__()
        self._check_source_workspace = CheckSourceWorkspaceNode()
        self._prepare_development_run_context = PrepareDevelopmentRunContextNode()
        self._prepare_workspace = PrepareWorkspaceNode()
        self._write_development_summary = WriteDevelopmentSummaryNode()
        self._spec_plan = SpecPlanPipeNode()
        self._implement = ImplementPipeNode()
        self._review = ReviewPipeNode()

    def _write_flow_checkpoint(
        self,
        *,
        repo_root: Path,
        params: Dict[str, Any],
        flow_result: Dict[str, Any],
        run_id: str,
        action: str,
    ) -> str:
        cp_dir = Path(str(params.get("checkpoint_dir") or ".nodeflow/checkpoints"))
        if not cp_dir.is_absolute():
            cp_dir = (repo_root / cp_dir).resolve()
        cp_dir.mkdir(parents=True, exist_ok=True)
        fp = cp_dir / f"{run_id}_{action}_flow.json"
        flow_for_disk = dict(flow_result)
        flow_for_disk["flow_checkpoint_path"] = str(fp)
        payload = {
            "schema_version": str(
                params.get("flow_checkpoint_schema_version") or "development_flow.flow.v1"
            ),
            "written_at": datetime.now(timezone.utc).isoformat(),
            "flow_result": flow_for_disk,
        }
        fp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(fp)

    def _review_context_from_checkpoint(
        self, review_cp: Path, prev_flow: Dict[str, Any]
    ) -> Dict[str, Any]:
        review_obj = _read_json_required(review_cp, label="previous review checkpoint")
        stage = review_obj.get("stage_result")
        if not isinstance(stage, dict):
            raise NodeExecutionFailure("previous review checkpoint missing stage_result")
        review_rr = (stage.get("raw_results") or {}).get("review_result") or {}
        if not isinstance(review_rr, dict):
            raise NodeExecutionFailure(
                "previous review checkpoint missing raw_results.review_result"
            )
        return {
            "blocking_findings": review_rr.get("blocking_findings") or [],
            "non_blocking_findings": review_rr.get("non_blocking_findings") or [],
            "spec_revision_needed": bool(review_rr.get("spec_revision_needed")),
            "previous_review_checkpoint_path": str(review_cp),
            "previous_approved_checkpoint_path": prev_flow.get("approved_checkpoint_path"),
        }

    def _raise_if_child_not_done(self, *, child_name: str, child: BaseNode) -> None:
        status = child.read_status()
        if status == "fatal":
            raise NodeExecutionFailure(f"{child_name} fatal: {child.read_error()}")
        if status == "limit":
            raise NodeExecutionLimit(f"{child_name} limit")
        if status != "done":
            raise NodeExecutionFailure(f"{child_name} unexpected status: {status}")

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        p = dict(params) if params else {}
        raw_action = inputs.get("action")
        if not isinstance(raw_action, str) or not raw_action.strip():
            raise NodeExecutionFailure("action is required")
        action = raw_action.strip()
        if "base_ref" in inputs:
            raise NodeExecutionFailure(
                "development_flow_pipe does not accept base_ref; "
                "checkout the desired source revision before start"
            )
        if "branch_name" in inputs:
            raise NodeExecutionFailure(
                "development_flow_pipe uses planned_branch_name, not branch_name"
            )
        if "approved_checkpoint_path" in inputs:
            raise NodeExecutionFailure(
                "development_flow_pipe does not accept approved_checkpoint_path; "
                "approve/rework use approved_candidate_path from flow checkpoint"
            )
        workspace = Path(str(p.get("_workspace_dir") or ".")).resolve()
        raw_repo_root = inputs.get("repo_root")
        if not isinstance(raw_repo_root, str) or not raw_repo_root.strip():
            raise NodeExecutionFailure("repo_root is required")
        repo_root = _as_path(workspace, raw_repo_root.strip()) or Path(raw_repo_root.strip())
        repo_root = resolve_git_toplevel(repo_root)

        apply_profiles_to_pipe_params(
            p,
            workspace=workspace,
            model_profiles_path=p.get("model_profiles_path")
            if isinstance(p.get("model_profiles_path"), str)
            else None,
            cost_profiles_path=p.get("cost_profiles_path")
            if isinstance(p.get("cost_profiles_path"), str)
            else None,
            model_profile=p.get("model_profile")
            if isinstance(p.get("model_profile"), str)
            else None,
            cost_profile=p.get("cost_profile") if isinstance(p.get("cost_profile"), str) else None,
        )

        flow_cp_in = _as_path(
            workspace,
            inputs.get("flow_checkpoint_path")
            if isinstance(inputs.get("flow_checkpoint_path"), str)
            else None,
        )
        if action == "start" and flow_cp_in is not None:
            raise NodeExecutionFailure("start does not accept flow_checkpoint_path")

        flow_cp_obj = (
            _read_json_required(flow_cp_in, label="flow_checkpoint_path")
            if flow_cp_in is not None
            else {}
        )
        prev_flow = (
            flow_cp_obj.get("flow_result")
            if isinstance(flow_cp_obj.get("flow_result"), dict)
            else {}
        )

        hc_text = ""
        hc_path = _as_path(
            workspace,
            inputs.get("human_comment_path")
            if isinstance(inputs.get("human_comment_path"), str)
            else None,
        )
        if hc_path and hc_path.exists():
            hc_text = hc_path.read_text(encoding="utf-8")
        elif isinstance(inputs.get("human_comment_text"), str):
            hc_text = str(inputs.get("human_comment_text"))

        def _attach_human(ctx: Dict[str, Any]) -> None:
            if hc_text.strip():
                ctx["human_comment_text"] = hc_text.strip()
            if hc_path:
                ctx["human_comment_path"] = str(hc_path)

        if action == "force_merge":
            if flow_cp_in is None or not flow_cp_in.exists():
                raise NodeExecutionFailure("flow_checkpoint_path is required for force_merge")
            flow_wrap = _read_json_required(flow_cp_in, label="flow_checkpoint_path")
            prev = flow_wrap.get("flow_result")
            prev_flow = prev if isinstance(prev, dict) else {}
            require_state(prev_flow, "awaiting_review_decision", action="force_merge")
            run_ctx = (
                prev_flow.get("run_context")
                if isinstance(prev_flow.get("run_context"), dict)
                else {}
            )
            if not run_ctx:
                raise NodeExecutionFailure(
                    "run_context is required in flow checkpoint for force_merge"
                )
            force_run_id = str(run_ctx.get("run_id") or "").strip()
            if not force_run_id:
                raise NodeExecutionFailure("run_context.run_id is required in flow checkpoint")
            workspace_ctx = (
                prev_flow.get("workspace_context")
                if isinstance(prev_flow.get("workspace_context"), dict)
                else None
            )
            if workspace_ctx is None:
                raise NodeExecutionFailure(
                    "workspace_context is required in flow checkpoint for force_merge"
                )
            dev_summary = (
                prev_flow.get("development_summary")
                if isinstance(prev_flow.get("development_summary"), dict)
                else None
            )
            if dev_summary is None:
                raise NodeExecutionFailure(
                    "development_summary is required in flow checkpoint for force_merge"
                )
            repo_root = _require_same_source_repo(repo_root, run_ctx)
            flow_result = {
                "ok": True,
                "merge_ready": False,
                "state": "merged",
                "forced": True,
                "previous_flow_checkpoint_path": str(flow_cp_in),
                "force_merge_reason": hc_text.strip() if hc_text.strip() else None,
                "human_comment_text": hc_text.strip() if hc_text.strip() else None,
                "human_comment_path": str(hc_path) if hc_path else None,
                "human_decision_required": False,
                "allowed_actions": ["stop"],
                "run_context": run_ctx,
                "workspace_context": workspace_ctx,
                "development_summary": dev_summary,
            }
            fp = self._write_flow_checkpoint(
                repo_root=repo_root,
                params=dict(p.get("flow_checkpoint") or {}),
                flow_result=flow_result,
                run_id=force_run_id,
                action=action,
            )
            flow_result["flow_checkpoint_path"] = fp
            return {"flow_result": flow_result}

        if action == "merge":
            if flow_cp_in is None or not flow_cp_in.exists():
                raise NodeExecutionFailure("flow_checkpoint_path is required for merge")
            flow_wrap = _read_json_required(flow_cp_in, label="flow_checkpoint_path")
            prev = flow_wrap.get("flow_result")
            if not isinstance(prev, dict):
                raise NodeExecutionFailure("flow checkpoint missing flow_result")
            validate_merge_gate(prev)
            run_ctx = prev.get("run_context") if isinstance(prev.get("run_context"), dict) else {}
            if not run_ctx:
                raise NodeExecutionFailure("run_context is required in flow checkpoint for merge")
            merge_run_id = str(run_ctx.get("run_id") or "").strip()
            if not merge_run_id:
                raise NodeExecutionFailure("run_context.run_id is required in flow checkpoint")
            workspace_ctx = (
                prev.get("workspace_context")
                if isinstance(prev.get("workspace_context"), dict)
                else None
            )
            if workspace_ctx is None:
                raise NodeExecutionFailure(
                    "workspace_context is required in flow checkpoint for merge"
                )
            dev_summary = (
                prev.get("development_summary")
                if isinstance(prev.get("development_summary"), dict)
                else None
            )
            if dev_summary is None:
                raise NodeExecutionFailure(
                    "development_summary is required in flow checkpoint for merge"
                )
            repo_root = _require_same_source_repo(repo_root, run_ctx)
            flow_result = {
                "ok": True,
                "merge_ready": True,
                "state": "merged",
                "human_decision_required": False,
                "allowed_actions": ["stop"],
                "run_context": run_ctx,
                "workspace_context": workspace_ctx,
                "development_summary": dev_summary,
                "previous_flow_checkpoint_path": str(flow_cp_in),
            }
            fp = self._write_flow_checkpoint(
                repo_root=repo_root,
                params=dict(p.get("flow_checkpoint") or {}),
                flow_result=flow_result,
                run_id=merge_run_id,
                action=action,
            )
            flow_result["flow_checkpoint_path"] = fp
            return {"flow_result": flow_result}

        if action in ("start", "revise_spec"):
            revision_context: Dict[str, Any] | None = None
            task_prompt = str(inputs.get("task_prompt") or "")
            run_context: Dict[str, Any] = {}
            workspace_context: Dict[str, Any] | None = None
            source_workspace_check: Dict[str, Any] | None = None
            if action == "start":
                self._check_source_workspace.reset_status()
                source_check_out = self._check_source_workspace.execute(
                    {"source_repo_root": str(repo_root)},
                    dict(p.get("check_source_workspace") or {}),
                )
                self._raise_if_child_not_done(
                    child_name="check_source_workspace",
                    child=self._check_source_workspace,
                )
                source_workspace_check = (
                    source_check_out.get("source_workspace_check")
                    if isinstance(source_check_out.get("source_workspace_check"), dict)
                    else {}
                )
                prepare_params = dict(p.get("prepare_development_run_context") or {})
                self._prepare_development_run_context.reset_status()
                run_ctx_out = self._prepare_development_run_context.execute(
                    {
                        "source_workspace_check": source_workspace_check,
                        "planned_branch_name": inputs.get("planned_branch_name"),
                        "run_id": inputs.get("run_id"),
                        "development_name": inputs.get("development_name"),
                        "task_prompt": task_prompt,
                    },
                    prepare_params,
                )
                self._raise_if_child_not_done(
                    child_name="prepare_development_run_context",
                    child=self._prepare_development_run_context,
                )
                run_context = (
                    run_ctx_out.get("run_context")
                    if isinstance(run_ctx_out.get("run_context"), dict)
                    else {}
                )
            if action == "revise_spec":
                if flow_cp_in is None or not flow_cp_in.exists():
                    raise NodeExecutionFailure("flow_checkpoint_path is required for revise_spec")
                flow_wrap = _read_json_required(flow_cp_in, label="flow_checkpoint_path")
                prev_rev = (
                    flow_wrap.get("flow_result")
                    if isinstance(flow_wrap.get("flow_result"), dict)
                    else {}
                )
                require_state(prev_rev, "awaiting_review_decision", action="revise_spec")
                run_context = (
                    prev_rev.get("run_context")
                    if isinstance(prev_rev.get("run_context"), dict)
                    else {}
                )
                repo_root = _require_same_source_repo(repo_root, run_context)
                if not str(run_context.get("source_base_revision") or "").strip():
                    raise NodeExecutionFailure(
                        "run_context.source_base_revision is required for revise_spec "
                        "(resume from a flow checkpoint produced by start with a current NodeFlow)"
                    )
                frozen_base = str(run_context.get("source_base_revision") or "").strip()
                self._check_source_workspace.reset_status()
                source_check_out = self._check_source_workspace.execute(
                    {"source_repo_root": str(repo_root)},
                    dict(p.get("check_source_workspace") or {}),
                )
                self._raise_if_child_not_done(
                    child_name="check_source_workspace",
                    child=self._check_source_workspace,
                )
                source_workspace_check = (
                    source_check_out.get("source_workspace_check")
                    if isinstance(source_check_out.get("source_workspace_check"), dict)
                    else {}
                )
                head_now = str(source_workspace_check.get("base_revision") or "").strip()
                if not head_now:
                    raise NodeExecutionFailure(
                        "check_source_workspace did not return base_revision"
                    )
                if head_now != frozen_base:
                    raise NodeExecutionFailure(
                        "source repository HEAD changed since flow start; "
                        "reset or stash previous implementation edits before revise_spec"
                    )
                # revise_spec invalidates implementation workspace context;
                # next approve must prepare a fresh workspace from run_context.
                workspace_context = None
                review_path_raw = prev_rev.get("review_checkpoint_path")
                if not isinstance(review_path_raw, str) or not review_path_raw.strip():
                    raise NodeExecutionFailure(
                        "revise_spec requires review_checkpoint_path in flow checkpoint"
                    )
                review_cp = _as_path(workspace, review_path_raw.strip())
                if review_cp is None or not review_cp.exists():
                    raise NodeExecutionFailure(
                        f"previous review checkpoint not found: {review_path_raw}"
                    )
                revision_context = self._review_context_from_checkpoint(review_cp, prev_rev)
                _attach_human(revision_context)
                if not task_prompt:
                    restored = (
                        prev_rev.get("task_prompt")
                        if isinstance(prev_rev.get("task_prompt"), str)
                        else None
                    )
                    if isinstance(restored, str) and restored.strip():
                        task_prompt = restored
                    else:
                        raise NodeExecutionFailure(
                            "revise_spec requires task_prompt input or flow_result.task_prompt"
                        )

            spec_params = dict(p.get("spec_plan_pipe") or {})
            if "_workspace_dir" in p:
                spec_params["_workspace_dir"] = p["_workspace_dir"]
            self._spec_plan.reset_status()
            spec_inputs: Dict[str, Any] = {
                "task_prompt": task_prompt,
                "repo_root": str(repo_root),
                "base_ref": str(run_context.get("source_base_revision") or ""),
                "revision_context": revision_context,
            }
            if not spec_inputs["base_ref"].strip():
                raise NodeExecutionFailure("run_context.source_base_revision is required")
            art = run_context.get("artifact_root")
            if isinstance(art, str) and art.strip():
                spec_inputs["artifact_root"] = art.strip()
            spec_out = self._spec_plan.execute(spec_inputs, spec_params)
            self._raise_if_child_not_done(child_name="spec_plan_pipe", child=self._spec_plan)
            sr = (
                spec_out.get("stage_result")
                if isinstance(spec_out.get("stage_result"), dict)
                else {}
            )
            allowed = ["approve", "stop"] if sr.get("ok") else ["start", "stop"]
            flow_result: Dict[str, Any] = {
                "ok": bool(sr.get("ok")),
                "merge_ready": False,
                "state": "awaiting_approval",
                "human_decision_required": True,
                "allowed_actions": allowed,
                "task_prompt": task_prompt,
                "stage_result": sr,
                "spec_plan_checkpoint_path": _extract_stage_checkpoint_path(sr),
                "approved_candidate_path": sr.get("approved_candidate_path"),
                "run_context": run_context,
                "workspace_context": workspace_context,
            }
            flow_run_id = str(run_context.get("run_id") or "").strip()
            if not flow_run_id:
                raise NodeExecutionFailure("run_context.run_id is required")
            fp = self._write_flow_checkpoint(
                repo_root=repo_root,
                params=dict(p.get("flow_checkpoint") or {}),
                flow_result=flow_result,
                run_id=flow_run_id,
                action=action,
            )
            flow_result["flow_checkpoint_path"] = fp
            return {"flow_result": flow_result}

        if action in ("approve", "rework_implementation"):
            if flow_cp_in is None or not flow_cp_in.exists():
                raise NodeExecutionFailure(
                    "flow_checkpoint_path is required for approve/rework_implementation"
                )
            resume_prev: Dict[str, Any] = dict(prev_flow)
            if flow_cp_in and flow_cp_in.exists():
                wrap_ap = _read_json_required(flow_cp_in, label="flow_checkpoint_path")
                inner = wrap_ap.get("flow_result")
                if isinstance(inner, dict):
                    resume_prev = inner
            if action == "approve":
                require_state(resume_prev, "awaiting_approval", action="approve")
            else:
                require_state(
                    resume_prev, "awaiting_review_decision", action="rework_implementation"
                )
            run_context = (
                resume_prev.get("run_context")
                if isinstance(resume_prev.get("run_context"), dict)
                else {}
            )
            if not run_context:
                raise NodeExecutionFailure("run_context is required in flow checkpoint")
            repo_root = _require_same_source_repo(repo_root, run_context)
            prev_workspace_context = (
                resume_prev.get("workspace_context")
                if isinstance(resume_prev.get("workspace_context"), dict)
                else None
            )
            if action == "rework_implementation" and prev_workspace_context is None:
                raise NodeExecutionFailure(
                    "workspace_context is required for rework_implementation"
                )

            frozen_base = str(run_context.get("source_base_revision") or "").strip()
            if not frozen_base:
                raise NodeExecutionFailure(
                    "run_context.source_base_revision is required for approve/rework_implementation"
                )

            approved_path: str | None = (
                resume_prev.get("approved_checkpoint_path")
                if isinstance(resume_prev.get("approved_checkpoint_path"), str)
                else None
            ) or (
                resume_prev.get("approved_candidate_path")
                if isinstance(resume_prev.get("approved_candidate_path"), str)
                else None
            )
            if not approved_path:
                raise NodeExecutionFailure(
                    "flow checkpoint must contain approved_candidate_path or approved_checkpoint_path"
                )

            if action == "rework_implementation":
                if flow_cp_in is None or not flow_cp_in.exists():
                    raise NodeExecutionFailure(
                        "flow_checkpoint_path is required for rework_implementation"
                    )
                _read_json_required(flow_cp_in, label="flow_checkpoint_path")
                rp = resume_prev.get("review_checkpoint_path")
                if not isinstance(rp, str) or not rp.strip():
                    raise NodeExecutionFailure(
                        "rework_implementation requires review_checkpoint_path in flow checkpoint"
                    )
                rc = _as_path(workspace, rp.strip())
                if rc is None or not rc.exists():
                    raise NodeExecutionFailure(f"previous review checkpoint not found: {rp}")
                review_ctx = self._review_context_from_checkpoint(rc, resume_prev)
                _attach_human(review_ctx)
            else:
                review_ctx = {}
                _attach_human(review_ctx)

            source_for_prepare = str(repo_root)
            self._prepare_workspace.reset_status()
            workspace_out = self._prepare_workspace.execute(
                {
                    "source_repo_root": source_for_prepare,
                    "run_context": run_context,
                    "workspace_context": prev_workspace_context,
                },
                dict(p.get("prepare_workspace") or {}),
            )
            self._raise_if_child_not_done(
                child_name="prepare_workspace",
                child=self._prepare_workspace,
            )
            workspace_context = (
                workspace_out.get("workspace_context")
                if isinstance(workspace_out.get("workspace_context"), dict)
                else {}
            )
            execution_root = workspace_context.get("workspace_root")
            if not isinstance(execution_root, str) or not execution_root.strip():
                raise NodeExecutionFailure("prepare_workspace missing workspace_root")
            base_revision = workspace_context.get("base_revision")
            if not isinstance(base_revision, str) or not base_revision.strip():
                raise NodeExecutionFailure("prepare_workspace missing base_revision")

            impl_params = dict(p.get("implement_pipe") or {})
            review_params = dict(p.get("review_pipe") or {})
            impl_params["_workspace_dir"] = execution_root
            review_params["_workspace_dir"] = execution_root

            self._implement.reset_status()
            impl_out = self._implement.execute(
                {
                    "approved_checkpoint_path": approved_path,
                    "repo_root": execution_root,
                    "artifact_root": run_context.get("artifact_root"),
                    "base_ref": base_revision,
                    "task_type": "implement",
                    "rework_context": review_ctx if action == "rework_implementation" else None,
                },
                impl_params,
            )
            self._raise_if_child_not_done(child_name="implement_pipe", child=self._implement)
            impl_sr = (
                impl_out.get("stage_result")
                if isinstance(impl_out.get("stage_result"), dict)
                else {}
            )

            self._review.reset_status()
            review_out = self._review.execute(
                {
                    "approved_checkpoint_path": approved_path,
                    "repo_root": execution_root,
                    "artifact_root": run_context.get("artifact_root"),
                    "base_ref": base_revision,
                    "task_type": "review",
                },
                review_params,
            )
            self._raise_if_child_not_done(child_name="review_pipe", child=self._review)
            review_sr = (
                review_out.get("stage_result")
                if isinstance(review_out.get("stage_result"), dict)
                else {}
            )

            flow_ok = bool(impl_sr.get("ok")) and bool(review_sr.get("ok"))
            merge_ready = flow_ok and review_sr.get("next_action") == "merge"
            allowed_review = review_allowed_actions(
                flow_ok=flow_ok, review_next_action=review_sr.get("next_action")
            )

            flow_result = {
                "ok": flow_ok,
                "merge_ready": merge_ready,
                "state": "awaiting_review_decision",
                "human_decision_required": True,
                "allowed_actions": allowed_review,
                "task_prompt": resume_prev.get("task_prompt"),
                "next_action": review_sr.get("next_action"),
                "implement_stage_result": impl_sr,
                "review_stage_result": review_sr,
                "approved_checkpoint_path": approved_path,
                "spec_plan_checkpoint_path": resume_prev.get("spec_plan_checkpoint_path"),
                "approved_candidate_path": resume_prev.get("approved_candidate_path"),
                "implement_checkpoint_path": _extract_stage_checkpoint_path(impl_sr),
                "review_checkpoint_path": _extract_stage_checkpoint_path(review_sr),
                "run_context": run_context,
                "workspace_context": workspace_context,
            }
            summary_params = dict(p.get("development_summary") or {})
            self._write_development_summary.reset_status()
            devsum_out = self._write_development_summary.execute(
                {
                    "workspace_context": workspace_context,
                    "run_context": run_context,
                    "action": action,
                    "task_prompt": str(flow_result.get("task_prompt") or ""),
                    "implement_stage_result": impl_sr,
                    "review_stage_result": review_sr,
                    "next_action": flow_result.get("next_action"),
                    "merge_ready": flow_result.get("merge_ready"),
                },
                summary_params,
            )
            self._raise_if_child_not_done(
                child_name="write_development_summary",
                child=self._write_development_summary,
            )
            if isinstance(devsum_out.get("development_summary"), dict):
                flow_result["development_summary"] = devsum_out["development_summary"]
            flow_run_id = str(run_context.get("run_id") or "").strip()
            if not flow_run_id:
                raise NodeExecutionFailure("run_context.run_id is required")
            fp = self._write_flow_checkpoint(
                repo_root=repo_root,
                params=dict(p.get("flow_checkpoint") or {}),
                flow_result=flow_result,
                run_id=flow_run_id,
                action=action,
            )
            flow_result["flow_checkpoint_path"] = fp
            return {"flow_result": flow_result}

        raise NodeExecutionFailure(f"unsupported action: {action}")
