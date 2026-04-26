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
from nodeflow.nodes.development_flow.development_flow_pipe.profiles import (
    apply_profiles_to_pipe_params,
)
from nodeflow.nodes.development_flow.development_flow_pipe.state_machine import (
    require_state,
    review_allowed_actions,
    validate_merge_gate,
)
from nodeflow.nodes.development_flow.implement_pipe import ImplementPipeNode
from nodeflow.nodes.development_flow.review_pipe import ReviewPipeNode
from nodeflow.nodes.development_flow.spec_plan_pipe import SpecPlanPipeNode


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
        action = str(inputs.get("action") or "start")
        workspace = Path(str(p.get("_workspace_dir") or ".")).resolve()
        repo_root = _as_path(workspace, str(inputs.get("repo_root") or ".")) or workspace
        run_id = str(p.get("run_id") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))

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
            _read_json_required(flow_cp_in, label="flow_checkpoint_path")
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
            }
            fp = self._write_flow_checkpoint(
                repo_root=repo_root,
                params=dict(p.get("flow_checkpoint") or {}),
                flow_result=flow_result,
                run_id=run_id,
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
            flow_result = {
                "ok": True,
                "merge_ready": True,
                "state": "merged",
                "human_decision_required": False,
                "allowed_actions": ["stop"],
            }
            fp = self._write_flow_checkpoint(
                repo_root=repo_root,
                params=dict(p.get("flow_checkpoint") or {}),
                flow_result=flow_result,
                run_id=run_id,
                action=action,
            )
            flow_result["flow_checkpoint_path"] = fp
            return {"flow_result": flow_result}

        if action in ("start", "revise_spec"):
            revision_context: Dict[str, Any] | None = None
            task_prompt = str(inputs.get("task_prompt") or "")
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
                    if not restored:
                        prev_stage = prev_rev.get("stage_result")
                        prev_raw = (
                            prev_stage.get("raw_results") if isinstance(prev_stage, dict) else {}
                        )
                        restored = (
                            prev_raw.get("task_prompt")
                            if isinstance(prev_raw, dict)
                            and isinstance(prev_raw.get("task_prompt"), str)
                            else None
                        )
                    if isinstance(restored, str) and restored.strip():
                        task_prompt = restored

            spec_params = dict(p.get("spec_plan_pipe") or {})
            if "_workspace_dir" in p:
                spec_params["_workspace_dir"] = p["_workspace_dir"]
            self._spec_plan.reset_status()
            spec_out = self._spec_plan.execute(
                {
                    "task_prompt": task_prompt,
                    "repo_root": str(repo_root),
                    "base_ref": str(inputs.get("base_ref") or "HEAD"),
                    "revision_context": revision_context,
                },
                spec_params,
            )
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
            }
            fp = self._write_flow_checkpoint(
                repo_root=repo_root,
                params=dict(p.get("flow_checkpoint") or {}),
                flow_result=flow_result,
                run_id=run_id,
                action=action,
            )
            flow_result["flow_checkpoint_path"] = fp
            return {"flow_result": flow_result}

        if action in ("approve", "rework_implementation"):
            resume_prev: Dict[str, Any] = dict(prev_flow)
            approved_raw = inputs.get("approved_checkpoint_path")
            has_approved_input = isinstance(approved_raw, str) and bool(approved_raw.strip())
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
            approved_path: str | None = (
                approved_raw.strip()
                if isinstance(approved_raw, str) and approved_raw.strip()
                else None
            )
            if not approved_path:
                approved_path = (
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
                    "approved_checkpoint_path is required unless resuming with flow_checkpoint_path "
                    "that contains approved_candidate_path or approved_checkpoint_path"
                )
            if not has_approved_input and not (flow_cp_in and flow_cp_in.exists()):
                raise NodeExecutionFailure(
                    "when omitting approved_checkpoint_path, flow_checkpoint_path is required"
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

            impl_params = dict(p.get("implement_pipe") or {})
            review_params = dict(p.get("review_pipe") or {})
            if "_workspace_dir" in p:
                impl_params["_workspace_dir"] = p["_workspace_dir"]
                review_params["_workspace_dir"] = p["_workspace_dir"]

            self._implement.reset_status()
            impl_out = self._implement.execute(
                {
                    "approved_checkpoint_path": approved_path,
                    "repo_root": str(repo_root),
                    "base_ref": str(inputs.get("base_ref") or "HEAD"),
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
                    "repo_root": str(repo_root),
                    "base_ref": str(inputs.get("base_ref") or "HEAD"),
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
            }
            fp = self._write_flow_checkpoint(
                repo_root=repo_root,
                params=dict(p.get("flow_checkpoint") or {}),
                flow_result=flow_result,
                run_id=run_id,
                action=action,
            )
            flow_result["flow_checkpoint_path"] = fp
            return {"flow_result": flow_result}

        raise NodeExecutionFailure(f"unsupported action: {action}")
