"""development_flow approve action node."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PipeNode
from nodeflow.workflows.development_flow.flow_action_context import prepare_action_context
from nodeflow.workflows.development_flow.flow_checkpoint import (
    read_json_required,
    write_flow_checkpoint,
)
from nodeflow.workflows.development_flow.flow_child_status import raise_if_child_not_done
from nodeflow.workflows.development_flow.flow_paths import require_same_source_repo
from nodeflow.workflows.development_flow.flow_review_context import attach_human_to_context
from nodeflow.workflows.development_flow.flow_stage_result import extract_stage_checkpoint_path
from nodeflow.workflows.development_flow.implement import ImplementPipeNode
from nodeflow.workflows.development_flow.prepare_workspace import PrepareWorkspaceNode
from nodeflow.workflows.development_flow.review import ReviewPipeNode
from nodeflow.workflows.development_flow.state_machine import require_state, review_allowed_actions
from nodeflow.workflows.development_flow.write_development_summary import (
    WriteDevelopmentSummaryNode,
)


class ApprovePipeNode(PipeNode):
    """Run development_flow approve action."""

    def __init__(self) -> None:
        super().__init__()
        self._prepare_workspace = PrepareWorkspaceNode()
        self._implement = ImplementPipeNode()
        self._review = ReviewPipeNode()
        self._write_development_summary = WriteDevelopmentSummaryNode()

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        prepared = prepare_action_context(action="approve", inputs=inputs, params=params)
        p = prepared["params"]
        repo_root = prepared["repo_root"]
        flow_cp_in = prepared["flow_checkpoint_path"]
        prev_flow = prepared["prev_flow"]
        hc_text = prepared["human_comment_text"]
        hc_path = prepared["human_comment_path"]

        if flow_cp_in is None or not flow_cp_in.exists():
            raise NodeExecutionFailure("flow_checkpoint_path is required for approve")
        resume_prev: Dict[str, Any] = dict(prev_flow)
        wrap_ap = read_json_required(flow_cp_in, label="flow_checkpoint_path")
        inner = wrap_ap.get("flow_result")
        if isinstance(inner, dict):
            resume_prev = inner
        require_state(resume_prev, "awaiting_approval", action="approve")
        run_context = (
            resume_prev.get("run_context")
            if isinstance(resume_prev.get("run_context"), dict)
            else {}
        )
        if not run_context:
            raise NodeExecutionFailure("run_context is required in flow checkpoint")
        repo_root = require_same_source_repo(repo_root, run_context)
        frozen_base = str(run_context.get("source_base_revision") or "").strip()
        if not frozen_base:
            raise NodeExecutionFailure(
                "run_context.source_base_revision is required for approve"
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

        review_ctx: Dict[str, Any] = {}
        attach_human_to_context(context=review_ctx, text=hc_text, path=hc_path)
        self._prepare_workspace.reset_status()
        workspace_out = self._prepare_workspace.execute(
            {
                "source_repo_root": str(repo_root),
                "run_context": run_context,
                "workspace_context": None,
            },
            dict(p.get("prepare_workspace") or {}),
        )
        raise_if_child_not_done(child_name="prepare_workspace", child=self._prepare_workspace)
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

        impl_params = dict(p.get("implement") or {})
        review_params = dict(p.get("review") or {})
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
                "rework_context": None,
            },
            impl_params,
        )
        raise_if_child_not_done(child_name="implement", child=self._implement)
        impl_sr = (
            impl_out.get("stage_result") if isinstance(impl_out.get("stage_result"), dict) else {}
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
        raise_if_child_not_done(child_name="review", child=self._review)
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
            "implement_checkpoint_path": extract_stage_checkpoint_path(impl_sr),
            "review_checkpoint_path": extract_stage_checkpoint_path(review_sr),
            "run_context": run_context,
            "workspace_context": workspace_context,
        }
        summary_params = dict(p.get("development_summary") or {})
        self._write_development_summary.reset_status()
        devsum_out = self._write_development_summary.execute(
            {
                "workspace_context": workspace_context,
                "run_context": run_context,
                "action": "approve",
                "task_prompt": str(flow_result.get("task_prompt") or ""),
                "implement_stage_result": impl_sr,
                "review_stage_result": review_sr,
                "next_action": flow_result.get("next_action"),
                "merge_ready": flow_result.get("merge_ready"),
            },
            summary_params,
        )
        raise_if_child_not_done(
            child_name="write_development_summary",
            child=self._write_development_summary,
        )
        if isinstance(devsum_out.get("development_summary"), dict):
            flow_result["development_summary"] = devsum_out["development_summary"]
        flow_run_id = str(run_context.get("run_id") or "").strip()
        if not flow_run_id:
            raise NodeExecutionFailure("run_context.run_id is required")
        fp = write_flow_checkpoint(
            repo_root=repo_root,
            params=dict(p.get("flow_checkpoint") or {}),
            flow_result=flow_result,
            run_id=flow_run_id,
            action="approve",
        )
        flow_result["flow_checkpoint_path"] = fp
        return {"flow_result": flow_result}
