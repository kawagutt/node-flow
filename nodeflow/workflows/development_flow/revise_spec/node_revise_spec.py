"""development_flow revise_spec action node."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PipeNode
from nodeflow.workflows.development_flow.check_source_workspace import CheckSourceWorkspaceNode
from nodeflow.workflows.development_flow.flow_action_context import prepare_action_context
from nodeflow.workflows.development_flow.flow_checkpoint import (
    read_json_required,
    write_flow_checkpoint,
)
from nodeflow.workflows.development_flow.flow_child_status import raise_if_child_not_done
from nodeflow.workflows.development_flow.flow_paths import as_path, require_same_source_repo
from nodeflow.workflows.development_flow.flow_review_context import (
    attach_human_to_context,
    review_context_from_checkpoint,
)
from nodeflow.workflows.development_flow.flow_stage_result import extract_stage_checkpoint_path
from nodeflow.workflows.development_flow.spec_plan import SpecPlanPipeNode
from nodeflow.workflows.development_flow.state_machine import require_state


class ReviseSpecPipeNode(PipeNode):
    """Run development_flow revise_spec action."""

    def __init__(self) -> None:
        super().__init__()
        self._check_source_workspace = CheckSourceWorkspaceNode()
        self._spec_plan = SpecPlanPipeNode()

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        prepared = prepare_action_context(action="revise_spec", inputs=inputs, params=params)
        p = prepared["params"]
        workspace = prepared["workspace"]
        repo_root = prepared["repo_root"]
        flow_cp_in = prepared["flow_checkpoint_path"]
        hc_text = prepared["human_comment_text"]
        hc_path = prepared["human_comment_path"]

        task_prompt = str(inputs.get("task_prompt") or "")
        if flow_cp_in is None or not flow_cp_in.exists():
            raise NodeExecutionFailure("flow_checkpoint_path is required for revise_spec")
        flow_wrap = read_json_required(flow_cp_in, label="flow_checkpoint_path")
        prev_rev = (
            flow_wrap.get("flow_result") if isinstance(flow_wrap.get("flow_result"), dict) else {}
        )
        require_state(prev_rev, "awaiting_review_decision", action="revise_spec")
        run_context = (
            prev_rev.get("run_context") if isinstance(prev_rev.get("run_context"), dict) else {}
        )
        repo_root = require_same_source_repo(repo_root, run_context)
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
        raise_if_child_not_done(
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
            raise NodeExecutionFailure("check_source_workspace did not return base_revision")
        if head_now != frozen_base:
            raise NodeExecutionFailure(
                "source repository HEAD changed since flow start; "
                "reset or stash previous implementation edits before revise_spec"
            )

        review_path_raw = prev_rev.get("review_checkpoint_path")
        if not isinstance(review_path_raw, str) or not review_path_raw.strip():
            raise NodeExecutionFailure(
                "revise_spec requires review_checkpoint_path in flow checkpoint"
            )
        review_cp = as_path(workspace, review_path_raw.strip())
        if review_cp is None or not review_cp.exists():
            raise NodeExecutionFailure(f"previous review checkpoint not found: {review_path_raw}")
        revision_context = review_context_from_checkpoint(review_cp, prev_rev)
        attach_human_to_context(context=revision_context, text=hc_text, path=hc_path)
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

        spec_params = dict(p.get("spec_plan") or {})
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
        raise_if_child_not_done(child_name="spec_plan", child=self._spec_plan)
        sr = spec_out.get("stage_result") if isinstance(spec_out.get("stage_result"), dict) else {}
        allowed = ["approve", "stop"] if sr.get("ok") else ["start", "stop"]
        flow_result: Dict[str, Any] = {
            "ok": bool(sr.get("ok")),
            "merge_ready": False,
            "state": "awaiting_approval",
            "human_decision_required": True,
            "allowed_actions": allowed,
            "task_prompt": task_prompt,
            "stage_result": sr,
            "spec_plan_checkpoint_path": extract_stage_checkpoint_path(sr),
            "approved_candidate_path": sr.get("approved_candidate_path"),
            "run_context": run_context,
            "workspace_context": None,
        }
        flow_run_id = str(run_context.get("run_id") or "").strip()
        if not flow_run_id:
            raise NodeExecutionFailure("run_context.run_id is required")
        fp = write_flow_checkpoint(
            repo_root=repo_root,
            params=dict(p.get("flow_checkpoint") or {}),
            flow_result=flow_result,
            run_id=flow_run_id,
            action="revise_spec",
        )
        flow_result["flow_checkpoint_path"] = fp
        return {"flow_result": flow_result}
