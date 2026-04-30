"""development_flow start action node."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PipeNode
from nodeflow.workflows.development_flow.check_source_workspace import CheckSourceWorkspaceNode
from nodeflow.workflows.development_flow.flow_action_context import prepare_action_context
from nodeflow.workflows.development_flow.flow_checkpoint import write_flow_checkpoint
from nodeflow.workflows.development_flow.flow_child_status import raise_if_child_not_done
from nodeflow.workflows.development_flow.flow_stage_result import extract_stage_checkpoint_path
from nodeflow.workflows.development_flow.prepare_development_run_context import (
    PrepareDevelopmentRunContextNode,
)
from nodeflow.workflows.development_flow.spec_plan import SpecPlanPipeNode


class StartPipeNode(PipeNode):
    """Run development_flow start action."""

    def __init__(self) -> None:
        super().__init__()
        self._check_source_workspace = CheckSourceWorkspaceNode()
        self._prepare_development_run_context = PrepareDevelopmentRunContextNode()
        self._spec_plan = SpecPlanPipeNode()

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        prepared = prepare_action_context(action="start", inputs=inputs, params=params)
        p = prepared["params"]
        repo_root = prepared["repo_root"]
        task_prompt = str(inputs.get("task_prompt") or "")

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
        raise_if_child_not_done(
            child_name="prepare_development_run_context",
            child=self._prepare_development_run_context,
        )
        run_context = (
            run_ctx_out.get("run_context")
            if isinstance(run_ctx_out.get("run_context"), dict)
            else {}
        )

        spec_params = dict(p.get("spec_plan") or {})
        if "_workspace_dir" in p:
            spec_params["_workspace_dir"] = p["_workspace_dir"]
        self._spec_plan.reset_status()
        spec_inputs: Dict[str, Any] = {
            "task_prompt": task_prompt,
            "repo_root": str(repo_root),
            "base_ref": str(run_context.get("source_base_revision") or ""),
            "revision_context": None,
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
            action="start",
        )
        flow_result["flow_checkpoint_path"] = fp
        return {"flow_result": flow_result}
