"""development_flow merge action node."""

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
from nodeflow.workflows.development_flow.flow_paths import require_same_source_repo
from nodeflow.workflows.development_flow.state_machine import validate_merge_gate


class MergePipeNode(PipeNode):
    """Run development_flow merge action."""

    def __init__(self) -> None:
        super().__init__()

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType | Dict[str, Any],
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        prepared = prepare_action_context(action="merge", inputs=inputs, params=params)
        p = prepared["params"]
        repo_root = prepared["repo_root"]
        flow_cp_in = prepared["flow_checkpoint_path"]
        if flow_cp_in is None or not flow_cp_in.exists():
            raise NodeExecutionFailure("flow_checkpoint_path is required for merge")
        flow_wrap = read_json_required(flow_cp_in, label="flow_checkpoint_path")
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
            raise NodeExecutionFailure("workspace_context is required in flow checkpoint for merge")
        dev_summary = (
            prev.get("development_summary")
            if isinstance(prev.get("development_summary"), dict)
            else None
        )
        if dev_summary is None:
            raise NodeExecutionFailure(
                "development_summary is required in flow checkpoint for merge"
            )
        repo_root = require_same_source_repo(repo_root, run_ctx)
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
        fp = write_flow_checkpoint(
            repo_root=repo_root,
            params=dict(p.get("flow_checkpoint") or {}),
            flow_result=flow_result,
            run_id=merge_run_id,
            action="merge",
        )
        flow_result["flow_checkpoint_path"] = fp
        return {"flow_result": flow_result}
