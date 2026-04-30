from __future__ import annotations

from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.development_flow.flow_checkpoint import read_json_required
from nodeflow.workflows.development_flow.flow_paths import as_path, resolve_git_toplevel
from nodeflow.workflows.development_flow.profiles import apply_profiles_to_pipe_params


def prepare_action_context(
    *,
    action: str,
    inputs: Dict[str, Any],
    params: MappingProxyType | Dict[str, Any],
) -> Dict[str, Any]:
    p = dict(params) if params else {}
    if "base_ref" in inputs:
        raise NodeExecutionFailure(
            "workflows.development_flow does not accept base_ref; "
            "checkout the desired source revision before start"
        )
    if "branch_name" in inputs:
        raise NodeExecutionFailure("workflows.development_flow uses planned_branch_name, not branch_name")
    if "approved_checkpoint_path" in inputs:
        raise NodeExecutionFailure(
            "workflows.development_flow does not accept approved_checkpoint_path; "
            "approve/rework use approved_candidate_path from flow checkpoint"
        )
    workspace = Path(str(p.get("_workspace_dir") or ".")).resolve()
    raw_repo_root = inputs.get("repo_root")
    if not isinstance(raw_repo_root, str) or not raw_repo_root.strip():
        raise NodeExecutionFailure("repo_root is required")
    repo_root = as_path(workspace, raw_repo_root.strip()) or Path(raw_repo_root.strip())
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
        model_profile=p.get("model_profile") if isinstance(p.get("model_profile"), str) else None,
        cost_profile=p.get("cost_profile") if isinstance(p.get("cost_profile"), str) else None,
    )

    flow_cp_in = as_path(
        workspace,
        inputs.get("flow_checkpoint_path")
        if isinstance(inputs.get("flow_checkpoint_path"), str)
        else None,
    )
    if action == "start" and flow_cp_in is not None:
        raise NodeExecutionFailure("start does not accept flow_checkpoint_path")
    flow_cp_obj = read_json_required(flow_cp_in, label="flow_checkpoint_path") if flow_cp_in else {}
    prev_flow = (
        flow_cp_obj.get("flow_result") if isinstance(flow_cp_obj.get("flow_result"), dict) else {}
    )
    hc_text = ""
    hc_path = as_path(
        workspace,
        inputs.get("human_comment_path")
        if isinstance(inputs.get("human_comment_path"), str)
        else None,
    )
    if hc_path and hc_path.exists():
        hc_text = hc_path.read_text(encoding="utf-8")
    elif isinstance(inputs.get("human_comment_text"), str):
        hc_text = str(inputs.get("human_comment_text"))
    return {
        "params": p,
        "workspace": workspace,
        "repo_root": repo_root,
        "flow_checkpoint_path": flow_cp_in,
        "prev_flow": prev_flow,
        "human_comment_text": hc_text,
        "human_comment_path": hc_path,
    }
