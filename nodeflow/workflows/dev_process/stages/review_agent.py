"""Single review-agent stage (one agent = one leaf node; no mini-orchestrator)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.node_runner import review_argv_override_from_body, run_node_exec
from nodeflow.workflows.dev_process.reuse import build_review_prompt
from nodeflow.workflows.dev_process.review_config import review_node_name
from nodeflow.workflows.dev_process.review_presets import normalize_preset
from nodeflow.workflows.dev_process.review_prompt_limits import prompt_params_for_review_node


def run_one_review_agent_stage(
    *,
    agent: str,
    body: Dict[str, Any],
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    base_revision: str,
    approved_spec: str,
    approved_plan: str,
    diff_result: Dict[str, Any],
    test_result: Dict[str, Any],
    review_depth_preset: str = "standard",
    argv_override: Optional[list[str]] = None,
) -> Tuple[Dict[str, Any], str]:
    """Run one v1 review agent via ``run_node_exec`` (does not call ``run_review_stage``)."""
    preset = normalize_preset(review_depth_preset)
    node_name = review_node_name(agent)
    text = build_review_prompt(
        node_name,
        repo_root=repo_root,
        base_revision=base_revision,
        diff_result=diff_result,
        test_result=test_result,
        approved_spec=approved_spec,
        approved_plan=approved_plan,
        prompt_params=prompt_params_for_review_node(preset, node_name),
    )
    cwd = str(repo_root)
    argv_use = argv_override if argv_override is not None else review_argv_override_from_body(body)
    execution_output, evidence_path, _rec = run_node_exec(
        body,
        node_name=node_name,
        stage="review",
        prompt=text,
        cwd=cwd,
        run_id=run_id,
        artifact_root=artifact_root,
        invoker_override=agent,
        argv_override=argv_use,
    )
    if not execution_output.get("ok"):
        raise NodeExecutionFailure(f"review subprocess failed for {node_name}")
    return execution_output, evidence_path
