"""Shared build logic for dev_process review prompt leaf nodes."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.reuse import build_review_prompt
from nodeflow.workflows.dev_process.review_presets import PRESET_DEEP, normalize_preset
from nodeflow.workflows.dev_process.review_config import review_node_name
from nodeflow.workflows.dev_process.review_prompt_limits import prompt_params_for_reviewer


def run_review_prompt_build(reviewer_key: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
    repo_root = inputs.get("repo_root")
    base_ref = inputs.get("base_ref") or inputs.get("base_revision")
    diff_result = inputs.get("diff_result") or {}
    test_result = inputs.get("test_result") or {}
    asp = inputs.get("approved_spec_plan") or {}
    if isinstance(asp, dict):
        approved_spec = str(asp.get("spec") or "")
        approved_plan = str(asp.get("plan") or "")
    else:
        approved_spec = ""
        approved_plan = ""
    preset_raw = inputs.get("review_depth_preset")
    if preset_raw is None:
        # Standalone registry leaves must support all five reviewer keys (use deep limits).
        preset = PRESET_DEEP
    else:
        preset = normalize_preset(str(preset_raw))
    try:
        limits = prompt_params_for_reviewer(preset, reviewer_key)
    except NodeExecutionFailure:
        # Registry leaves are callable in isolation; preset may omit this reviewer.
        limits = prompt_params_for_reviewer(PRESET_DEEP, reviewer_key)
    node_name = review_node_name(reviewer_key)
    text = build_review_prompt(
        node_name,
        repo_root=Path(str(repo_root)),
        base_revision=str(base_ref or "HEAD"),
        diff_result=diff_result if isinstance(diff_result, dict) else {},
        test_result=test_result if isinstance(test_result, dict) else {},
        approved_spec=approved_spec,
        approved_plan=approved_plan,
        prompt_params=limits,
    )
    return {"codex_task_prompt": {"text": text}}
