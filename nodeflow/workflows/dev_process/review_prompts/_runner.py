"""Shared build logic for dev_process review prompt leaf nodes."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from nodeflow.workflows.dev_process.reuse import build_review_prompt


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
    text = build_review_prompt(
        reviewer_key,
        repo_root=Path(str(repo_root)),
        base_revision=str(base_ref or "HEAD"),
        diff_result=diff_result if isinstance(diff_result, dict) else {},
        test_result=test_result if isinstance(test_result, dict) else {},
        approved_spec=approved_spec,
        approved_plan=approved_plan,
    )
    return {"codex_task_prompt": {"text": text}}
