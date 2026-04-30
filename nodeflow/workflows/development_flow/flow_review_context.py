from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.development_flow.flow_checkpoint import read_json_required


def review_context_from_checkpoint(review_cp: Path, prev_flow: Dict[str, Any]) -> Dict[str, Any]:
    review_obj = read_json_required(review_cp, label="previous review checkpoint")
    stage = review_obj.get("stage_result")
    if not isinstance(stage, dict):
        raise NodeExecutionFailure("previous review checkpoint missing stage_result")
    review_rr = (stage.get("raw_results") or {}).get("review_result") or {}
    if not isinstance(review_rr, dict):
        raise NodeExecutionFailure("previous review checkpoint missing raw_results.review_result")
    return {
        "blocking_findings": review_rr.get("blocking_findings") or [],
        "non_blocking_findings": review_rr.get("non_blocking_findings") or [],
        "spec_revision_needed": bool(review_rr.get("spec_revision_needed")),
        "previous_review_checkpoint_path": str(review_cp),
        "previous_approved_checkpoint_path": prev_flow.get("approved_checkpoint_path"),
    }


def attach_human_to_context(*, context: Dict[str, Any], text: str, path: Path | None) -> None:
    if text.strip():
        context["human_comment_text"] = text.strip()
    if path:
        context["human_comment_path"] = str(path)
