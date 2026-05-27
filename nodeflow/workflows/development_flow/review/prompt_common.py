"""Shared helpers for review prompt builders."""

from __future__ import annotations

import json
from typing import Any, Dict, List

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.development_flow.review.review_parse import REVIEW_JSON_CONTRACT_TEXT

REVIEW_REPOSITORY_INSPECTION_TEXT = """## Review scope (required)
Do not rely only on the provided diff_result.
Review the current repository state and all files relevant to the requested change,
including implementation files, tests, configs, generated artifacts, and documentation when applicable.
If diff_result is incomplete, stale, or missing, inspect the repository directly before concluding.
"""


def as_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, indent=2)


def extract_diff_context(
    inputs: Dict[str, Any], params: Dict[str, Any] | Any
) -> tuple[str, str, str, List[Any], List[Any]]:
    diff_result = inputs.get("diff_result") if isinstance(inputs.get("diff_result"), dict) else {}
    raw_base_ref = inputs.get("base_ref")
    if not isinstance(raw_base_ref, str) or not raw_base_ref.strip():
        raise NodeExecutionFailure("base_ref is required")
    base_ref = raw_base_ref.strip()
    max_chars = int(params.get("max_diff_chars", 12000))
    diff_clipped = str(diff_result.get("diff") or "")[:max_chars]

    status_short = str(diff_result.get("status_short") or "")
    untracked = diff_result.get("untracked_files")
    if not isinstance(untracked, list):
        untracked = []
    excerpts = diff_result.get("untracked_file_excerpts")
    if not isinstance(excerpts, list):
        excerpts = []
    return base_ref, diff_clipped, status_short, untracked, excerpts


def resolve_reviewer_mission(params: Any, default: str) -> str:
    """Use per-node SKILL text when provided via ``reviewer_mission`` param."""
    custom = params.get("reviewer_mission") if hasattr(params, "get") else None
    if isinstance(custom, str) and custom.strip():
        return custom.strip() + "\n\n"
    return default


def render_common_context(
    *,
    mission: str,
    base_ref: str,
    status_short: str,
    untracked: List[Any],
    excerpts: List[Any] | None,
    diff_clipped: str,
    status_title: str = "## Git status (short)",
    untracked_title: str = "## Untracked paths",
    excerpts_title: str = "## Untracked file excerpts",
    diff_title: str = "## Git diff (committed changes since base ref)",
    extra_sections: str = "",
) -> str:
    excerpt_block = (
        f"{excerpts_title}\n{json.dumps(excerpts, ensure_ascii=False, indent=2) if excerpts else '[]'}\n\n"
        if excerpts is not None
        else ""
    )
    return (
        f"{mission}"
        f"{REVIEW_REPOSITORY_INSPECTION_TEXT}\n"
        f"{REVIEW_JSON_CONTRACT_TEXT}\n\n"
        f"{extra_sections}"
        f"## Base ref\n{base_ref}\n\n"
        f"{status_title}\n{status_short or '(empty)'}\n\n"
        f"{untracked_title}\n{json.dumps(untracked, ensure_ascii=False) if untracked else '[]'}\n\n"
        f"{excerpt_block}"
        f"{diff_title}\n{diff_clipped or '(empty diff)'}\n"
    )
