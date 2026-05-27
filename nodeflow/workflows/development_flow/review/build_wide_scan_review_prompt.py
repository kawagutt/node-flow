"""Build stdin prompt for wide file-scan review."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.workflows.development_flow.review.prompt_common import (
    extract_diff_context,
    render_common_context,
    resolve_reviewer_mission,
)


class BuildWideScanReviewPromptNode(PythonActionNode):
    role = "build_wide_scan_review_prompt"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        base_ref, diff_clipped, status_short, untracked, excerpts = extract_diff_context(
            inputs, params
        )
        mission = resolve_reviewer_mission(
            params,
            (
                "Run a wider change-set review (not a repository-wide file scan). "
                "Focus on architectural side-effects, cross-file contract drift, "
                "and risky dependencies that may be outside the primary edit scope.\n\n"
            ),
        )
        text = render_common_context(
            mission=mission,
            base_ref=base_ref,
            status_short=status_short,
            untracked=untracked,
            excerpts=excerpts,
            diff_clipped=diff_clipped,
        )
        return {"codex_task_prompt": {"text": text}}
