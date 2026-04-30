"""Build stdin prompt for diff-focused review (includes diff text + JSON contract)."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.workflows.development_flow.review.prompt_common import (
    extract_diff_context,
    render_common_context,
)


class BuildDiffReviewPromptNode(PythonActionNode):
    role = "build_diff_review_prompt"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        base_ref, diff_clipped, status_short, untracked, excerpts = extract_diff_context(
            inputs, params
        )
        mission = (
            "Review the following implementation diff. "
            "Focus on correctness, unintended scope changes, missing error handling, "
            "bad naming, broken contracts, and likely test gaps. "
            "Do not comment on style-only issues unless they affect maintainability.\n\n"
        )
        text = render_common_context(
            mission=mission,
            base_ref=base_ref,
            status_short=status_short,
            untracked=untracked,
            excerpts=excerpts,
            diff_clipped=diff_clipped,
            untracked_title="## Untracked paths (git ls-files --others --exclude-standard)",
            excerpts_title="## Untracked file excerpts (text only; may be truncated)",
            diff_title="## Git diff (working tree vs base ref)",
        )
        return {"codex_task_prompt": {"text": text}}
