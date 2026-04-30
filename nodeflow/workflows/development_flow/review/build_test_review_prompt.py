"""Build stdin prompt for test-focused review."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.workflows.development_flow.review.prompt_common import (
    extract_diff_context,
    render_common_context,
)


class BuildTestReviewPromptNode(PythonActionNode):
    role = "build_test_review_prompt"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        base_ref, diff_clipped, status_short, untracked, _excerpts = extract_diff_context(
            inputs, params
        )
        mission = (
            "Run a test-focused review. "
            "Find likely missing tests, flaky assertions, and behavior that should be covered "
            "by unit/integration/end-to-end checks.\n\n"
        )
        text = render_common_context(
            mission=mission,
            base_ref=base_ref,
            status_short=status_short,
            untracked=untracked,
            excerpts=None,
            diff_clipped=diff_clipped,
        )
        return {"codex_task_prompt": {"text": text}}
