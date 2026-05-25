"""Build stdin prompt for spec conformance review (approved spec/plan + diff)."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.workflows.development_flow.review.prompt_common import (
    as_text,
    extract_diff_context,
    render_common_context,
)


class BuildSpecReviewPromptNode(PythonActionNode):
    role = "build_spec_review_prompt"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        asp = (
            inputs.get("approved_spec_plan")
            if isinstance(inputs.get("approved_spec_plan"), dict)
            else {}
        )
        base_ref, diff_clipped, status_short, untracked, excerpts = extract_diff_context(
            inputs, params
        )

        mission = (
            "Decide whether the diff conforms to the approved SPEC and PLAN. "
            'If the change requires revising the spec (not just the code), set "spec_revision_needed": true.\n\n'
        )
        extra_sections = (
            "## Approved SPEC\n"
            f"{as_text(asp.get('spec'))}\n\n"
            "## Approved PLAN\n"
            f"{as_text(asp.get('plan'))}\n\n"
        )
        text = render_common_context(
            mission=mission,
            base_ref=base_ref,
            status_short=status_short,
            untracked=untracked,
            excerpts=excerpts,
            diff_clipped=diff_clipped,
            diff_title="## Git diff (committed changes since base ref)",
            extra_sections=extra_sections,
        )
        return {"codex_task_prompt": {"text": text}}
