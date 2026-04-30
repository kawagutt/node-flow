"""Build stdin prompt dedicated to spec revision decisions."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.workflows.development_flow.review_pipe.prompt_common import (
    as_text,
    extract_diff_context,
    render_common_context,
)


class BuildSpecRevisionReviewPromptNode(PythonActionNode):
    role = "build_spec_revision_review_prompt"

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
            "Assess whether the approved SPEC/PLAN must be revised. "
            'Set "spec_revision_needed": true only when the currently observed change cannot be '
            "accepted by implementation rework alone.\n\n"
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
            extra_sections=extra_sections,
        )
        return {"codex_task_prompt": {"text": text}}
