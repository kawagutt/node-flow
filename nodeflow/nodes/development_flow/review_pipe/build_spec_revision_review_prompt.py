"""Build stdin prompt dedicated to spec revision decisions."""

from __future__ import annotations

import json
from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.nodes.development_flow.review_pipe.review_parse import REVIEW_JSON_CONTRACT_TEXT


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, indent=2)


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
        diff_result = (
            inputs.get("diff_result") if isinstance(inputs.get("diff_result"), dict) else {}
        )
        base_ref = str(inputs.get("base_ref") or "HEAD")
        max_chars = int(params.get("max_diff_chars", 12000))
        diff_clipped = str(diff_result.get("diff") or "")[:max_chars]
        status_short = str(diff_result.get("status_short") or "")
        untracked = diff_result.get("untracked_files")
        if not isinstance(untracked, list):
            untracked = []
        excerpts = diff_result.get("untracked_file_excerpts")
        if not isinstance(excerpts, list):
            excerpts = []

        text = (
            f"{REVIEW_JSON_CONTRACT_TEXT}\n\n"
            "Assess whether the approved SPEC/PLAN must be revised. "
            'Set "spec_revision_needed": true only when the currently observed change cannot be '
            "accepted by implementation rework alone.\n\n"
            f"## Base ref\n{base_ref}\n\n"
            "## Git status (short)\n"
            f"{status_short or '(empty)'}\n\n"
            "## Untracked paths\n"
            f"{json.dumps(untracked, ensure_ascii=False)}\n\n"
            "## Untracked file excerpts\n"
            f"{json.dumps(excerpts, ensure_ascii=False, indent=2)}\n\n"
            "## Approved SPEC\n"
            f"{_as_text(asp.get('spec'))}\n\n"
            "## Approved PLAN\n"
            f"{_as_text(asp.get('plan'))}\n\n"
            "## Git diff\n"
            f"{diff_clipped or '(empty diff)'}\n"
        )
        return {"codex_task_prompt": {"text": text}}
