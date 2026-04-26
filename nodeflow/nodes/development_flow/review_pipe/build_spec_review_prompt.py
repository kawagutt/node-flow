"""Build stdin prompt for spec conformance review (approved spec/plan + diff)."""

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
        diff_result = (
            inputs.get("diff_result") if isinstance(inputs.get("diff_result"), dict) else {}
        )
        base_ref = str(inputs.get("base_ref") or "HEAD")

        spec_text = _as_text(asp.get("spec"))
        plan_text = _as_text(asp.get("plan"))
        diff_text = str(diff_result.get("diff") or "")
        max_chars = int(params.get("max_diff_chars", 12000))
        diff_clipped = diff_text[:max_chars]

        status_short = str(diff_result.get("status_short") or "")
        untracked = diff_result.get("untracked_files")
        if not isinstance(untracked, list):
            untracked = []
        excerpts = diff_result.get("untracked_file_excerpts")
        if not isinstance(excerpts, list):
            excerpts = []

        status_block = status_short or "(empty)"
        untracked_block = json.dumps(untracked, ensure_ascii=False) if untracked else "[]"
        excerpt_block = json.dumps(excerpts, ensure_ascii=False, indent=2) if excerpts else "[]"

        text = (
            f"{REVIEW_JSON_CONTRACT_TEXT}\n\n"
            "Decide whether the diff conforms to the approved SPEC and PLAN. "
            'If the change requires revising the spec (not just the code), set "spec_revision_needed": true.\n\n'
            f"## Base ref\n{base_ref}\n\n"
            "## Approved SPEC\n"
            f"{spec_text}\n\n"
            "## Approved PLAN\n"
            f"{plan_text}\n\n"
            "## Git status (short)\n"
            f"{status_block}\n\n"
            "## Untracked paths\n"
            f"{untracked_block}\n\n"
            "## Untracked file excerpts\n"
            f"{excerpt_block}\n\n"
            "## Git diff (working tree vs base ref)\n"
            f"{diff_clipped or '(empty diff)'}\n"
        )
        return {"codex_task_prompt": {"text": text}}
