"""Build stdin prompt for diff-focused review (includes diff text + JSON contract)."""

from __future__ import annotations

import json
from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.nodes.development_flow.review_pipe.review_parse import REVIEW_JSON_CONTRACT_TEXT


class BuildDiffReviewPromptNode(PythonActionNode):
    role = "build_diff_review_prompt"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        diff_result = (
            inputs.get("diff_result") if isinstance(inputs.get("diff_result"), dict) else {}
        )
        base_ref = str(inputs.get("base_ref") or "HEAD")
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

        mission = (
            "Review the following implementation diff. "
            "Focus on correctness, unintended scope changes, missing error handling, "
            "bad naming, broken contracts, and likely test gaps. "
            "Do not comment on style-only issues unless they affect maintainability.\n\n"
        )

        text = (
            f"{mission}"
            f"{REVIEW_JSON_CONTRACT_TEXT}\n\n"
            f"## Base ref\n{base_ref}\n\n"
            "## Git status (short)\n"
            f"{status_block}\n\n"
            "## Untracked paths (git ls-files --others --exclude-standard)\n"
            f"{untracked_block}\n\n"
            "## Untracked file excerpts (text only; may be truncated)\n"
            f"{excerpt_block}\n\n"
            "## Git diff (working tree vs base ref)\n"
            f"{diff_clipped or '(empty diff)'}\n"
        )
        return {"codex_task_prompt": {"text": text}}
