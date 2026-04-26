"""Build stdin prompt for wide file-scan review."""

from __future__ import annotations

import json
from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.nodes.development_flow.review_pipe.review_parse import REVIEW_JSON_CONTRACT_TEXT


class BuildWideScanReviewPromptNode(PythonActionNode):
    role = "build_wide_scan_review_prompt"

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

        mission = (
            "Run a wider change-set review (not a repository-wide file scan). "
            "Focus on architectural side-effects, cross-file contract drift, "
            "and risky dependencies that may be outside the primary edit scope.\n\n"
        )
        text = (
            f"{mission}"
            f"{REVIEW_JSON_CONTRACT_TEXT}\n\n"
            f"## Base ref\n{base_ref}\n\n"
            "## Git status (short)\n"
            f"{status_short or '(empty)'}\n\n"
            "## Untracked paths\n"
            f"{json.dumps(untracked, ensure_ascii=False) if untracked else '[]'}\n\n"
            "## Untracked file excerpts\n"
            f"{json.dumps(excerpts, ensure_ascii=False, indent=2) if excerpts else '[]'}\n\n"
            "## Git diff\n"
            f"{diff_clipped or '(empty diff)'}\n"
        )
        return {"codex_task_prompt": {"text": text}}
