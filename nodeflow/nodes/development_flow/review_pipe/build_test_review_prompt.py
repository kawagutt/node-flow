"""Build stdin prompt for test-focused review."""

from __future__ import annotations

import json
from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.nodes.development_flow.review_pipe.review_parse import REVIEW_JSON_CONTRACT_TEXT


class BuildTestReviewPromptNode(PythonActionNode):
    role = "build_test_review_prompt"

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

        mission = (
            "Run a test-focused review. "
            "Find likely missing tests, flaky assertions, and behavior that should be covered "
            "by unit/integration/end-to-end checks.\n\n"
        )
        text = (
            f"{mission}"
            f"{REVIEW_JSON_CONTRACT_TEXT}\n\n"
            f"## Base ref\n{base_ref}\n\n"
            "## Git status (short)\n"
            f"{str(diff_result.get('status_short') or '(empty)')}\n\n"
            "## Untracked paths\n"
            f"{json.dumps(diff_result.get('untracked_files') or [], ensure_ascii=False)}\n\n"
            "## Git diff\n"
            f"{diff_clipped or '(empty diff)'}\n"
        )
        return {"codex_task_prompt": {"text": text}}
