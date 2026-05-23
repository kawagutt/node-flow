"""dev_process.review_prompt.* leaf nodes (reuse.py boundary)."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.workflows.dev_process.review_prompts._runner import run_review_prompt_build


def _make_prompt_node(reviewer_key: str, role: str) -> type[PythonActionNode]:
    class _Node(PythonActionNode):
        pass

    _Node.role = role  # type: ignore[attr-defined]

    def run(
        self: PythonActionNode,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        del params, context
        return run_review_prompt_build(reviewer_key, inputs)

    _Node.run = run  # type: ignore[method-assign]
    _Node.__name__ = f"DevProcessReview{reviewer_key}_Node"
    return _Node


DevProcessReviewDiffPromptNode = _make_prompt_node("review_diff", "dev_process_review_prompt_diff")
DevProcessReviewWideScanPromptNode = _make_prompt_node(
    "review_wide", "dev_process_review_prompt_wide_scan"
)
DevProcessReviewTestsPromptNode = _make_prompt_node(
    "review_tests", "dev_process_review_prompt_tests"
)
DevProcessReviewSpecConformancePromptNode = _make_prompt_node(
    "review_spec", "dev_process_review_prompt_spec_conformance"
)
DevProcessReviewSpecRevisionPromptNode = _make_prompt_node(
    "review_spec_revision", "dev_process_review_prompt_spec_revision"
)

__all__ = [
    "DevProcessReviewDiffPromptNode",
    "DevProcessReviewWideScanPromptNode",
    "DevProcessReviewTestsPromptNode",
    "DevProcessReviewSpecConformancePromptNode",
    "DevProcessReviewSpecRevisionPromptNode",
]
