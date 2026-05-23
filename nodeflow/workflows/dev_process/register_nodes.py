"""Register dev_process.* node types (not development_flow.*)."""

from __future__ import annotations

from nodeflow.core.registry import registry
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)
from nodeflow.workflows.dev_process.review_prompts.node_review_prompts import (
    DevProcessReviewDiffPromptNode,
    DevProcessReviewSpecConformancePromptNode,
    DevProcessReviewSpecRevisionPromptNode,
    DevProcessReviewTestsPromptNode,
    DevProcessReviewWideScanPromptNode,
)


def register_dev_process_nodes() -> None:
    registry.register("dev_process.flow", DevProcessFlowNode, override=True)
    registry.register(
        "dev_process.review_prompt.diff",
        DevProcessReviewDiffPromptNode,
        override=True,
    )
    registry.register(
        "dev_process.review_prompt.wide_scan",
        DevProcessReviewWideScanPromptNode,
        override=True,
    )
    registry.register(
        "dev_process.review_prompt.tests",
        DevProcessReviewTestsPromptNode,
        override=True,
    )
    registry.register(
        "dev_process.review_prompt.spec_conformance",
        DevProcessReviewSpecConformancePromptNode,
        override=True,
    )
    registry.register(
        "dev_process.review_prompt.spec_revision",
        DevProcessReviewSpecRevisionPromptNode,
        override=True,
    )
