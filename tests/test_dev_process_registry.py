"""dev_process registry — distinct from legacy development_flow composite keys."""

from __future__ import annotations

from nodeflow.core.registry import registry
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)


def test_dev_process_flow_is_registered() -> None:
    assert registry.resolve("dev_process.flow") is DevProcessFlowNode


def test_dev_process_review_prompt_leaves_registered() -> None:
    for key in (
        "dev_process.review_prompt.diff",
        "dev_process.review_prompt.wide_scan",
        "dev_process.review_prompt.tests",
        "dev_process.review_prompt.spec_conformance",
        "dev_process.review_prompt.spec_revision",
    ):
        assert registry.get(key) is not None, key


def test_development_flow_composite_still_unregistered() -> None:
    """v1.6 removed development_flow.* pipes; dev_process is a new composite."""
    keys = [
        "workflows.development_flow",
        "development_flow",
        "workflows.development_flow.spec_plan",
    ]
    for key in keys:
        assert registry.get(key) is None, f"expected {key} unregistered"
