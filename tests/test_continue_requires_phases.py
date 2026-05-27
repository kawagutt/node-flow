"""continue_implementation requires phase-based plan state."""

from __future__ import annotations

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.flow_actions import _handle_continue_implementation


def test_continue_without_phases_raises() -> None:
    body = {
        "dev_process": {"total_phases": 0, "phase_results": {}},
        "run_context": {"artifact_root": "/tmp", "repo_root": "/tmp"},
        "stages": {},
    }
    with pytest.raises(NodeExecutionFailure, match="Phase-based plan is required"):
        _handle_continue_implementation(
            body,
            run_id="r1",
            force_review_blocking=False,
        )
