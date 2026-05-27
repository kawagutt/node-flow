"""load_plan_data rejects old non-phase plan artifacts."""

from __future__ import annotations

import json

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.phase_loop import load_plan_data


def test_load_plan_data_rejects_non_phase_markdown(tmp_path) -> None:
    artifact = tmp_path / "run"
    plan_dir = artifact / "plan"
    plan_dir.mkdir(parents=True)
    (plan_dir / "plan.md").write_text("Old prose plan without phase headings.\n", encoding="utf-8")
    (plan_dir / "plan.json").write_text(
        json.dumps({"phases": [], "plan_sha256": "x"}),
        encoding="utf-8",
    )

    with pytest.raises(NodeExecutionFailure, match="old non-phase plan format"):
        load_plan_data(str(artifact))
