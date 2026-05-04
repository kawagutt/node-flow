"""LoadCheckpointNode."""

from __future__ import annotations

import json
from pathlib import Path

from nodeflow.workflows.development_flow.load_checkpoint import LoadCheckpointNode


def test_load_checkpoint_ok(tmp_path: Path) -> None:
    ck = tmp_path / "approved.json"
    ck.write_text(
        json.dumps({"spec": "SPEC body", "plan": "PLAN body"}),
        encoding="utf-8",
    )
    node = LoadCheckpointNode()
    out = node.execute(
        {
            "repo_root": str(tmp_path),
            "approved_checkpoint_path": "approved.json",
        },
        {},
    )
    assert node.read_status() == "done"
    ap = out["approved_spec_plan"]
    assert ap["spec"] == "SPEC body"
    assert ap["plan"] == "PLAN body"
    text = out["codex_task_prompt"]["text"]
    assert "SPEC body" in text and "PLAN body" in text


def test_load_checkpoint_missing_file_is_fatal(tmp_path: Path) -> None:
    node = LoadCheckpointNode()
    node.execute(
        {
            "repo_root": str(tmp_path),
            "approved_checkpoint_path": "missing.json",
        },
        {},
    )
    assert node.read_status() == "fatal"
    assert node.read_error() is not None
