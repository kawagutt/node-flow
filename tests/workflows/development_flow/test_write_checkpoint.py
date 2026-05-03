"""WriteCheckpointNode."""

from __future__ import annotations

import json
from pathlib import Path

from nodeflow.workflows.development_flow.write_checkpoint import WriteCheckpointNode


def test_write_checkpoint_ok_reflects_child_and_next_action_on_failure(tmp_path: Path) -> None:
    node = WriteCheckpointNode()
    cp_dir = str(tmp_path / "checkpoints")
    base_params = {
        "checkpoint_dir": cp_dir,
        "next_action_default": "review",
        "next_action_on_failure": "rework",
    }
    out = node.execute(
        {
            "request": {
                "stage": "implement",
                "summary": "done",
                "artifacts": [],
                "human_decision_required": True,
            },
            "execution_output": {
                "ok": False,
                "external_executor": "codex",
                "provider": "codex",
                "raw_output": {},
                "artifacts": [],
                "provider_meta": {},
            },
        },
        base_params,
    )
    sr = out["stage_result"]
    assert sr["ok"] is False
    assert sr["next_action"] == "rework"

    node.reset_status()
    out_stale = node.execute(
        {
            "request": {
                "stage": "spec_plan",
                "summary": "draft",
                "artifacts": [],
                "human_decision_required": True,
                "next_action": "approve",
            },
            "execution_output": {
                "ok": False,
                "external_executor": "codex",
                "provider": "codex",
                "raw_output": {},
                "artifacts": [],
                "provider_meta": {},
            },
        },
        {
            "checkpoint_dir": cp_dir,
            "next_action_default": "approve",
            "next_action_on_failure": "revise_spec",
        },
    )
    assert out_stale["stage_result"]["ok"] is False
    assert out_stale["stage_result"]["next_action"] == "revise_spec"

    node.reset_status()
    out2 = node.execute(
        {
            "request": {
                "stage": "implement",
                "summary": "done",
                "artifacts": [],
                "human_decision_required": True,
            },
            "execution_output": {
                "ok": True,
                "external_executor": "codex",
                "provider": "codex",
                "raw_output": {},
                "artifacts": [],
                "provider_meta": {},
            },
        },
        {
            "checkpoint_dir": cp_dir,
            "next_action_default": "review",
            "next_action_on_failure": "rework",
        },
    )
    sr2 = out2["stage_result"]
    assert sr2["ok"] is True
    assert sr2["next_action"] == "review"
    checkpoint_path = Path(sr2["artifacts"][-1]["path"])
    payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "development_flow.v1"


def test_write_spec_plan_writes_approved_candidate(tmp_path: Path) -> None:
    cp = tmp_path / "checkpoints"
    slim = {"spec": "# SPEC\nx", "plan": "# PLAN\ny"}
    stdout = json.dumps(slim)
    node = WriteCheckpointNode()
    out = node.execute(
        {
            "request": {
                "stage": "spec_plan",
                "summary": "draft done",
                "artifacts": [],
                "human_decision_required": True,
            },
            "execution_output": {
                "ok": True,
                "stdout": stdout,
                "stderr": "",
                "external_executor": "codex",
                "provider": "codex",
                "raw_output": {},
                "artifacts": [],
                "provider_meta": {},
            },
        },
        {
            "checkpoint_dir": str(cp),
            "run_id": "001",
            "write_spec_plan_candidate": True,
            "spec_plan_candidate_suffix": "approved_candidate",
        },
    )
    sr = out["stage_result"]
    assert sr.get("approved_candidate_path")
    loaded = json.loads(Path(sr["approved_candidate_path"]).read_text(encoding="utf-8"))
    assert loaded == slim
    kinds = [a.get("kind") for a in sr.get("artifacts", [])]
    assert "spec_plan_candidate" in kinds
    assert "checkpoint" in kinds
