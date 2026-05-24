"""Audit contracts: checkpoint actions, evidence gates, path safety."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import (
    ACTION_REVISE_SPEC,
    ACTION_REWORK,
)
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)
from nodeflow.workflows.dev_process.evidence import (
    record_exec_evidence,
    validate_evidence_store,
)
from nodeflow.workflows.dev_process.paths import (
    checkpoint_path_under_artifact_root,
    validate_run_id,
)
from nodeflow.workflows.dev_process.review_presets import reviewer_keys_for_preset
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit


def _timeline_actions(artifact_root: Path) -> list[str]:
    lines = (artifact_root / "timeline.jsonl").read_text(encoding="utf-8").strip().splitlines()
    return [json.loads(line).get("action") for line in lines if line]


def test_revise_spec_checkpoint_and_timeline_action(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "rev"},
        {},
    )
    cp = start["flow_output"]["flow_result"]["flow_checkpoint_path"]
    out = DevProcessFlowNode().execute(
        {
            "action": ACTION_REVISE_SPEC,
            "repo_root": str(repo),
            "flow_checkpoint_path": cp,
            "task_prompt": "please revise",
        },
        {},
    )
    cp2 = out["flow_output"]["flow_result"]["flow_checkpoint_path"]
    assert cp2.endswith(f"_{ACTION_REVISE_SPEC}_flow.json")
    artifact_root = Path(out["flow_output"]["run_context"]["artifact_root"])
    assert ACTION_REVISE_SPEC in _timeline_actions(artifact_root)


def test_rework_checkpoint_and_timeline_action(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "rw"},
        {},
    )
    cp = start["flow_output"]["flow_result"]["flow_checkpoint_path"]
    appr = DevProcessFlowNode().execute(
        {
            "action": "approve_spec",
            "repo_root": str(repo),
            "flow_checkpoint_path": cp,
        },
        {},
    )
    cp2 = appr["flow_output"]["flow_result"]["flow_checkpoint_path"]
    rework = DevProcessFlowNode().execute(
        {
            "action": ACTION_REWORK,
            "repo_root": str(repo),
            "flow_checkpoint_path": cp2,
        },
        {},
    )
    cp3 = rework["flow_output"]["flow_result"]["flow_checkpoint_path"]
    assert cp3.endswith(f"_{ACTION_REWORK}_flow.json")
    artifact_root = Path(rework["flow_output"]["run_context"]["artifact_root"])
    actions = [a for a in _timeline_actions(artifact_root) if a]
    assert ACTION_REWORK in actions


def test_validate_evidence_store_detects_manual_marker(tmp_path) -> None:
    artifact_root = tmp_path / "run"
    evidence_dir = artifact_root / "evidence"
    evidence_dir.mkdir(parents=True)
    doc = {
        "evidence_id": "e1",
        "run_id": "run-x",
        "stage": "implement",
        "invoker": "codex_exec",
        "execution_fingerprint": "fp1",
        "stdout_sha256": "a" * 64,
        "stderr_sha256": "b" * 64,
        "prompt_sha256": "c" * 64,
        "argv": ["x"],
        "cwd": "/r",
        "started_at": "2026-01-01T00:00:00+00:00",
        "ended_at": "2026-01-01T00:00:01+00:00",
        "exit_code": 0,
        "provider_meta": {"marker": "manual"},
    }
    (evidence_dir / "bad.json").write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(NodeExecutionFailure, match="stub/manual"):
        validate_evidence_store(str(artifact_root), run_id="run-x")


def test_validate_evidence_store_detects_nonzero_exit_code(tmp_path) -> None:
    artifact_root = tmp_path / "run"
    evidence_dir = artifact_root / "evidence"
    evidence_dir.mkdir(parents=True)
    doc = {
        "evidence_id": "e2",
        "run_id": "run-y",
        "stage": "implement",
        "invoker": "codex_exec",
        "execution_fingerprint": "fp2",
        "stdout_sha256": "a" * 64,
        "stderr_sha256": "b" * 64,
        "prompt_sha256": "c" * 64,
        "argv": ["x"],
        "cwd": "/r",
        "started_at": "2026-01-01T00:00:00+00:00",
        "ended_at": "2026-01-01T00:00:01+00:00",
        "exit_code": 1,
        "provider_meta": {},
    }
    (evidence_dir / "exit1.json").write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(NodeExecutionFailure, match="exit_code"):
        validate_evidence_store(str(artifact_root), run_id="run-y")


def test_merge_revalidates_tampered_evidence(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "m"},
        {},
    )
    cp = start["flow_output"]["flow_result"]["flow_checkpoint_path"]
    appr = DevProcessFlowNode().execute(
        {"action": "approve_spec", "repo_root": str(repo), "flow_checkpoint_path": cp},
        {},
    )
    cp2 = appr["flow_output"]["flow_result"]["flow_checkpoint_path"]
    final = DevProcessFlowNode().execute(
        {"action": "approve_final", "repo_root": str(repo), "flow_checkpoint_path": cp2},
        {},
    )
    cp3 = final["flow_output"]["flow_result"]["flow_checkpoint_path"]
    artifact_root = Path(final["flow_output"]["run_context"]["artifact_root"])
    (artifact_root / "evidence" / "tampered.json").write_text("{not-json", encoding="utf-8")
    with pytest.raises(NodeExecutionFailure, match="invalid evidence JSON"):
        DevProcessFlowNode().execute(
            {"action": "merge", "repo_root": str(repo), "flow_checkpoint_path": cp3},
            {},
        )


def test_unsafe_run_id_rejected(tmp_path) -> None:
    with pytest.raises(NodeExecutionFailure, match="unsafe characters"):
        validate_run_id("../evil")
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    with pytest.raises(NodeExecutionFailure, match="unsafe characters"):
        DevProcessFlowNode().execute(
            {
                "action": "start",
                "repo_root": str(repo),
                "task_prompt": "x",
                "run_id": "../evil",
            },
            {},
        )


def test_checkpoint_path_rejects_traversal(tmp_path) -> None:
    root = tmp_path / "artifact"
    root.mkdir()
    (root / "checkpoints").mkdir()
    with pytest.raises(NodeExecutionFailure, match="bare name"):
        checkpoint_path_under_artifact_root(str(root), "../escape.json")


def test_review_prompt_registry_has_five_leaves() -> None:
    from nodeflow.core.registry import registry

    keys = (
        "dev_process.review_prompt.diff",
        "dev_process.review_prompt.wide_scan",
        "dev_process.review_prompt.tests",
        "dev_process.review_prompt.spec_conformance",
        "dev_process.review_prompt.spec_revision",
    )
    for key in keys:
        assert registry.get(key) is not None, key


def test_reviewer_order_is_deterministic() -> None:
    assert reviewer_keys_for_preset("light") == ("review_diff", "review_tests")
    assert reviewer_keys_for_preset("standard") == (
        "review_diff",
        "review_tests",
        "review_spec",
    )
    assert reviewer_keys_for_preset("deep") == (
        "review_diff",
        "review_wide",
        "review_tests",
        "review_spec",
        "review_spec_revision",
    )


def test_checkpoint_self_reference_mismatch_on_resume(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "selfref"},
        {"run_spec_plan_on_start": False},
    )
    cp = Path(start["flow_output"]["flow_result"]["flow_checkpoint_path"])
    doc = json.loads(cp.read_text(encoding="utf-8"))
    other = cp.parent / "other_flow.json"
    other.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(NodeExecutionFailure, match="self-reference mismatch"):
        DevProcessFlowNode().execute(
            {
                "action": "approve_spec",
                "repo_root": str(repo),
                "flow_checkpoint_path": str(other),
            },
            {},
        )


def test_merge_fails_when_evidence_files_removed(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "ev"},
        {},
    )
    cp = start["flow_output"]["flow_result"]["flow_checkpoint_path"]
    appr = DevProcessFlowNode().execute(
        {"action": "approve_spec", "repo_root": str(repo), "flow_checkpoint_path": cp},
        {},
    )
    cp2 = appr["flow_output"]["flow_result"]["flow_checkpoint_path"]
    final = DevProcessFlowNode().execute(
        {"action": "approve_final", "repo_root": str(repo), "flow_checkpoint_path": cp2},
        {},
    )
    cp3 = final["flow_output"]["flow_result"]["flow_checkpoint_path"]
    artifact_root = Path(final["flow_output"]["run_context"]["artifact_root"])
    for p in (artifact_root / "evidence").glob("*.json"):
        p.unlink()
    with pytest.raises(NodeExecutionFailure, match="evidence file missing"):
        DevProcessFlowNode().execute(
            {"action": "merge", "repo_root": str(repo), "flow_checkpoint_path": cp3},
            {},
        )


def test_approve_final_state_is_awaiting_merge(tmp_path) -> None:
    from nodeflow.workflows.dev_process.constants import STATE_AWAITING_FINAL

    assert STATE_AWAITING_FINAL == "awaiting_merge"
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "m"},
        {},
    )
    cp = start["flow_output"]["flow_result"]["flow_checkpoint_path"]
    appr = DevProcessFlowNode().execute(
        {"action": "approve_spec", "repo_root": str(repo), "flow_checkpoint_path": cp},
        {},
    )
    cp2 = appr["flow_output"]["flow_result"]["flow_checkpoint_path"]
    final = DevProcessFlowNode().execute(
        {"action": "approve_final", "repo_root": str(repo), "flow_checkpoint_path": cp2},
        {},
    )
    assert final["flow_output"]["flow_result"]["state"] == "awaiting_merge"


def test_revise_spec_allowed_from_awaiting_review(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "rev2"},
        {},
    )
    cp = start["flow_output"]["flow_result"]["flow_checkpoint_path"]
    appr = DevProcessFlowNode().execute(
        {"action": "approve_spec", "repo_root": str(repo), "flow_checkpoint_path": cp},
        {},
    )
    assert "revise_spec" in appr["flow_output"]["flow_result"]["allowed_actions"]


def test_record_rejects_nonzero_exit_code_at_write(tmp_path) -> None:
    with pytest.raises(NodeExecutionFailure, match="non-zero exit_code"):
        record_exec_evidence(
            artifact_root=str(tmp_path / "run"),
            run_id="r1",
            stage="implement",
            invoker="codex_exec",
            execution_output={
                "ok": False,
                "stdout": "",
                "stderr": "err",
                "raw_output": {"returncode": 1},
                "provider_meta": {},
            },
            argv=["false"],
            prompt="p",
            cwd="/r",
        )
