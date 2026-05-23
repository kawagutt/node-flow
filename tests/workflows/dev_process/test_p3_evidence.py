"""P3: exec evidence validation."""

from __future__ import annotations

import time

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.evidence import record_exec_evidence, validate_evidence_store


def _base_out() -> dict:
    return {"ok": True, "provider": "hermetic", "provider_meta": {}, "stdout": "ok", "stderr": ""}


def test_duplicate_execution_fingerprint_detected(tmp_path) -> None:
    artifact_root = str(tmp_path / "run")
    run_id = "run-abc"
    argv = ["python", "-c", "print(1)"]
    prompt = "same prompt"
    cwd = "/tmp/repo"
    started = "2026-01-01T00:00:00+00:00"
    ended = "2026-01-01T00:00:01+00:00"
    out = _base_out()
    record_exec_evidence(
        artifact_root=artifact_root,
        run_id=run_id,
        stage="implement",
        invoker="codex_exec",
        execution_output=out,
        argv=argv,
        prompt=prompt,
        cwd=cwd,
        started_at=started,
        ended_at=ended,
    )
    with pytest.raises(NodeExecutionFailure, match="execution_fingerprint"):
        record_exec_evidence(
            artifact_root=artifact_root,
            run_id=run_id,
            stage="implement",
            invoker="codex_exec",
            execution_output=out,
            argv=argv,
            prompt=prompt,
            cwd=cwd,
            started_at=started,
            ended_at=ended,
        )


def test_rework_same_argv_different_timestamps_allowed(tmp_path) -> None:
    artifact_root = str(tmp_path / "run")
    run_id = "run-rework"
    argv = ["python", "-c", "print(1)"]
    out = _base_out()
    record_exec_evidence(
        artifact_root=artifact_root,
        run_id=run_id,
        stage="implement",
        invoker="codex_exec",
        execution_output=out,
        argv=argv,
        prompt="p1",
        cwd="/repo",
    )
    time.sleep(0.01)
    record_exec_evidence(
        artifact_root=artifact_root,
        run_id=run_id,
        stage="implement",
        invoker="codex_exec",
        execution_output=out,
        argv=argv,
        prompt="p2",
        cwd="/repo",
    )
    validate_evidence_store(artifact_root, run_id=run_id)


def test_invalid_evidence_json_fails(tmp_path) -> None:
    artifact_root = tmp_path / "run"
    evidence_dir = artifact_root / "evidence"
    evidence_dir.mkdir(parents=True)
    bad = evidence_dir / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    with pytest.raises(NodeExecutionFailure, match="invalid evidence JSON"):
        validate_evidence_store(str(artifact_root), run_id="run-x")


def test_stub_marker_rejected(tmp_path) -> None:
    with pytest.raises(NodeExecutionFailure, match="stub/manual"):
        record_exec_evidence(
            artifact_root=str(tmp_path / "run"),
            run_id="r1",
            stage="spec_plan",
            invoker="codex_exec",
            execution_output={
                "ok": True,
                "provider_meta": {"marker": "stub"},
            },
            argv=["x"],
            prompt="p",
            cwd="/r",
        )
