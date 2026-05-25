"""P10 contract: node execution on main path, node_runs[] populated, 1 node exec = 1 logical session.

Every Codex exec on the main flow path must go through ``run_node_exec()`` and
produce a ``node_runs[]`` entry with a unique ``session_id``.

``model`` in ``NodeRun`` is audit metadata only; it is not injected into
worker argv (see ``node_runner.py`` module docstring).

``session_id`` is a logical id derived from ``(run_id, node_name, index)``.
Provider-level session isolation is worker-dependent and not guaranteed.

Node names follow the canonical registry:
  write_spec, review_spec, write_plan, review_plan,
  write_implementation, write_tests,
  review_diff, review_tests, review_spec_conformance, review_wide, review_spec_revision
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint
from nodeflow.workflows.dev_process.constants import (
    STATE_AWAITING_SPEC_REVISION,
)
from nodeflow.workflows.dev_process.flow_runner import run_flow
from nodeflow.workflows.dev_process.node_run import NodeRun
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit
from tests.workflows.dev_process.v2_flow_helpers import (
    approve_spec_to_implementation,
    full_through_review,
    start_spec_human_gate,
)


def _artifact_root(repo: Path) -> Path:
    runs = list((repo / ".nodeflow/runs").iterdir())
    assert len(runs) == 1
    return runs[0]


# -- spec / plan cycle node_runs --


def test_start_populates_node_runs_for_spec_cycle(tmp_path: Path) -> None:
    """start produces node_runs for write_spec + review_spec."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = start_spec_human_gate(repo)
    cp = load_flow_checkpoint(flow["flow_result"]["flow_checkpoint_path"])
    runs = cp.get("node_runs") or []
    assert len(runs) >= 2
    names = [r["node_name"] for r in runs]
    assert "write_spec" in names
    assert "review_spec" in names
    for r in runs:
        assert r.get("session_id"), f"missing session_id on {r['node_name']}"
        assert r.get("evidence_path"), f"missing evidence_path on {r['node_name']}"
        assert r.get("worker"), f"missing worker on {r['node_name']}"
        assert isinstance(r.get("argv"), list), f"argv must be list on {r['node_name']}"
        assert r.get("node_type", "").startswith(
            "dev_process."
        ), f"missing node_type on {r['node_name']}"


def test_approve_spec_populates_node_runs_for_plan_cycle(tmp_path: Path) -> None:
    """approve_spec adds write_plan + review_plan node_runs."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    after = approve_spec_to_implementation(repo, start["flow_result"]["flow_checkpoint_path"])
    cp = load_flow_checkpoint(after["flow_result"]["flow_checkpoint_path"])
    runs = cp.get("node_runs") or []
    names = [r["node_name"] for r in runs]
    assert "write_spec" in names
    assert "review_spec" in names
    assert "write_plan" in names
    assert "review_plan" in names
    assert len(runs) >= 4


def test_all_session_ids_are_unique(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    after = approve_spec_to_implementation(repo, start["flow_result"]["flow_checkpoint_path"])
    cp = load_flow_checkpoint(after["flow_result"]["flow_checkpoint_path"])
    runs = cp.get("node_runs") or []
    session_ids = [r["session_id"] for r in runs]
    assert len(session_ids) == len(set(session_ids)), "duplicate session_ids"


def test_node_run_dataclass_roundtrip() -> None:
    rec = NodeRun(
        node_name="write_spec",
        node_type="dev_process.write_spec",
        stage="spec",
        worker="codex",
        model="gpt-4.1",
        session_id="abc123",
        evidence_path="/evidence/x.json",
        argv=["codex", "exec"],
    )
    d = rec.to_dict()
    assert d["node_name"] == "write_spec"
    assert d["node_type"] == "dev_process.write_spec"
    assert d["model"] == "gpt-4.1"
    assert d["session_id"] == "abc123"


def test_revise_spec_adds_more_node_runs(tmp_path: Path, monkeypatch) -> None:
    from nodeflow.workflows.dev_process import flow_actions as fa

    review_calls = 0
    real_review = fa.run_spec_review_stage

    def _review(**kwargs):
        nonlocal review_calls
        review_calls += 1
        if review_calls == 1:
            return {
                "decision": "fail",
                "status": "completed",
                "aggregate": {"blocking_count": 1, "blocking_findings": [{"id": "S1"}]},
            }
        return real_review(**kwargs)

    monkeypatch.setattr(fa, "run_spec_review_stage", _review)

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    assert start["flow_result"]["state"] == STATE_AWAITING_SPEC_REVISION
    cp1 = load_flow_checkpoint(start["flow_result"]["flow_checkpoint_path"])
    runs_before = len(cp1.get("node_runs") or [])

    revised = run_flow(
        action="revise_spec",
        repo_root=str(repo),
        flow_checkpoint_path=start["flow_result"]["flow_checkpoint_path"],
        interactive=False,
    )
    cp2 = load_flow_checkpoint(revised["flow_result"]["flow_checkpoint_path"])
    runs_after = len(cp2.get("node_runs") or [])
    assert runs_after > runs_before


def test_checkpoint_node_runs_field_present_on_start_without_spec(tmp_path: Path) -> None:
    """Even run_spec_on_start=False creates node_runs=[] on checkpoint."""
    from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
        DevProcessFlowNode,
    )

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    out = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "t"},
        {"run_spec_on_start": False},
    )
    cp = load_flow_checkpoint(out["flow_output"]["flow_result"]["flow_checkpoint_path"])
    assert cp.get("node_runs") == []


# -- implementation / review cycle node_runs --


def test_continue_implementation_populates_all_node_runs(tmp_path: Path) -> None:
    """continue_implementation adds impl, test, and reviewer node_runs."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo)
    cp = load_flow_checkpoint(flow["flow_checkpoint_path"])
    runs = cp.get("node_runs") or []
    names = [r["node_name"] for r in runs]
    assert "write_implementation" in names
    assert "write_tests" in names
    for rk in ("review_diff", "review_tests", "review_spec_conformance"):
        assert rk in names, f"missing reviewer node_run {rk}"


def test_node_runs_count_matches_evidence_count(tmp_path: Path) -> None:
    """len(node_runs) == number of evidence JSON files on disk."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo)
    cp = load_flow_checkpoint(flow["flow_checkpoint_path"])
    runs = cp.get("node_runs") or []
    art = _artifact_root(repo)
    evidence_dir = art / "evidence"
    evidence_files = list(evidence_dir.glob("*.json"))
    assert len(runs) == len(
        evidence_files
    ), f"node_runs={len(runs)} evidence_files={len(evidence_files)}"


def test_all_evidence_paths_exist(tmp_path: Path) -> None:
    """Every node_runs[].evidence_path is a real file."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo)
    cp = load_flow_checkpoint(flow["flow_checkpoint_path"])
    runs = cp.get("node_runs") or []
    assert len(runs) > 0
    for r in runs:
        ep = r.get("evidence_path")
        assert ep, f"evidence_path missing on {r['node_name']}"
        assert Path(ep).is_file(), f"evidence file missing: {ep}"


def test_full_flow_session_ids_unique(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo)
    cp = load_flow_checkpoint(flow["flow_checkpoint_path"])
    runs = cp.get("node_runs") or []
    sids = [r["session_id"] for r in runs]
    assert len(sids) == len(set(sids)), "duplicate session_ids in full flow"


# -- evidence traceability --


def test_evidence_contains_node_context_fields(tmp_path: Path) -> None:
    """Evidence JSON written via run_node_exec() includes node_name, session_id, model, worker."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = start_spec_human_gate(repo)
    cp = load_flow_checkpoint(flow["flow_result"]["flow_checkpoint_path"])
    runs = cp.get("node_runs") or []
    assert len(runs) >= 2
    for r in runs:
        ep = Path(r["evidence_path"])
        assert ep.is_file(), f"missing evidence: {ep}"
        doc = json.loads(ep.read_text(encoding="utf-8"))
        assert doc.get("node_name") == r["node_name"], f"evidence node_name mismatch: {ep}"
        assert doc.get("session_id") == r["session_id"], f"evidence session_id mismatch: {ep}"
        assert doc.get("worker") == r["worker"], f"evidence worker mismatch: {ep}"
        assert "model" in doc, f"evidence must always contain 'model' key: {ep}"


def test_provider_session_id_does_not_overwrite_logical(tmp_path: Path) -> None:
    """provider_meta.session_id -> provider_session_id, not session_id."""
    from nodeflow.workflows.dev_process.evidence import record_exec_evidence

    art = tmp_path / "evidence"
    art.mkdir()
    execution_output = {
        "ok": True,
        "stdout": "out",
        "stderr": "",
        "provider": "test",
        "provider_meta": {"session_id": "provider-123"},
    }
    ep = record_exec_evidence(
        execution_output=execution_output,
        stage="spec",
        invoker="ci",
        prompt="p",
        cwd=str(tmp_path),
        run_id="r1",
        artifact_root=str(tmp_path),
        session_id="logical-abc",
        node_name="write_spec",
        model=None,
        worker="codex",
    )
    doc = json.loads(Path(ep).read_text(encoding="utf-8"))
    assert doc["session_id"] == "logical-abc"
    assert doc["provider_session_id"] == "provider-123"


def test_exec_policy_path_loads_into_snapshot(tmp_path: Path) -> None:
    """exec_policy_path overrides per-node worker/model in the snapshot."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)

    policy_file = tmp_path / "policy.json"
    policy_file.write_text(
        json.dumps(
            {
                "default_model": "gpt-4.1-mini",
                "nodes": {
                    "write_spec": {"model": "gpt-4.1"},
                },
            }
        ),
        encoding="utf-8",
    )
    flow = run_flow(
        action="start",
        repo_root=str(repo),
        task_prompt="t",
        exec_policy_path=str(policy_file),
    )
    cp = load_flow_checkpoint(flow["flow_checkpoint_path"])
    snap = cp.get("dev_process", {}).get("exec_policy_snapshot", {})
    assert snap.get("default_model") == "gpt-4.1-mini"
    assert snap["nodes"]["write_spec"]["model"] == "gpt-4.1"
    ps = snap.get("policy_source")
    assert isinstance(ps, dict), "policy_source missing from snapshot"
    assert ps["path"] == str(policy_file.resolve())
    assert isinstance(ps["sha256"], str) and len(ps["sha256"]) == 64


def test_resume_rejects_exec_policy_path(tmp_path: Path) -> None:
    """exec_policy_path is start-only; resume raises."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    cp_path = start["flow_result"]["flow_checkpoint_path"]
    policy_file = tmp_path / "policy.json"
    policy_file.write_text('{"default_model": "x"}', encoding="utf-8")
    with pytest.raises(NodeExecutionFailure, match="start-only"):
        run_flow(
            action="approve_spec",
            repo_root=str(repo),
            flow_checkpoint_path=cp_path,
            exec_policy_path=str(policy_file),
        )


def test_force_blocking_review_still_uses_run_node_exec(tmp_path: Path) -> None:
    """force_blocking=True review goes through run_node_exec (argv_override) and records node_runs."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo, force_blocking=True)
    cp = load_flow_checkpoint(flow["flow_checkpoint_path"])
    runs = cp.get("node_runs") or []
    reviewer_runs = [r for r in runs if r["stage"] == "review"]
    assert len(reviewer_runs) >= 3, f"expected >=3 reviewer node_runs, got {len(reviewer_runs)}"
    for r in reviewer_runs:
        assert Path(r["evidence_path"]).is_file()


# -- exec_policy validation --


def test_unknown_node_name_in_policy_rejected(tmp_path: Path) -> None:
    """Policy file with unknown node name raises at start."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    policy_file = tmp_path / "policy.json"
    policy_file.write_text(
        json.dumps({"nodes": {"spec.write": {"model": "gpt-5.5"}}}),
        encoding="utf-8",
    )
    with pytest.raises(NodeExecutionFailure, match="unknown node name"):
        run_flow(
            action="start",
            repo_root=str(repo),
            task_prompt="t",
            exec_policy_path=str(policy_file),
        )


def test_invalid_argv_type_in_policy_rejected(tmp_path: Path) -> None:
    """Policy file with non-list argv raises at start."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    policy_file = tmp_path / "policy.json"
    policy_file.write_text(
        json.dumps({"nodes": {"write_spec": {"argv": "not-a-list"}}}),
        encoding="utf-8",
    )
    with pytest.raises(NodeExecutionFailure, match="list of strings"):
        run_flow(
            action="start",
            repo_root=str(repo),
            task_prompt="t",
            exec_policy_path=str(policy_file),
        )


def test_unsupported_worker_in_policy_rejected(tmp_path: Path) -> None:
    """Policy file with unsupported worker raises at start."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    policy_file = tmp_path / "policy.json"
    policy_file.write_text(
        json.dumps({"nodes": {"write_spec": {"worker": "openai_direct"}}}),
        encoding="utf-8",
    )
    with pytest.raises(NodeExecutionFailure, match="unsupported worker"):
        run_flow(
            action="start",
            repo_root=str(repo),
            task_prompt="t",
            exec_policy_path=str(policy_file),
        )


def test_legacy_jobs_key_in_policy_rejected(tmp_path: Path) -> None:
    """Policy file using legacy 'jobs' key instead of 'nodes' raises at start."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    policy_file = tmp_path / "policy.json"
    policy_file.write_text(
        json.dumps({"jobs": {"write_spec": {"model": "gpt-4.1"}}}),
        encoding="utf-8",
    )
    with pytest.raises(NodeExecutionFailure, match="no longer supported.*nodes"):
        run_flow(
            action="start",
            repo_root=str(repo),
            task_prompt="t",
            exec_policy_path=str(policy_file),
        )


def test_reference_exec_policy_is_valid(tmp_path: Path) -> None:
    """examples/reference/dev_process/exec_policy.json passes validation."""
    from nodeflow.workflows.dev_process.exec_policy import load_exec_policy_file

    example = (
        Path(__file__).resolve().parents[3] / "examples/reference/dev_process/exec_policy.json"
    )
    if not example.is_file():
        pytest.skip("reference exec_policy.json not found")
    doc = load_exec_policy_file(example)
    assert "nodes" in doc
    assert "jobs" not in doc
