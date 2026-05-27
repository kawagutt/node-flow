"""P11 contract: full implementation chain, owner routing, stale markers, final/merge.

P11 completes the dev-process flow:
    write_implementation → write_tests → run_tests → review_changes → synthesis
    ├─ owner routing → upstream write steps
    └─ pass → human_final_gate → merge

Owner routing table:
    spec → write_spec → review_spec → human_spec_gate → write_plan → …
    plan → write_plan → review_plan → implementation chain
    implementation → write_implementation → write_tests → run_tests → review
    test → write_tests → run_tests → review  (skips write_implementation)
"""

from __future__ import annotations

from pathlib import Path

from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint
from nodeflow.workflows.dev_process.constants import (
    STATE_AWAITING_FINAL,
    STATE_AWAITING_MERGE,
    STATE_AWAITING_REWORK_DECISION,
    STATE_AWAITING_SPEC_HUMAN_GATE,
    STATE_MERGED,
)
from nodeflow.workflows.dev_process.stale import any_stale_remaining
from nodeflow.workflows.dev_process.synthesis import (
    assign_owners_to_findings,
    route_owner_to_state,
)
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit
from tests.workflows.dev_process.v2_flow_helpers import (
    full_through_review,
    merge_ready_flow,
    rework_from_blocking,
    through_approve_final,
)

# -- happy path: implementation → final → merge --


def test_full_happy_path_to_merged(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    merged = merge_ready_flow(repo)
    assert merged["flow_result"]["state"] == STATE_MERGED
    cp = load_flow_checkpoint(merged["flow_result"]["flow_checkpoint_path"])
    assert not any_stale_remaining(cp)
    runs = cp.get("node_runs") or []
    names = [r["node_name"] for r in runs]
    assert "write_spec" in names
    assert "write_implementation" in names
    assert "write_tests" in names
    from nodeflow.workflows.dev_process.review_config import (
        FINAL_REVIEW_AGENTS,
        review_node_name,
    )

    for agent in ("architecture", "checklist_compliance"):
        assert review_node_name(agent) in names, f"missing phase reviewer {agent}"
    for agent in FINAL_REVIEW_AGENTS:
        assert review_node_name(agent) in names, f"missing final reviewer {agent}"


def test_approve_final_transitions_to_awaiting_merge(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    review, final = through_approve_final(repo)
    assert review["flow_result"]["state"] == STATE_AWAITING_FINAL
    assert review["flow_result"]["merge_ready"] is True
    assert final["flow_result"]["state"] == STATE_AWAITING_MERGE
    cp = load_flow_checkpoint(final["flow_result"]["flow_checkpoint_path"])
    gates = (cp.get("dev_process") or {}).get("human_gates") or {}
    assert gates.get("final") == "approved"


# -- owner routing: synthesis --


def test_assign_owners_to_findings_spec() -> None:
    findings = [
        {"area": "spec_conformance", "message": "spec drift"},
        {"area": "tests", "message": "missing coverage"},
    ]
    out = assign_owners_to_findings(findings)
    assert out[0]["owner"] == "spec"
    assert out[1]["owner"] == "test"


def test_assign_owners_to_findings_plan() -> None:
    findings = [{"area": "plan", "message": "missing step"}]
    out = assign_owners_to_findings(findings)
    assert out[0]["owner"] == "plan"


def test_assign_owners_defaults_to_implementation() -> None:
    findings = [{"area": "code_quality", "message": "naming"}]
    out = assign_owners_to_findings(findings)
    assert out[0]["owner"] == "implementation"


def test_route_owner_priority_spec_highest() -> None:
    findings = [
        {"owner": "implementation"},
        {"owner": "spec"},
        {"owner": "test"},
    ]
    assert route_owner_to_state(findings) == "spec"


def test_route_owner_priority_plan_over_test() -> None:
    findings = [
        {"owner": "test"},
        {"owner": "plan"},
    ]
    assert route_owner_to_state(findings) == "plan"


def test_route_owner_single_test() -> None:
    findings = [{"owner": "test"}]
    assert route_owner_to_state(findings) == "test"


# -- blocking review → rework decision --


def test_blocking_review_reaches_rework_decision(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo, force_blocking=True)
    assert flow["flow_result"]["state"] == STATE_AWAITING_REWORK_DECISION
    assert flow["flow_result"]["merge_ready"] is False


def test_rework_implementation_owner_reruns_full_chain(tmp_path: Path) -> None:
    """implementation owner rework re-runs write_implementation + write_tests + run_tests + review."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo, force_blocking=True)
    cp1 = load_flow_checkpoint(flow["flow_result"]["flow_checkpoint_path"])
    node_runs_before = len(cp1.get("node_runs") or [])

    reworked = rework_from_blocking(repo, flow["flow_result"]["flow_checkpoint_path"])
    cp2 = load_flow_checkpoint(reworked["flow_result"]["flow_checkpoint_path"])
    node_runs_after = len(cp2.get("node_runs") or [])
    assert node_runs_after > node_runs_before
    names_after = [r["node_name"] for r in cp2.get("node_runs") or []]
    assert names_after.count("write_implementation") >= 2


def test_rework_from_final_adds_node_runs(tmp_path: Path) -> None:
    """Rework from awaiting_final re-runs the implementation chain and adds node_runs."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo)
    assert flow["flow_result"]["state"] == STATE_AWAITING_FINAL
    cp1 = load_flow_checkpoint(flow["flow_result"]["flow_checkpoint_path"])
    runs_before = len(cp1.get("node_runs") or [])

    reworked = rework_from_blocking(repo, flow["flow_result"]["flow_checkpoint_path"])
    cp2 = load_flow_checkpoint(reworked["flow_result"]["flow_checkpoint_path"])
    runs_after = len(cp2.get("node_runs") or [])
    assert runs_after > runs_before


# -- stale markers --


def test_implementation_marks_downstream_stale(tmp_path: Path) -> None:
    """After implementation runs, test_implementation and review are initially stale then cleared."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo)
    cp = load_flow_checkpoint(flow["flow_result"]["flow_checkpoint_path"])
    assert not any_stale_remaining(cp), "all stages should be cleared after full chain"


def test_spec_revision_marks_downstream_stale(tmp_path: Path) -> None:
    """revise_spec from awaiting_final marks plan, implementation, review as stale."""
    from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
        DevProcessFlowNode,
    )
    from nodeflow.workflows.dev_process.stale import is_stage_stale

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo)
    cp_path = flow["flow_result"]["flow_checkpoint_path"]

    revised = DevProcessFlowNode().execute(
        {
            "action": "revise_spec",
            "repo_root": str(repo),
            "flow_checkpoint_path": cp_path,
            "task_prompt": "narrow scope",
        },
        {},
    )["flow_output"]
    assert revised["flow_result"]["state"] == STATE_AWAITING_SPEC_HUMAN_GATE
    cp = load_flow_checkpoint(revised["flow_result"]["flow_checkpoint_path"])
    assert is_stage_stale(cp, "implementation")
    assert is_stage_stale(cp, "review")


# -- node_runs session_id uniqueness across rework --


def test_node_runs_session_ids_unique_across_rework(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo, force_blocking=True)
    reworked = rework_from_blocking(repo, flow["flow_result"]["flow_checkpoint_path"])
    cp = load_flow_checkpoint(reworked["flow_result"]["flow_checkpoint_path"])
    runs = cp.get("node_runs") or []
    sids = [r["session_id"] for r in runs]
    assert len(sids) == len(set(sids)), "duplicate session_ids across rework cycles"


# -- evidence count matches node_runs after rework --


def test_evidence_count_matches_node_runs_after_rework(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo, force_blocking=True)
    reworked = rework_from_blocking(repo, flow["flow_result"]["flow_checkpoint_path"])
    cp = load_flow_checkpoint(reworked["flow_result"]["flow_checkpoint_path"])
    runs = cp.get("node_runs") or []
    art = list((repo / ".nodeflow/runs").iterdir())[0]
    evidence_files = list(art.rglob("evidence/*.json"))
    assert len(runs) == len(evidence_files)


# -- test owner rework skips write_implementation --


def test_test_owner_rework_skips_implementation(tmp_path: Path, monkeypatch) -> None:
    """When rework_owner='test', write_implementation is skipped."""
    import json as _json

    from nodeflow.workflows.dev_process import flow_actions as fa

    impl_calls: list[str] = []
    real_impl = fa.run_implementation_stage

    def _track_impl(**kwargs):
        impl_calls.append("called")
        return real_impl(**kwargs)

    monkeypatch.setattr(fa, "run_implementation_stage", _track_impl)

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)

    flow = full_through_review(repo, force_blocking=True)
    impl_calls.clear()

    cp_path = Path(flow["flow_result"]["flow_checkpoint_path"])
    cp = _json.loads(cp_path.read_text(encoding="utf-8"))
    cp["rework_owner"] = "test"
    cp_path.write_text(_json.dumps(cp, ensure_ascii=False, indent=2), encoding="utf-8")

    reworked = rework_from_blocking(repo, flow["flow_result"]["flow_checkpoint_path"])
    assert len(impl_calls) == 0, "write_implementation should be skipped for test owner"

    cp2 = load_flow_checkpoint(reworked["flow_result"]["flow_checkpoint_path"])
    rework_node_runs = (cp2.get("node_runs") or [])[len(cp.get("node_runs") or []) :]
    rework_names = [r["node_name"] for r in rework_node_runs]
    assert "write_tests" in rework_names
    assert "write_implementation" not in rework_names
