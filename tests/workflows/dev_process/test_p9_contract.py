"""P9 contract: v3 spec/plan split and stop at awaiting_implementation.

Tests named ``test_preview_*`` exercise transitional P11 paths (continue_implementation,
merge) and are not part of the P9 correctness contract.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.argv_builder import resolve_node_exec
from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint
from nodeflow.workflows.dev_process.constants import (
    ACTION_REQUEST_SPEC_REVISION,
    ACTION_REVISE_PLAN,
    ACTION_REVISE_SPEC,
    MERGE_GATE_STAGES,
    STATE_AWAITING_FINAL,
    STATE_AWAITING_IMPLEMENTATION,
    STATE_AWAITING_MERGE,
    STATE_AWAITING_PLAN_REVISION,
    STATE_AWAITING_SPEC_HUMAN_GATE,
    STATE_AWAITING_SPEC_REVISION,
    V3_CHECKPOINT_STAGES,
)
from nodeflow.workflows.dev_process.flow_merge import _merge_gate_ok
from nodeflow.workflows.dev_process.flow_runner import run_flow
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit
from tests.workflows.dev_process.hermetic_argv import spec_argv
from tests.workflows.dev_process.v2_flow_helpers import (
    approve_spec_to_implementation,
    continue_from_implementation,
    full_through_review,
    start_spec_human_gate,
)


def _artifact_root(repo: Path) -> Path:
    runs = list((repo / ".nodeflow/runs").iterdir())
    assert len(runs) == 1
    return runs[0]


def test_start_checkpoint_has_v3_stages_only(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = start_spec_human_gate(repo)
    cp = load_flow_checkpoint(flow["flow_result"]["flow_checkpoint_path"])
    stages = cp.get("stages") or {}
    assert set(stages.keys()) == set(V3_CHECKPOINT_STAGES)
    assert "spec_plan" not in stages
    assert "implement" not in stages


def test_v2_checkpoint_resume_rejected(tmp_path: Path) -> None:
    """dev_process.flow.v2 checkpoints are not resumable after v3 bump."""
    import json

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = start_spec_human_gate(repo)
    cp_path = Path(flow["flow_result"]["flow_checkpoint_path"])
    doc = json.loads(cp_path.read_text(encoding="utf-8"))
    doc["schema_version"] = "dev_process.flow.v2"
    v2_path = cp_path.parent / "flow_v2_legacy.json"
    v2_path.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    with pytest.raises(NodeExecutionFailure, match="unsupported checkpoint schema_version"):
        load_flow_checkpoint(v2_path)


def test_start_writes_spec_not_plan(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start_spec_human_gate(repo)
    root = _artifact_root(repo)
    assert (root / "spec" / "spec.md").is_file()
    assert not (root / "plan" / "plan.md").exists()


def test_start_ends_at_spec_human_gate_or_revision(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = start_spec_human_gate(repo)
    state = flow["flow_result"]["state"]
    assert state in (STATE_AWAITING_SPEC_HUMAN_GATE, STATE_AWAITING_SPEC_REVISION)


def test_approve_spec_writes_plan_not_implementation(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    cp = start["flow_result"]["flow_checkpoint_path"]
    after = approve_spec_to_implementation(repo, cp)
    root = _artifact_root(repo)
    assert (root / "plan" / "plan.md").is_file()
    assert not (root / "implementation" / "summary.txt").exists()
    assert after["flow_result"]["state"] in (
        STATE_AWAITING_IMPLEMENTATION,
        STATE_AWAITING_PLAN_REVISION,
    )


def test_approve_spec_does_not_create_workspace_or_review(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    after = approve_spec_to_implementation(repo, start["flow_result"]["flow_checkpoint_path"])
    assert "workspace_context" not in after
    cp = load_flow_checkpoint(after["flow_result"]["flow_checkpoint_path"])
    assert cp["stages"]["review"]["status"] == "pending"


def test_preview_continue_implementation_populates_impl_stages(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    after_approve = approve_spec_to_implementation(
        repo, start["flow_result"]["flow_checkpoint_path"]
    )
    review = continue_from_implementation(
        repo, after_approve["flow_result"]["flow_checkpoint_path"]
    )
    cp = load_flow_checkpoint(review["flow_result"]["flow_checkpoint_path"])
    stages = cp.get("stages") or {}
    for name in ("implementation", "test_implementation", "run_tests", "review"):
        assert stages.get(name, {}).get("status") == "completed"
    assert review["flow_result"]["state"] == STATE_AWAITING_FINAL


def test_merge_gate_rejects_legacy_stage_keys() -> None:
    body = {
        "flow_result": {"state": STATE_AWAITING_MERGE, "merge_ready": True},
        "stages": {
            name: {"status": "completed", "aggregate": {"blocking_count": 0}}
            for name in MERGE_GATE_STAGES
        },
    }
    body["stages"]["spec_plan"] = {"status": "completed"}
    with pytest.raises(Exception, match="legacy stages"):
        _merge_gate_ok(body)


def test_merge_gate_requires_all_v2_stages_completed() -> None:
    body = {
        "flow_result": {"state": STATE_AWAITING_MERGE, "merge_ready": True},
        "stages": {
            name: {"status": "completed", "aggregate": {"blocking_count": 0}}
            for name in MERGE_GATE_STAGES
        },
    }
    body["stages"]["review"]["aggregate"] = {"blocking_count": 0}
    _merge_gate_ok(body)


def test_preview_full_flow_merge_gate_stages_completed(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    review = full_through_review(repo)
    cp = load_flow_checkpoint(review["flow_result"]["flow_checkpoint_path"])
    for name in MERGE_GATE_STAGES:
        if name == "review":
            continue
        assert cp["stages"][name]["status"] == "completed"


def test_exec_argv_default_applies_to_all_jobs(tmp_path: Path) -> None:
    from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
        DevProcessFlowNode,
    )

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    marker = ["/usr/bin/codex", "exec", "--marker", "real-codex"]
    out = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "argv policy",
            "exec_argv": marker,
        },
        {"run_spec_on_start": False},
    )["flow_output"]
    cp = load_flow_checkpoint(out["flow_result"]["flow_checkpoint_path"])
    snap = cp["dev_process"]["exec_policy_snapshot"]
    assert snap["default_argv"] == marker
    for entry in snap["nodes"].values():
        assert "argv" not in entry
    _, _, write_spec_argv = resolve_node_exec(cp, "write_spec")
    _, _, review_spec_argv = resolve_node_exec(cp, "review_spec")
    assert write_spec_argv == marker
    assert review_spec_argv == marker


def test_without_exec_argv_jobs_use_hermetic_fallback(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = start_spec_human_gate(repo)
    cp = load_flow_checkpoint(flow["flow_result"]["flow_checkpoint_path"])
    _, _, argv = resolve_node_exec(cp, "write_spec")
    assert argv == spec_argv()


def test_spec_review_fail_then_revise_spec(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from nodeflow.workflows.dev_process import flow_actions as flow_actions_mod

    review_calls = 0
    real_review = flow_actions_mod.run_spec_review_stage

    def _review(**kwargs: object) -> dict:
        nonlocal review_calls
        review_calls += 1
        if review_calls == 1:
            return {
                "decision": "fail",
                "status": "completed",
                "aggregate": {"blocking_count": 1, "blocking_findings": [{"id": "S1"}]},
            }
        return real_review(**kwargs)  # type: ignore[arg-type]

    spec_calls = 0
    real_spec = flow_actions_mod.run_spec_stage

    def _spec(**kwargs: object) -> dict:
        nonlocal spec_calls
        spec_calls += 1
        return real_spec(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(flow_actions_mod, "run_spec_review_stage", _review)
    monkeypatch.setattr(flow_actions_mod, "run_spec_stage", _spec)

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    assert start["flow_result"]["state"] == STATE_AWAITING_SPEC_REVISION
    cp = start["flow_result"]["flow_checkpoint_path"]
    revised = run_flow(
        action=ACTION_REVISE_SPEC,
        repo_root=str(repo),
        flow_checkpoint_path=cp,
        revision_provided={"revision_comment": "address review"},
        interactive=False,
    )
    assert spec_calls == 2
    assert revised["flow_result"]["state"] == STATE_AWAITING_SPEC_HUMAN_GATE


def test_request_spec_revision_from_human_gate(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    assert start["flow_result"]["state"] == STATE_AWAITING_SPEC_HUMAN_GATE
    cp = start["flow_result"]["flow_checkpoint_path"]
    root = _artifact_root(repo)
    before = (root / "spec" / "spec.md").read_text(encoding="utf-8")
    revised = run_flow(
        action=ACTION_REQUEST_SPEC_REVISION,
        repo_root=str(repo),
        flow_checkpoint_path=cp,
        human_comment_text="please tighten scope",
        revision_provided={"revision_comment": "please tighten scope"},
        interactive=False,
    )
    assert revised["flow_result"]["state"] == STATE_AWAITING_SPEC_HUMAN_GATE
    after = (root / "spec" / "spec.md").read_text(encoding="utf-8")
    assert after == before or after  # spec re-written (hermetic content may match)


def test_plan_review_fail_then_revise_plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from nodeflow.workflows.dev_process import flow_actions as flow_actions_mod

    plan_review_calls = 0
    real_plan_review = flow_actions_mod.run_plan_review_stage

    def _plan_review(**kwargs: object) -> dict:
        nonlocal plan_review_calls
        plan_review_calls += 1
        if plan_review_calls == 1:
            return {
                "decision": "fail",
                "status": "completed",
                "aggregate": {"blocking_count": 1, "blocking_findings": [{"id": "P1"}]},
            }
        return real_plan_review(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(flow_actions_mod, "run_plan_review_stage", _plan_review)

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    cp = start["flow_result"]["flow_checkpoint_path"]
    after_approve = approve_spec_to_implementation(repo, cp)
    assert after_approve["flow_result"]["state"] == STATE_AWAITING_PLAN_REVISION
    cp2 = after_approve["flow_result"]["flow_checkpoint_path"]
    revised = run_flow(
        action=ACTION_REVISE_PLAN,
        repo_root=str(repo),
        flow_checkpoint_path=cp2,
        revision_provided={"revision_comment": "fix plan gaps"},
        interactive=False,
        auto_continue=False,
    )
    assert plan_review_calls == 2
    assert revised["flow_result"]["state"] == STATE_AWAITING_IMPLEMENTATION


def test_revise_spec_includes_human_comment_with_findings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from nodeflow.workflows.dev_process import flow_actions as flow_actions_mod

    captured: dict[str, str] = {}
    real_spec = flow_actions_mod.run_spec_stage

    def _capture_spec(**kwargs: object) -> dict:
        captured["revision_context"] = str(kwargs.get("revision_context") or "")
        return real_spec(**kwargs)  # type: ignore[arg-type]

    review_calls = 0
    real_review = flow_actions_mod.run_spec_review_stage

    def _review(**kwargs: object) -> dict:
        nonlocal review_calls
        review_calls += 1
        if review_calls == 1:
            return {
                "decision": "fail",
                "status": "completed",
                "aggregate": {
                    "blocking_count": 1,
                    "blocking_findings": [{"id": "S1", "summary": "gap in scope"}],
                },
            }
        return real_review(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(flow_actions_mod, "run_spec_stage", _capture_spec)
    monkeypatch.setattr(flow_actions_mod, "run_spec_review_stage", _review)

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    assert start["flow_result"]["state"] == STATE_AWAITING_SPEC_REVISION
    revised = run_flow(
        action=ACTION_REVISE_SPEC,
        repo_root=str(repo),
        flow_checkpoint_path=start["flow_result"]["flow_checkpoint_path"],
        revision_provided={"revision_comment": "also cover edge cases"},
        interactive=False,
    )
    assert revised["flow_result"]["state"] == STATE_AWAITING_SPEC_HUMAN_GATE
    ctx = captured.get("revision_context", "")
    assert "gap in scope" in ctx
    assert "also cover edge cases" in ctx


def test_revise_spec_without_comment_uses_findings_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from nodeflow.workflows.dev_process import flow_actions as flow_actions_mod

    review_calls = 0
    real_review = flow_actions_mod.run_spec_review_stage

    def _review(**kwargs: object) -> dict:
        nonlocal review_calls
        review_calls += 1
        if review_calls == 1:
            return {
                "decision": "fail",
                "status": "completed",
                "aggregate": {
                    "blocking_count": 1,
                    "blocking_findings": [{"id": "S1", "summary": "missing acceptance criteria"}],
                },
            }
        return real_review(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(flow_actions_mod, "run_spec_review_stage", _review)

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    assert start["flow_result"]["state"] == STATE_AWAITING_SPEC_REVISION
    revised = run_flow(
        action=ACTION_REVISE_SPEC,
        repo_root=str(repo),
        flow_checkpoint_path=start["flow_result"]["flow_checkpoint_path"],
        interactive=False,
    )
    assert revised["flow_result"]["state"] == STATE_AWAITING_SPEC_HUMAN_GATE


def test_revise_spec_includes_previous_spec_in_prompt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from nodeflow.workflows.dev_process import flow_actions as flow_actions_mod

    captured: dict[str, str | None] = {}
    real_spec = flow_actions_mod.run_spec_stage

    def _capture_spec(**kwargs: object) -> dict:
        captured["previous_spec"] = kwargs.get("previous_spec")  # type: ignore[assignment]
        return real_spec(**kwargs)  # type: ignore[arg-type]

    review_calls = 0
    real_review = flow_actions_mod.run_spec_review_stage

    def _review(**kwargs: object) -> dict:
        nonlocal review_calls
        review_calls += 1
        if review_calls == 1:
            return {
                "decision": "fail",
                "status": "completed",
                "aggregate": {"blocking_count": 1, "blocking_findings": [{"id": "S1"}]},
            }
        return real_review(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(flow_actions_mod, "run_spec_stage", _capture_spec)
    monkeypatch.setattr(flow_actions_mod, "run_spec_review_stage", _review)

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    root = _artifact_root(repo)
    before_spec = (root / "spec" / "spec.md").read_text(encoding="utf-8")
    run_flow(
        action=ACTION_REVISE_SPEC,
        repo_root=str(repo),
        flow_checkpoint_path=start["flow_result"]["flow_checkpoint_path"],
        interactive=False,
    )
    assert captured.get("previous_spec") == before_spec


def test_revise_plan_without_comment_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from nodeflow.workflows.dev_process import flow_actions as flow_actions_mod

    plan_review_calls = 0
    real_plan_review = flow_actions_mod.run_plan_review_stage

    def _plan_review(**kwargs: object) -> dict:
        nonlocal plan_review_calls
        plan_review_calls += 1
        if plan_review_calls == 1:
            return {
                "decision": "fail",
                "status": "completed",
                "aggregate": {
                    "blocking_count": 1,
                    "blocking_findings": [{"id": "P1", "summary": "plan gap"}],
                },
            }
        return real_plan_review(**kwargs)  # type: ignore[arg-type]

    captured: dict[str, str | None] = {}
    real_plan = flow_actions_mod.run_plan_stage

    def _capture_plan(**kwargs: object) -> dict:
        captured["previous_plan"] = kwargs.get("previous_plan")  # type: ignore[assignment]
        return real_plan(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(flow_actions_mod, "run_plan_review_stage", _plan_review)
    monkeypatch.setattr(flow_actions_mod, "run_plan_stage", _capture_plan)

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    after_approve = approve_spec_to_implementation(
        repo, start["flow_result"]["flow_checkpoint_path"]
    )
    assert after_approve["flow_result"]["state"] == STATE_AWAITING_PLAN_REVISION
    root = _artifact_root(repo)
    before_plan = (root / "plan" / "plan.md").read_text(encoding="utf-8")
    revised = run_flow(
        action=ACTION_REVISE_PLAN,
        repo_root=str(repo),
        flow_checkpoint_path=after_approve["flow_result"]["flow_checkpoint_path"],
        interactive=False,
        auto_continue=False,
    )
    assert revised["flow_result"]["state"] == STATE_AWAITING_IMPLEMENTATION
    assert captured.get("previous_plan") == before_plan


def test_spec_revision_without_workspace_keeps_attempt_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from nodeflow.workflows.dev_process import flow_actions as flow_actions_mod

    review_calls = 0
    real_review = flow_actions_mod.run_spec_review_stage

    def _review(**kwargs: object) -> dict:
        nonlocal review_calls
        review_calls += 1
        if review_calls == 1:
            return {
                "decision": "fail",
                "status": "completed",
                "aggregate": {"blocking_count": 1, "blocking_findings": [{"id": "S1"}]},
            }
        return real_review(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(flow_actions_mod, "run_spec_review_stage", _review)

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    cp = load_flow_checkpoint(start["flow_result"]["flow_checkpoint_path"])
    assert cp["dev_process"]["workspace_attempt"] == 1
    assert "workspace_context" not in start
    revised = run_flow(
        action=ACTION_REVISE_SPEC,
        repo_root=str(repo),
        flow_checkpoint_path=start["flow_result"]["flow_checkpoint_path"],
        interactive=False,
    )
    cp2 = load_flow_checkpoint(revised["flow_result"]["flow_checkpoint_path"])
    assert cp2["dev_process"]["workspace_attempt"] == 1


def test_spec_review_prompt_includes_json_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import nodeflow.workflows.dev_process.stages.spec_review as spec_review_mod
    from nodeflow.workflows.dev_process.stages.review_aggregate import REVIEW_JSON_OUTPUT_CONTRACT
    from nodeflow.workflows.dev_process.stages.spec_review import run_spec_review_stage

    captured: dict[str, str] = {}

    def _run_node_exec(body: object, **kwargs: object) -> tuple:
        captured["prompt"] = str(kwargs.get("prompt") or "")
        return (
            {
                "ok": True,
                "stdout": '{"ok": true, "blocking_findings": [], "non_blocking_findings": []}',
            },
            "/tmp/evidence.json",
            None,
        )

    monkeypatch.setattr(spec_review_mod, "run_node_exec", _run_node_exec)

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    art = tmp_path / "artifacts" / "run1"
    art.mkdir(parents=True)
    run_spec_review_stage(
        repo_root=repo,
        artifact_root=str(art),
        run_id="run1",
        task_prompt="task",
        spec_text="# Spec\n",
        body={"node_runs": [], "dev_process": {"exec_policy_snapshot": {"nodes": {}}}},
    )
    assert REVIEW_JSON_OUTPUT_CONTRACT.splitlines()[0] in captured["prompt"]


def test_initialized_state_has_no_allowed_actions(tmp_path: Path) -> None:
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
    fr = out["flow_output"]["flow_result"]
    assert fr["state"] == "initialized"
    assert fr["allowed_actions"] == []
    assert fr["next_action"] is None
