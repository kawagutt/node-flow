"""PR4: coordinator ↔ subpipe contracts (ordering, argv, artifact roots)."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.core.loader import load_pipeline
from nodeflow.workflows.dev_process import register_dev_process_nodes
from nodeflow.workflows.dev_process.exec_policy import (
    apply_snapshot_to_body,
    build_exec_policy_snapshot,
)
from nodeflow.workflows.dev_process.flow_actions import (
    _apply_force_blocking_review_argv,
    _clear_review_argv_override,
    _run_plan_cycle,
)
from nodeflow.workflows.dev_process.nodes import STAGE_NODE_REGISTRY, make_flow_ctx
from nodeflow.workflows.dev_process.subpipe_runner import run_subpipe
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit
from tests.workflows.dev_process.hermetic_argv import implement_argv, spec_argv

register_dev_process_nodes()

REPO_ROOT = Path(__file__).resolve().parents[3]
PHASE_STEP_SPEC = "examples/pipes/dev_process/phase_step.json"
PHASE_STEP_NODE_COUNT = 12


def _minimal_body(tmp_path: Path) -> dict[str, Any]:
    import subprocess

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    head = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    artifact = tmp_path / "artifacts"
    artifact.mkdir()
    body: dict[str, Any] = {
        "run_context": {
            "run_id": "pr4_test_run",
            "repo_root": str(repo.resolve()),
            "artifact_root": str(artifact),
            "source_base_revision": head,
        },
        "task_prompt": "task",
        "stages": {},
        "node_runs": [],
        "dev_process": {"current_phase_id": "phase_001", "total_phases": 2},
    }
    apply_snapshot_to_body(body, build_exec_policy_snapshot(exec_argv=spec_argv()))
    return body


def test_plan_contract_validation_failure_skips_plan_review_subpipe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """plan review must run only after write_plan + contract validation succeed."""
    import nodeflow.workflows.dev_process.flow_actions as flow_actions_mod

    body = _minimal_body(tmp_path)
    artifact = body["run_context"]["artifact_root"]
    spec_dir = Path(artifact) / "spec"
    spec_dir.mkdir(parents=True, exist_ok=True)
    (spec_dir / "spec.md").write_text("# Spec\n", encoding="utf-8")
    plan_dir = Path(artifact) / "plan"
    plan_dir.mkdir(parents=True, exist_ok=True)
    (plan_dir / "plan.md").write_text("# Plan\n", encoding="utf-8")
    (plan_dir / "plan.json").write_text('{"phases": []}', encoding="utf-8")
    body["stages"]["plan"] = {
        "status": "completed",
        "plan_json_path": str(plan_dir / "plan.json"),
    }

    subpipe_calls: list[str] = []
    real_run_subpipe = flow_actions_mod.run_subpipe

    def _track_subpipe(spec_path: str, ctx: dict, *, workspace: str) -> dict:
        subpipe_calls.append(spec_path)
        if "plan_cycle.json" in spec_path:
            return ctx
        return real_run_subpipe(spec_path, ctx, workspace=workspace)

    monkeypatch.setattr(flow_actions_mod, "run_subpipe", _track_subpipe)
    monkeypatch.setattr(
        "nodeflow.workflows.dev_process.contract_check.validate_rework_contracts",
        MagicMock(side_effect=NodeExecutionFailure("contract rejected")),
    )
    monkeypatch.setattr(
        "nodeflow.workflows.dev_process.phase_loop.load_plan_data",
        MagicMock(return_value=MagicMock(phases=[MagicMock()], plan_sha256="abc")),
    )
    monkeypatch.setattr(
        flow_actions_mod,
        "_archive_failed_plan_attempt",
        MagicMock(),
    )

    with pytest.raises(NodeExecutionFailure, match="contract rejected"):
        _run_plan_cycle(
            body,
            run_id="pr4_test_run",
            action="revise_plan",
            revision_context="fix contracts",
        )

    assert any("plan_cycle.json" in p for p in subpipe_calls)
    assert not any("plan_review.json" in p for p in subpipe_calls)


@patch("nodeflow.workflows.dev_process.nodes._ctx.repo_root_from_ctx")
@patch("nodeflow.workflows.dev_process.phase_git.collect_phase_changed_paths")
@patch("nodeflow.workflows.dev_process.nodes.stage_nodes.run_lint_fix_stage")
def test_lint_fix_node_collects_changed_paths_after_implementation(
    mock_lint: MagicMock,
    mock_collect: MagicMock,
    mock_repo: MagicMock,
    tmp_path: Path,
) -> None:
    mock_collect.return_value = ["src/foo.py"]
    mock_lint.return_value = {"lint_fix": "ok", "evidence_paths": []}

    body = _minimal_body(tmp_path)
    mock_repo.return_value = Path(body["run_context"]["repo_root"])
    body["stages"]["implementation"] = {"status": "completed", "phase_id": "phase_001"}
    ctx = make_flow_ctx(body, params={"phase_id": "phase_001"})
    node = STAGE_NODE_REGISTRY["lint_fix"]()
    node.execute({"ctx": ctx}, {})

    assert node.read_status() == "done"
    mock_collect.assert_called_once()
    assert mock_lint.call_args is not None
    assert mock_lint.call_args.kwargs["changed_paths"] == ["src/foo.py"]


@patch("nodeflow.workflows.dev_process.nodes._ctx.repo_root_from_ctx")
@patch("nodeflow.workflows.dev_process.reuse.collect_diff")
@patch("nodeflow.workflows.dev_process.nodes.stage_nodes.run_run_tests_stage")
def test_run_tests_node_uses_default_argv_not_exec_argv(
    mock_run_tests: MagicMock,
    mock_diff: MagicMock,
    mock_repo: MagicMock,
    tmp_path: Path,
) -> None:
    mock_diff.side_effect = [{"files": ["pre"]}, {"files": ["pre", "post"]}]
    mock_run_tests.return_value = {
        "status": "completed",
        "test_result": {"ok": True},
        "stage_checkpoint_path": "",
        "stage_result": {},
    }

    body = _minimal_body(tmp_path)
    mock_repo.return_value = Path(body["run_context"]["repo_root"])
    apply_snapshot_to_body(
        body,
        build_exec_policy_snapshot(exec_argv=["codex", "exec", "--sandbox", "workspace-write"]),
    )
    body["stages"]["implementation"] = {"execution_output": {}}
    ctx = make_flow_ctx(
        body,
        params={
            "phase_id": "phase_001",
            "base_revision": body["run_context"]["source_base_revision"],
        },
    )
    node = STAGE_NODE_REGISTRY["run_tests"]()
    out = node.execute({"ctx": ctx}, {})

    assert node.read_status() == "done"
    mock_run_tests.assert_called_once()
    assert mock_run_tests.call_args.kwargs["test_argv"] is None
    assert mock_diff.call_count == 2
    run_tests_stage = out["ctx"]["body"]["stages"]["run_tests"]
    assert run_tests_stage["pre_test_diff_result"] == {"files": ["pre"]}
    assert run_tests_stage["diff_result"] == {"files": ["pre", "post"]}


def test_skip_implementation_phase_mismatch_is_fatal(tmp_path: Path) -> None:
    body = _minimal_body(tmp_path)
    body["stages"]["implementation"] = {"phase_id": "phase_000", "status": "completed"}
    ctx = make_flow_ctx(body, params={"phase_id": "phase_001", "skip_implementation": True})
    node = STAGE_NODE_REGISTRY["write_implementation"]()
    node.execute({"ctx": ctx}, {})
    assert node.read_status() == "fatal"
    assert "cannot skip safely" in str(node.read_error())


def test_force_blocking_review_argv_cleared_after_phase_step_params(
    tmp_path: Path,
) -> None:
    body = _minimal_body(tmp_path)
    _apply_force_blocking_review_argv(body, force=True)
    assert body["dev_process"].get("review_argv_override")

    out_body = dict(body)
    _clear_review_argv_override(out_body)
    assert "review_argv_override" not in out_body.get("dev_process", {})


def test_leaf_missing_ctx_is_fatal_without_allow_pending_noop() -> None:
    node = STAGE_NODE_REGISTRY["write_spec"]()
    node.execute({}, {})
    assert node.read_status() == "fatal"
    assert "missing input port 'ctx'" in str(node.read_error())


def test_leaf_missing_ctx_is_noop_with_allow_pending_noop_flag() -> None:
    node = STAGE_NODE_REGISTRY["write_spec"]()
    out = node.execute({}, {"_allow_pending_inputs_noop": True})
    assert node.read_status() == "idle"
    assert node.read_error() is None
    assert isinstance(out, dict)
    assert out["_state"]["value"] == "idle"


@patch("nodeflow.workflows.dev_process.reuse.aggregate_reviews")
def test_phase_step_coordinator_path_node_runs_match_pipe_nodes(
    mock_aggregate: Any,
    tmp_path: Path,
) -> None:
    """Same contract as PR3 smoke, using coordinator-style params (no pre-collected diff)."""
    from contextlib import ExitStack
    from unittest.mock import patch as _patch

    mock_aggregate.return_value = (
        {"ok": True, "blocking_findings": [], "decision": "merge_ok"},
        {"ok": True},
    )

    _RUN_NODE_EXEC_PATCH_TARGETS = (
        "nodeflow.workflows.dev_process.stages.spec.run_node_exec",
        "nodeflow.workflows.dev_process.stages.implementation.run_node_exec",
        "nodeflow.workflows.dev_process.stages.test_implementation.run_node_exec",
        "nodeflow.workflows.dev_process.stages.review_agent.run_node_exec",
    )

    def _hermetic(body: dict, *, node_name: str, stage: str, **kwargs: Any) -> tuple:
        from nodeflow.workflows.dev_process.node_run import NodeRun
        from nodeflow.workflows.dev_process.node_runner import append_node_run_record

        record = NodeRun(
            node_name=node_name,
            node_type=f"dev_process.{node_name}",
            stage=stage,
            kind="llm",
            worker="codex",
            model="hermetic",
            session_id="s",
            evidence_path=f"/ev/{node_name}.json",
            argv=["echo"],
        )
        append_node_run_record(body, record)
        return ({"ok": True, "stdout": "{}"}, f"/ev/{node_name}.json", record)

    body = _minimal_body(tmp_path)
    apply_snapshot_to_body(body, build_exec_policy_snapshot(exec_argv=implement_argv()))
    runs_before = len(body["node_runs"])
    ctx = make_flow_ctx(
        body,
        segment="phase_step",
        params={
            "phase_id": "phase_001",
            "review_agents": ["architecture"],
            "review_scope": "phase",
            "approved_spec": "# Spec",
            "approved_plan": "# Plan",
            "base_revision": body["run_context"]["source_base_revision"],
        },
    )
    spec = load_pipeline(str(REPO_ROOT), PHASE_STEP_SPEC)
    with ExitStack() as stack:
        for target in _RUN_NODE_EXEC_PATCH_TARGETS:
            stack.enter_context(_patch(target, side_effect=_hermetic))
        result = run_subpipe(PHASE_STEP_SPEC, ctx, workspace=str(REPO_ROOT))

    out_body = result["body"]
    added = len(out_body["node_runs"]) - runs_before
    assert added == PHASE_STEP_NODE_COUNT
    assert list(spec.graph_node_order) == [
        r["node_name"] for r in out_body["node_runs"][runs_before:]
    ]


def test_spec_cycle_subpipe_failure_does_not_adopt_partial_ctx(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Atomic subpipe failure: coordinator must not treat partial ctx as SOT."""
    import nodeflow.workflows.dev_process.nodes.stage_nodes as stage_nodes_mod

    review_calls = 0

    def _fail_review(**kwargs: object) -> dict:
        nonlocal review_calls
        review_calls += 1
        raise NodeExecutionFailure("review failed")

    monkeypatch.setattr(stage_nodes_mod, "run_spec_review_stage", _fail_review)

    body = _minimal_body(tmp_path)
    runs_before = len(body["node_runs"])
    ctx = make_flow_ctx(body, segment="spec_cycle")

    with pytest.raises(NodeExecutionFailure):
        run_subpipe(
            "examples/pipes/dev_process/spec_cycle.json",
            ctx,
            workspace=str(REPO_ROOT),
        )

    assert review_calls == 1
    assert len(body["node_runs"]) == runs_before
    assert "spec_review" not in body.get("stages", {})


def test_record_skipped_node_run_writes_evidence_from_run_context(tmp_path: Path) -> None:
    """Skipped node_runs must always have a real evidence_path (no caller artifact_root)."""
    from nodeflow.workflows.dev_process.node_runner import record_skipped_node_run

    body = _minimal_body(tmp_path)
    record = record_skipped_node_run(
        body,
        node_name="review_requirements",
        stage="review",
        skip_reason="inactive_review_agent",
    )
    assert record.evidence_path
    assert Path(record.evidence_path).is_file()
    assert ".review_requirements.0.skipped.json" in Path(record.evidence_path).name


def test_record_skipped_node_run_evidence_paths_are_unique_per_attempt(tmp_path: Path) -> None:
    from nodeflow.workflows.dev_process.node_runner import record_skipped_node_run

    body = _minimal_body(tmp_path)
    first = record_skipped_node_run(
        body,
        node_name="review_requirements",
        stage="review",
        skip_reason="inactive_review_agent",
    )
    second = record_skipped_node_run(
        body,
        node_name="review_requirements",
        stage="review",
        skip_reason="inactive_review_agent",
    )
    assert first.evidence_path != second.evidence_path
    assert Path(first.evidence_path).is_file()
    assert Path(second.evidence_path).is_file()
    assert ".review_requirements.0.skipped.json" in Path(first.evidence_path).name
    assert ".review_requirements.1.skipped.json" in Path(second.evidence_path).name


def test_review_prompt_includes_repository_inspection_contract() -> None:
    """Review agents must not treat diff_result as the sole review source."""
    from nodeflow.workflows.dev_process.reuse import build_review_prompt
    from nodeflow.workflows.dev_process.review_config import review_node_name

    text = build_review_prompt(
        review_node_name("requirements"),
        repo_root="/tmp",
        base_revision="abc",
        diff_result={"diff": "", "status_short": "", "untracked_files": []},
        test_result={},
        approved_spec="spec",
        approved_plan="plan",
    )
    assert "Do not rely only on the provided diff_result" in text
    assert "inspect the repository directly" in text


@patch("nodeflow.workflows.dev_process.nodes._ctx.repo_root_from_ctx")
@patch("nodeflow.workflows.dev_process.nodes.stage_nodes.run_one_review_agent_stage")
def test_review_agent_passes_augmented_plan_to_stage(
    mock_review_stage: MagicMock,
    mock_repo: MagicMock,
    tmp_path: Path,
) -> None:
    """Phase review supplements must reach run_one_review_agent_stage via ReviewAgentNode."""
    mock_review_stage.return_value = ({"ok": True}, "/ev/review_requirements.json")
    body = _minimal_body(tmp_path)
    mock_repo.return_value = Path(body["run_context"]["repo_root"])
    body["stages"]["lint_fix"] = {"lint_fix": "passed", "evidence_paths": ["/ev/lint.json"]}
    ctx = make_flow_ctx(
        body,
        params={
            "review_agents": ["requirements"],
            "review_scope": "phase",
            "review_targets": ["implementation_phase"],
            "review_checklist": ["Checklist A"],
            "review_acceptance_criteria": ["Criteria B"],
            "approved_spec": "# Spec",
            "approved_plan": "# Plan",
            "base_revision": body["run_context"]["source_base_revision"],
        },
    )
    node = STAGE_NODE_REGISTRY["review_requirements"]()
    out = node.execute({"ctx": ctx}, {})

    assert node.read_status() == "done"
    mock_review_stage.assert_called_once()
    plan_text = mock_review_stage.call_args.kwargs["approved_plan"]
    assert "Review checklist:" in plan_text
    assert "Checklist A" in plan_text
    assert "Review acceptance criteria:" in plan_text
    assert "Criteria B" in plan_text
    assert "Review targets: implementation_phase" in plan_text
    assert "Lint result:" in plan_text
    assert out["ctx"]["body"]["_review_inputs"]["review_requirements"]["ok"] is True
    assert out["ctx"]["body"]["_review_evidence_paths"]["review_requirements"] == (
        "/ev/review_requirements.json"
    )


def test_augment_review_plan_includes_phase_supplements() -> None:
    from nodeflow.workflows.dev_process.nodes.stage_nodes import _augment_review_plan

    text = _augment_review_plan(
        "# Plan",
        {
            "review_targets": ["implementation_phase", "test_phase"],
            "review_checklist": ["Checklist A"],
            "review_acceptance_criteria": ["Criteria B"],
            "review_scope": "phase",
        },
        {"lint_fix": "ok"},
    )
    assert "Review targets: implementation_phase, test_phase" in text
    assert "Review checklist:" in text
    assert "Checklist A" in text
    assert "Review acceptance criteria:" in text
    assert "Criteria B" in text
    assert "Review scope: phase" in text
    assert "Lint result:" in text


def test_review_aggregate_filters_and_clears_stale_review_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Aggregate must ignore stale keys and clear _review_inputs after completion."""
    import nodeflow.workflows.dev_process.reuse as reuse_mod

    captured: dict[str, Any] = {}

    def _fake_aggregate_reviews(**kwargs: Any) -> tuple[dict[str, Any], dict[str, Any]]:
        captured["review_inputs"] = kwargs.get("review_inputs")
        return ({"ok": True, "blocking_findings": [], "decision": "merge_ok"}, {"ok": True})

    monkeypatch.setattr(reuse_mod, "aggregate_reviews", _fake_aggregate_reviews)
    monkeypatch.setattr(
        reuse_mod,
        "write_stage_checkpoint",
        lambda **kwargs: {"artifacts": []},
    )

    body = _minimal_body(tmp_path)
    body["_review_inputs"] = {
        "review_requirements": {"ok": True},
        "review_architecture": {"ok": True},
        "review_impact": {"ok": False},  # stale input from a different segment
    }
    body["_review_evidence_paths"] = {
        "review_requirements": "/current/review_requirements.json",
        "review_architecture": "/current/review_architecture.json",
        "review_impact": "/stale/review_impact.json",
    }
    body["node_runs"].extend(
        [
            {
                "node_name": "review_requirements",
                "evidence_path": "/old_phase/review_requirements.json",
                "skipped": False,
            },
            {
                "node_name": "review_architecture",
                "evidence_path": "/old_phase/review_architecture.json",
                "skipped": False,
            },
        ]
    )
    ctx = make_flow_ctx(
        body,
        segment="phase_step",
        params={
            "review_agents": ["requirements", "architecture"],
            "review_scope": "phase",
            "approved_spec": "# Spec",
            "approved_plan": "# Plan",
            "base_revision": body["run_context"]["source_base_revision"],
        },
    )

    node = STAGE_NODE_REGISTRY["review_aggregate"]()
    out = node.execute({"ctx": ctx}, {})
    assert node.read_status() == "done"

    assert captured["review_inputs"] == {
        "review_requirements": {"ok": True},
        "review_architecture": {"ok": True},
    }
    assert out["ctx"]["body"]["_review_inputs"] == {}
    assert out["ctx"]["body"]["_review_evidence_paths"] == {}
    assert out["ctx"]["body"]["stages"]["review"]["evidence_paths"] == [
        "/current/review_requirements.json",
        "/current/review_architecture.json",
    ]


def test_flow_actions_has_no_stages_direct_import() -> None:
    """PR4: coordinator must not call stage runners directly."""
    source = (REPO_ROOT / "nodeflow/workflows/dev_process/flow_actions.py").read_text(
        encoding="utf-8"
    )
    assert "from nodeflow.workflows.dev_process.stages" not in source
    assert "import nodeflow.workflows.dev_process.stages" not in source


def test_skip_implementation_skipped_evidence_under_phase_artifact_root(
    tmp_path: Path,
) -> None:
    body = _minimal_body(tmp_path)
    run_artifact = Path(body["run_context"]["artifact_root"])
    ctx = make_flow_ctx(
        body,
        params={"phase_id": "phase_001", "skip_implementation": True},
    )
    node = STAGE_NODE_REGISTRY["write_implementation"]()
    out = node.execute({"ctx": ctx}, {})
    assert node.read_status() == "done"

    runs = out["ctx"]["body"]["node_runs"]
    assert len(runs) == 1
    assert runs[0]["skipped"] is True
    assert runs[0]["skip_reason"] == "skip_implementation"
    ep = Path(runs[0]["evidence_path"])
    assert ep.is_file()
    assert ep.is_relative_to(run_artifact / "phases" / "phase_001")


def test_phase_step_failure_clears_force_blocking_review_argv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """review_argv_override must not leak when phase_step subpipe fails."""
    import nodeflow.workflows.dev_process.flow_actions as flow_actions_mod

    body = _minimal_body(tmp_path)
    artifact = Path(body["run_context"]["artifact_root"])
    (artifact / "spec").mkdir(parents=True, exist_ok=True)
    (artifact / "spec" / "spec.md").write_text("# Spec\n", encoding="utf-8")
    (artifact / "plan").mkdir(parents=True, exist_ok=True)
    (artifact / "plan" / "plan.md").write_text("# Plan\n", encoding="utf-8")
    dp = body.setdefault("dev_process", {})
    dp["current_phase_id"] = "phase_001"
    dp["total_phases"] = 2
    dp["phase_index"] = 0
    dp["phase_results"] = {"phase_001": {}}
    dp["task_branch"] = {"name": "main", "base_ref": body["run_context"]["source_base_revision"]}

    def _fail_subpipe(*args: object, **kwargs: object) -> dict:
        raise NodeExecutionFailure("phase_step failed")

    monkeypatch.setattr(flow_actions_mod, "run_subpipe", _fail_subpipe)
    monkeypatch.setattr(flow_actions_mod, "_fail_checkpoint", lambda **kwargs: None)

    phase_ctx = {
        "phase_id": "phase_001",
        "phase_index": 0,
        "total_phases": 2,
        "phase_title": "Phase 1",
        "phase_goal": "goal",
        "phase_scope_include": ["a"],
        "phase_scope_exclude": [],
        "phase_test_plan": ["t"],
        "phase_review_targets": ["implementation_phase"],
        "phase_review_agents": ["architecture"],
        "phase_review_checklist": [],
        "phase_acceptance_criteria": [],
    }

    with pytest.raises(NodeExecutionFailure, match="phase_step failed"):
        flow_actions_mod._run_single_phase(
            body,
            run_id=body["run_context"]["run_id"],
            phase_ctx=phase_ctx,
            force_review_blocking=True,
        )

    assert "review_argv_override" not in body.get("dev_process", {})
