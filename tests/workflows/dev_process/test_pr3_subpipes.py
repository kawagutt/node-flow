"""PR3: dev_process subpipe JSON load + run_subpipe smoke."""

from __future__ import annotations

import json
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from nodeflow.core.loader import load_pipeline
from nodeflow.workflows.dev_process import register_dev_process_nodes
from nodeflow.workflows.dev_process.exec_policy import (
    apply_snapshot_to_body,
    build_exec_policy_snapshot,
)
from nodeflow.workflows.dev_process.node_run import NodeRun
from nodeflow.workflows.dev_process.node_runner import append_node_run_record
from nodeflow.workflows.dev_process.nodes import make_flow_ctx
from nodeflow.workflows.dev_process.subpipe_runner import run_subpipe
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit
from tests.workflows.dev_process.hermetic_argv import implement_argv, spec_argv

register_dev_process_nodes()

REPO_ROOT = Path(__file__).resolve().parents[3]

SUBPIPE_PATHS = (
    "examples/pipes/dev_process/spec_cycle.json",
    "examples/pipes/dev_process/plan_cycle.json",
    "examples/pipes/dev_process/plan_review.json",
    "examples/pipes/dev_process/phase_step.json",
    "examples/pipes/dev_process/final_review.json",
)

PHASE_STEP_SPEC = "examples/pipes/dev_process/phase_step.json"
PHASE_STEP_NODE_COUNT = 12
FINAL_REVIEW_SPEC = "examples/pipes/dev_process/final_review.json"
FINAL_REVIEW_NODE_COUNT = 4

_RUN_NODE_EXEC_PATCH_TARGETS = (
    "nodeflow.workflows.dev_process.stages.spec.run_node_exec",
    "nodeflow.workflows.dev_process.stages.spec_review.run_node_exec",
    "nodeflow.workflows.dev_process.stages.implementation.run_node_exec",
    "nodeflow.workflows.dev_process.stages.test_implementation.run_node_exec",
    "nodeflow.workflows.dev_process.stages.review_agent.run_node_exec",
)


def _stdout_for_node(node_name: str) -> str:
    if node_name == "write_spec":
        return json.dumps({"spec": "# Spec\n"})
    if node_name in {"review_spec", "review_plan"}:
        return json.dumps({"ok": True, "blocking_findings": []})
    if node_name.startswith("review_"):
        return json.dumps({"ok": True, "blocking_findings": []})
    return "stub ok"


def _hermetic_run_node_exec(
    body: dict[str, Any],
    *,
    node_name: str,
    stage: str,
    **kwargs: Any,
) -> tuple[dict[str, Any], str, NodeRun]:
    stdout = _stdout_for_node(node_name)
    evidence_path = f"/ev/{node_name}.json"
    record = NodeRun(
        node_name=node_name,
        node_type=f"dev_process.{node_name}",
        stage=stage,
        kind="llm",
        worker="codex",
        model="hermetic",
        session_id=f"{kwargs.get('run_id', 'run')}_{node_name}_0",
        evidence_path=evidence_path,
        argv=["echo", "ok"],
    )
    append_node_run_record(body, record)
    return ({"ok": True, "stdout": stdout, "stderr": ""}, evidence_path, record)


@pytest.fixture
def hermetic_run_node_exec():
    with ExitStack() as stack:
        for target in _RUN_NODE_EXEC_PATCH_TARGETS:
            stack.enter_context(patch(target, side_effect=_hermetic_run_node_exec))
        yield


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
            "run_id": "pr3_test_run",
            "repo_root": str(repo.resolve()),
            "artifact_root": str(artifact),
            "source_base_revision": head,
        },
        "task_prompt": "task",
        "stages": {},
        "node_runs": [],
        "dev_process": {"current_phase_id": "phase_001"},
    }
    apply_snapshot_to_body(body, build_exec_policy_snapshot(exec_argv=spec_argv()))
    return body


@pytest.mark.parametrize("spec_path", SUBPIPE_PATHS, ids=lambda p: Path(p).stem)
def test_subpipe_loads(spec_path: str) -> None:
    spec = load_pipeline(str(REPO_ROOT), spec_path)
    assert "cycle_result" in spec.pipe.output_sources
    assert len(spec.graph_node_order) >= 1


def test_spec_cycle_smoke(hermetic_run_node_exec: None, tmp_path: Path) -> None:
    body = _minimal_body(tmp_path)
    runs_before = len(body["node_runs"])
    ctx = make_flow_ctx(body, segment="spec_cycle")
    result = run_subpipe(
        "examples/pipes/dev_process/spec_cycle.json",
        ctx,
        workspace=str(REPO_ROOT),
    )
    assert isinstance(result, dict)
    out_body = result["body"]
    assert out_body["stages"]["spec"]["status"] == "completed"
    assert out_body["stages"]["spec_review"]["status"] == "completed"
    assert len(out_body["node_runs"]) - runs_before == 2


def test_run_subpipe_passes_allow_pending_inputs_noop_to_pipe_node(
    hermetic_run_node_exec: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_subpipe must enable leaf no-op for Runner pre-delivery execute calls."""
    import nodeflow.workflows.dev_process.subpipe_runner as subpipe_mod

    captured: dict[str, Any] = {}
    real_execute = subpipe_mod.execute_or_raise

    def _spy(node: Any, inputs: dict, params: dict | None = None) -> dict:
        captured["params"] = dict(params or {})
        return real_execute(node, inputs, params)

    monkeypatch.setattr(subpipe_mod, "execute_or_raise", _spy)

    body = _minimal_body(tmp_path)
    ctx = make_flow_ctx(body, segment="spec_cycle")
    run_subpipe(
        "examples/pipes/dev_process/spec_cycle.json",
        ctx,
        workspace=str(REPO_ROOT),
    )

    assert captured["params"].get("_allow_pending_inputs_noop") is True
    assert captured["params"].get("_workspace_dir") == str(REPO_ROOT)


@patch("nodeflow.workflows.dev_process.reuse.aggregate_reviews")
def test_phase_step_smoke_node_runs_match_pipe_nodes(
    mock_aggregate: Any,
    hermetic_run_node_exec: None,
    tmp_path: Path,
) -> None:
    mock_aggregate.return_value = (
        {"ok": True, "blocking_findings": [], "decision": "merge_ok"},
        {"ok": True},
    )

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
            "diff_result": {},
            "test_result": {"ok": True},
            "changed_paths": [],
            "test_argv": implement_argv(),
        },
    )
    spec = load_pipeline(str(REPO_ROOT), PHASE_STEP_SPEC)
    assert len(spec.graph_node_order) == PHASE_STEP_NODE_COUNT

    result = run_subpipe(PHASE_STEP_SPEC, ctx, workspace=str(REPO_ROOT))
    out_body = result["body"]
    added = len(out_body["node_runs"]) - runs_before
    assert added == PHASE_STEP_NODE_COUNT

    names = [r["node_name"] for r in out_body["node_runs"][runs_before:]]
    assert names == list(spec.graph_node_order)

    skipped = [r for r in out_body["node_runs"][runs_before:] if r.get("skipped")]
    assert len(skipped) == 6
    assert all(r["skip_reason"] == "inactive_review_agent" for r in skipped)
    assert "review_architecture" in names
    assert out_body["stages"]["review"]["status"] == "completed"


@patch("nodeflow.workflows.dev_process.reuse.aggregate_reviews")
def test_final_review_smoke_node_runs_match_pipe_nodes(
    mock_aggregate: Any,
    hermetic_run_node_exec: None,
    tmp_path: Path,
) -> None:
    mock_aggregate.return_value = (
        {"ok": True, "blocking_findings": [], "decision": "merge_ok"},
        {"ok": True},
    )
    body = _minimal_body(tmp_path)
    run_artifact = Path(body["run_context"]["artifact_root"])
    final_artifact_root = str(run_artifact / "final_review")
    runs_before = len(body["node_runs"])
    ctx = make_flow_ctx(
        body,
        segment="final_review",
        params={
            "artifact_root": final_artifact_root,
            "review_agents": ["requirements", "test_quality", "checklist_compliance"],
            "review_scope": "final",
            "approved_spec": "# Spec",
            "approved_plan": "# Plan",
            "diff_result": {},
            "test_result": {"ok": True},
        },
    )
    spec = load_pipeline(str(REPO_ROOT), FINAL_REVIEW_SPEC)
    assert len(spec.graph_node_order) == FINAL_REVIEW_NODE_COUNT

    result = run_subpipe(FINAL_REVIEW_SPEC, ctx, workspace=str(REPO_ROOT))
    out_body = result["body"]
    added = len(out_body["node_runs"]) - runs_before
    assert added == FINAL_REVIEW_NODE_COUNT
    names = [r["node_name"] for r in out_body["node_runs"][runs_before:]]
    assert names == list(spec.graph_node_order)

    final_aggregate = Path(final_artifact_root) / "review" / "aggregate.json"
    assert final_aggregate.is_file(), "final review aggregate must live under final_artifact_root"
    phase_aggregate = run_artifact / "phases" / "phase_001" / "review" / "aggregate.json"
    assert not phase_aggregate.exists(), "final review must not write under phase artifact root"


@patch("nodeflow.workflows.dev_process.reuse.aggregate_reviews")
def test_final_review_scope_final_uses_run_root_without_explicit_artifact_root(
    mock_aggregate: Any,
    hermetic_run_node_exec: None,
    tmp_path: Path,
) -> None:
    """review_scope=final alone must not route review artifacts to phases/<id>/."""
    mock_aggregate.return_value = (
        {"ok": True, "blocking_findings": [], "decision": "merge_ok"},
        {"ok": True},
    )
    body = _minimal_body(tmp_path)
    run_artifact = Path(body["run_context"]["artifact_root"])
    ctx = make_flow_ctx(
        body,
        segment="final_review",
        params={
            "review_agents": ["requirements"],
            "review_scope": "final",
            "approved_spec": "# Spec",
            "approved_plan": "# Plan",
            "diff_result": {},
            "test_result": {"ok": True},
        },
    )
    run_subpipe(FINAL_REVIEW_SPEC, ctx, workspace=str(REPO_ROOT))

    run_aggregate = run_artifact / "review" / "aggregate.json"
    assert run_aggregate.is_file()
    phase_aggregate = run_artifact / "phases" / "phase_001" / "review" / "aggregate.json"
    assert not phase_aggregate.exists()


def test_final_review_pipe_matches_final_review_agents() -> None:
    from nodeflow.workflows.dev_process.review_config import FINAL_REVIEW_AGENTS, review_node_name

    spec = load_pipeline(str(REPO_ROOT), FINAL_REVIEW_SPEC)
    expected = [review_node_name(a) for a in FINAL_REVIEW_AGENTS] + ["review_aggregate"]
    assert list(spec.graph_node_order) == expected


def test_run_subpipe_missing_cycle_result_is_fatal(
    hermetic_run_node_exec: None, tmp_path: Path
) -> None:
    bad = tmp_path / "bad_pipe.json"
    bad.write_text(
        json.dumps(
            {
                "kind": "pipe",
                "version": "1.7",
                "pipe": {"outputs": {"not_cycle_result": "write_spec.ctx"}},
                "nodes": [
                    {
                        "id": "write_spec",
                        "path": "examples/nodes/dev_process/write_spec/node.json",
                        "inputs": {"ctx": "input.ctx"},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    body = _minimal_body(tmp_path)
    ctx = make_flow_ctx(body, segment="bad")
    from nodeflow.core.base_node import NodeExecutionFailure

    with pytest.raises(NodeExecutionFailure, match="cycle_result"):
        run_subpipe(str(bad), ctx, workspace=str(REPO_ROOT))
