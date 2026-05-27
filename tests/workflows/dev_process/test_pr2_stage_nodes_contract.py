"""PR2 contracts: leaf ActionNodes, FlowCtx immutability, node_runs recording."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from nodeflow.core.registry import registry
from nodeflow.workflows.dev_process import register_dev_process_nodes
from nodeflow.workflows.dev_process.exec_policy import (
    NODE_NAMES,
    apply_snapshot_to_body,
    build_exec_policy_snapshot,
)
from nodeflow.workflows.dev_process.nodes import STAGE_NODE_REGISTRY, copy_flow_ctx, make_flow_ctx
from nodeflow.workflows.dev_process.nodes.stage_nodes import STAGE_NODE_CLASSES
from nodeflow.workflows.dev_process.review_config import REVIEW_AGENT_TO_NODE
from nodeflow.workflows.dev_process.stages.review import run_review_stage
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit

register_dev_process_nodes()


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
            "run_id": "pr2_test_run",
            "repo_root": str(repo.resolve()),
            "artifact_root": str(artifact),
            "source_base_revision": head,
        },
        "task_prompt": "task",
        "stages": {},
        "node_runs": [],
        "dev_process": {},
    }
    apply_snapshot_to_body(body, build_exec_policy_snapshot(exec_argv=["echo", "ok"]))
    return body


@pytest.mark.parametrize("cls", STAGE_NODE_CLASSES, ids=lambda c: c.node_name)
def test_stage_node_registered(cls) -> None:
    reg_type = f"dev_process.{cls.node_name}"
    assert registry.get(reg_type) is cls


@pytest.mark.parametrize("node_name", sorted(STAGE_NODE_REGISTRY), ids=lambda n: n)
def test_stage_node_json_ports(node_name: str) -> None:
    path = (
        Path(__file__).resolve().parents[3]
        / "examples"
        / "nodes"
        / "dev_process"
        / node_name
        / "node.json"
    )
    assert path.is_file(), f"missing node.json for {node_name}"
    doc = json.loads(path.read_text(encoding="utf-8"))
    assert doc.get("input_ports") == ["ctx"]
    assert doc.get("output_ports") == ["ctx"]
    assert doc.get("type") == f"dev_process.{node_name}"


def test_copy_flow_ctx_does_not_mutate_input() -> None:
    raw = make_flow_ctx({"node_runs": [], "stages": {}, "x": 1}, params={"a": 1})
    snapshot = deepcopy(raw)
    ctx, body = copy_flow_ctx(raw)
    body["x"] = 99
    ctx["params"]["a"] = 99
    assert raw == snapshot


@patch("nodeflow.workflows.dev_process.stages.spec.run_node_exec")
def test_write_spec_node_returns_updated_ctx(mock_exec: MagicMock, tmp_path: Path) -> None:
    mock_exec.return_value = (
        {"ok": True, "stdout": json.dumps({"spec": "# Spec\n"}), "stderr": ""},
        "/ev/spec.json",
        MagicMock(),
    )
    body = _minimal_body(tmp_path)
    ctx = make_flow_ctx(body, segment="spec_cycle")
    node = STAGE_NODE_REGISTRY["write_spec"]()
    out = node.execute({"ctx": ctx}, {})
    assert node.read_status() == "done"
    result_ctx = out["ctx"]
    assert result_ctx is not ctx
    assert ctx["body"] is not result_ctx["body"]
    assert result_ctx["body"]["stages"]["spec"]["status"] == "completed"
    mock_exec.assert_called_once()
    assert mock_exec.call_args.kwargs["node_name"] == "write_spec"


@patch("nodeflow.workflows.dev_process.stages.review_agent.run_node_exec")
def test_inactive_review_agent_records_skipped(mock_exec: MagicMock, tmp_path: Path) -> None:
    body = _minimal_body(tmp_path)
    ctx = make_flow_ctx(
        body,
        params={
            "review_agents": ["architecture"],
            "review_scope": "phase",
            "diff_result": {},
            "test_result": {},
            "approved_spec": "s",
            "approved_plan": "p",
        },
    )
    node = STAGE_NODE_REGISTRY["review_requirements"]()
    out = node.execute({"ctx": ctx}, {})
    assert node.read_status() == "done"
    runs = out["ctx"]["body"]["node_runs"]
    assert len(runs) == 1
    assert runs[0]["node_name"] == "review_requirements"
    assert runs[0]["skipped"] is True
    assert runs[0]["skip_reason"] == "inactive_review_agent"
    assert runs[0]["kind"] == "skipped"
    assert runs[0]["session_id"] is None
    assert runs[0]["evidence_path"]
    assert Path(runs[0]["evidence_path"]).is_file()
    mock_exec.assert_not_called()


def test_skip_implementation_checks_cached_phase_before_skip(tmp_path: Path) -> None:
    body = _minimal_body(tmp_path)
    body["dev_process"]["current_phase_id"] = "phase_002"
    body["stages"]["implementation"] = {"phase_id": "phase_001", "status": "completed"}
    ctx = make_flow_ctx(body, params={"phase_id": "phase_002", "skip_implementation": True})
    node = STAGE_NODE_REGISTRY["write_implementation"]()
    node.execute({"ctx": ctx}, {})
    assert node.read_status() == "fatal"
    assert "cannot skip safely" in str(node.read_error())
    assert not (body.get("node_runs") or [])


@patch("nodeflow.workflows.dev_process.stages.review_agent.run_node_exec")
def test_active_review_agent_calls_one_agent_only(mock_exec: MagicMock, tmp_path: Path) -> None:
    mock_exec.return_value = (
        {"ok": True, "stdout": "{}", "stderr": ""},
        "/ev/review.json",
        MagicMock(),
    )
    body = _minimal_body(tmp_path)
    ctx = make_flow_ctx(
        body,
        params={
            "review_agents": ["requirements"],
            "review_scope": "phase",
            "diff_result": {},
            "test_result": {},
            "approved_spec": "s",
            "approved_plan": "p",
        },
    )
    node = STAGE_NODE_REGISTRY["review_requirements"]()
    node.execute({"ctx": ctx}, {})
    assert mock_exec.call_count == 1
    assert mock_exec.call_args.kwargs["node_name"] == "review_requirements"


@patch("nodeflow.workflows.dev_process.stages.review.run_one_review_agent_stage")
def test_run_review_stage_delegates_per_agent(mock_one: MagicMock, tmp_path: Path) -> None:
    mock_one.return_value = ({"ok": True}, "/ev.json")
    body = _minimal_body(tmp_path)
    with patch("nodeflow.workflows.dev_process.stages.review.aggregate_reviews") as mock_agg:
        mock_agg.return_value = (
            {"ok": True, "blocking_findings": [], "decision": "merge_ok"},
            {"ok": True},
        )
        with patch(
            "nodeflow.workflows.dev_process.stages.review.write_stage_checkpoint"
        ) as mock_cp:
            mock_cp.return_value = {"artifacts": []}
            (Path(body["run_context"]["artifact_root"]) / "review").mkdir(
                parents=True, exist_ok=True
            )
            body["dev_process"]["review_argv_override"] = ["echo", "blocking"]
            run_review_stage(
                repo_root=Path(body["run_context"]["repo_root"]),
                artifact_root=body["run_context"]["artifact_root"],
                run_id="pr2_test_run",
                base_revision="abc",
                approved_spec="s",
                approved_plan="p",
                diff_result={},
                test_result={},
                body=body,
                review_agents=["architecture", "test_quality"],
                review_scope="phase",
            )
    assert "review_argv_override" not in body.get("dev_process", {})
    assert mock_one.call_count == 2
    agents = [c.kwargs["agent"] for c in mock_one.call_args_list]
    assert agents == ["architecture", "test_quality"]


def test_all_exec_policy_node_names_have_stage_nodes() -> None:
    stage_names = {cls.node_name for cls in STAGE_NODE_CLASSES}
    for name in NODE_NAMES:
        assert name in stage_names, f"missing ActionNode for exec_policy node {name!r}"
    assert "review_aggregate" in stage_names
    for agent, node_name in REVIEW_AGENT_TO_NODE.items():
        assert node_name in stage_names, f"missing node for agent {agent!r}"


def test_node_run_optional_session_id_roundtrip() -> None:
    from nodeflow.workflows.dev_process.node_run import NodeRun

    rec = NodeRun(
        node_name="review_requirements",
        node_type="dev_process.review_requirements",
        stage="review",
        kind="skipped",
        worker="local",
        model=None,
        session_id=None,
        evidence_path="",
        argv=[],
        skipped=True,
        skip_reason="inactive_review_agent",
    )
    d = rec.to_dict()
    assert d["session_id"] is None
    assert d["kind"] == "skipped"
