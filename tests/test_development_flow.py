"""Tests for development_flow helpers and example pipelines."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from nodeflow.execution.loader import load_pipeline
from nodeflow.nodes.development_flow.common.collect_diff import CollectDiffNode
from nodeflow.nodes.development_flow.common.write_checkpoint import WriteCheckpointNode
from nodeflow.nodes.development_flow.review_pipe.aggregate_reviews import AggregateReviewsNode
from nodeflow.nodes.development_flow.review_pipe.review_parse import (
    parse_review_contract_from_execution_result,
    validate_review_contract_payload,
)
from nodeflow.nodes.exec.codex_exec import CodexExecNode


def test_dev_cycle_example_yamls_load():
    repo = Path(__file__).resolve().parents[1]
    for name in (
        "dev_cycle_spec_plan.yaml",
        "dev_cycle_implement.yaml",
        "dev_cycle_review.yaml",
    ):
        path = repo / "examples" / "pipelines" / name
        load_pipeline(str(repo), str(path))


def test_parse_review_contract_braces_inside_json_string():
    payload = {
        "ok": True,
        "blocking_findings": [],
        "non_blocking_findings": [],
        "spec_revision_needed": False,
        "summary": "The returned dict {'ok': true} is noted.",
    }
    text = json.dumps(payload)
    er = {"ok": True, "stdout": text, "stderr": None, "raw_response": {}}
    parsed_ok, out = parse_review_contract_from_execution_result(er)
    assert parsed_ok
    assert out.get("summary") == payload["summary"]


def test_write_checkpoint_ok_reflects_child_and_next_action_on_failure(tmp_path: Path):
    node = WriteCheckpointNode()
    cp_dir = str(tmp_path / "checkpoints")
    base_params = {
        "checkpoint_dir": cp_dir,
        "next_action_default": "review",
        "next_action_on_failure": "rework_implementation",
    }
    out = node.execute(
        {
            "request": {
                "stage": "implement",
                "summary": "done",
                "artifacts": [],
                "human_decision_required": True,
            },
            "execution_result": {
                "ok": False,
                "executor": "codex",
                "provider": "codex",
                "raw_response": {},
                "artifacts": [],
                "provider_meta": {},
            },
        },
        base_params,
    )
    sr = out["stage_result"]
    assert sr["ok"] is False
    assert sr["next_action"] == "rework_implementation"

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
            "execution_result": {
                "ok": False,
                "executor": "codex",
                "provider": "codex",
                "raw_response": {},
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
            "execution_result": {
                "ok": True,
                "executor": "codex",
                "provider": "codex",
                "raw_response": {},
                "artifacts": [],
                "provider_meta": {},
            },
        },
        {
            "checkpoint_dir": cp_dir,
            "next_action_default": "review",
            "next_action_on_failure": "rework_implementation",
        },
    )
    sr2 = out2["stage_result"]
    assert sr2["ok"] is True
    assert sr2["next_action"] == "review"


def test_codex_exec_passes_prompt_to_stdin(tmp_path: Path) -> None:
    node = CodexExecNode()
    out = node.execute(
        {"prompt": "hello-stdin"},
        {
            "argv": [sys.executable, "-c", "import sys; print(sys.stdin.read(), end='')"],
            "_workspace_dir": str(tmp_path),
        },
    )
    assert node.read_status() == "done"
    assert out["execution_result"]["ok"] is True
    assert (out["execution_result"].get("stdout") or "").strip() == "hello-stdin"


def test_collect_diff_includes_untracked(tmp_path: Path) -> None:
    subprocess.run(
        ["git", "init", "-b", "main"], cwd=str(tmp_path), check=True, capture_output=True
    )
    (tmp_path / "README.md").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=str(tmp_path), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "init"],
        cwd=str(tmp_path),
        check=True,
        capture_output=True,
    )
    (tmp_path / "new_untracked.txt").write_text("secret\n", encoding="utf-8")

    node = CollectDiffNode()
    out = node.execute(
        {"repo_root": str(tmp_path), "base_ref": "HEAD"},
        {},
    )
    assert node.read_status() == "done"
    dr = out["diff_result"]
    assert "new_untracked.txt" in dr.get("untracked_files", [])
    assert any(e.get("path") == "new_untracked.txt" for e in dr.get("untracked_file_excerpts", []))


def test_collect_diff_ignores_nodeflow_untracked_by_default(tmp_path: Path) -> None:
    subprocess.run(
        ["git", "init", "-b", "main"], cwd=str(tmp_path), check=True, capture_output=True
    )
    (tmp_path / "README.md").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=str(tmp_path), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "init"],
        cwd=str(tmp_path),
        check=True,
        capture_output=True,
    )
    nf = tmp_path / ".nodeflow" / "checkpoints"
    nf.mkdir(parents=True)
    (nf / "x.json").write_text("{}", encoding="utf-8")
    (tmp_path / "visible.txt").write_text("u\n", encoding="utf-8")

    node = CollectDiffNode()
    out = node.execute({"repo_root": str(tmp_path), "base_ref": "HEAD"}, {})
    dr = out["diff_result"]
    assert "visible.txt" in dr.get("untracked_files", [])
    assert ".nodeflow/checkpoints/x.json" not in dr.get("untracked_files", [])
    assert all(not p.startswith(".nodeflow/") for p in dr.get("untracked_files", []))


def test_validate_review_contract_payload_requires_schema() -> None:
    assert validate_review_contract_payload(
        {
            "ok": True,
            "blocking_findings": [],
            "non_blocking_findings": [],
            "spec_revision_needed": False,
        }
    )
    assert not validate_review_contract_payload({"summary": "looks good"})


def test_aggregate_reviews_blocks_when_diff_collect_failed() -> None:
    node = AggregateReviewsNode()
    valid = json.dumps(
        {
            "ok": True,
            "blocking_findings": [],
            "non_blocking_findings": [],
            "spec_revision_needed": False,
        }
    )
    er = {"ok": True, "stdout": valid, "stderr": "", "raw_response": {}}
    out = node.execute(
        {
            "review_diff": er,
            "review_spec": er,
            "test_result": {"ok": True},
            "diff_result": {"ok": False, "diff": "", "untracked_files": []},
        },
        {},
    )
    rr = out["review_result"]
    assert rr["ok"] is False
    assert any(b.get("id") == "R_DIFF_COLLECT" for b in rr["blocking_findings"])
    assert rr["decision"] == "rework_implementation"
    assert rr["suggested_next_action"] == "rework_implementation"


def test_aggregate_reviews_schema_parse_failure_blocks() -> None:
    node = AggregateReviewsNode()
    bad = json.dumps({"summary": "looks good"})
    er_bad = {"ok": True, "stdout": bad, "stderr": "", "raw_response": {}}
    valid = json.dumps(
        {
            "ok": True,
            "blocking_findings": [],
            "non_blocking_findings": [],
            "spec_revision_needed": False,
        }
    )
    er_ok = {"ok": True, "stdout": valid, "stderr": "", "raw_response": {}}
    out = node.execute(
        {
            "review_diff": er_bad,
            "review_spec": er_ok,
            "test_result": {"ok": True},
            "diff_result": {"ok": True, "diff": "x", "untracked_files": []},
        },
        {},
    )
    rr = out["review_result"]
    assert rr["ok"] is False
    assert any(b.get("id") == "R_DIFF_PARSE" for b in rr["blocking_findings"])
