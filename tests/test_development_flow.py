"""Tests for development_flow helpers and example pipelines."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from nodeflow.execution.loader import load_pipeline
from nodeflow.execution.run import load_and_kick_pipeline
from nodeflow.nodes.development_flow.common.collect_diff import CollectDiffNode
from nodeflow.nodes.development_flow.common.write_checkpoint import WriteCheckpointNode
from nodeflow.nodes.development_flow.review_pipe.aggregate_reviews import AggregateReviewsNode
from nodeflow.nodes.development_flow.review_pipe.review_parse import (
    parse_review_contract_from_execution_result,
    validate_review_contract_payload,
)
from nodeflow.nodes.development_flow.spec_plan_pipe.collect_repo_context import (
    CollectRepoContextNode,
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
    assert "?? visible.txt" in (dr.get("status_short") or "")
    assert ".nodeflow/" not in (dr.get("status_short") or "")
    assert "?? .nodeflow/" in (dr.get("status_short_raw") or "")


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


def _git_repo_with_commit(repo: Path) -> None:
    subprocess.run(["git", "init", "-b", "main"], cwd=str(repo), check=True, capture_output=True)
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=str(repo), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "init"],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )


def test_collect_repo_context_includes_untracked_excerpts(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git_repo_with_commit(repo)
    (repo / "draft.py").write_text("# new untracked\n", encoding="utf-8")

    node = CollectRepoContextNode()
    out = node.execute(
        {"repo_root": str(repo), "base_ref": "HEAD", "task_prompt": "Add feature"},
        {},
    )
    text = out["codex_task_prompt"]["text"]
    assert "Untracked paths" in text
    assert "draft.py" in text
    assert "Untracked file excerpts" in text
    rc = out["repo_context"].get("untracked_ls_returncode")
    assert rc == 0


def test_collect_repo_context_filters_ignored_untracked_from_status(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git_repo_with_commit(repo)
    (repo / ".nodeflow" / "checkpoints").mkdir(parents=True)
    (repo / ".nodeflow" / "checkpoints" / "x.json").write_text("{}", encoding="utf-8")
    (repo / "visible.txt").write_text("u\n", encoding="utf-8")

    node = CollectRepoContextNode()
    out = node.execute({"repo_root": str(repo), "base_ref": "HEAD", "task_prompt": "t"}, {})
    rc = out["repo_context"]
    assert "visible.txt" in (rc.get("status_short") or "")
    assert ".nodeflow/" not in (rc.get("status_short") or "")
    assert "?? .nodeflow/" in (rc.get("status_short_raw") or "")


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
            "execution_result": {
                "ok": True,
                "stdout": stdout,
                "stderr": "",
                "executor": "codex",
                "provider": "codex",
                "raw_response": {},
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


def test_dev_cycle_example_pipelines_smoke(tmp_path: Path) -> None:
    repo_pkg = Path(__file__).resolve().parents[1]
    work = tmp_path / "workspace"
    work.mkdir()
    _git_repo_with_commit(work)

    spec_yaml = str(repo_pkg / "examples" / "pipelines" / "dev_cycle_spec_plan.yaml")
    out1 = load_and_kick_pipeline(
        str(work),
        spec_yaml,
        {"task_prompt": "smoke", "repo_root": str(work), "base_ref": "HEAD"},
    )
    sr1 = out1["stage_result"]
    assert sr1.get("ok") is True
    cand = sr1.get("approved_candidate_path")
    assert cand and Path(cand).is_file()
    approved = json.loads(Path(cand).read_text(encoding="utf-8"))
    assert "spec" in approved and "plan" in approved

    impl_yaml = str(repo_pkg / "examples" / "pipelines" / "dev_cycle_implement.yaml")
    out2 = load_and_kick_pipeline(
        str(work),
        impl_yaml,
        {
            "approved_checkpoint_path": ".nodeflow/checkpoints/001_approved_candidate.json",
            "repo_root": str(work),
            "base_ref": "HEAD",
            "task_type": "implement",
        },
    )
    assert out2["stage_result"].get("ok") is True

    review_yaml = str(repo_pkg / "examples" / "pipelines" / "dev_cycle_review.yaml")
    out3 = load_and_kick_pipeline(
        str(work),
        review_yaml,
        {
            "approved_checkpoint_path": ".nodeflow/checkpoints/001_approved_candidate.json",
            "repo_root": str(work),
            "base_ref": "HEAD",
            "task_type": "review",
        },
    )
    assert out3["stage_result"].get("ok") is True
