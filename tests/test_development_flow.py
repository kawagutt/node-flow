"""Tests for development_flow helpers and example pipelines."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.execution.loader import load_pipeline
from nodeflow.execution.run import load_and_kick_pipeline
from nodeflow.nodes.development_flow.common.check_source_workspace import (
    CheckSourceWorkspaceNode,
)
from nodeflow.nodes.development_flow.common.collect_diff import CollectDiffNode
from nodeflow.nodes.development_flow.common.git_status import (
    status_has_non_ignored_changes,
    status_violates_start_policy,
)
from nodeflow.nodes.development_flow.common.load_checkpoint import LoadCheckpointNode
from nodeflow.nodes.development_flow.common.prepare_development_run_context import (
    PrepareDevelopmentRunContextNode,
)
from nodeflow.nodes.development_flow.common.prepare_workspace import PrepareWorkspaceNode
from nodeflow.nodes.development_flow.common.write_checkpoint import WriteCheckpointNode
from nodeflow.nodes.development_flow.common.write_development_summary import (
    WriteDevelopmentSummaryNode,
)
from nodeflow.nodes.development_flow.development_flow_pipe import DevelopmentFlowPipeNode
from nodeflow.nodes.development_flow.implement_pipe import ImplementPipeNode
from nodeflow.nodes.development_flow.review_pipe import ReviewPipeNode
from nodeflow.nodes.development_flow.review_pipe.aggregate_reviews import AggregateReviewsNode
from nodeflow.nodes.development_flow.review_pipe.prompt_common import extract_diff_context
from nodeflow.nodes.development_flow.review_pipe.review_parse import (
    parse_review_contract_from_execution_result,
    validate_review_contract_payload,
)
from nodeflow.nodes.development_flow.spec_plan_pipe import SpecPlanPipeNode
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
        "dev_cycle_spec_plan_codex_template.yaml",
        "dev_cycle_implement_codex_template.yaml",
        "dev_cycle_review_codex_template.yaml",
        "development_flow_hermetic.yaml",
        "development_flow_codex_template.yaml",
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
    checkpoint_path = Path(sr2["artifacts"][-1]["path"])
    payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "development_flow.v1"


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


def test_aggregate_reviews_missing_review_input_blocks() -> None:
    node = AggregateReviewsNode()
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
            "review_diff": er_ok,
            # review_wide missing
            "review_tests": er_ok,
            "review_spec": er_ok,
            "review_spec_revision": er_ok,
            "test_result": {"ok": True},
            "diff_result": {"ok": True, "diff": "x", "untracked_files": []},
        },
        {},
    )
    rr = out["review_result"]
    assert rr["ok"] is False
    assert any(b.get("id") == "R_WIDE_MISSING" for b in rr["blocking_findings"])


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


def _source_workspace_check(repo: Path) -> dict:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(repo),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    branch = subprocess.run(
        ["git", "branch", "--show-current"],
        cwd=str(repo),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return {
        "source_repo_root": str(repo.resolve()),
        "current_branch": branch,
        "base_revision": head,
        "clean": True,
    }


def _run_context_for_workspace(repo: Path, planned_branch_name: str) -> dict:
    src = _source_workspace_check(repo)
    return {
        "planned_branch_name": planned_branch_name,
        "source_repo_root": src["source_repo_root"],
        "source_base_revision": src["base_revision"],
        "source_current_branch": src["current_branch"],
    }


def _ok_review_pipe_params(
    checkpoint_dir: Path,
    run_id: str,
    *,
    spec_revision_needed: bool = False,
    include_checkpoint_dir: bool = False,
):
    payload = (
        "import json,sys; sys.stdin.read(); "
        f"print(json.dumps({{'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':{str(spec_revision_needed)}}}))"
    )
    out = {
        "review_diff_focused": {"argv": ["python3", "-c", payload]},
        "review_wide_scan": {"argv": ["python3", "-c", payload]},
        "review_test_focused": {"argv": ["python3", "-c", payload]},
        "review_spec_conformance": {"argv": ["python3", "-c", payload]},
        "review_spec_revision": {"argv": ["python3", "-c", payload]},
        "write_checkpoint": {"run_id": run_id},
    }
    if include_checkpoint_dir:
        out["write_checkpoint"]["checkpoint_dir"] = str(checkpoint_dir)
    return out


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


def test_development_flow_pipe_checkpoint_resume(tmp_path: Path) -> None:
    repo = tmp_path / "workspace"
    repo.mkdir()
    _git_repo_with_commit(repo)

    node = DevelopmentFlowPipeNode()
    start_out = node.execute(
        {
            "action": "start",
            "task_prompt": "build feature",
            "repo_root": str(repo),
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "001",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    fr = start_out["flow_result"]
    assert fr["state"] == "awaiting_approval"
    assert Path(fr["flow_checkpoint_path"]).is_file()
    assert isinstance(fr.get("run_context"), dict)
    assert fr.get("workspace_context") is None
    approved = fr.get("approved_candidate_path")
    assert isinstance(approved, str) and approved

    node.reset_status()
    approve_out = node.execute(
        {
            "action": "approve",
            "repo_root": str(repo),
            "flow_checkpoint_path": fr["flow_checkpoint_path"],
        },
        {
            "implement_pipe": {
                "codex_exec": {
                    "argv": ["python3", "-c", "import sys; sys.stdin.read(); print('ok')"]
                },
                "run_tests": {"argv": ["python3", "-c", "print('tests ok')"]},
                "write_checkpoint": {
                    "run_id": "002",
                },
            },
            "review_pipe": _ok_review_pipe_params(repo / ".nodeflow" / "checkpoints", "003"),
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    afr = approve_out["flow_result"]
    assert afr["state"] == "awaiting_review_decision"
    assert Path(afr["flow_checkpoint_path"]).is_file()
    assert afr["merge_ready"] is True
    assert "merge" in afr["allowed_actions"]
    assert isinstance(afr.get("workspace_context"), dict)
    assert Path(afr["workspace_context"]["workspace_root"]).is_dir()
    assert isinstance(afr["workspace_context"].get("base_revision"), str)
    assert "development_summary" in afr
    assert isinstance(afr["development_summary"].get("commit_message_suggestion"), str)
    assert Path(afr["development_summary"]["artifact_path"]).is_file()
    disk = json.loads(Path(afr["flow_checkpoint_path"]).read_text(encoding="utf-8"))
    assert disk["flow_result"]["flow_checkpoint_path"] == afr["flow_checkpoint_path"]

    node.reset_status()
    merge_out = node.execute(
        {
            "action": "merge",
            "repo_root": str(repo),
            "flow_checkpoint_path": afr["flow_checkpoint_path"],
        },
        {"flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")}},
    )
    mfr = merge_out["flow_result"]
    assert mfr["state"] == "merged"
    assert isinstance(mfr.get("run_context"), dict)
    assert isinstance(mfr.get("workspace_context"), dict)
    assert isinstance(mfr.get("development_summary"), dict)


def test_development_flow_merge_rejects_wrong_state(tmp_path: Path) -> None:
    repo = tmp_path / "workspace"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = DevelopmentFlowPipeNode()
    start_out = node.execute(
        {
            "action": "start",
            "task_prompt": "t",
            "repo_root": str(repo),
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "s1",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    fp = start_out["flow_result"]["flow_checkpoint_path"]
    node.reset_status()
    node.execute(
        {"action": "merge", "repo_root": str(repo), "flow_checkpoint_path": fp},
        {"flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")}},
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "awaiting_review_decision" in str(node.read_error())


def test_development_flow_force_merge(tmp_path: Path) -> None:
    repo = tmp_path / "workspace"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = DevelopmentFlowPipeNode()
    start_out = node.execute(
        {
            "action": "start",
            "task_prompt": "t",
            "repo_root": str(repo),
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "s2",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    start_fp = start_out["flow_result"]["flow_checkpoint_path"]
    node.reset_status()
    approve_out = node.execute(
        {
            "action": "approve",
            "repo_root": str(repo),
            "flow_checkpoint_path": start_fp,
        },
        {
            "implement_pipe": {
                "codex_exec": {
                    "argv": ["python3", "-c", "import sys; sys.stdin.read(); print('ok')"]
                },
                "run_tests": {"argv": ["python3", "-c", "print('t')"]},
                "write_checkpoint": {
                    "run_id": "i2",
                },
            },
            "review_pipe": _ok_review_pipe_params(repo / ".nodeflow" / "checkpoints", "r2"),
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    fp = approve_out["flow_result"]["flow_checkpoint_path"]
    node.reset_status()
    out = node.execute(
        {
            "action": "force_merge",
            "repo_root": str(repo),
            "flow_checkpoint_path": fp,
            "human_comment_text": "manual override due hotfix",
        },
        {"flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")}},
    )
    assert out["flow_result"]["state"] == "merged"
    assert out["flow_result"]["forced"] is True
    assert out["flow_result"]["previous_flow_checkpoint_path"] == fp
    assert out["flow_result"]["force_merge_reason"] == "manual override due hotfix"
    assert isinstance(out["flow_result"].get("run_context"), dict)
    assert isinstance(out["flow_result"].get("workspace_context"), dict)


def test_development_flow_force_merge_rejects_non_review_state(tmp_path: Path) -> None:
    repo = tmp_path / "workspace_force_bad_state"
    repo.mkdir()
    _git_repo_with_commit(repo)
    cp = repo / ".nodeflow" / "checkpoints" / "wrong_state.json"
    cp.parent.mkdir(parents=True, exist_ok=True)
    cp.write_text(
        json.dumps(
            {
                "flow_result": {
                    "state": "awaiting_approval",
                    "run_context": {"source_repo_root": str(repo.resolve())},
                }
            }
        ),
        encoding="utf-8",
    )
    node = DevelopmentFlowPipeNode()
    node.execute(
        {
            "action": "force_merge",
            "repo_root": str(repo),
            "flow_checkpoint_path": str(cp),
            "human_comment_text": "override",
        },
        {"flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")}},
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "force_merge requires previous state awaiting_review_decision" in str(node.read_error())


def test_development_flow_implement_fail_marks_flow_not_ok(tmp_path: Path) -> None:
    repo = tmp_path / "workspace"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = DevelopmentFlowPipeNode()
    start_out = node.execute(
        {
            "action": "start",
            "task_prompt": "t",
            "repo_root": str(repo),
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "s3",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    fp = start_out["flow_result"]["flow_checkpoint_path"]
    node.reset_status()
    approve_out = node.execute(
        {
            "action": "approve",
            "repo_root": str(repo),
            "flow_checkpoint_path": fp,
        },
        {
            "implement_pipe": {
                "codex_exec": {"argv": ["python3", "-c", "import sys; sys.exit(1)"]},
                "run_tests": {"argv": ["python3", "-c", "print('t')"]},
                "write_checkpoint": {
                    "run_id": "i3",
                },
            },
            "review_pipe": {
                "review_diff_focused": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_wide_scan": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_test_focused": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_spec_conformance": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_spec_revision": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "r3",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    assert approve_out["flow_result"]["ok"] is False
    assert approve_out["flow_result"]["merge_ready"] is False
    assert "merge" not in approve_out["flow_result"]["allowed_actions"]
    approve_fp = approve_out["flow_result"]["flow_checkpoint_path"]

    node.reset_status()
    node.execute(
        {
            "action": "merge",
            "repo_root": str(repo),
            "flow_checkpoint_path": approve_fp,
        },
        {"flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")}},
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "flow_result.ok == true" in str(node.read_error())


def test_development_flow_review_revise_spec_hides_merge_action(tmp_path: Path) -> None:
    repo = tmp_path / "workspace"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = DevelopmentFlowPipeNode()
    start_out = node.execute(
        {
            "action": "start",
            "task_prompt": "t",
            "repo_root": str(repo),
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "s4",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    fp = start_out["flow_result"]["flow_checkpoint_path"]
    node.reset_status()
    approve_out = node.execute(
        {
            "action": "approve",
            "repo_root": str(repo),
            "flow_checkpoint_path": fp,
        },
        {
            "implement_pipe": {
                "codex_exec": {
                    "argv": ["python3", "-c", "import sys; sys.stdin.read(); print('ok')"]
                },
                "run_tests": {"argv": ["python3", "-c", "print('t')"]},
                "write_checkpoint": {
                    "run_id": "i4",
                },
            },
            "review_pipe": {
                "review_diff_focused": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_wide_scan": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_test_focused": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_spec_conformance": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_spec_revision": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':True}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "r4",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    afr = approve_out["flow_result"]
    assert afr["ok"] is True
    assert afr["merge_ready"] is False
    assert afr["next_action"] == "revise_spec"
    assert "merge" not in afr["allowed_actions"]
    assert "revise_spec" in afr["allowed_actions"]


def test_development_flow_revise_spec_restores_task_prompt_from_checkpoint(tmp_path: Path) -> None:
    repo = tmp_path / "workspace"
    repo.mkdir()
    _git_repo_with_commit(repo)

    review_cp = repo / ".nodeflow" / "checkpoints" / "prev_review.json"
    review_cp.parent.mkdir(parents=True, exist_ok=True)
    review_cp.write_text(
        json.dumps(
            {
                "stage_result": {
                    "raw_results": {
                        "review_result": {
                            "ok": True,
                            "blocking_findings": [],
                            "non_blocking_findings": [],
                            "spec_revision_needed": True,
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(repo),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    flow_cp = repo / ".nodeflow" / "checkpoints" / "prev_flow.json"
    flow_cp.write_text(
        json.dumps(
            {
                "flow_result": {
                    "state": "awaiting_review_decision",
                    "task_prompt": "restore-this-task",
                    "review_checkpoint_path": str(review_cp),
                    "run_context": {
                        "run_id": "fixture-rev",
                        "planned_branch_name": "feat/nodeflow-fixture-rev",
                        "artifact_root": str(repo / ".nodeflow" / "runs" / "fixture-rev"),
                        "source_repo_root": str(repo.resolve()),
                        "source_base_revision": head,
                        "source_current_branch": "main",
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    node = DevelopmentFlowPipeNode()
    out = node.execute(
        {
            "action": "revise_spec",
            "task_prompt": "",
            "repo_root": str(repo),
            "flow_checkpoint_path": str(flow_cp),
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "rev1",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    raw = out["flow_result"]["stage_result"]["raw_results"]
    assert raw.get("task_prompt") == "restore-this-task"


def test_development_flow_revise_spec_restores_task_prompt_from_real_flow(tmp_path: Path) -> None:
    repo = tmp_path / "workspace2"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = DevelopmentFlowPipeNode()
    start_out = node.execute(
        {
            "action": "start",
            "task_prompt": "keep-this-task-prompt",
            "repo_root": str(repo),
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "s5",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    start_fp = start_out["flow_result"]["flow_checkpoint_path"]
    node.reset_status()
    approve_out = node.execute(
        {
            "action": "approve",
            "repo_root": str(repo),
            "flow_checkpoint_path": start_fp,
        },
        {
            "implement_pipe": {
                "codex_exec": {
                    "argv": ["python3", "-c", "import sys; sys.stdin.read(); print('ok')"]
                },
                "run_tests": {"argv": ["python3", "-c", "print('t')"]},
                "write_checkpoint": {
                    "run_id": "i5",
                },
            },
            "review_pipe": {
                "review_diff_focused": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_wide_scan": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_test_focused": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_spec_conformance": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_spec_revision": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':True}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "r5",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    approve_fp = approve_out["flow_result"]["flow_checkpoint_path"]
    node.reset_status()
    revise_out = node.execute(
        {
            "action": "revise_spec",
            "task_prompt": "",
            "repo_root": str(repo),
            "flow_checkpoint_path": approve_fp,
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s2','plan':'p2'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "rev2",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    revised_raw = revise_out["flow_result"]["stage_result"]["raw_results"]
    assert revised_raw.get("task_prompt") == "keep-this-task-prompt"


def test_development_flow_approve_requires_awaiting_approval(tmp_path: Path) -> None:
    repo = tmp_path / "workspace3"
    repo.mkdir()
    _git_repo_with_commit(repo)
    cp = repo / ".nodeflow" / "checkpoints" / "wrong.json"
    cp.parent.mkdir(parents=True, exist_ok=True)
    cp.write_text(
        json.dumps(
            {"flow_result": {"state": "awaiting_review_decision", "approved_candidate_path": "x"}}
        ),
        encoding="utf-8",
    )
    node = DevelopmentFlowPipeNode()
    node.execute(
        {
            "action": "approve",
            "repo_root": str(repo),
            "flow_checkpoint_path": str(cp),
        },
        {
            "implement_pipe": {
                "codex_exec": {"argv": ["python3", "-c", "print('x')"]},
                "run_tests": {"argv": ["python3", "-c", "print('t')"]},
            },
            "review_pipe": {"review_diff_focused": {"argv": ["python3", "-c", "print('{}')"]}},
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "approve requires previous state awaiting_approval" in str(node.read_error())


def test_development_flow_rework_requires_awaiting_review_decision(tmp_path: Path) -> None:
    repo = tmp_path / "workspace4"
    repo.mkdir()
    _git_repo_with_commit(repo)
    cp = repo / ".nodeflow" / "checkpoints" / "wrong2.json"
    cp.parent.mkdir(parents=True, exist_ok=True)
    cp.write_text(
        json.dumps({"flow_result": {"state": "awaiting_approval", "approved_candidate_path": "x"}}),
        encoding="utf-8",
    )
    node = DevelopmentFlowPipeNode()
    node.execute(
        {
            "action": "rework_implementation",
            "repo_root": str(repo),
            "flow_checkpoint_path": str(cp),
        },
        {
            "implement_pipe": {
                "codex_exec": {"argv": ["python3", "-c", "print('x')"]},
                "run_tests": {"argv": ["python3", "-c", "print('t')"]},
            },
            "review_pipe": {"review_diff_focused": {"argv": ["python3", "-c", "print('{}')"]}},
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "rework_implementation requires previous state awaiting_review_decision" in str(
        node.read_error()
    )


def test_rework_requires_workspace_context(tmp_path: Path) -> None:
    repo = tmp_path / "workspace_rework_ws_required"
    repo.mkdir()
    _git_repo_with_commit(repo)
    cp = repo / ".nodeflow" / "checkpoints" / "missing_ws.json"
    cp.parent.mkdir(parents=True, exist_ok=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(repo),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    cp.write_text(
        json.dumps(
            {
                "flow_result": {
                    "state": "awaiting_review_decision",
                    "run_context": {
                        "source_repo_root": str(repo.resolve()),
                        "source_base_revision": head,
                    },
                    "review_checkpoint_path": str(
                        repo / ".nodeflow" / "checkpoints" / "review.json"
                    ),
                }
            }
        ),
        encoding="utf-8",
    )
    node = DevelopmentFlowPipeNode()
    node.execute(
        {
            "action": "rework_implementation",
            "repo_root": str(repo),
            "flow_checkpoint_path": str(cp),
        },
        {},
    )
    assert node.read_status() == "fatal"
    assert "workspace_context is required for rework_implementation" in str(node.read_error())


def test_approve_validates_approved_checkpoint_before_prepare_workspace(tmp_path: Path) -> None:
    repo = tmp_path / "workspace_approve_validate_first"
    repo.mkdir()
    _git_repo_with_commit(repo)
    (repo / "dirty.txt").write_text("dirty\n", encoding="utf-8")
    cp = repo / ".nodeflow" / "checkpoints" / "awaiting_approval.json"
    cp.parent.mkdir(parents=True, exist_ok=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(repo),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    cp.write_text(
        json.dumps(
            {
                "flow_result": {
                    "state": "awaiting_approval",
                    "run_context": {
                        "source_repo_root": str(repo.resolve()),
                        "source_base_revision": head,
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    node = DevelopmentFlowPipeNode()
    node.execute(
        {
            "action": "approve",
            "repo_root": str(repo),
            "flow_checkpoint_path": str(cp),
        },
        {},
    )
    assert node.read_status() == "fatal"
    # ensure we fail on missing approved candidate/checkpoint before dirty-worktree prepare check
    assert "approved_candidate_path or approved_checkpoint_path" in str(node.read_error())


def test_resume_rejects_mismatched_repo_root(tmp_path: Path) -> None:
    repo_a = tmp_path / "repo_a"
    repo_b = tmp_path / "repo_b"
    repo_a.mkdir()
    repo_b.mkdir()
    _git_repo_with_commit(repo_a)
    _git_repo_with_commit(repo_b)

    node = DevelopmentFlowPipeNode()
    start_out = node.execute(
        {
            "action": "start",
            "task_prompt": "t",
            "repo_root": str(repo_a),
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "mismatch1",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo_a / ".nodeflow" / "checkpoints")},
        },
    )

    node.reset_status()
    node.execute(
        {
            "action": "approve",
            "repo_root": str(repo_b),
            "flow_checkpoint_path": start_out["flow_result"]["flow_checkpoint_path"],
        },
        {
            "implement_pipe": {
                "codex_exec": {
                    "argv": ["python3", "-c", "import sys; sys.stdin.read(); print('ok')"]
                },
                "run_tests": {"argv": ["python3", "-c", "print('t')"]},
            },
            "review_pipe": {
                "review_diff_focused": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                }
            },
        },
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "does not match checkpoint source_repo_root" in str(node.read_error())


def test_development_flow_profile_unknown_raises(tmp_path: Path) -> None:
    repo = tmp_path / "r"
    repo.mkdir()
    _git_repo_with_commit(repo)
    profiles = tmp_path / "p.json"
    profiles.write_text(json.dumps({"default": {"spec_plan": {}}}), encoding="utf-8")
    node = DevelopmentFlowPipeNode()
    node.execute(
        {"action": "start", "task_prompt": "x", "repo_root": str(repo)},
        {
            "model_profiles_path": str(profiles),
            "cost_profiles_path": str(profiles),
            "model_profile": "nope",
            "cost_profile": "default",
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "x",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "cp")},
        },
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "unknown profile" in str(node.read_error())


def test_development_flow_profile_partial_config_fails_fast(tmp_path: Path) -> None:
    repo = tmp_path / "r2"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = DevelopmentFlowPipeNode()
    node.execute(
        {"action": "start", "task_prompt": "x", "repo_root": str(repo)},
        {
            "model_profiles_path": str(tmp_path / "m.json"),
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "x2",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "cp")},
        },
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "must be set together" in str(node.read_error())


def test_development_flow_start_propagates_child_fatal(tmp_path: Path) -> None:
    repo = tmp_path / "workspace"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = DevelopmentFlowPipeNode()
    node.execute(
        {
            "action": "start",
            "task_prompt": "t",
            "repo_root": str(repo),
        },
        {
            # spec_plan_pipe child should become fatal and parent must propagate fatal.
            "spec_plan_pipe": {},
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "spec_plan_pipe fatal" in str(node.read_error())


def test_development_flow_rejects_missing_action(tmp_path: Path) -> None:
    repo = tmp_path / "repo_missing_action"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = DevelopmentFlowPipeNode()
    node.execute(
        {
            "task_prompt": "t",
            "repo_root": str(repo),
        },
        {},
    )
    assert node.read_status() == "fatal"
    assert "action is required" in str(node.read_error())


def test_development_flow_rejects_missing_repo_root() -> None:
    node = DevelopmentFlowPipeNode()
    node.execute(
        {
            "action": "start",
            "task_prompt": "t",
        },
        {},
    )
    assert node.read_status() == "fatal"
    assert "repo_root is required" in str(node.read_error())


def test_load_checkpoint_includes_rework_context(tmp_path: Path) -> None:
    stub = tmp_path / "ap.json"
    stub.write_text(json.dumps({"spec": "S", "plan": "P"}), encoding="utf-8")
    node = LoadCheckpointNode()
    out = node.execute(
        {
            "approved_checkpoint_path": str(stub),
            "repo_root": str(tmp_path),
            "rework_context": "fix tests per review",
        },
        {},
    )
    assert "## Rework context" in out["codex_task_prompt"]["text"]
    assert "fix tests" in out["codex_task_prompt"]["text"]


def test_load_checkpoint_two_file_legacy_is_rejected(tmp_path: Path) -> None:
    node = LoadCheckpointNode()
    node.execute(
        {
            "approved_spec_path": str(tmp_path / "spec.json"),
            "approved_plan_path": str(tmp_path / "plan.json"),
            "repo_root": str(tmp_path),
        },
        {},
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "approved_checkpoint_path is required" in str(node.read_error())


def test_development_flow_invalid_explicit_flow_checkpoint_fails_fast(tmp_path: Path) -> None:
    repo = tmp_path / "workspace"
    repo.mkdir()
    _git_repo_with_commit(repo)
    bad = repo / ".nodeflow" / "checkpoints" / "bad.json"
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_text("{not-json", encoding="utf-8")
    node = DevelopmentFlowPipeNode()
    node.execute(
        {
            "action": "start",
            "task_prompt": "x",
            "repo_root": str(repo),
            "flow_checkpoint_path": str(bad),
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "badcp",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "start does not accept flow_checkpoint_path" in str(node.read_error())


def test_development_flow_start_rejects_flow_checkpoint_path_even_when_valid(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "workspace_valid_cp"
    repo.mkdir()
    _git_repo_with_commit(repo)
    cp = repo / ".nodeflow" / "checkpoints" / "valid.json"
    cp.parent.mkdir(parents=True, exist_ok=True)
    cp.write_text(json.dumps({"flow_result": {"state": "awaiting_approval"}}), encoding="utf-8")
    node = DevelopmentFlowPipeNode()
    node.execute(
        {
            "action": "start",
            "task_prompt": "x",
            "repo_root": str(repo),
            "flow_checkpoint_path": str(cp),
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "fresh",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "start does not accept flow_checkpoint_path" in str(node.read_error())


def test_run_tests_requires_argv() -> None:
    from nodeflow.nodes.development_flow.implement_pipe.run_tests import RunTestsNode

    node = RunTestsNode()
    node.execute({"repo_root": "."}, {})
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "run_tests.argv must be a non-empty list[str]" in str(node.read_error())


def test_development_flow_hermetic_yaml_start(tmp_path: Path) -> None:
    repo_pkg = Path(__file__).resolve().parents[1]
    work = tmp_path / "ws"
    work.mkdir()
    _git_repo_with_commit(work)
    yml = str(repo_pkg / "examples" / "pipelines" / "development_flow_hermetic.yaml")
    out = load_and_kick_pipeline(
        str(work),
        yml,
        {
            "action": "start",
            "task_prompt": "hello",
            "repo_root": str(work),
            "flow_checkpoint_path": "",
            "human_comment_path": "",
            "human_comment_text": "",
            "planned_branch_name": "",
            "development_name": "",
            "run_id": "",
        },
    )
    assert out["flow_result"]["state"] == "awaiting_approval"


def test_start_requires_clean_source_repo(tmp_path: Path) -> None:
    repo = tmp_path / "dirty_repo"
    repo.mkdir()
    _git_repo_with_commit(repo)
    (repo / "dirty.txt").write_text("dirty\n", encoding="utf-8")
    node = DevelopmentFlowPipeNode()
    node.execute(
        {"action": "start", "task_prompt": "x", "repo_root": str(repo)},
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                }
            }
        },
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "source repository is dirty" in str(node.read_error())


def test_check_source_workspace_node_ok(tmp_path: Path) -> None:
    repo = tmp_path / "repo_source_check"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = CheckSourceWorkspaceNode()
    out = node.execute({"source_repo_root": str(repo)}, {})
    check = out["source_workspace_check"]
    assert check["clean"] is True
    assert check["current_branch"] == "main"
    assert isinstance(check.get("base_revision"), str) and check["base_revision"]


def test_check_source_workspace_rejects_detached_head_by_default(tmp_path: Path) -> None:
    repo = tmp_path / "repo_source_detached"
    repo.mkdir()
    _git_repo_with_commit(repo)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(repo),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        ["git", "switch", "--detach", head],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    node = CheckSourceWorkspaceNode()
    node.execute({"source_repo_root": str(repo)}, {})
    assert node.read_status() == "fatal"
    assert "detached HEAD is not supported" in str(node.read_error())


def test_status_has_non_ignored_changes_ignores_unstaged_nodeflow_change() -> None:
    status = " M .nodeflow/runs/x.json\n"
    assert status_has_non_ignored_changes(status, [".nodeflow/"]) is False


def test_status_has_non_ignored_changes_rename_from_src_to_nodeflow_is_dirty() -> None:
    status = "R  src/a.py -> .nodeflow/a.py\n"
    assert status_has_non_ignored_changes(status, [".nodeflow/"]) is True


def test_status_has_non_ignored_changes_rename_inside_nodeflow_is_ignored() -> None:
    status = "R  .nodeflow/a.json -> .nodeflow/b.json\n"
    assert status_has_non_ignored_changes(status, [".nodeflow/"]) is False


def test_status_violates_start_policy_tracked_change_is_forbidden() -> None:
    status = " M src/a.py\n"
    assert (
        status_violates_start_policy(
            status,
            ignored_prefixes=[".nodeflow/"],
            fail_on_tracked_changes=True,
            fail_on_untracked=False,
            allowed_untracked_prefixes=[],
            blocked_untracked_globs=[],
        )
        is True
    )


def test_status_violates_start_policy_untracked_is_allowed_by_default() -> None:
    status = "?? notes/todo.txt\n"
    assert (
        status_violates_start_policy(
            status,
            ignored_prefixes=[".nodeflow/"],
            fail_on_tracked_changes=True,
            fail_on_untracked=False,
            allowed_untracked_prefixes=[],
            blocked_untracked_globs=[],
        )
        is False
    )


def test_status_violates_start_policy_blocked_untracked_glob_is_forbidden() -> None:
    status = "?? src/new_module.py\n"
    assert (
        status_violates_start_policy(
            status,
            ignored_prefixes=[".nodeflow/"],
            fail_on_tracked_changes=True,
            fail_on_untracked=False,
            allowed_untracked_prefixes=[],
            blocked_untracked_globs=["src/**", "*.py"],
        )
        is True
    )


def test_status_violates_start_policy_allowed_untracked_prefix_overrides_fail_flag() -> None:
    status = "?? docs/notes.md\n"
    assert (
        status_violates_start_policy(
            status,
            ignored_prefixes=[".nodeflow/"],
            fail_on_tracked_changes=True,
            fail_on_untracked=True,
            allowed_untracked_prefixes=["docs/"],
            blocked_untracked_globs=[],
        )
        is False
    )


def test_approve_current_repo_uses_source_repo_as_workspace(tmp_path: Path) -> None:
    repo = tmp_path / "repo_current"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = PrepareWorkspaceNode()
    out = node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-current"),
        },
        {"strategy": "current_repo"},
    )
    ctx = out["workspace_context"]
    assert ctx["strategy"] == "current_repo"
    assert Path(ctx["workspace_root"]).resolve() == repo.resolve()
    assert isinstance(ctx.get("current_branch"), str)
    assert ctx.get("planned_branch_name") == "feat/nodeflow-current"


def test_prepare_development_run_context_creates_artifact_root_only(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = PrepareDevelopmentRunContextNode()
    out = node.execute(
        {
            "source_workspace_check": _source_workspace_check(repo),
            "planned_branch_name": "feat/nodeflow-sample",
            "run_id": "r1",
        },
        {},
    )
    ctx = out["run_context"]
    assert ctx["run_id"] == "r1"
    assert ctx["run_index"] == 1
    assert ctx["run_slug"] == "development-flow"
    assert ctx["planned_branch_name"] == "feat/nodeflow-sample"
    assert "/.nodeflow/runs/001_" in ctx["artifact_root"]
    assert ctx["run_dir_name"] in ctx["artifact_root"]


def test_prepare_development_run_context_rejects_run_id_in_params(tmp_path: Path) -> None:
    repo = tmp_path / "repo_run_id_param"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = PrepareDevelopmentRunContextNode()
    node.execute(
        {"source_workspace_check": _source_workspace_check(repo)},
        {"run_id": "run-from-param"},
    )
    assert node.read_status() == "fatal"
    assert "does not accept params.run_id" in str(node.read_error())


def test_prepare_development_run_context_uses_human_readable_dir(tmp_path: Path) -> None:
    repo = tmp_path / "repo_human"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = PrepareDevelopmentRunContextNode()
    out = node.execute(
        {
            "source_workspace_check": _source_workspace_check(repo),
            "run_id": "internal-only",
            "task_prompt": "Add config validation\ndetails",
        },
        {},
    )
    ctx = out["run_context"]
    assert ctx["run_id"] == "internal-only"
    assert ctx["run_slug"] == "add-config-validation"
    assert ctx["run_dir_name"].startswith("001_")
    assert "internal-only" not in Path(ctx["artifact_root"]).name
    assert Path(ctx["artifact_root"]).is_dir()


def test_prepare_development_run_context_rejects_invalid_branch_name(tmp_path: Path) -> None:
    repo = tmp_path / "repo_invalid_branch"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = PrepareDevelopmentRunContextNode()
    node.execute(
        {
            "source_workspace_check": _source_workspace_check(repo),
            "planned_branch_name": "bad..branch",
            "run_id": "r-invalid",
        },
        {},
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)


def test_prepare_development_run_context_normalizes_branch_prefix_trailing_slash(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_branch_prefix_normalized"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = PrepareDevelopmentRunContextNode()
    out = node.execute(
        {"source_workspace_check": _source_workspace_check(repo), "task_prompt": "x"},
        {"branch_prefix": "feat/nodeflow/"},
    )
    assert "//" not in out["run_context"]["planned_branch_name"]
    assert out["run_context"]["planned_branch_name"].startswith("feat/nodeflow/")


def test_prepare_development_run_context_rejects_empty_branch_prefix(tmp_path: Path) -> None:
    repo = tmp_path / "repo_branch_prefix_empty"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = PrepareDevelopmentRunContextNode()
    node.execute(
        {"source_workspace_check": _source_workspace_check(repo), "task_prompt": "x"},
        {"branch_prefix": "/"},
    )
    assert node.read_status() == "fatal"
    assert "branch_prefix must not be empty" in str(node.read_error())


def test_prepare_development_run_context_invalid_branch_does_not_create_run_dir(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_no_run_dir_on_invalid_branch"
    repo.mkdir()
    _git_repo_with_commit(repo)
    runs = repo / ".nodeflow" / "runs"
    node = PrepareDevelopmentRunContextNode()
    node.execute(
        {
            "source_workspace_check": _source_workspace_check(repo),
            "planned_branch_name": "bad..branch",
            "run_id": "r-inv2",
        },
        {},
    )
    assert node.read_status() == "fatal"
    if runs.exists():
        assert not any(re.match(r"^\d{3}_", p.name) for p in runs.iterdir())


def test_prepare_development_run_context_invalid_run_dir_format_raises_node_failure(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_bad_run_dir_format"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = PrepareDevelopmentRunContextNode()
    node.execute(
        {"source_workspace_check": _source_workspace_check(repo)},
        {"run_dir_format": "{bad_placeholder}"},
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "invalid run_dir_format" in str(node.read_error())


def test_rework_current_repo_reuses_dirty_workspace(tmp_path: Path) -> None:
    repo = tmp_path / "repo_rework_dirty"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = PrepareWorkspaceNode()
    first = node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-rework-dirty"),
        },
        {"strategy": "current_repo"},
    )["workspace_context"]
    (repo / "dirty_impl.py").write_text("x\n", encoding="utf-8")
    subprocess.run(["git", "add", "dirty_impl.py"], cwd=str(repo), check=True, capture_output=True)
    node.reset_status()
    second = node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-rework-dirty"),
            "workspace_context": first,
        },
        {"strategy": "current_repo"},
    )["workspace_context"]
    assert second["workspace_root"] == first["workspace_root"]
    assert second["base_revision"] == first["base_revision"]


def test_prepare_workspace_current_repo_rejects_changed_branch_on_reuse(tmp_path: Path) -> None:
    repo = tmp_path / "repo_reuse_branch"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = PrepareWorkspaceNode()
    first = node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-reuse-branch"),
        },
        {"strategy": "current_repo"},
    )["workspace_context"]
    subprocess.run(
        ["git", "switch", "-c", "tmp-branch"],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    node.reset_status()
    node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-reuse-branch"),
            "workspace_context": first,
        },
        {"strategy": "current_repo"},
    )
    assert node.read_status() == "fatal"
    assert "branch changed since previous checkpoint" in str(node.read_error())


def test_prepare_workspace_rejects_missing_source_repo_root(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist"
    node = PrepareWorkspaceNode()
    node.execute(
        {
            "source_repo_root": str(missing),
            "run_context": {"planned_branch_name": "feat/nodeflow-missing"},
        },
        {"strategy": "current_repo"},
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "source_repo_root does not exist" in str(node.read_error())


def test_prepare_workspace_requires_source_repo_root_not_repo_root_fallback(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_source_required"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = PrepareWorkspaceNode()
    node.execute(
        {
            "repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-source-required"),
        },
        {"strategy": "current_repo"},
    )
    assert node.read_status() == "fatal"
    assert "source_repo_root is required" in str(node.read_error())


def test_prepare_workspace_current_repo_rejects_source_branch_change_on_fresh(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_source_branch_changed"
    repo.mkdir()
    _git_repo_with_commit(repo)
    subprocess.run(
        ["git", "switch", "-c", "other-branch"],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    node = PrepareWorkspaceNode()
    node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": {
                "planned_branch_name": "feat/nodeflow-branch-check",
                "source_repo_root": str(repo.resolve()),
                "source_base_revision": _source_workspace_check(repo)["base_revision"],
                "source_current_branch": "main",
            },
        },
        {"strategy": "current_repo"},
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "source branch changed since flow start" in str(node.read_error())


def test_prepare_workspace_rejects_unsupported_strategy(tmp_path: Path) -> None:
    repo = tmp_path / "repo_bad_strategy"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = PrepareWorkspaceNode()
    node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-bad-strategy"),
        },
        {"strategy": "unsupported"},
    )
    assert node.read_status() == "fatal"
    assert "unsupported prepare_workspace.strategy" in str(node.read_error())


def test_prepare_workspace_current_repo_rejects_mismatched_existing_workspace(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_bad_existing"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = PrepareWorkspaceNode()
    first = node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-existing-a"),
        },
        {"strategy": "current_repo"},
    )["workspace_context"]
    first["planned_branch_name"] = "feat/nodeflow-existing-b"
    node.reset_status()
    node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-existing-a"),
            "workspace_context": first,
        },
        {"strategy": "current_repo"},
    )
    assert node.read_status() == "fatal"
    assert "planned_branch_name" in str(node.read_error())


def test_prepare_workspace_current_repo_rejects_changed_head_on_fresh_approve(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_changed_head"
    repo.mkdir()
    _git_repo_with_commit(repo)
    base_revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(repo),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    (repo / "next.txt").write_text("next\n", encoding="utf-8")
    subprocess.run(["git", "add", "next.txt"], cwd=str(repo), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "next"],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    node = PrepareWorkspaceNode()
    node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": {
                "planned_branch_name": "feat/nodeflow-changed-head",
                "source_repo_root": str(repo.resolve()),
                "source_base_revision": base_revision,
                "source_current_branch": "main",
            },
        },
        {"strategy": "current_repo"},
    )
    assert node.read_status() == "fatal"
    assert "HEAD changed since flow start" in str(node.read_error())


def test_prepare_workspace_reuse_requires_current_branch(tmp_path: Path) -> None:
    repo = tmp_path / "repo_reuse_current_branch_required"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = PrepareWorkspaceNode()
    first = node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(
                repo, "feat/nodeflow-reuse-current-branch-required"
            ),
        },
        {"strategy": "current_repo"},
    )["workspace_context"]
    first.pop("current_branch", None)
    node.reset_status()
    node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(
                repo, "feat/nodeflow-reuse-current-branch-required"
            ),
            "workspace_context": first,
        },
        {"strategy": "current_repo"},
    )
    assert node.read_status() == "fatal"
    assert "existing workspace_context.current_branch is required" in str(node.read_error())


def test_revise_spec_requires_clean_source_repo(tmp_path: Path) -> None:
    repo = tmp_path / "repo_revise_clean"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = DevelopmentFlowPipeNode()
    start_out = node.execute(
        {"action": "start", "task_prompt": "t", "repo_root": str(repo)},
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {"run_id": "rs0"},
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    start_fp = start_out["flow_result"]["flow_checkpoint_path"]
    node.reset_status()
    approve_out = node.execute(
        {"action": "approve", "repo_root": str(repo), "flow_checkpoint_path": start_fp},
        {
            "implement_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "open(%r + '/impl_touch.txt', 'w').write('x'); import sys; sys.stdin.read(); print('ok')"
                        % str(repo),
                    ]
                },
                "run_tests": {"argv": ["python3", "-c", "print('t')"]},
                "write_checkpoint": {"run_id": "ri0"},
            },
            "review_pipe": {
                "review_diff_focused": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_wide_scan": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_test_focused": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_spec_conformance": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_spec_revision": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':True}))",
                    ]
                },
                "write_checkpoint": {"run_id": "rr0"},
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    approve_fp = approve_out["flow_result"]["flow_checkpoint_path"]
    node.reset_status()
    node.execute(
        {
            "action": "revise_spec",
            "task_prompt": "",
            "repo_root": str(repo),
            "flow_checkpoint_path": approve_fp,
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s2','plan':'p2'}))",
                    ]
                },
                "write_checkpoint": {"run_id": "rv0"},
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "dirty" in str(node.read_error()).lower()


def test_revise_spec_rejects_changed_head_even_if_clean(tmp_path: Path) -> None:
    repo = tmp_path / "repo_revise_head_changed"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = DevelopmentFlowPipeNode()
    start_out = node.execute(
        {"action": "start", "task_prompt": "t", "repo_root": str(repo)},
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {"run_id": "h0"},
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    start_fp = start_out["flow_result"]["flow_checkpoint_path"]
    node.reset_status()
    approve_out = node.execute(
        {"action": "approve", "repo_root": str(repo), "flow_checkpoint_path": start_fp},
        {
            "implement_pipe": {
                "codex_exec": {
                    "argv": ["python3", "-c", "import sys; sys.stdin.read(); print('ok')"]
                },
                "run_tests": {"argv": ["python3", "-c", "print('t')"]},
                "write_checkpoint": {"run_id": "h1"},
            },
            "review_pipe": {
                "review_diff_focused": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_wide_scan": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_test_focused": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_spec_conformance": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_spec_revision": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':True}))",
                    ]
                },
                "write_checkpoint": {"run_id": "h2"},
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    approve_fp = approve_out["flow_result"]["flow_checkpoint_path"]
    (repo / "advance.txt").write_text("advance\n", encoding="utf-8")
    subprocess.run(["git", "add", "advance.txt"], cwd=str(repo), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "advance"],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )

    node.reset_status()
    node.execute(
        {
            "action": "revise_spec",
            "task_prompt": "",
            "repo_root": str(repo),
            "flow_checkpoint_path": approve_fp,
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s2','plan':'p2'}))",
                    ]
                },
                "write_checkpoint": {"run_id": "h3"},
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "HEAD changed since flow start" in str(node.read_error())


def test_child_pipes_receive_artifact_root(tmp_path: Path) -> None:
    repo = tmp_path / "repo_child_art"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = DevelopmentFlowPipeNode()
    out = node.execute(
        {"action": "start", "task_prompt": "doc", "repo_root": str(repo)},
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {"run_id": "sp_art"},
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    fr = out["flow_result"]
    art = str(Path(fr["run_context"]["artifact_root"]).resolve())
    cand = fr.get("approved_candidate_path")
    assert isinstance(cand, str)
    assert str(Path(cand).resolve()).startswith(art)
    assert "spec_plan" in cand.replace("\\", "/")
    scp = fr.get("spec_plan_checkpoint_path")
    assert isinstance(scp, str)
    assert str(Path(scp).resolve()).startswith(art)


def test_stage_pipes_reject_artifact_root_and_checkpoint_dir_conflict(tmp_path: Path) -> None:
    repo = tmp_path / "repo_stage_conflict"
    repo.mkdir()
    _git_repo_with_commit(repo)
    approved = repo / "approved.json"
    approved.write_text(json.dumps({"spec": "s", "plan": "p"}), encoding="utf-8")

    impl = ImplementPipeNode()
    impl.execute(
        {
            "approved_checkpoint_path": str(approved),
            "repo_root": str(repo),
            "artifact_root": str(repo / ".nodeflow" / "runs" / "r1"),
            "base_ref": "HEAD",
            "task_type": "implement",
        },
        {
            "codex_exec": {"argv": ["python3", "-c", "import sys; sys.stdin.read(); print('ok')"]},
            "run_tests": {"argv": ["python3", "-c", "print('t')"]},
            "write_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "override")},
        },
    )
    assert impl.read_status() == "fatal"
    assert (
        "implement_pipe: artifact_root and write_checkpoint.checkpoint_dir cannot both be set"
        in str(impl.read_error())
    )

    review = ReviewPipeNode()
    review.execute(
        {
            "approved_checkpoint_path": str(approved),
            "repo_root": str(repo),
            "artifact_root": str(repo / ".nodeflow" / "runs" / "r1"),
            "base_ref": "HEAD",
            "task_type": "review",
        },
        {"write_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "override")}},
    )
    assert review.read_status() == "fatal"
    assert (
        "review_pipe: artifact_root and write_checkpoint.checkpoint_dir cannot both be set"
        in str(review.read_error())
    )

    spec = SpecPlanPipeNode()
    spec.execute(
        {
            "task_prompt": "x",
            "repo_root": str(repo),
            "base_ref": "HEAD",
            "artifact_root": str(repo / ".nodeflow" / "runs" / "r1"),
        },
        {"write_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "override")}},
    )
    assert spec.read_status() == "fatal"
    assert (
        "spec_plan_pipe: artifact_root and write_checkpoint.checkpoint_dir cannot both be set"
        in str(spec.read_error())
    )


def test_write_development_summary_node_outputs_commit_suggestion(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git_repo_with_commit(repo)
    ws_node = PrepareWorkspaceNode()
    ws_out = ws_node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-summary"),
        },
        {},
    )
    ws_ctx = ws_out["workspace_context"]
    run_context = {
        "run_id": "r3",
        "artifact_root": str(repo / ".nodeflow" / "runs" / "r3"),
        "source_repo_root": str(repo.resolve()),
        "source_base_revision": ws_ctx["base_revision"],
    }
    ws_root = Path(ws_ctx["workspace_root"])
    (ws_root / "x.py").write_text("print('x')\n", encoding="utf-8")
    (repo / "x.py").write_text("print('x')\n", encoding="utf-8")
    node = WriteDevelopmentSummaryNode()
    out = node.execute(
        {
            "workspace_context": ws_ctx,
            "run_context": run_context,
            "action": "approve",
            "task_prompt": "add x",
            "implement_stage_result": {"ok": True},
            "review_stage_result": {"ok": True},
            "next_action": "merge",
            "merge_ready": True,
        },
        {},
    )
    ds = out["development_summary"]
    assert isinstance(ds.get("commit_message_suggestion"), str)
    assert ds.get("commit_message_suggestion")
    assert Path(ds["artifact_path"]).is_file()
    assert "/summary/" in ds["artifact_path"]


def test_write_development_summary_prefers_repo_style_and_template(tmp_path: Path) -> None:
    repo = tmp_path / "repo2"
    repo.mkdir()
    _git_repo_with_commit(repo)
    subprocess.run(
        [
            "git",
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "--allow-empty",
            "-m",
            "fix: baseline style",
        ],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    ws_node = PrepareWorkspaceNode()
    ws_out = ws_node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-summary-style"),
        },
        {"strategy": "current_repo"},
    )
    ws_ctx = ws_out["workspace_context"]
    (repo / ".gitmessage").write_text(
        "# subject\n\nWhy\n{{WHY}}\n\nWhat\n{{WHAT}}\n\nImpact\n{{IMPACT}}\n",
        encoding="utf-8",
    )
    run_context = {
        "run_id": "r4",
        "artifact_root": str(repo / ".nodeflow" / "runs" / "r4"),
        "source_repo_root": str(repo.resolve()),
        "source_base_revision": ws_ctx["base_revision"],
    }
    ws_root = Path(ws_ctx["workspace_root"])
    (ws_root / "docs.md").write_text("docs\n", encoding="utf-8")
    node = WriteDevelopmentSummaryNode()
    out = node.execute(
        {
            "workspace_context": ws_ctx,
            "run_context": run_context,
            "action": "approve",
            "task_prompt": "update docs",
            "implement_stage_result": {"ok": True},
            "review_stage_result": {"ok": True},
            "next_action": "merge",
            "merge_ready": True,
        },
        {},
    )
    msg = out["development_summary"]["commit_message_suggestion"]
    assert msg.startswith("feat:")
    assert "Why" in msg
    assert "What" in msg
    assert "Impact" in msg


def test_write_development_summary_rejects_invalid_base_revision(tmp_path: Path) -> None:
    repo = tmp_path / "repo4"
    repo.mkdir()
    _git_repo_with_commit(repo)
    ws_node = PrepareWorkspaceNode()
    ws_ctx = ws_node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-badrev"),
        },
        {},
    )["workspace_context"]
    ws_ctx["base_revision"] = "deadbeef"
    run_context = {
        "run_id": "r5",
        "artifact_root": str(repo / ".nodeflow" / "runs" / "r5"),
        "source_repo_root": str(repo.resolve()),
        "source_base_revision": "HEAD",
    }
    node = WriteDevelopmentSummaryNode()
    node.execute(
        {
            "workspace_context": ws_ctx,
            "run_context": run_context,
            "action": "approve",
        },
        {},
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)


def test_write_development_summary_rejects_missing_workspace_root(tmp_path: Path) -> None:
    repo = tmp_path / "repo_missing_workspace_root"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = WriteDevelopmentSummaryNode()
    node.execute(
        {
            "workspace_context": {
                "workspace_root": str(repo / "gone"),
                "source_repo_root": str(repo),
                "base_revision": "HEAD",
            },
            "run_context": {
                "run_id": "missing-root",
                "artifact_root": str(repo / ".nodeflow" / "runs" / "missing-root"),
                "source_repo_root": str(repo.resolve()),
                "source_base_revision": "HEAD",
            },
            "action": "approve",
        },
        {},
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "repo_root does not exist" in str(node.read_error())


def test_write_development_summary_requires_workspace_source_repo_root(tmp_path: Path) -> None:
    repo = tmp_path / "repo_summary_source_required"
    repo.mkdir()
    _git_repo_with_commit(repo)
    ws_node = PrepareWorkspaceNode()
    ws_ctx = ws_node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-summary-source"),
        },
        {"strategy": "current_repo"},
    )["workspace_context"]
    ws_ctx.pop("source_repo_root", None)
    node = WriteDevelopmentSummaryNode()
    node.execute(
        {
            "workspace_context": ws_ctx,
            "run_context": {
                "run_id": "run-source-required",
                "artifact_root": str(repo / ".nodeflow" / "runs" / "run-source-required"),
                "source_repo_root": str(repo.resolve()),
                "source_base_revision": ws_ctx["base_revision"],
            },
            "action": "approve",
        },
        {},
    )
    assert node.read_status() == "fatal"
    assert "workspace_context.source_repo_root is required" in str(node.read_error())


def test_write_development_summary_includes_untracked_files(tmp_path: Path) -> None:
    repo = tmp_path / "repo_untracked"
    repo.mkdir()
    _git_repo_with_commit(repo)
    ws_node = PrepareWorkspaceNode()
    ws_ctx = ws_node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-untracked"),
        },
        {"strategy": "current_repo"},
    )["workspace_context"]
    ws_root = Path(ws_ctx["workspace_root"])
    (ws_root / "new_file.py").write_text("print('n')\n", encoding="utf-8")

    node = WriteDevelopmentSummaryNode()
    out = node.execute(
        {
            "workspace_context": ws_ctx,
            "run_context": {
                "run_id": "run-untracked",
                "artifact_root": str(repo / ".nodeflow" / "runs" / "run-untracked"),
                "source_repo_root": str(repo.resolve()),
                "source_base_revision": ws_ctx["base_revision"],
            },
            "task_prompt": "add new file",
            "action": "approve",
        },
        {},
    )
    artifact = Path(out["development_summary"]["artifact_path"])
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    assert "new_file.py" in payload.get("changed_files", [])
    assert isinstance(payload.get("workspace_context"), dict)
    assert payload["workspace_context"].get("base_revision") == ws_ctx.get("base_revision")
    assert isinstance(payload.get("run_context"), dict)
    assert payload["run_context"].get("run_id") == "run-untracked"


def test_write_development_summary_writes_outside_workspace(tmp_path: Path) -> None:
    repo = tmp_path / "repo_output_loc"
    repo.mkdir()
    _git_repo_with_commit(repo)
    ws_node = PrepareWorkspaceNode()
    ws_ctx = ws_node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-output-loc"),
        },
        {"strategy": "current_repo"},
    )["workspace_context"]
    run_art_root = repo / ".nodeflow" / "runs" / "r-out"
    node = WriteDevelopmentSummaryNode()
    out = node.execute(
        {
            "workspace_context": ws_ctx,
            "run_context": {
                "run_id": "r-out",
                "artifact_root": str(run_art_root),
                "source_repo_root": str(repo.resolve()),
                "source_base_revision": ws_ctx["base_revision"],
            },
            "task_prompt": "task",
            "action": "approve",
        },
        {},
    )
    artifact = Path(out["development_summary"]["artifact_path"]).resolve()
    assert str(artifact).startswith(str(run_art_root.resolve()))
    assert artifact.parent.name == "summary"
    assert "/.nodeflow/runs/r-out/summary/" in str(artifact).replace("\\", "/")


def test_rework_development_summary_does_not_overwrite_previous_rework_summary(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_rework_summary"
    repo.mkdir()
    _git_repo_with_commit(repo)
    ws_node = PrepareWorkspaceNode()
    ws_ctx = ws_node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": _run_context_for_workspace(repo, "feat/nodeflow-rework-summary"),
        },
        {"strategy": "current_repo"},
    )["workspace_context"]
    node = WriteDevelopmentSummaryNode()
    out1 = node.execute(
        {
            "workspace_context": ws_ctx,
            "run_context": {
                "run_id": "r-rework",
                "artifact_root": str(repo / ".nodeflow" / "runs" / "r-rework"),
                "source_repo_root": str(repo.resolve()),
                "source_base_revision": ws_ctx["base_revision"],
            },
            "task_prompt": "rework",
            "action": "rework_implementation",
        },
        {},
    )
    node.reset_status()
    out2 = node.execute(
        {
            "workspace_context": ws_ctx,
            "run_context": {
                "run_id": "r-rework",
                "artifact_root": str(repo / ".nodeflow" / "runs" / "r-rework"),
                "source_repo_root": str(repo.resolve()),
                "source_base_revision": ws_ctx["base_revision"],
            },
            "task_prompt": "rework",
            "action": "rework_implementation",
        },
        {},
    )
    p1 = out1["development_summary"]["artifact_path"]
    p2 = out2["development_summary"]["artifact_path"]
    assert p1 != p2
    assert Path(p1).is_file()
    assert Path(p2).is_file()


def test_development_flow_revise_spec_drops_workspace_context(tmp_path: Path) -> None:
    repo = tmp_path / "workspace_revise_drop"
    repo.mkdir()
    _git_repo_with_commit(repo)
    node = DevelopmentFlowPipeNode()
    start_out = node.execute(
        {
            "action": "start",
            "task_prompt": "keep-task",
            "repo_root": str(repo),
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s','plan':'p'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "rs1",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    start_fp = start_out["flow_result"]["flow_checkpoint_path"]
    node.reset_status()
    approve_out = node.execute(
        {
            "action": "approve",
            "repo_root": str(repo),
            "flow_checkpoint_path": start_fp,
        },
        {
            "implement_pipe": {
                "codex_exec": {
                    "argv": ["python3", "-c", "import sys; sys.stdin.read(); print('ok')"]
                },
                "run_tests": {"argv": ["python3", "-c", "print('t')"]},
                "write_checkpoint": {
                    "run_id": "ri1",
                },
            },
            "review_pipe": {
                "review_diff_focused": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_wide_scan": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_test_focused": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_spec_conformance": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':False}))",
                    ]
                },
                "review_spec_revision": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'ok':True,'blocking_findings':[],'non_blocking_findings':[],'spec_revision_needed':True}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "rr1",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    approve_fp = approve_out["flow_result"]["flow_checkpoint_path"]
    node.reset_status()
    revised = node.execute(
        {
            "action": "revise_spec",
            "task_prompt": "",
            "repo_root": str(repo),
            "flow_checkpoint_path": approve_fp,
        },
        {
            "spec_plan_pipe": {
                "codex_exec": {
                    "argv": [
                        "python3",
                        "-c",
                        "import json,sys; sys.stdin.read(); print(json.dumps({'spec':'s2','plan':'p2'}))",
                    ]
                },
                "write_checkpoint": {
                    "run_id": "rv1",
                },
            },
            "flow_checkpoint": {"checkpoint_dir": str(repo / ".nodeflow" / "checkpoints")},
        },
    )
    assert revised["flow_result"].get("workspace_context") is None


def test_review_prompt_requires_base_ref() -> None:
    try:
        extract_diff_context({"diff_result": {}}, {})
    except NodeExecutionFailure as e:
        assert "base_ref is required" in str(e)
        return
    assert False, "expected NodeExecutionFailure"


def test_collect_diff_uses_only_ignored_changed_file_prefixes(tmp_path: Path) -> None:
    repo = tmp_path / "repo_collect_diff_param_name"
    repo.mkdir()
    _git_repo_with_commit(repo)
    (repo / ".nodeflow" / "checkpoints").mkdir(parents=True)
    (repo / ".nodeflow" / "checkpoints" / "x.json").write_text("{}", encoding="utf-8")
    node = CollectDiffNode()
    out = node.execute(
        {"repo_root": str(repo), "base_ref": "HEAD"},
        {"ignored_untracked_prefixes": []},
    )
    dr = out["diff_result"]
    # legacy param name is ignored; default ignore still applies.
    assert ".nodeflow/checkpoints/x.json" not in dr.get("untracked_files", [])


def test_template_exposes_planned_branch_name_development_name_run_id() -> None:
    repo = Path(__file__).resolve().parents[1]
    for name in ("development_flow_hermetic.yaml", "development_flow_codex_template.yaml"):
        yml = repo / "examples" / "pipelines" / name
        data = yml.read_text(encoding="utf-8")
        assert "planned_branch_name" in data
        assert "development_name" in data
        assert "run_id" in data
