"""Verify that collect_diff captures committed branch changes for review.

When the implementation executor commits changes on the attempt branch,
collect_diff (using ``git diff <base_ref> HEAD``) must return the committed
diff so that review prompts contain the actual change, not an empty diff.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint
from nodeflow.workflows.dev_process.constants import STATE_AWAITING_FINAL
from nodeflow.workflows.dev_process.reuse import collect_diff
from nodeflow.workflows.dev_process.stages.implementation import run_implementation_stage
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit
from tests.workflows.dev_process.v2_flow_helpers import (
    approve_spec_to_implementation,
    continue_to_review,
    start_spec_human_gate,
)


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )


def _commit_file(repo: Path, name: str, content: str, msg: str) -> str:
    """Create, stage, and commit a file; return the new HEAD sha."""
    (repo / name).write_text(content, encoding="utf-8")
    _git(repo, "add", name)
    _git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", msg)
    cp = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=str(repo), capture_output=True, text=True, check=True
    )
    return cp.stdout.strip()


# -- unit: CollectDiffNode picks up committed changes --


def test_collect_diff_includes_committed_changes(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=str(repo), capture_output=True, text=True, check=True
    ).stdout.strip()

    _commit_file(repo, "CONTRIBUTING.md", "# Contributing\n", "add contributing")

    result = collect_diff(repo_root=repo, base_revision=base)
    assert result["ok"]
    assert "CONTRIBUTING.md" in result["diff"]
    assert len(result["diff"]) > 0


def test_collect_diff_empty_when_no_changes(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=str(repo), capture_output=True, text=True, check=True
    ).stdout.strip()

    result = collect_diff(repo_root=repo, base_revision=base)
    assert result["ok"]
    assert result["diff"] == ""


# -- unit: implementation stage collects committed diff --


def test_implementation_stage_diff_contains_committed_file(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=str(repo), capture_output=True, text=True, check=True
    ).stdout.strip()

    artifact = tmp_path / "artifacts"
    artifact.mkdir()
    (artifact / "implementation").mkdir(parents=True)

    def _fake_exec(worker, *, prompt, cwd, argv, timeout=120):
        _commit_file(Path(cwd), "CONTRIBUTING.md", "# Contributing\n", "impl commit")
        return {
            "ok": True,
            "stdout": "done",
            "stderr": "",
            "raw_output": {"returncode": 0},
            "provider_meta": {},
        }

    with patch("nodeflow.workflows.dev_process.stages.implementation.run_exec", _fake_exec), patch(
        "nodeflow.workflows.dev_process.stages.implementation.record_exec_evidence",
        return_value="/tmp/ev.json",
    ):
        result = run_implementation_stage(
            repo_root=repo,
            artifact_root=str(artifact),
            run_id="r1",
            task_prompt="t",
            base_revision=base,
            approved_spec="s",
            approved_plan="p",
        )

    dr = result["diff_result"]
    assert dr["ok"]
    assert "CONTRIBUTING.md" in dr["diff"], "committed file must appear in diff_result.diff"


# -- integration: full flow diff reaches review --


def test_full_flow_allows_empty_diff_when_head_not_advanced(tmp_path: Path) -> None:
    """Hermetic stubs don't commit, so HEAD == base_revision and diff is empty.

    Verifies the pipeline completes without failing on empty diff when no
    branch advancement occurred (hermetic path).
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = start_spec_human_gate(repo, workspace_strategy="git_worktree")
    cp = flow["flow_result"]["flow_checkpoint_path"]
    after_approve = approve_spec_to_implementation(repo, cp)
    cp2 = after_approve["flow_result"]["flow_checkpoint_path"]
    reviewed = continue_to_review(repo, cp2)
    assert reviewed["flow_result"]["state"] == STATE_AWAITING_FINAL

    checkpoint = load_flow_checkpoint(reviewed["flow_result"]["flow_checkpoint_path"])
    impl_dr = (checkpoint.get("stages") or {}).get("implementation", {}).get("diff_result", {})
    assert impl_dr.get("ok") is True
