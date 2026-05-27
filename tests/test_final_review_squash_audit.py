"""Final review + squash tree audit metadata."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

from nodeflow.workflows.dev_process.paths import git_tree_hash
from nodeflow.workflows.dev_process.squash import squash_phase_commits


def _init_repo(path: Path) -> Path:
    subprocess.run(["git", "init", str(path)], capture_output=True, check=True)
    subprocess.run(
        ["git", "-C", str(path), "config", "user.email", "test@test.com"],
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(path), "config", "user.name", "Test"],
        capture_output=True,
        check=True,
    )
    (path / "README.md").write_text("init", encoding="utf-8")
    subprocess.run(["git", "-C", str(path), "add", "."], capture_output=True, check=True)
    subprocess.run(
        ["git", "-C", str(path), "commit", "-m", "initial"],
        capture_output=True,
        check=True,
    )
    return path


def _commit_file(repo: Path, name: str, content: str, msg: str) -> None:
    (repo / name).write_text(content, encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", name], capture_output=True, check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-m", msg],
        capture_output=True,
        check=True,
    )


def test_squash_preserves_tree_for_final_review_audit(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path / "repo")
    base_ref = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    _commit_file(repo, "a.py", "a=1", "phase_000")
    _commit_file(repo, "b.py", "b=1", "phase_001")
    reviewed_head = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    reviewed_tree = git_tree_hash(repo, reviewed_head)

    dp: Dict[str, Any] = {
        "task_branch": {"base_ref": base_ref},
        "total_phases": 2,
        "phase_results": {"phase_000": {"title": "A"}, "phase_001": {"title": "B"}},
    }
    result = squash_phase_commits(repo, dp)
    assert result["squash_tree_matches_reviewed_tree"]
    assert result["reviewed_tree"] == reviewed_tree
    assert result["squash_tree"] == reviewed_tree
    assert result["squash_commit"] != reviewed_head


@patch("nodeflow.workflows.dev_process.flow_actions.run_review_stage")
def test_final_review_records_reviewed_tree(mock_review, tmp_path: Path) -> None:
    from nodeflow.workflows.dev_process.flow_actions import _run_final_review

    repo = _init_repo(tmp_path / "repo")
    base_ref = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    mock_review.return_value = {
        "review_result": {"blocking_findings": []},
        "status": "completed",
    }

    artifact = tmp_path / "artifacts"
    artifact.mkdir()
    (artifact / "spec").mkdir()
    (artifact / "spec" / "spec.md").write_text("# S\n", encoding="utf-8")
    (artifact / "plan").mkdir()
    (artifact / "plan" / "plan.md").write_text("plan\n", encoding="utf-8")

    from nodeflow.workflows.dev_process.phase_git import create_task_branch

    task_branch = create_task_branch(repo, "final-review-test", workspace_strategy="current_repo")
    branch_name = task_branch["name"]
    _commit_file(repo, "feat.py", "x=1", "work")
    head = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    expected_tree = git_tree_hash(repo, head)
    body: dict[str, Any] = {
        "run_context": {
            "artifact_root": str(artifact),
            "repo_root": str(repo),
            "run_id": "r1",
            "source_base_revision": base_ref,
        },
        "workspace_context": {
            "current_branch": branch_name,
            "source_repo_root": str(repo),
        },
        "dev_process": {
            "total_phases": 1,
            "phase_index": 1,
            "task_branch": task_branch,
            "phase_results": {"phase_000": {"status": "completed"}},
            "review_depth_preset": "standard",
            "human_gates": {},
        },
        "stages": {},
    }

    _run_final_review(body, run_id="r1")
    final_rev = body["stages"]["final_review"]
    assert final_rev["reviewed_branch_head"] == head
    assert final_rev["reviewed_tree"] == expected_tree
