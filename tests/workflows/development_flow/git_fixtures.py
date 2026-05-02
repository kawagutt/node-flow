"""Git repo helpers shared by development_flow workflow tests."""

from __future__ import annotations

import subprocess
from pathlib import Path


def git_repo_with_commit(repo: Path) -> None:
    subprocess.run(["git", "init", "-b", "main"], cwd=str(repo), check=True, capture_output=True)
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=str(repo), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "init"],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )


def source_workspace_check(repo: Path) -> dict:
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


def run_context_for_workspace(repo: Path, planned_branch_name: str) -> dict:
    src = source_workspace_check(repo)
    return {
        "planned_branch_name": planned_branch_name,
        "source_repo_root": src["source_repo_root"],
        "source_base_revision": src["base_revision"],
        "source_current_branch": src["current_branch"],
    }
