"""Git helpers for dev_process tests."""

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
