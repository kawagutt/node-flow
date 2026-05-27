"""Squash merge: collapse phase commits into a single commit."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.paths import git_head_revision, git_tree_hash
from nodeflow.workflows.dev_process.phase_git import (
    collect_phase_changed_paths,
    verify_on_task_branch,
)


def _rollback(repo_root: Path, ref: str) -> None:
    """Best-effort rollback to ref after a failed squash operation."""
    subprocess.run(
        ["git", "-C", str(repo_root), "reset", "--hard", ref],
        capture_output=True,
        text=True,
        check=False,
    )


def build_squash_message(dp: Dict[str, Any], spec_title: str = "") -> str:
    """Build squash commit message from spec title + phase titles."""
    lines: list[str] = []
    if spec_title:
        lines.append(spec_title)
    else:
        lines.append("Squashed phase commits")

    lines.append("")
    results = dp.get("phase_results", {})
    total = dp.get("total_phases", 0)
    for i in range(total):
        pid = f"phase_{i:03d}"
        pr = results.get(pid, {})
        title = pr.get("title", pid)
        lines.append(f"- {pid}: {title}")

    return "\n".join(lines)


def squash_phase_commits(
    repo_root: Path,
    dp: Dict[str, Any],
    *,
    spec_title: str = "",
    record_only: bool = False,
    artifact_roots: list[str] | None = None,
) -> Dict[str, Any]:
    """Squash all phase commits into one using git reset --soft to base_ref.

    Returns dict with: squashed, squash_commit, pre_squash_head, reviewed_tree, message.
    Verifies branch, clean working tree (excluding artifact dirs), and tree equivalence.
    """
    task_branch = dp.get("task_branch", {})
    base_ref = task_branch.get("base_ref")
    if not base_ref:
        raise NodeExecutionFailure("Cannot squash: task_branch.base_ref not found")

    message = build_squash_message(dp, spec_title)

    if record_only:
        return {
            "squashed": False,
            "record_only": True,
            "message": message,
            "base_ref": base_ref,
        }

    expected_branch = task_branch.get("name", "")
    if expected_branch:
        verify_on_task_branch(repo_root, expected_branch)

    dirty_paths = collect_phase_changed_paths(repo_root, artifact_roots=artifact_roots or [])
    if dirty_paths:
        raise NodeExecutionFailure(
            f"Cannot squash: working tree has uncommitted project changes: "
            f"{dirty_paths[:5]}; commit or stash changes first"
        )

    pre_squash_head = git_head_revision(repo_root)

    cp = subprocess.run(
        ["git", "-C", str(repo_root), "reset", "--soft", base_ref],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode != 0:
        raise NodeExecutionFailure(f"git reset --soft {base_ref} failed: {cp.stderr.strip()}")

    cp2 = subprocess.run(
        ["git", "-C", str(repo_root), "commit", "-m", message],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp2.returncode != 0:
        _rollback(repo_root, pre_squash_head)
        if "nothing to commit" in (cp2.stdout or ""):
            return {
                "squashed": False,
                "reason": "nothing to commit after reset",
                "message": message,
            }
        raise NodeExecutionFailure(f"git commit after squash failed: {cp2.stderr.strip()}")

    squash_sha = git_head_revision(repo_root)

    pre_tree = git_tree_hash(repo_root, pre_squash_head)
    post_tree = git_tree_hash(repo_root, squash_sha)
    if pre_tree and post_tree and pre_tree != post_tree:
        _rollback(repo_root, pre_squash_head)
        raise NodeExecutionFailure(
            f"Squash tree mismatch: pre-squash tree {pre_tree[:16]} != "
            f"post-squash tree {post_tree[:16]}; rolled back to {pre_squash_head[:12]}"
        )

    return {
        "squashed": True,
        "squash_commit": squash_sha,
        "pre_squash_head": pre_squash_head,
        "reviewed_tree": pre_tree,
        "squash_tree": post_tree,
        "squash_tree_matches_reviewed_tree": bool(pre_tree and post_tree and pre_tree == post_tree),
        "message": message,
        "base_ref": base_ref,
    }
