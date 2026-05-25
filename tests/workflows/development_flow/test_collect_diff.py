"""CollectDiffNode — committed (default) and working_tree modes."""

from __future__ import annotations

import subprocess
from pathlib import Path

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.nodes.git.collect_diff import CollectDiffNode
from tests.workflows.development_flow.git_fixtures import git_repo_with_commit


def test_collect_diff_includes_untracked(tmp_path: Path) -> None:
    git_repo_with_commit(tmp_path)
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
    repo = tmp_path / "repo2"
    repo.mkdir()
    git_repo_with_commit(repo)
    nf = repo / ".nodeflow" / "checkpoints"
    nf.mkdir(parents=True)
    (nf / "x.json").write_text("{}", encoding="utf-8")
    (repo / "visible.txt").write_text("u\n", encoding="utf-8")

    node = CollectDiffNode()
    out = node.execute({"repo_root": str(repo), "base_ref": "HEAD"}, {})
    dr = out["diff_result"]
    assert "visible.txt" in dr.get("untracked_files", [])
    assert ".nodeflow/checkpoints/x.json" not in dr.get("untracked_files", [])
    assert all(not p.startswith(".nodeflow/") for p in dr.get("untracked_files", []))
    assert "?? visible.txt" in (dr.get("status_short") or "")
    assert ".nodeflow/" not in (dr.get("status_short") or "")
    assert "?? .nodeflow/" in (dr.get("status_short_raw") or "")


def test_collect_diff_default_mode_is_committed(tmp_path: Path) -> None:
    git_repo_with_commit(tmp_path)
    node = CollectDiffNode()
    out = node.execute({"repo_root": str(tmp_path), "base_ref": "HEAD"}, {})
    assert out["diff_result"]["diff_mode"] == "committed"


def test_collect_diff_working_tree_mode_includes_unstaged(tmp_path: Path) -> None:
    git_repo_with_commit(tmp_path)
    tracked = tmp_path / "README.md"
    tracked.write_text("modified\n", encoding="utf-8")

    node = CollectDiffNode()
    out = node.execute(
        {"repo_root": str(tmp_path), "base_ref": "HEAD", "diff_mode": "working_tree"},
        {},
    )
    dr = out["diff_result"]
    assert dr["diff_mode"] == "working_tree"
    assert "README.md" in dr["diff"]


def test_collect_diff_committed_mode_ignores_unstaged(tmp_path: Path) -> None:
    git_repo_with_commit(tmp_path)
    tracked = tmp_path / "README.md"
    tracked.write_text("modified\n", encoding="utf-8")

    node = CollectDiffNode()
    out = node.execute(
        {"repo_root": str(tmp_path), "base_ref": "HEAD", "diff_mode": "committed"},
        {},
    )
    dr = out["diff_result"]
    assert dr["diff_mode"] == "committed"
    assert dr["diff"] == ""


def test_collect_diff_committed_mode_shows_committed_changes(tmp_path: Path) -> None:
    git_repo_with_commit(tmp_path)
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    (tmp_path / "new.txt").write_text("new\n", encoding="utf-8")
    subprocess.run(["git", "add", "new.txt"], cwd=str(tmp_path), check=True)
    subprocess.run(
        ["git", "commit", "-m", "add new"],
        cwd=str(tmp_path),
        capture_output=True,
        check=True,
    )

    node = CollectDiffNode()
    out = node.execute({"repo_root": str(tmp_path), "base_ref": base}, {})
    dr = out["diff_result"]
    assert dr["diff_mode"] == "committed"
    assert "new.txt" in dr["diff"]


def test_collect_diff_rejects_unknown_diff_mode(tmp_path: Path) -> None:
    git_repo_with_commit(tmp_path)
    node = CollectDiffNode()
    node.execute(
        {"repo_root": str(tmp_path), "base_ref": "HEAD", "diff_mode": "typo"},
        {},
    )
    assert node.read_status() == "fatal"
    err = node.read_error()
    assert isinstance(err, NodeExecutionFailure)
    assert "diff_mode" in str(err)
