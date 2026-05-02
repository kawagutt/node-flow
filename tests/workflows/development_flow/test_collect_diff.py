"""CollectDiffNode."""

from __future__ import annotations

from pathlib import Path

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
