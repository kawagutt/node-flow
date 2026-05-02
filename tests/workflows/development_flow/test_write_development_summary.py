"""WriteDevelopmentSummaryNode."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.development_flow.prepare_workspace import PrepareWorkspaceNode
from nodeflow.workflows.development_flow.write_development_summary import (
    WriteDevelopmentSummaryNode,
)
from tests.workflows.development_flow.git_fixtures import (
    git_repo_with_commit,
    run_context_for_workspace,
)


def test_write_development_summary_node_outputs_commit_suggestion(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    ws_node = PrepareWorkspaceNode()
    ws_out = ws_node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-summary"),
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
    git_repo_with_commit(repo)
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
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-summary-style"),
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
    git_repo_with_commit(repo)
    ws_node = PrepareWorkspaceNode()
    ws_ctx = ws_node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-badrev"),
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
    git_repo_with_commit(repo)
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


def test_write_development_summary_requires_workspace_source_repo_root(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_summary_source_required"
    repo.mkdir()
    git_repo_with_commit(repo)
    ws_node = PrepareWorkspaceNode()
    ws_ctx = ws_node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-summary-source"),
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
    git_repo_with_commit(repo)
    ws_node = PrepareWorkspaceNode()
    ws_ctx = ws_node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-untracked"),
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
    git_repo_with_commit(repo)
    ws_node = PrepareWorkspaceNode()
    ws_ctx = ws_node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-output-loc"),
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
    git_repo_with_commit(repo)
    ws_node = PrepareWorkspaceNode()
    ws_ctx = ws_node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-rework-summary"),
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
            "action": "rework",
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
            "action": "rework",
        },
        {},
    )
    p1 = out1["development_summary"]["artifact_path"]
    p2 = out2["development_summary"]["artifact_path"]
    assert p1 != p2
    assert Path(p1).is_file()
    assert Path(p2).is_file()
