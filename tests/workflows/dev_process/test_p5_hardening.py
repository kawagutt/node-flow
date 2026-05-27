"""P5 hardening: branch names, rework/revise worktree, CLI inputs."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import (
    MERGE_POLICY_GIT_MERGE_BRANCH,
    MERGE_POLICY_RECORD_ONLY,
    STATE_AWAITING_FINAL,
    STATE_AWAITING_IMPLEMENTATION,
    STATE_AWAITING_SPEC_HUMAN_GATE,
)
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)
from nodeflow.workflows.dev_process.flow_actions import run_flow
from nodeflow.workflows.dev_process.paths import (
    planned_branch_name_for_run,
)
from nodeflow.workflows.dev_process.reuse import remove_git_worktree
from nodeflow.workflows.development_flow.prepare_workspace import PrepareWorkspaceNode
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit
from tests.workflows.dev_process.v2_flow_helpers import (
    approve_and_continue,
    approve_spec_to_implementation,
    full_through_review,
)


def test_planned_branch_name_sanitizes_special_chars() -> None:
    assert planned_branch_name_for_run("20260524T12.00_000001Z") == (
        "feat/nodeflow/20260524T12-00-000001Z"
    )


def test_planned_branch_name_unique_across_runs_same_day() -> None:
    a = planned_branch_name_for_run("20260524T120000000001Z")
    b = planned_branch_name_for_run("20260524T130000000002Z")
    assert a != b
    assert a == "feat/nodeflow/20260524T120000000001Z"
    assert b == "feat/nodeflow/20260524T130000000002Z"


def test_start_freezes_run_context_workspace_root_on_worktree(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "x",
            "workspace_strategy": "git_worktree",
        },
        {},
    )["flow_output"]
    source_root = str(repo.resolve())
    assert start["run_context"]["workspace_root"] == source_root
    cp = start["flow_result"]["flow_checkpoint_path"]
    after_approve = approve_spec_to_implementation(repo, cp)
    assert after_approve["run_context"]["workspace_root"] == source_root
    assert "workspace_context" not in after_approve
    from tests.workflows.dev_process.v2_flow_helpers import continue_from_implementation

    continued = continue_from_implementation(
        repo, after_approve["flow_result"]["flow_checkpoint_path"]
    )
    assert continued["run_context"]["workspace_root"] == source_root
    assert continued["workspace_context"]["workspace_root"] != source_root


def _start(tmp_path: Path) -> dict:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    out = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "feature",
            "workspace_strategy": "git_worktree",
        },
        {},
    )["flow_output"]
    return {"repo": repo, "flow": out}


def test_git_worktree_rework_reuses_workspace(tmp_path: Path) -> None:
    ctx = _start(tmp_path)
    cp = ctx["flow"]["flow_result"]["flow_checkpoint_path"]
    first = approve_and_continue(ctx["repo"], cp)
    wt1 = first["workspace_context"]["workspace_root"]
    cp2 = first["flow_result"]["flow_checkpoint_path"]
    rework = DevProcessFlowNode().execute(
        {
            "action": "rework_implementation",
            "repo_root": str(ctx["repo"]),
            "flow_checkpoint_path": cp2,
        },
        {},
    )["flow_output"]
    assert rework["flow_result"]["state"] == STATE_AWAITING_FINAL
    rework_wc = rework.get("workspace_context") or {}
    assert rework_wc.get("workspace_root") == wt1


def test_revise_spec_then_approve_uses_new_worktree_attempt(tmp_path: Path) -> None:
    import json as _json

    ctx = _start(tmp_path)
    cp = ctx["flow"]["flow_result"]["flow_checkpoint_path"]
    first = approve_and_continue(ctx["repo"], cp)
    cp2 = first["flow_result"]["flow_checkpoint_path"]
    revised = DevProcessFlowNode().execute(
        {
            "action": "revise_spec",
            "repo_root": str(ctx["repo"]),
            "flow_checkpoint_path": cp2,
            "task_prompt": "revise",
        },
        {},
    )["flow_output"]
    assert revised["flow_result"]["state"] == STATE_AWAITING_SPEC_HUMAN_GATE
    cp3 = revised["flow_result"]["flow_checkpoint_path"]
    after_approve = approve_spec_to_implementation(ctx["repo"], cp3)
    from tests.workflows.dev_process.v2_flow_helpers import continue_from_implementation

    second = continue_from_implementation(
        ctx["repo"], after_approve["flow_result"]["flow_checkpoint_path"]
    )
    cp2_doc = _json.loads(Path(second["flow_result"]["flow_checkpoint_path"]).read_text("utf-8"))
    tb2 = cp2_doc.get("dev_process", {}).get("task_branch", {})
    assert tb2.get("created") is True
    wt2_path = tb2.get("worktree_path", "")
    if wt2_path:
        wt2 = Path(wt2_path)
        assert wt2.is_dir()


def test_git_worktree_rejects_existing_branch(tmp_path: Path) -> None:
    repo = tmp_path / "repo_branch"
    repo.mkdir()
    git_repo_with_commit(repo)
    branch = "feat/nodeflow/collision-branch"
    subprocess.run(
        ["git", "branch", branch],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    run_context = {
        "planned_branch_name": branch,
        "source_repo_root": str(repo.resolve()),
        "source_base_revision": subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo),
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip(),
        "source_current_branch": "main",
        "artifact_root": str(artifact_root.resolve()),
        "workspace_attempt": 1,
        "worktree_subdirectory": "worktrees/001",
    }
    node = PrepareWorkspaceNode()
    node.execute(
        {"source_repo_root": str(repo), "run_context": run_context},
        {"strategy": "git_worktree"},
    )
    err = node.read_error()
    assert isinstance(err, NodeExecutionFailure)
    assert "planned_branch_name already exists" in str(err)


def test_input_port_workspace_strategy(tmp_path: Path) -> None:
    repo = tmp_path / "repo_cli"
    repo.mkdir()
    git_repo_with_commit(repo)
    out = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "cli",
            "workspace_strategy": "git_worktree",
        },
        {},
    )["flow_output"]
    assert out["run_context"]["workspace_strategy"] == "git_worktree"


def test_rework_blocking_review_resets_human_gates_final(tmp_path: Path) -> None:
    ctx = _start(tmp_path)
    cp = ctx["flow"]["flow_result"]["flow_checkpoint_path"]
    first = approve_and_continue(ctx["repo"], cp)
    assert first["flow_result"]["merge_ready"] is True
    assert first["run_context"]["artifact_root"]
    cp2 = first["flow_result"]["flow_checkpoint_path"]
    rework = DevProcessFlowNode().execute(
        {
            "action": "rework_implementation",
            "repo_root": str(ctx["repo"]),
            "flow_checkpoint_path": cp2,
        },
        {"force_review_blocking": True, "auto_continue": False},
    )["flow_output"]
    assert rework["flow_result"]["merge_ready"] is False
    from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint

    doc = load_flow_checkpoint(rework["flow_result"]["flow_checkpoint_path"])
    gates = (doc.get("dev_process") or {}).get("human_gates") or {}
    assert gates.get("final") == "not_reached"


def test_revise_spec_resets_human_gates_final(tmp_path: Path) -> None:
    ctx = _start(tmp_path)
    review = full_through_review(ctx["repo"])
    cp2 = review["flow_result"]["flow_checkpoint_path"]
    revised = DevProcessFlowNode().execute(
        {
            "action": "revise_spec",
            "repo_root": str(ctx["repo"]),
            "flow_checkpoint_path": cp2,
            "task_prompt": "narrow the scope",
        },
        {},
    )["flow_output"]
    from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint

    doc = load_flow_checkpoint(revised["flow_result"]["flow_checkpoint_path"])
    gates = (doc.get("dev_process") or {}).get("human_gates") or {}
    assert gates.get("spec") == "pending"
    assert gates.get("final") == "not_reached"


def test_prepare_workspace_rejects_malicious_worktree_subdirectory(tmp_path: Path) -> None:
    repo = tmp_path / "repo_escape"
    repo.mkdir()
    git_repo_with_commit(repo)
    artifact_root = tmp_path / "artifacts_escape"
    artifact_root.mkdir()
    run_context = {
        "planned_branch_name": "feat/nodeflow-escape",
        "source_repo_root": str(repo.resolve()),
        "source_base_revision": subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo),
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip(),
        "source_current_branch": "main",
        "artifact_root": str(artifact_root.resolve()),
        "workspace_attempt": 1,
        "worktree_subdirectory": "../outside",
    }
    node = PrepareWorkspaceNode()
    node.execute(
        {"source_repo_root": str(repo), "run_context": run_context},
        {"strategy": "git_worktree"},
    )
    err = node.read_error()
    assert isinstance(err, NodeExecutionFailure)
    assert "invalid worktree_subdirectory" in str(err)


def test_remove_git_worktree_rejects_path_outside_worktrees(tmp_path: Path) -> None:
    repo = tmp_path / "repo_remove"
    repo.mkdir()
    git_repo_with_commit(repo)
    artifact_root = tmp_path / "artifacts_remove"
    artifact_root.mkdir()
    with pytest.raises(NodeExecutionFailure, match="workspace_root must be under"):
        remove_git_worktree(
            source_repo_root=str(repo),
            artifact_root=str(artifact_root),
            workspace_root=str(tmp_path / "outside"),
        )


def test_resume_workspace_strategy_mismatch_fails(tmp_path: Path) -> None:
    repo = tmp_path / "repo_resume"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "resume",
            "workspace_strategy": "current_repo",
        },
        {},
    )["flow_output"]
    cp = start["flow_result"]["flow_checkpoint_path"]
    with pytest.raises(NodeExecutionFailure, match="workspace_strategy mismatch"):
        run_flow(
            action="approve_spec",
            repo_root=str(repo),
            flow_checkpoint_path=cp,
            workspace_strategy="git_worktree",
        )


def test_resume_merge_policy_mismatch_fails(tmp_path: Path) -> None:
    repo = tmp_path / "repo_resume_mp"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "resume mp",
            "merge_policy": MERGE_POLICY_RECORD_ONLY,
        },
        {},
    )["flow_output"]
    cp = start["flow_result"]["flow_checkpoint_path"]
    with pytest.raises(NodeExecutionFailure, match="merge_policy mismatch"):
        run_flow(
            action="approve_spec",
            repo_root=str(repo),
            flow_checkpoint_path=cp,
            merge_policy=MERGE_POLICY_GIT_MERGE_BRANCH,
        )


def test_resume_exec_worker_kind_mismatch_fails(tmp_path: Path) -> None:
    repo = tmp_path / "repo_resume_worker"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "resume worker",
            "exec_worker_kind": "codex",
        },
        {},
    )["flow_output"]
    cp = start["flow_result"]["flow_checkpoint_path"]
    with pytest.raises(NodeExecutionFailure, match="exec_worker_kind mismatch"):
        run_flow(
            action="approve_spec",
            repo_root=str(repo),
            flow_checkpoint_path=cp,
            exec_worker_kind="claude",
        )


def test_resume_workspace_strategy_params_fallback_matches_checkpoint(tmp_path: Path) -> None:
    repo = tmp_path / "repo_params_ws"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "params ws",
            "workspace_strategy": "git_worktree",
        },
        {},
    )["flow_output"]
    cp = start["flow_result"]["flow_checkpoint_path"]
    approved = DevProcessFlowNode().execute(
        {
            "action": "approve_spec",
            "repo_root": str(repo),
            "flow_checkpoint_path": cp,
        },
        {"workspace_strategy": "git_worktree", "auto_continue": False},
    )["flow_output"]
    assert approved["flow_result"]["state"] == STATE_AWAITING_IMPLEMENTATION


def test_resume_workspace_strategy_params_mismatch_fails(tmp_path: Path) -> None:
    repo = tmp_path / "repo_params_mismatch"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "params mismatch",
            "workspace_strategy": "current_repo",
        },
        {},
    )["flow_output"]
    cp = start["flow_result"]["flow_checkpoint_path"]
    with pytest.raises(NodeExecutionFailure, match="workspace_strategy mismatch"):
        DevProcessFlowNode().execute(
            {
                "action": "approve_spec",
                "repo_root": str(repo),
                "flow_checkpoint_path": cp,
            },
            {"workspace_strategy": "git_worktree"},
        )


def test_exec_argv_via_params_hermetic(tmp_path: Path) -> None:
    from tests.workflows.dev_process.hermetic_argv import spec_argv

    repo = tmp_path / "repo_exec_argv"
    repo.mkdir()
    git_repo_with_commit(repo)
    out = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "argv params",
        },
        {"exec_argv": spec_argv()},
    )["flow_output"]
    assert out["flow_result"]["state"] == STATE_AWAITING_SPEC_HUMAN_GATE
    artifact_root = Path(out["run_context"]["artifact_root"])
    assert (artifact_root / "spec" / "spec.md").is_file()


def test_exec_argv_persisted_in_checkpoint(tmp_path: Path) -> None:
    from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint
    from nodeflow.workflows.dev_process.flow_context import _resolve_exec_argv, _stored_exec_argv
    from tests.workflows.dev_process.hermetic_argv import spec_argv

    repo = tmp_path / "repo_exec_argv_resume"
    repo.mkdir()
    git_repo_with_commit(repo)
    argv = spec_argv()
    start = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "argv resume",
            "exec_argv": argv,
            "workspace_strategy": "git_worktree",
        },
        {},
    )["flow_output"]
    cp = start["flow_result"]["flow_checkpoint_path"]
    body = load_flow_checkpoint(cp)
    assert body["dev_process"]["exec_policy_snapshot"]["default_argv"] == argv
    assert _stored_exec_argv(body) == argv
    assert _resolve_exec_argv(None, body=body) == argv


def test_exec_model_from_params_on_start(tmp_path: Path) -> None:
    from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint

    repo = tmp_path / "repo_exec_model_params"
    repo.mkdir()
    git_repo_with_commit(repo)
    out = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "model params"},
        {"exec_model": "from-params-only"},
    )["flow_output"]
    body = load_flow_checkpoint(out["flow_result"]["flow_checkpoint_path"])
    assert body["dev_process"]["exec_policy_snapshot"]["default_model"] == "from-params-only"


def test_exec_model_mismatch_on_resume(tmp_path: Path) -> None:
    import pytest

    from nodeflow.core.base_node import NodeExecutionFailure
    from tests.workflows.dev_process.hermetic_argv import spec_argv

    repo = tmp_path / "repo_exec_model_resume"
    repo.mkdir()
    git_repo_with_commit(repo)
    argv = spec_argv()
    start = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "model resume",
            "exec_argv": argv,
            "exec_model": "gpt-test",
        },
        {},
    )["flow_output"]
    cp = start["flow_result"]["flow_checkpoint_path"]
    with pytest.raises(NodeExecutionFailure, match="exec_model mismatch"):
        DevProcessFlowNode().execute(
            {
                "action": "approve_spec",
                "repo_root": str(repo),
                "flow_checkpoint_path": cp,
                "exec_model": "other-model",
            },
            {},
        )
