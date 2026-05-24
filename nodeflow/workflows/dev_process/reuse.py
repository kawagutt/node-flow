"""Bridge to development_flow internals — single import boundary for dev_process."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Dict, Tuple

from nodeflow.core.base_node import NodeExecutionFailure

# Field mapping: dev_process run_context -> development_flow stage inputs
RUN_CONTEXT_TO_DF: Dict[str, str] = {
    "repo_root": "source_repo_root",
    "source_base_revision": "source_base_revision",
    "artifact_root": "artifact_root",
    "planned_branch_name": "planned_branch_name",
    "workspace_strategy": "strategy",
}

DF_ACTION_REWORK = "rework"
DEV_PROCESS_ACTION_REWORK = "rework_implementation"


def dev_process_action_to_df(action: str) -> str:
    if action == DEV_PROCESS_ACTION_REWORK:
        return DF_ACTION_REWORK
    return action


def run_context_for_df(run_context: Dict[str, Any]) -> Dict[str, Any]:
    """Map dev_process checkpoint run_context to development_flow keys."""
    return {
        "source_repo_root": run_context.get("repo_root"),
        "source_base_revision": run_context.get("source_base_revision"),
        "source_current_branch": run_context.get("source_current_branch"),
        "planned_branch_name": run_context.get("planned_branch_name"),
        "artifact_root": run_context.get("artifact_root"),
    }


def check_source_workspace(
    repo_root: Path | str,
    *,
    ignored_dirty_prefixes: Tuple[str, ...] = (".nodeflow/",),
) -> Dict[str, Any]:
    from nodeflow.workflows.development_flow.check_source_workspace.node_check_source_workspace import (
        CheckSourceWorkspaceNode,
    )

    node = CheckSourceWorkspaceNode()
    out = node.execute(
        {"source_repo_root": str(repo_root)},
        {"ignored_dirty_prefixes": list(ignored_dirty_prefixes)},
    )
    return out.get("source_workspace_check") or {}


def prepare_workspace(
    *,
    source_repo_root: str,
    run_context: Dict[str, Any],
    strategy: str = "current_repo",
    existing_workspace: Dict[str, Any] | None = None,
    ignored_dirty_prefixes: Tuple[str, ...] = (".nodeflow/",),
) -> Dict[str, Any]:
    from nodeflow.workflows.development_flow.prepare_workspace.node_prepare_workspace import (
        PrepareWorkspaceNode,
    )

    if "source_repo_root" in run_context:
        df_rc = dict(run_context)
    else:
        df_rc = run_context_for_df(run_context)
        artifact_root = run_context.get("artifact_root")
        if isinstance(artifact_root, str) and artifact_root.strip():
            df_rc["artifact_root"] = artifact_root.strip()
        attempt = run_context.get("workspace_attempt")
        if isinstance(attempt, int) and attempt >= 1:
            df_rc["workspace_attempt"] = attempt
        subdir = run_context.get("worktree_subdirectory")
        if isinstance(subdir, str) and subdir.strip():
            df_rc["worktree_subdirectory"] = subdir.strip()
    inputs: Dict[str, Any] = {
        "source_repo_root": source_repo_root,
        "run_context": df_rc,
    }
    if isinstance(existing_workspace, dict) and existing_workspace:
        inputs["workspace_context"] = existing_workspace
    node = PrepareWorkspaceNode()
    out = node.execute(
        inputs,
        {"strategy": strategy, "ignored_dirty_prefixes": list(ignored_dirty_prefixes)},
    )
    return out.get("workspace_context") or {}


def remove_git_worktree(
    *,
    source_repo_root: str,
    artifact_root: str,
    workspace_root: str,
) -> None:
    """Remove a git worktree only when workspace_root is under artifact_root/worktrees/."""
    artifact = Path(artifact_root).resolve()
    worktrees_root = (artifact / "worktrees").resolve()
    path = Path(workspace_root).resolve()
    try:
        path.relative_to(worktrees_root)
    except ValueError as e:
        raise NodeExecutionFailure(
            f"workspace_root must be under {worktrees_root}, got {path}"
        ) from e
    if not path.exists():
        return
    cp = subprocess.run(
        ["git", "-C", source_repo_root, "worktree", "remove", "--force", str(path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode != 0:
        err = (cp.stderr or cp.stdout or "").strip() or "git worktree remove failed"
        raise NodeExecutionFailure(f"failed to remove git worktree {path}: {err}")


def collect_repo_context(
    *,
    repo_root: Path | str,
    task_prompt: str,
    base_revision: str,
    revision_context: str | None = None,
    ignored_untracked_prefixes: Tuple[str, ...] = (".nodeflow/",),
) -> Dict[str, Any]:
    from nodeflow.workflows.development_flow.spec_plan.node_spec_plan import CollectRepoContextNode

    inputs: Dict[str, Any] = {
        "repo_root": str(repo_root),
        "task_prompt": task_prompt,
        "base_ref": base_revision,
    }
    if revision_context:
        inputs["revision_context"] = revision_context
    node = CollectRepoContextNode()
    out = node.execute(inputs, {"ignored_untracked_prefixes": list(ignored_untracked_prefixes)})
    return out.get("repo_context") or {}


def collect_diff(
    *,
    repo_root: Path | str,
    base_revision: str,
    ignored_changed_file_prefixes: Tuple[str, ...] = (".nodeflow/",),
) -> Dict[str, Any]:
    from nodeflow.nodes.git.collect_diff.node_collect_diff import CollectDiffNode

    node = CollectDiffNode()
    out = node.execute(
        {"repo_root": str(repo_root), "base_ref": base_revision},
        {"ignored_changed_file_prefixes": list(ignored_changed_file_prefixes)},
    )
    return out.get("diff_result") or {}


def run_tests(
    *,
    repo_root: Path | str,
    argv: list[str],
    timeout: int = 60,
) -> Dict[str, Any]:
    from nodeflow.workflows.development_flow.implement.node_implement import RunTestsNode

    node = RunTestsNode()
    out = node.execute({"repo_root": str(repo_root)}, {"argv": argv, "timeout": timeout})
    return out.get("test_result") or {}


def write_stage_checkpoint(
    *,
    request: Dict[str, Any],
    checkpoint_dir: str,
    run_id: str,
    stage: str,
    repo_root: Path | str,
    extra_inputs: Dict[str, Any] | None = None,
    params: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    from nodeflow.workflows.development_flow.write_checkpoint.node_write_checkpoint import (
        WriteCheckpointNode,
    )

    node = WriteCheckpointNode()
    inputs = {"request": request, **(extra_inputs or {})}
    p = {
        "checkpoint_dir": checkpoint_dir,
        "run_id": run_id,
        "stage": stage,
        "_repo_root_for_paths": str(repo_root),
        **(params or {}),
    }
    out = node.execute(inputs, p)
    return out.get("stage_result") or {}


def build_review_prompt(
    reviewer_key: str,
    *,
    repo_root: Path | str,
    base_revision: str,
    diff_result: Dict[str, Any],
    test_result: Dict[str, Any],
    approved_spec: str,
    approved_plan: str,
) -> str:
    from nodeflow.workflows.development_flow.review.build_diff_review_prompt import (
        BuildDiffReviewPromptNode,
    )
    from nodeflow.workflows.development_flow.review.build_spec_review_prompt import (
        BuildSpecReviewPromptNode,
    )
    from nodeflow.workflows.development_flow.review.build_spec_revision_review_prompt import (
        BuildSpecRevisionReviewPromptNode,
    )
    from nodeflow.workflows.development_flow.review.build_test_review_prompt import (
        BuildTestReviewPromptNode,
    )
    from nodeflow.workflows.development_flow.review.build_wide_scan_review_prompt import (
        BuildWideScanReviewPromptNode,
    )

    _PROMPT_NODES = {
        "review_diff": BuildDiffReviewPromptNode,
        "review_wide": BuildWideScanReviewPromptNode,
        "review_tests": BuildTestReviewPromptNode,
        "review_spec": BuildSpecReviewPromptNode,
        "review_spec_revision": BuildSpecRevisionReviewPromptNode,
    }
    cls = _PROMPT_NODES.get(reviewer_key)
    if cls is None:
        raise NodeExecutionFailure(f"unknown reviewer key {reviewer_key!r}")
    node = cls()
    out = node.execute(
        {
            "repo_root": str(repo_root),
            "base_ref": base_revision,
            "diff_result": diff_result,
            "test_result": test_result,
            "approved_spec_plan": {"spec": approved_spec, "plan": approved_plan},
        },
        {},
    )
    prompt = out.get("codex_task_prompt") or {}
    if isinstance(prompt, dict):
        return str(prompt.get("text") or "")
    return str(prompt)


def aggregate_reviews(
    *,
    review_inputs: Dict[str, Any],
    test_result: Dict[str, Any],
    diff_result: Dict[str, Any],
    spec_revision_needed_default: bool = False,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    from nodeflow.workflows.development_flow.review.aggregate_reviews import AggregateReviewsNode

    node = AggregateReviewsNode()
    out = node.execute(
        {**review_inputs, "test_result": test_result, "diff_result": diff_result},
        {"spec_revision_needed_default": spec_revision_needed_default},
    )
    return out.get("review_result") or {}, out.get("checkpoint_request") or {}
