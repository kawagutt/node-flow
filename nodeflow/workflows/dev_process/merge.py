"""Merge-time git operations and development summary for dev_process."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Dict, List

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import (
    MERGE_POLICY_GIT_MERGE_BRANCH,
    MERGE_POLICY_RECORD_ONLY,
    WORKSPACE_STRATEGY_GIT_WORKTREE,
)
from nodeflow.workflows.dev_process.paths import planned_branch_name_for_attempt

_DEFAULT_IGNORED_DIRTY_PREFIXES = (".nodeflow/",)


def _run_git(cwd: Path, argv: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *argv],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )


def _parse_porcelain_path(line: str) -> str:
    path_part = line[3:].strip() if len(line) >= 3 else line.strip()
    if path_part.startswith('"') and path_part.endswith('"'):
        path_part = path_part[1:-1]
    if " -> " in path_part:
        _, new_path = path_part.split(" -> ", 1)
        return new_path.strip().strip('"')
    return path_part


def _is_ignored_dirty_path(path: str, ignored_prefixes: List[str]) -> bool:
    return any(path.startswith(prefix) for prefix in ignored_prefixes)


def validate_merge_policy(policy: str) -> str:
    """Validate merge_policy string; raise on unknown values."""
    name = (policy or MERGE_POLICY_RECORD_ONLY).strip()
    if name not in (MERGE_POLICY_RECORD_ONLY, MERGE_POLICY_GIT_MERGE_BRANCH):
        raise NodeExecutionFailure(f"unsupported merge_policy: {name!r}")
    return name


def assert_merge_policy_allowed_at_start(
    *,
    merge_policy: str,
    workspace_strategy: str,
    source_current_branch: str,
) -> None:
    """Fail fast on incompatible merge_policy / workspace / branch at flow start."""
    policy = validate_merge_policy(merge_policy)
    if policy != MERGE_POLICY_GIT_MERGE_BRANCH:
        return
    if workspace_strategy != WORKSPACE_STRATEGY_GIT_WORKTREE:
        raise NodeExecutionFailure(
            f"merge_policy {MERGE_POLICY_GIT_MERGE_BRANCH!r} requires workspace_strategy=git_worktree"
        )
    branch = str(source_current_branch or "").strip()
    if not branch or branch == "HEAD":
        raise NodeExecutionFailure(
            "git_merge_branch requires source repo on a named branch (not detached HEAD)"
        )


def assert_source_repo_clean_for_merge(
    source: Path,
    *,
    ignored_dirty_prefixes: tuple[str, ...] = _DEFAULT_IGNORED_DIRTY_PREFIXES,
) -> None:
    """Ensure source repo has no blocking dirty paths before git checkout/merge."""
    cp = _run_git(source, ["status", "--porcelain"])
    if cp.returncode != 0:
        err = (cp.stderr or cp.stdout or "").strip() or "git status failed"
        raise NodeExecutionFailure(f"git status failed before merge: {err}")
    prefixes = list(ignored_dirty_prefixes)
    blocking: list[str] = []
    for raw in (cp.stdout or "").splitlines():
        line = raw.rstrip("\r\n")
        if not line:
            continue
        path = _parse_porcelain_path(line)
        if _is_ignored_dirty_path(path, prefixes):
            continue
        blocking.append(path)
    if blocking:
        preview = ", ".join(blocking[:5])
        suffix = "..." if len(blocking) > 5 else ""
        raise NodeExecutionFailure(
            "source repo has uncommitted changes before merge; "
            f"commit/stash first: {preview}{suffix}"
        )


def assert_target_branch_exists(source: Path, target_branch: str) -> None:
    if not target_branch.strip() or target_branch.strip() == "HEAD":
        raise NodeExecutionFailure(
            f"invalid merge target branch: {target_branch!r} (named branch required)"
        )
    cp = _run_git(
        source,
        ["show-ref", "--verify", "--quiet", f"refs/heads/{target_branch}"],
    )
    if cp.returncode != 0:
        raise NodeExecutionFailure(f"merge target branch not found: {target_branch!r}")


def git_branch_head(repo_root: str | Path, branch: str) -> str:
    cp = _run_git(Path(repo_root), ["rev-parse", f"refs/heads/{branch}"])
    if cp.returncode != 0:
        err = (cp.stderr or cp.stdout or "").strip() or "git rev-parse failed"
        raise NodeExecutionFailure(f"failed to resolve branch {branch!r}: {err}")
    head = (cp.stdout or "").strip()
    if not head:
        raise NodeExecutionFailure(f"failed to resolve branch {branch!r}")
    return head


def reviewed_branch_from_body(body: Dict[str, Any]) -> str:
    wc = body.get("workspace_context") if isinstance(body.get("workspace_context"), dict) else {}
    branch = str(wc.get("current_branch") or wc.get("planned_branch_name") or "").strip()
    if branch:
        return branch
    run_context = body.get("run_context") if isinstance(body.get("run_context"), dict) else {}
    return str(run_context.get("source_current_branch") or "").strip()


def record_reviewed_branch_snapshot(body: Dict[str, Any]) -> tuple[str, str]:
    """Capture workspace branch name and HEAD at review completion."""
    branch = reviewed_branch_from_body(body)
    if not branch:
        raise NodeExecutionFailure("reviewed branch name could not be resolved from workspace")
    run_context = body["run_context"]
    wc = body.get("workspace_context") if isinstance(body.get("workspace_context"), dict) else {}
    source_repo = str(wc.get("source_repo_root") or run_context.get("repo_root") or "")
    if not source_repo:
        raise NodeExecutionFailure("source repo root is required to snapshot reviewed branch")
    return branch, git_branch_head(source_repo, branch)


def assert_worktree_clean_for_merge(workspace_root: str | Path) -> None:
    """Require committed worktree changes before git_merge_branch."""
    root = Path(workspace_root).resolve()
    cp = _run_git(root, ["status", "--porcelain"])
    if cp.returncode != 0:
        err = (cp.stderr or cp.stdout or "").strip() or "git status failed"
        raise NodeExecutionFailure(f"git status failed in worktree before merge: {err}")
    lines = [ln for ln in (cp.stdout or "").splitlines() if ln.strip()]
    if lines:
        preview = ", ".join(_parse_porcelain_path(ln) for ln in lines[:5])
        suffix = "..." if len(lines) > 5 else ""
        raise NodeExecutionFailure(
            "git_merge_branch requires committed worktree changes; "
            f"worktree has uncommitted changes: {preview}{suffix}"
        )


def assert_reviewed_branch_unchanged(
    body: Dict[str, Any], *, branch: str, source_repo: str | Path
) -> None:
    review_st = (body.get("stages") or {}).get("review") or {}
    if not isinstance(review_st, dict):
        raise NodeExecutionFailure("stages.review is required for git merge")
    expected_head = str(review_st.get("reviewed_branch_head") or "").strip()
    reviewed_branch = str(review_st.get("reviewed_branch_name") or "").strip()
    if not expected_head:
        raise NodeExecutionFailure("stages.review.reviewed_branch_head is required for git merge")
    if reviewed_branch and reviewed_branch != branch:
        raise NodeExecutionFailure(
            f"reviewed branch name mismatch: expected {branch!r}, got {reviewed_branch!r}"
        )
    current_head = git_branch_head(source_repo, branch)
    if current_head != expected_head:
        raise NodeExecutionFailure("merge branch changed after review; rerun review before merge")


def assert_revision_is_ancestor(
    source: Path,
    ancestor: str,
    descendant: str,
    *,
    label: str,
) -> None:
    cp = _run_git(source, ["merge-base", "--is-ancestor", ancestor, descendant])
    if cp.returncode != 0:
        err = (cp.stderr or cp.stdout or "").strip()
        detail = f": {err}" if err else ""
        raise NodeExecutionFailure(f"{label}{detail}")


def assert_source_base_revision_ancestry(
    source_repo: str | Path,
    *,
    source_base_revision: str,
    attempt_branch: str,
    target_branch: str,
) -> None:
    """Ensure merge branches relate to the flow-start base revision."""
    base = str(source_base_revision or "").strip()
    if not base:
        raise NodeExecutionFailure("run_context.source_base_revision is required for git merge")
    source = Path(source_repo).resolve()
    assert_revision_is_ancestor(
        source,
        base,
        f"refs/heads/{attempt_branch}",
        label=(f"attempt branch {attempt_branch!r} is unrelated to flow start base {base!r}"),
    )
    assert_revision_is_ancestor(
        source,
        base,
        f"refs/heads/{target_branch}",
        label=(f"merge target branch {target_branch!r} is unrelated to flow start base {base!r}"),
    )


def perform_git_merge_branch(
    *,
    source_repo_root: str,
    planned_branch_name: str,
    target_branch: str,
) -> Dict[str, Any]:
    """Merge ``planned_branch_name`` into ``target_branch`` at the source repository."""
    source = Path(source_repo_root).resolve()
    if not planned_branch_name.strip():
        raise NodeExecutionFailure("perform_git_merge_branch requires planned_branch_name")
    if not target_branch.strip():
        raise NodeExecutionFailure("perform_git_merge_branch requires target_branch")

    assert_source_repo_clean_for_merge(source)
    assert_target_branch_exists(source, target_branch)

    verify = _run_git(
        source,
        ["show-ref", "--verify", "--quiet", f"refs/heads/{planned_branch_name}"],
    )
    if verify.returncode != 0:
        raise NodeExecutionFailure(
            f"merge branch not found in source repo: {planned_branch_name!r}"
        )

    checkout = _run_git(source, ["checkout", target_branch])
    if checkout.returncode != 0:
        err = (checkout.stderr or checkout.stdout or "").strip() or "git checkout failed"
        raise NodeExecutionFailure(f"git checkout {target_branch!r} failed: {err}")

    merge_cp = _run_git(source, ["merge", "--no-edit", planned_branch_name])
    if merge_cp.returncode != 0:
        err = (merge_cp.stderr or merge_cp.stdout or "").strip() or "git merge failed"
        abort_cp = _run_git(source, ["merge", "--abort"])
        if abort_cp.returncode != 0:
            abort_err = (
                abort_cp.stderr or abort_cp.stdout or ""
            ).strip() or "git merge --abort failed"
            raise NodeExecutionFailure(
                f"git merge {planned_branch_name!r} into {target_branch!r} failed: {err}; "
                f"merge --abort also failed: {abort_err}"
            )
        raise NodeExecutionFailure(
            f"git merge {planned_branch_name!r} into {target_branch!r} failed (aborted): {err}"
        )

    return {
        "ok": True,
        "merged_branch": planned_branch_name,
        "target_branch": target_branch,
        "stdout": (merge_cp.stdout or "").strip(),
    }


def resolve_merge_policy(body: Dict[str, Any]) -> str:
    dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
    return validate_merge_policy(str(dp.get("merge_policy") or MERGE_POLICY_RECORD_ONLY))


def _workspace_attempt_from_body(body: Dict[str, Any]) -> int:
    dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
    attempt = dp.get("workspace_attempt")
    if isinstance(attempt, int) and attempt >= 1:
        return attempt
    return 1


def assert_merge_branch_owned_by_run(
    body: Dict[str, Any],
    *,
    run_context: Dict[str, Any],
    wc: Dict[str, Any],
) -> str:
    """Validate merge branch ownership and workspace_context before git merge."""
    run_id = str(run_context.get("run_id") or "").strip()
    if not run_id:
        raise NodeExecutionFailure("run_context.run_id is required for git merge")

    attempt = _workspace_attempt_from_body(body)
    expected_branch = planned_branch_name_for_attempt(run_id, attempt)
    branch = str(wc.get("planned_branch_name") or "").strip()
    if branch != expected_branch:
        raise NodeExecutionFailure(
            f"merge branch mismatch: expected {expected_branch!r}, got {branch!r}"
        )

    repo_root = str(run_context.get("repo_root") or "").strip()
    source_repo = str(wc.get("source_repo_root") or "").strip()
    if not repo_root or not source_repo:
        raise NodeExecutionFailure(
            "run_context.repo_root and workspace_context.source_repo_root are required"
        )
    if Path(repo_root).resolve() != Path(source_repo).resolve():
        raise NodeExecutionFailure(
            "workspace_context.source_repo_root must match run_context.repo_root for git merge"
        )

    current_branch = str(wc.get("current_branch") or "").strip()
    if current_branch != expected_branch:
        raise NodeExecutionFailure(
            f"workspace_context.current_branch mismatch: expected {expected_branch!r}, "
            f"got {current_branch!r}"
        )

    artifact_root = str(run_context.get("artifact_root") or "").strip()
    workspace_root = str(wc.get("workspace_root") or "").strip()
    if not artifact_root or not workspace_root:
        raise NodeExecutionFailure(
            "run_context.artifact_root and workspace_context.workspace_root are required for git merge"
        )
    worktrees_root = (Path(artifact_root).resolve() / "worktrees").resolve()
    try:
        Path(workspace_root).resolve().relative_to(worktrees_root)
    except ValueError as e:
        raise NodeExecutionFailure(
            f"workspace_context.workspace_root must be under {worktrees_root}, got {workspace_root!r}"
        ) from e

    return expected_branch


def execute_merge_policy(body: Dict[str, Any]) -> Dict[str, Any]:
    """Apply merge policy; returns merge_result dict stored on checkpoint."""
    policy = resolve_merge_policy(body)
    run_context = body["run_context"]
    strategy = str(run_context.get("workspace_strategy") or "current_repo")

    if policy == MERGE_POLICY_RECORD_ONLY:
        return {"policy": policy, "ok": True, "mechanical": True}

    if policy != MERGE_POLICY_GIT_MERGE_BRANCH:
        raise NodeExecutionFailure(f"unsupported merge_policy: {policy!r}")

    if strategy != WORKSPACE_STRATEGY_GIT_WORKTREE:
        raise NodeExecutionFailure(
            f"merge_policy {MERGE_POLICY_GIT_MERGE_BRANCH!r} requires workspace_strategy git_worktree"
        )

    wc = body.get("workspace_context")
    if not isinstance(wc, dict):
        raise NodeExecutionFailure("git_merge_branch requires workspace_context on checkpoint")

    branch = assert_merge_branch_owned_by_run(body, run_context=run_context, wc=wc)
    source_repo = str(run_context.get("repo_root") or "")
    assert_worktree_clean_for_merge(str(wc.get("workspace_root") or ""))
    assert_reviewed_branch_unchanged(body, branch=branch, source_repo=source_repo)
    target = str(run_context.get("source_current_branch") or "").strip()
    if not target:
        raise NodeExecutionFailure("run_context.source_current_branch is required for git merge")
    assert_target_branch_exists(Path(source_repo), target)
    assert_source_base_revision_ancestry(
        source_repo,
        source_base_revision=str(run_context.get("source_base_revision") or ""),
        attempt_branch=branch,
        target_branch=target,
    )

    git_out = perform_git_merge_branch(
        source_repo_root=run_context["repo_root"],
        planned_branch_name=branch,
        target_branch=target,
    )
    return {"policy": policy, **git_out}
