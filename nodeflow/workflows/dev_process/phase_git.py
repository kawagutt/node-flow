"""Git operations for phase-based dev-process: task branch + phase commit."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Dict, List

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.paths import git_head_revision


def _existing_branch_ref(repo_root: Path, branch_name: str) -> str | None:
    """Return the commit SHA a branch points to, or None if it doesn't exist."""
    cp = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "--verify", f"refs/heads/{branch_name}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode == 0:
        return cp.stdout.strip()
    return None


def _worktree_branch(wt_path: Path) -> str | None:
    """Return the branch name checked out in a worktree, or None."""
    cp = subprocess.run(
        ["git", "-C", str(wt_path), "branch", "--show-current"],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode == 0:
        return cp.stdout.strip() or None
    return None


def _task_branch_name(run_id: str) -> str:
    """Non-colliding task branch name (distinct from attempt branches)."""
    import re

    safe = re.sub(r"[^A-Za-z0-9-]+", "-", run_id).strip("-") or "task"
    return f"phase-base/{safe}"


def create_task_branch(
    repo_root: Path,
    run_id: str,
    *,
    workspace_strategy: str = "current_repo",
) -> Dict[str, Any]:
    """Create the task branch and return branch metadata.

    Branch name uses ``phase-base/<run_id>`` to avoid collision with
    ``feat/nodeflow/<run_id>/attempt-NNN`` created by prepare_workspace.

    For git_worktree: worktree is placed at ``<repo_parent>/.nodeflow-worktrees/<safe>/``.
    Returns dict with: name, base_ref, created, and optionally worktree_path.
    """
    base_ref = git_head_revision(repo_root)
    branch_name = _task_branch_name(run_id)

    if workspace_strategy == "current_repo":
        dirty = collect_phase_changed_paths(repo_root, artifact_roots=[])
        if dirty:
            raise NodeExecutionFailure(
                f"Cannot create task branch: working tree has uncommitted changes: "
                f"{dirty[:5]}; commit or stash before starting phase-based flow"
            )

    if workspace_strategy == "git_worktree":
        wt_root = repo_root.resolve().parent / ".nodeflow-worktrees"
        safe_name = branch_name.replace("/", "_")
        wt_path = wt_root / safe_name
        wt_root.mkdir(parents=True, exist_ok=True)

        existing = _existing_branch_ref(repo_root, branch_name)
        if existing:
            if existing != base_ref:
                raise NodeExecutionFailure(
                    f"Task branch {branch_name!r} already exists at {existing[:12]} "
                    f"but current HEAD is {base_ref[:12]}; "
                    "delete the branch or resume from checkpoint"
                )
            if wt_path.is_dir():
                wt_branch = _worktree_branch(wt_path)
                if wt_branch == branch_name:
                    return {
                        "name": branch_name,
                        "base_ref": base_ref,
                        "worktree_path": str(wt_path),
                        "created": True,
                        "reused": True,
                    }
            cp = subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo_root),
                    "worktree",
                    "add",
                    str(wt_path),
                    branch_name,
                ],
                capture_output=True,
                text=True,
                check=False,
            )
        else:
            cp = subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo_root),
                    "worktree",
                    "add",
                    "-b",
                    branch_name,
                    str(wt_path),
                    base_ref,
                ],
                capture_output=True,
                text=True,
                check=False,
            )
        if cp.returncode != 0:
            raise NodeExecutionFailure(
                f"Failed to create worktree task branch {branch_name!r}: {cp.stderr.strip()}"
            )
        return {
            "name": branch_name,
            "base_ref": base_ref,
            "worktree_path": str(wt_path),
            "worktree_root": str(wt_root),
            "created": True,
            "workspace_strategy": workspace_strategy,
        }

    cp = subprocess.run(
        ["git", "-C", str(repo_root), "checkout", "-b", branch_name, base_ref],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode != 0:
        existing = _existing_branch_ref(repo_root, branch_name)
        if existing == base_ref:
            co = subprocess.run(
                ["git", "-C", str(repo_root), "checkout", branch_name],
                capture_output=True,
                text=True,
                check=False,
            )
            if co.returncode != 0:
                raise NodeExecutionFailure(
                    f"Failed to checkout existing task branch {branch_name!r}: "
                    f"{co.stderr.strip()}"
                )
            return {
                "name": branch_name,
                "base_ref": base_ref,
                "created": True,
                "reused": True,
            }
        raise NodeExecutionFailure(
            f"Failed to create task branch {branch_name!r}: {cp.stderr.strip()}; "
            f"if branch already exists from a different base, delete it or resume from checkpoint"
        )

    return {
        "name": branch_name,
        "base_ref": base_ref,
        "created": True,
    }


def collect_phase_changed_paths(
    repo_root: Path,
    *,
    artifact_roots: list[str],
) -> List[str]:
    """Collect changed paths (tracked + untracked), excluding artifact dirs.

    Handles rename entries in ``git status --porcelain -z`` where the renamed-to
    path follows the entry as a separate NUL-delimited field.
    """
    cp = subprocess.run(
        ["git", "-C", str(repo_root), "status", "--porcelain", "-z"],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode != 0:
        raise NodeExecutionFailure(f"git status failed: {cp.stderr.strip()}")

    raw = cp.stdout or ""
    fields = raw.split("\0")
    exclude_prefixes = [".nodeflow/", ".hermes/"]
    for root in artifact_roots:
        rel = _try_relative(repo_root, root)
        if rel:
            exclude_prefixes.append(rel if rel.endswith("/") else rel + "/")

    def _excluded(p: str) -> bool:
        return any(p.startswith(pfx) for pfx in exclude_prefixes)

    paths: List[str] = []
    i = 0
    while i < len(fields):
        entry = fields[i]
        if len(entry) < 4:
            i += 1
            continue
        status_xy = entry[:2]
        file_path = entry[3:].strip()
        if not file_path:
            i += 1
            continue
        is_rename = "R" in status_xy or "C" in status_xy
        if is_rename:
            if not _excluded(file_path):
                paths.append(file_path)
            i += 1
            if i < len(fields) and fields[i]:
                new_path = fields[i].strip()
                if new_path and not _excluded(new_path):
                    paths.append(new_path)
            i += 1
        else:
            if not _excluded(file_path):
                paths.append(file_path)
            i += 1

    return paths


def _try_relative(repo_root: Path, artifact_root: str) -> str:
    """Try to make artifact_root relative to repo_root."""
    try:
        return str(Path(artifact_root).resolve().relative_to(repo_root.resolve()))
    except ValueError:
        return ""


def verify_on_task_branch(repo_root: Path, expected_branch: str) -> None:
    """Raise if the repo is not on the expected task branch."""
    cp = subprocess.run(
        ["git", "-C", str(repo_root), "branch", "--show-current"],
        capture_output=True,
        text=True,
        check=False,
    )
    actual = (cp.stdout or "").strip()
    if actual != expected_branch:
        raise NodeExecutionFailure(
            f"Expected task branch {expected_branch!r} but repo is on {actual!r}; "
            "aborting phase commit to prevent committing to wrong branch"
        )


def phase_commit(
    repo_root: Path,
    *,
    phase_id: str,
    phase_title: str,
    artifact_roots: list[str],
    expected_branch: str = "",
) -> Dict[str, Any]:
    """Create a phase boundary commit with exact path filter.

    Returns dict with: phase_commit (sha), actual_commit_created.
    If no project diff, returns HEAD without creating a commit.
    When ``expected_branch`` is given, verifies the repo is on that branch.
    """
    if expected_branch:
        verify_on_task_branch(repo_root, expected_branch)

    paths = collect_phase_changed_paths(repo_root, artifact_roots=artifact_roots)

    if not paths:
        head = git_head_revision(repo_root)
        return {
            "phase_commit": head,
            "actual_commit_created": False,
            "committed_paths": [],
        }

    rst = subprocess.run(
        ["git", "-C", str(repo_root), "reset"],
        capture_output=True,
        text=True,
        check=False,
    )
    if rst.returncode != 0:
        raise NodeExecutionFailure(f"git reset (unstage) failed: {rst.stderr.strip()}")

    cp = subprocess.run(
        ["git", "-C", str(repo_root), "add", "--"] + paths,
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode != 0:
        raise NodeExecutionFailure(f"git add failed: {cp.stderr.strip()}")

    message = f"{phase_id}: {phase_title}"
    cp = subprocess.run(
        ["git", "-C", str(repo_root), "commit", "-m", message],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode != 0:
        stderr = cp.stderr.strip()
        if "nothing to commit" in (cp.stdout or "") or "nothing to commit" in stderr:
            head = git_head_revision(repo_root)
            return {
                "phase_commit": head,
                "actual_commit_created": False,
                "committed_paths": paths,
            }
        raise NodeExecutionFailure(f"git commit failed: {stderr}")

    commit_sha = git_head_revision(repo_root)
    return {
        "phase_commit": commit_sha,
        "actual_commit_created": True,
        "committed_paths": paths,
    }


def save_uncommitted_diff(
    repo_root: Path,
    *,
    artifact_root: str,
    phase_id: str,
    artifact_roots: list[str] | None = None,
) -> Dict[str, str]:
    """Save tracked diff and untracked file list before rework reset.

    Returns dict with patch_path and untracked_list_path.
    ``artifact_roots`` are excluded from untracked file cleanup (same filter
    as ``collect_phase_changed_paths``).
    """
    phase_dir = Path(artifact_root) / "phases" / phase_id / "rework_backup"
    phase_dir.mkdir(parents=True, exist_ok=True)

    cp = subprocess.run(
        ["git", "-C", str(repo_root), "diff"],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode != 0:
        raise NodeExecutionFailure(
            f"git diff failed (exit {cp.returncode}): {(cp.stderr or '').strip()}"
        )
    patch_path = str(phase_dir / "tracked.patch")
    Path(patch_path).write_text(cp.stdout or "", encoding="utf-8")

    cp_staged = subprocess.run(
        ["git", "-C", str(repo_root), "diff", "--cached"],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp_staged.returncode != 0:
        raise NodeExecutionFailure(
            f"git diff --cached failed (exit {cp_staged.returncode}): "
            f"{(cp_staged.stderr or '').strip()}"
        )
    staged_path = str(phase_dir / "staged.patch")
    Path(staged_path).write_text(cp_staged.stdout or "", encoding="utf-8")

    cp2 = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files", "--others", "--exclude-standard"],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp2.returncode != 0:
        raise NodeExecutionFailure(
            f"git ls-files failed (exit {cp2.returncode}): {(cp2.stderr or '').strip()}"
        )
    untracked_list_path = str(phase_dir / "untracked_files.txt")
    all_untracked = (cp2.stdout or "").strip().splitlines()

    exclude_prefixes = [".nodeflow/", ".hermes/"]
    for root in artifact_roots or []:
        rel = _try_relative(repo_root, root)
        if rel:
            exclude_prefixes.append(rel if rel.endswith("/") else rel + "/")

    project_untracked = [
        f for f in all_untracked if not any(f.startswith(pfx) for pfx in exclude_prefixes)
    ]
    Path(untracked_list_path).write_text("\n".join(project_untracked), encoding="utf-8")

    untracked_backup_dir = phase_dir / "untracked_files"
    import shutil

    for rel in project_untracked:
        src = repo_root / rel
        if src.is_file():
            dst = untracked_backup_dir / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(dst))

    return {
        "patch_path": patch_path,
        "staged_patch_path": staged_path,
        "untracked_list_path": untracked_list_path,
        "untracked_backup_dir": str(untracked_backup_dir),
    }


def _clean_untracked_relpaths(repo_root: Path, relpaths: list[str]) -> None:
    """Remove untracked paths listed by ``git ls-files --others`` and prune empty parents."""
    import shutil

    repo_resolved = repo_root.resolve()
    parents_to_prune: set[Path] = set()

    for rel in relpaths:
        rel = rel.strip()
        if not rel:
            continue
        full = repo_root / rel
        try:
            full.resolve().relative_to(repo_resolved)
        except ValueError:
            continue

        if full.is_symlink() or full.is_file():
            full.unlink(missing_ok=True)
            parent = full.parent
        elif full.is_dir():
            shutil.rmtree(full)
            parent = full.parent
        else:
            continue

        while True:
            if parent == repo_root or parent == parent.parent:
                break
            parents_to_prune.add(parent)
            parent = parent.parent

    for parent in sorted(parents_to_prune, key=lambda p: len(p.parts), reverse=True):
        try:
            parent.rmdir()
        except OSError:
            pass


def reset_to_ref(
    repo_root: Path,
    ref: str,
    *,
    clean_untracked: list[str] | None = None,
    expected_branch: str = "",
) -> None:
    """Hard-reset to ref and optionally clean untracked project files.

    ``clean_untracked`` removes listed files and prunes empty parent directories
    left behind (``git ls-files --others`` lists files only, not empty dirs).

    When ``expected_branch`` is given, verifies the repo is on that branch
    before executing the destructive reset.
    """
    if expected_branch:
        verify_on_task_branch(repo_root, expected_branch)

    cp = subprocess.run(
        ["git", "-C", str(repo_root), "reset", "--hard", ref],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp.returncode != 0:
        raise NodeExecutionFailure(f"git reset --hard {ref} failed: {cp.stderr.strip()}")

    if clean_untracked:
        _clean_untracked_relpaths(repo_root, clean_untracked)
