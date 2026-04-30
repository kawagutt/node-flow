"""Collect repository context for spec/plan drafting."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PythonActionNode


def _is_untracked_ignored(rel: str, prefixes: List[str]) -> bool:
    for x in prefixes:
        base = x.rstrip("/")
        if rel == base or rel.startswith(base + "/"):
            return True
    return False


def _run_git_rc_out(repo_root: Path, argv: List[str]) -> tuple[int, str]:
    proc = subprocess.run(
        ["git", *argv],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    out = (proc.stdout or proc.stderr or "").strip()
    return proc.returncode, out


def _filtered_untracked_paths(
    repo_root: Path, ignored_prefixes: List[str]
) -> tuple[int, List[str]]:
    rc, untracked_out = _run_git_rc_out(repo_root, ["ls-files", "--others", "--exclude-standard"])
    if rc != 0:
        return rc, []
    paths = [
        line.strip()
        for line in untracked_out.splitlines()
        if line.strip() and not _is_untracked_ignored(line.strip(), ignored_prefixes)
    ]
    return rc, paths


def _filter_status_short(status_short: str, ignored_prefixes: List[str]) -> str:
    lines: List[str] = []
    for line in status_short.splitlines():
        if line.startswith("?? "):
            rel = line[3:].strip()
            if _is_untracked_ignored(rel, ignored_prefixes):
                continue
        lines.append(line)
    return "\n".join(lines)


def _read_text_excerpt(path: Path, max_bytes: int) -> tuple[str, bool]:
    try:
        total = path.stat().st_size
    except OSError:
        return "", False
    try:
        raw = path.read_bytes()[:max_bytes]
    except OSError:
        return "", False
    truncated_file = total > max_bytes or len(raw) >= max_bytes
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return f"<binary or non-utf8, {len(raw)} bytes>", truncated_file
    return text, truncated_file


def _untracked_file_excerpts(
    repo_root: Path,
    untracked_files: List[str],
    *,
    max_files: int,
    max_bytes: int,
    content_clip: int = 8000,
) -> List[Dict[str, Any]]:
    excerpts: List[Dict[str, Any]] = []
    for rel in untracked_files[:max_files]:
        fp = (repo_root / rel).resolve()
        try:
            fp.relative_to(repo_root)
        except ValueError:
            continue
        if not fp.is_file():
            continue
        content, trunc = _read_text_excerpt(fp, max_bytes)
        excerpts.append(
            {
                "path": rel,
                "content": content[:content_clip],
                "truncated": trunc or len(content) >= content_clip,
            }
        )
    return excerpts


def _git_required(repo_root: Path, argv: List[str]) -> str:
    proc = subprocess.run(
        ["git", *argv],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip() or f"git {' '.join(argv)} failed"
        raise NodeExecutionFailure(err)
    return (proc.stdout or "").strip()


class CollectRepoContextNode(PythonActionNode):
    role = "collect_repo_context"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        repo_root = Path(str(inputs.get("repo_root") or ".")).resolve()
        task_prompt = str(inputs.get("task_prompt") or "")
        revision_context = inputs.get("revision_context")
        base_ref = str(inputs.get("base_ref") or "HEAD")
        max_diff_chars = int(params.get("max_diff_chars", 4000))
        excerpt_max_files = int(params.get("untracked_excerpt_max_files", 10))
        excerpt_max_bytes = int(params.get("untracked_excerpt_max_bytes", 2000))

        raw_ignored = params.get("ignored_untracked_prefixes")
        if isinstance(raw_ignored, (list, tuple)):
            ignored_prefixes: List[str] = [str(p) for p in raw_ignored]
        else:
            ignored_prefixes = [".nodeflow/"]

        _git_required(repo_root, ["rev-parse", "--show-toplevel"])
        status_short_raw = _git_required(repo_root, ["status", "--short"])
        status_short = _filter_status_short(status_short_raw, ignored_prefixes)

        proc = subprocess.run(
            ["git", "diff", base_ref],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise NodeExecutionFailure((proc.stderr or proc.stdout or "git diff failed").strip())
        diff_excerpt = (proc.stdout or "")[:max_diff_chars]

        names_proc = subprocess.run(
            ["git", "diff", "--name-only", base_ref],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=False,
        )
        if names_proc.returncode != 0:
            changed_files: List[str] = []
        else:
            changed_files = [
                line.strip() for line in (names_proc.stdout or "").splitlines() if line.strip()
            ][:50]

        rc_untracked, untracked_files = _filtered_untracked_paths(repo_root, ignored_prefixes)
        untracked_excerpts = _untracked_file_excerpts(
            repo_root,
            untracked_files,
            max_files=excerpt_max_files,
            max_bytes=excerpt_max_bytes,
        )

        repo_context_block = (
            f"Repository status (git status --short):\n{status_short or '(clean)'}\n\n"
            f"Changed files (up to 50):\n{json.dumps(changed_files, ensure_ascii=False)}\n\n"
            f"Diff excerpt vs {base_ref}:\n{diff_excerpt or '(empty)'}\n\n"
            "## Untracked paths (git ls-files --others --exclude-standard)\n"
            f"{json.dumps(untracked_files, ensure_ascii=False)}\n\n"
            "## Untracked file excerpts (text only; may be truncated)\n"
            f"{json.dumps(untracked_excerpts, ensure_ascii=False, indent=2)}\n"
        )

        revision_block = ""
        if revision_context:
            revision_text = (
                revision_context
                if isinstance(revision_context, str)
                else json.dumps(revision_context, ensure_ascii=False, indent=2)
            )
            revision_block = f"## Revision context\n{revision_text}\n\n"

        codex_body = (
            "Draft SPEC and PLAN for the following task.\n\n"
            f"## Task\n{task_prompt}\n\n"
            f"## Base ref\n{base_ref}\n\n"
            f"{revision_block}"
            "## Repository context\n"
            f"{repo_context_block}\n"
        )

        checkpoint_request = {
            "ok": True,
            "stage": "spec_plan",
            "summary": "spec/plan draft is ready for human approval",
            "artifacts": [],
            "human_decision_required": True,
            "raw_results": {
                "repo_context": {
                    "repo_root": str(repo_root),
                    "base_ref": base_ref,
                    "changed_files": changed_files,
                    "status_short": status_short,
                    "status_short_raw": status_short_raw,
                    "diff_excerpt": diff_excerpt,
                    "untracked_files": untracked_files,
                    "untracked_file_excerpts": untracked_excerpts,
                    "untracked_ls_returncode": rc_untracked,
                },
                "task_prompt": task_prompt,
                "revision_context": revision_context,
            },
        }

        return {
            "repo_context": {
                "repo_root": str(repo_root),
                "base_ref": base_ref,
                "changed_files": changed_files,
                "status_short": status_short,
                "status_short_raw": status_short_raw,
                "diff_excerpt": diff_excerpt,
                "untracked_files": untracked_files,
                "untracked_file_excerpts": untracked_excerpts,
                "untracked_ls_returncode": rc_untracked,
            },
            "codex_task_prompt": {"text": codex_body},
            "task_meta": {"task_type": "spec_plan"},
            "checkpoint_request": checkpoint_request,
        }
