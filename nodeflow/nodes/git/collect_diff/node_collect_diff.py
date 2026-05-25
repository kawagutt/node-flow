"""Collect git diff, status, and untracked files.

Diff mode (input ``diff_mode`` or params ``diff_mode``):
  ``committed`` (default) — ``git diff <base_ref> HEAD``
  ``working_tree``        — ``git diff <base_ref>`` (includes unstaged changes)
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List, Sequence

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PythonActionNode


def _is_untracked_ignored(rel: str, prefixes: Sequence[str]) -> bool:
    for x in prefixes:
        base = x.rstrip("/")
        if rel == base or rel.startswith(base + "/"):
            return True
    return False


def _run_git(repo_root: Path, argv: List[str]) -> tuple[int, str]:
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
    repo_root: Path, ignored_prefixes: Sequence[str]
) -> tuple[int, List[str]]:
    rc, untracked_out = _run_git(repo_root, ["ls-files", "--others", "--exclude-standard"])
    if rc != 0:
        return rc, []
    paths = [
        line.strip()
        for line in untracked_out.splitlines()
        if line.strip() and not _is_untracked_ignored(line.strip(), ignored_prefixes)
    ]
    return rc, paths


def _filter_status_short(status_short: str, ignored_prefixes: Sequence[str]) -> str:
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
    untracked_files: Sequence[str],
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


class CollectDiffNode(PythonActionNode):
    role = "collect_diff"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        repo_root = Path(str(inputs.get("repo_root") or ".")).resolve()
        base_ref = str(inputs.get("base_ref") or "HEAD")
        diff_mode = str(inputs.get("diff_mode") or params.get("diff_mode") or "committed")
        _VALID_DIFF_MODES = ("committed", "working_tree")
        if diff_mode not in _VALID_DIFF_MODES:
            raise NodeExecutionFailure(
                f"diff_mode must be 'committed' or 'working_tree', got {diff_mode!r}"
            )
        max_chars = int(params.get("max_chars", 8000))
        excerpt_max_files = int(params.get("untracked_excerpt_max_files", 10))
        excerpt_max_bytes = int(params.get("untracked_excerpt_max_bytes", 2000))

        raw_ignored = params.get("ignored_changed_file_prefixes")
        if isinstance(raw_ignored, (list, tuple)):
            ignored_prefixes: List[str] = [str(p) for p in raw_ignored]
        else:
            ignored_prefixes = [".nodeflow/"]

        diff_argv = ["diff", base_ref, "HEAD"] if diff_mode == "committed" else ["diff", base_ref]
        rc_diff, diff_text_full = _run_git(repo_root, diff_argv)
        rc_status, status_short_raw = _run_git(repo_root, ["status", "--short"])
        status_short = _filter_status_short(status_short_raw, ignored_prefixes)
        rc_untracked, untracked_files = _filtered_untracked_paths(repo_root, ignored_prefixes)

        diff_text = diff_text_full[:max_chars]
        truncated = len(diff_text_full) > max_chars

        excerpts = _untracked_file_excerpts(
            repo_root,
            untracked_files,
            max_files=excerpt_max_files,
            max_bytes=excerpt_max_bytes,
        )

        ok = rc_diff == 0 and rc_status == 0 and rc_untracked == 0

        return {
            "diff_result": {
                "ok": ok,
                "base_ref": base_ref,
                "diff_mode": diff_mode,
                "compare_target": base_ref,
                "diff": diff_text,
                "truncated": truncated,
                "status_short": status_short,
                "status_short_raw": status_short_raw,
                "untracked_files": untracked_files,
                "untracked_file_excerpts": excerpts,
                "git_returncodes": {
                    "diff": rc_diff,
                    "status": rc_status,
                    "untracked": rc_untracked,
                },
            }
        }
