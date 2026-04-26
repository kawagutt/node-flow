"""Collect git diff, status, and untracked files for implement/review stages."""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List, Sequence

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode


def _untracked_path_ignored(rel: str, prefixes: Sequence[str]) -> bool:
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
        max_chars = int(params.get("max_chars", 8000))
        excerpt_max_files = int(params.get("untracked_excerpt_max_files", 10))
        excerpt_max_bytes = int(params.get("untracked_excerpt_max_bytes", 2000))

        raw_ignored = params.get("ignored_untracked_prefixes")
        if isinstance(raw_ignored, (list, tuple)):
            ignored_prefixes: List[str] = [str(p) for p in raw_ignored]
        else:
            ignored_prefixes = [".nodeflow/"]

        rc_diff, diff_text_full = _run_git(repo_root, ["diff", base_ref])
        rc_status, status_short = _run_git(repo_root, ["status", "--short"])
        rc_untracked, untracked_out = _run_git(
            repo_root, ["ls-files", "--others", "--exclude-standard"]
        )
        untracked_files = [
            line.strip()
            for line in untracked_out.splitlines()
            if line.strip() and not _untracked_path_ignored(line.strip(), ignored_prefixes)
        ]

        diff_text = diff_text_full[:max_chars]
        truncated = len(diff_text_full) > max_chars

        excerpts: List[Dict[str, Any]] = []
        for rel in untracked_files[:excerpt_max_files]:
            fp = (repo_root / rel).resolve()
            try:
                fp.relative_to(repo_root)
            except ValueError:
                continue
            if not fp.is_file():
                continue
            content, trunc = _read_text_excerpt(fp, excerpt_max_bytes)
            excerpts.append(
                {
                    "path": rel,
                    "content": content[:8000],
                    "truncated": trunc or len(content) >= 8000,
                }
            )

        ok = rc_diff == 0 and rc_status == 0 and rc_untracked == 0

        return {
            "diff_result": {
                "ok": ok,
                "base_ref": base_ref,
                "compare_target": base_ref,
                "diff": diff_text,
                "truncated": truncated,
                "status_short": status_short,
                "untracked_files": untracked_files,
                "untracked_file_excerpts": excerpts,
                "git_returncodes": {
                    "diff": rc_diff,
                    "status": rc_status,
                    "untracked": rc_untracked,
                },
            }
        }
