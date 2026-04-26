"""Untracked paths and text excerpts (shared by collect_diff and collect_repo_context)."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Dict, List, Sequence


def is_untracked_ignored(rel: str, prefixes: Sequence[str]) -> bool:
    for x in prefixes:
        base = x.rstrip("/")
        if rel == base or rel.startswith(base + "/"):
            return True
    return False


def run_git(repo_root: Path, argv: List[str]) -> tuple[int, str]:
    proc = subprocess.run(
        ["git", *argv],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )
    out = (proc.stdout or proc.stderr or "").strip()
    return proc.returncode, out


def filtered_untracked_paths(
    repo_root: Path, ignored_prefixes: Sequence[str]
) -> tuple[int, List[str]]:
    rc, untracked_out = run_git(repo_root, ["ls-files", "--others", "--exclude-standard"])
    if rc != 0:
        return rc, []
    paths = [
        line.strip()
        for line in untracked_out.splitlines()
        if line.strip() and not is_untracked_ignored(line.strip(), ignored_prefixes)
    ]
    return rc, paths


def filter_status_short(status_short: str, ignored_prefixes: Sequence[str]) -> str:
    """Drop ignored untracked lines ('?? path') from git status --short output."""
    lines: List[str] = []
    for line in status_short.splitlines():
        if line.startswith("?? "):
            rel = line[3:].strip()
            if is_untracked_ignored(rel, ignored_prefixes):
                continue
        lines.append(line)
    return "\n".join(lines)


def read_text_excerpt(path: Path, max_bytes: int) -> tuple[str, bool]:
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


def untracked_file_excerpts(
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
        content, trunc = read_text_excerpt(fp, max_bytes)
        excerpts.append(
            {
                "path": rel,
                "content": content[:content_clip],
                "truncated": trunc or len(content) >= content_clip,
            }
        )
    return excerpts
