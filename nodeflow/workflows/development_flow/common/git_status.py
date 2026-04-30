"""Shared helpers for simple `git status --porcelain` output parsing."""

from __future__ import annotations

from fnmatch import fnmatch
from typing import Dict, List


def default_ignored_dirty_prefixes() -> List[str]:
    """Paths under these prefixes are ignored when deciding if the repo is dirty."""
    return [".nodeflow/"]


def _is_ignored(path: str, ignored_prefixes: List[str]) -> bool:
    return any(path.startswith(prefix) for prefix in ignored_prefixes)


def _parse_porcelain_v1_line(line: str) -> Dict[str, str]:
    """Parse one porcelain-v1 line into status/path fields."""
    xy = line[:2] if len(line) >= 2 else ""
    path_part = line[3:].strip() if len(line) >= 3 else line.strip()
    if path_part.startswith('"') and path_part.endswith('"'):
        path_part = path_part[1:-1]
    if " -> " in path_part:
        old_path, new_path = path_part.split(" -> ", 1)
        old_path = old_path.strip().strip('"')
        new_path = new_path.strip().strip('"')
        return {"xy": xy, "path": new_path, "old_path": old_path, "new_path": new_path}
    return {"xy": xy, "path": path_part, "old_path": path_part, "new_path": path_part}


def status_has_non_ignored_changes(status_text: str, ignored_prefixes: List[str]) -> bool:
    """Return True if simple porcelain-v1 output contains non-ignored changes."""
    for raw in status_text.splitlines():
        line = raw.rstrip("\r\n")
        if not line:
            continue
        parsed = _parse_porcelain_v1_line(line)
        old_path = parsed["old_path"]
        new_path = parsed["new_path"]
        if old_path != new_path:
            if _is_ignored(old_path, ignored_prefixes) and _is_ignored(new_path, ignored_prefixes):
                continue
            return True
        if _is_ignored(parsed["path"], ignored_prefixes):
            continue
        return True
    return False


def status_violates_start_policy(
    status_text: str,
    *,
    ignored_prefixes: List[str],
    fail_on_tracked_changes: bool,
    fail_on_untracked: bool,
    allowed_untracked_prefixes: List[str],
    blocked_untracked_globs: List[str],
) -> bool:
    """Return True if status contains changes forbidden by start policy."""
    for raw in status_text.splitlines():
        line = raw.rstrip("\r\n")
        if not line:
            continue
        parsed = _parse_porcelain_v1_line(line)
        xy = parsed["xy"]
        path = parsed["path"]
        old_path = parsed["old_path"]
        new_path = parsed["new_path"]
        if _is_ignored(old_path, ignored_prefixes) and _is_ignored(new_path, ignored_prefixes):
            continue

        is_untracked = xy == "??"
        if is_untracked:
            if any(fnmatch(path, pattern) for pattern in blocked_untracked_globs):
                return True
            if any(path.startswith(prefix) for prefix in allowed_untracked_prefixes):
                continue
            if fail_on_untracked:
                return True
            continue

        if fail_on_tracked_changes:
            return True
    return False
