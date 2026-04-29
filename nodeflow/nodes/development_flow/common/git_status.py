"""Shared helpers for simple `git status --porcelain` output parsing."""

from __future__ import annotations

from typing import List


def default_ignored_dirty_prefixes() -> List[str]:
    """Paths under these prefixes are ignored when deciding if the repo is dirty."""
    return [".nodeflow/"]


def _is_ignored(path: str, ignored_prefixes: List[str]) -> bool:
    return any(path.startswith(prefix) for prefix in ignored_prefixes)


def status_has_non_ignored_changes(status_text: str, ignored_prefixes: List[str]) -> bool:
    """Return True if simple porcelain-v1 output contains non-ignored changes."""
    for raw in status_text.splitlines():
        line = raw.rstrip("\r\n")
        if not line:
            continue
        # porcelain v1 line format is "XY PATH". Keep leading spaces in XY.
        path_part = line[3:].strip() if len(line) >= 3 else line.strip()
        if path_part.startswith('"') and path_part.endswith('"'):
            path_part = path_part[1:-1]
        if " -> " in path_part:
            old_path, new_path = path_part.split(" -> ", 1)
            old_path = old_path.strip().strip('"')
            new_path = new_path.strip().strip('"')
            if _is_ignored(old_path, ignored_prefixes) and _is_ignored(new_path, ignored_prefixes):
                continue
            return True
        if _is_ignored(path_part, ignored_prefixes):
            continue
        return True
    return False
