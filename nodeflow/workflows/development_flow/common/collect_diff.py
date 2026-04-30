"""Collect git diff, status, and untracked files for implement/review stages."""

from __future__ import annotations

from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.workflows.development_flow.common.git_untracked import (
    filter_status_short,
    filtered_untracked_paths,
    run_git,
    untracked_file_excerpts,
)


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

        raw_ignored = params.get("ignored_changed_file_prefixes")
        if isinstance(raw_ignored, (list, tuple)):
            ignored_prefixes: List[str] = [str(p) for p in raw_ignored]
        else:
            ignored_prefixes = [".nodeflow/"]

        rc_diff, diff_text_full = run_git(repo_root, ["diff", base_ref])
        rc_status, status_short_raw = run_git(repo_root, ["status", "--short"])
        status_short = filter_status_short(status_short_raw, ignored_prefixes)
        rc_untracked, untracked_files = filtered_untracked_paths(repo_root, ignored_prefixes)

        diff_text = diff_text_full[:max_chars]
        truncated = len(diff_text_full) > max_chars

        excerpts = untracked_file_excerpts(
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
