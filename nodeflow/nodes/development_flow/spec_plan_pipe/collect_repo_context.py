"""Collect repository context for spec/plan drafting."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PythonActionNode


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
        base_ref = str(inputs.get("base_ref") or "HEAD")
        max_diff_chars = int(params.get("max_diff_chars", 4000))

        _git_required(repo_root, ["rev-parse", "--show-toplevel"])
        status_short = _git_required(repo_root, ["status", "--short"])

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

        repo_context_block = (
            f"Repository status (git status --short):\n{status_short or '(clean)'}\n\n"
            f"Changed files (up to 50):\n{json.dumps(changed_files, ensure_ascii=False)}\n\n"
            f"Diff excerpt vs {base_ref}:\n{diff_excerpt or '(empty)'}\n"
        )

        codex_body = (
            "Draft SPEC and PLAN for the following task.\n\n"
            f"## Task\n{task_prompt}\n\n"
            f"## Base ref\n{base_ref}\n\n"
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
                    "diff_excerpt": diff_excerpt,
                },
                "task_prompt": task_prompt,
            },
        }

        return {
            "repo_context": {
                "repo_root": str(repo_root),
                "base_ref": base_ref,
                "changed_files": changed_files,
                "status_short": status_short,
                "diff_excerpt": diff_excerpt,
            },
            "codex_task_prompt": {"text": codex_body},
            "task_meta": {"task_type": "spec_plan"},
            "checkpoint_request": checkpoint_request,
        }
