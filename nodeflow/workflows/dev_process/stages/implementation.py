"""write_implementation stage — codex exec + post-diff."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.node_runner import run_node_exec
from nodeflow.workflows.dev_process.reuse import collect_diff


def run_implementation_stage(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    task_prompt: str,
    base_revision: str,
    approved_spec: str,
    approved_plan: str,
    rework_context: str | None = None,
    body: Dict[str, Any],
) -> Dict[str, Any]:
    prompt = (
        "Implement the approved plan in the repository working tree.\n\n"
        f"## Spec\n{approved_spec}\n\n## Plan\n{approved_plan}\n\n## Task\n{task_prompt}\n\n"
    )
    if rework_context and rework_context.strip():
        prompt += f"## Rework feedback\n{rework_context.strip()}\n\n"
    prompt += (
        "When finished, stage and commit all intentional changes on the current branch "
        '(for example: git add -A && git commit -m "<short message>"). '
        "Leave the worktree clean except for ignored paths.\n"
    )
    cwd = str(repo_root)

    execution_output, evidence_path, _rec = run_node_exec(
        body,
        node_name="write_implementation",
        stage="implementation",
        prompt=prompt,
        cwd=cwd,
        run_id=run_id,
        artifact_root=artifact_root,
    )

    diff_result = collect_diff(repo_root=repo_root, base_revision=base_revision)

    head_rev = _git_head_rev(repo_root)
    branch_advanced = head_rev is not None and head_rev != base_revision
    diff_text = str(diff_result.get("diff") or "")
    if branch_advanced and not diff_text.strip():
        raise NodeExecutionFailure(
            f"implementation branch advanced (HEAD={head_rev[:12] if head_rev else '?'}) "
            f"but collected diff is empty — review would receive no change context"
        )

    summary_path = Path(artifact_root) / "implementation" / "summary.txt"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(str(execution_output.get("stdout") or ""), encoding="utf-8")
    return {
        "status": "completed",
        "execution_output": execution_output,
        "diff_result": diff_result,
        "evidence_paths": [evidence_path],
        "summary_artifact": str(summary_path),
    }


def _git_head_rev(repo_root: Path) -> str | None:
    try:
        cp = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=False,
        )
        if cp.returncode == 0:
            return (cp.stdout or "").strip() or None
    except OSError:
        pass
    return None
