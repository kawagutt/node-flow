"""write_implementation stage — codex exec + post-diff."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from nodeflow.workflows.dev_process.constants import EXEC_TIMEOUT_SECONDS
from nodeflow.workflows.dev_process.evidence import record_exec_evidence
from nodeflow.workflows.dev_process.hermetic_argv import implement_argv
from nodeflow.workflows.dev_process.reuse import collect_diff
from nodeflow.workflows.dev_process.workers import ExecWorker, resolve_exec_worker, run_exec


def run_implementation_stage(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    task_prompt: str,
    base_revision: str,
    approved_spec: str,
    approved_plan: str,
    exec_argv: list[str] | None = None,
    rework_context: str | None = None,
    exec_worker_kind: Optional[str] = None,
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
    worker: ExecWorker = resolve_exec_worker(exec_worker_kind)
    argv = exec_argv if exec_argv is not None else implement_argv()
    cwd = str(repo_root)
    execution_output = run_exec(
        worker, prompt=prompt, cwd=cwd, argv=argv, timeout=EXEC_TIMEOUT_SECONDS
    )
    evidence_path = record_exec_evidence(
        artifact_root=artifact_root,
        run_id=run_id,
        stage="implementation",
        invoker=worker.invoker,
        execution_output=execution_output,
        argv=argv,
        prompt=prompt,
        cwd=cwd,
    )
    diff_result = collect_diff(repo_root=repo_root, base_revision=base_revision)
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
