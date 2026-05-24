"""implement stage — codex + post-diff + tests + checkpoint."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, Optional

from nodeflow.workflows.dev_process.constants import EXEC_TIMEOUT_SECONDS
from nodeflow.workflows.dev_process.evidence import record_exec_evidence
from nodeflow.workflows.dev_process.hermetic_argv import implement_argv
from nodeflow.workflows.dev_process.paths import assert_path_under_run_dir
from nodeflow.workflows.dev_process.reuse import collect_diff, run_tests, write_stage_checkpoint
from nodeflow.workflows.dev_process.workers import ExecWorker, resolve_exec_worker, run_exec


def run_implement_stage(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    task_prompt: str,
    base_revision: str,
    approved_spec: str,
    approved_plan: str,
    exec_argv: list[str] | None = None,
    codex_argv: list[str] | None = None,
    test_argv: list[str] | None = None,
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
    argv = exec_argv if exec_argv is not None else codex_argv
    argv = argv if argv is not None else implement_argv()
    cwd = str(repo_root)
    execution_output = run_exec(
        worker, prompt=prompt, cwd=cwd, argv=argv, timeout=EXEC_TIMEOUT_SECONDS
    )
    evidence_path = record_exec_evidence(
        artifact_root=artifact_root,
        run_id=run_id,
        stage="implement",
        invoker=worker.invoker,
        execution_output=execution_output,
        argv=argv,
        prompt=prompt,
        cwd=cwd,
    )

    # Post-implementation diff so review sees Codex changes.
    diff_result = collect_diff(repo_root=repo_root, base_revision=base_revision)

    test_argv_use = (
        test_argv
        if test_argv is not None
        else [
            sys.executable,
            "-c",
            "import sys; sys.exit(0)",
        ]
    )
    test_result = run_tests(repo_root=repo_root, argv=test_argv_use, timeout=60)

    stage_cp_dir = str(Path(artifact_root) / "implement")
    stage_result = write_stage_checkpoint(
        request={
            "stage": "implement",
            "ok": bool(test_result.get("ok", True)),
            "summary": "implement completed",
            "next_action": "review",
        },
        checkpoint_dir=stage_cp_dir,
        run_id=run_id,
        stage="implement",
        repo_root=repo_root,
        extra_inputs={
            "execution_output": execution_output,
            "test_result": test_result,
            "diff_result": diff_result,
        },
        params={"next_action_default": "review"},
    )
    cp_path = None
    for art in stage_result.get("artifacts") or []:
        if isinstance(art, dict) and art.get("kind") == "checkpoint":
            cp_path = art.get("path")
            break
    if cp_path:
        assert_path_under_run_dir(artifact_root, cp_path)

    impl_summary = Path(artifact_root) / "implement" / "summary.txt"
    impl_summary.parent.mkdir(parents=True, exist_ok=True)
    impl_summary.write_text(str(execution_output.get("stdout") or ""), encoding="utf-8")

    return {
        "status": "completed" if stage_result.get("ok") else "failed",
        "stage_checkpoint_path": cp_path,
        "stage_result": stage_result,
        "test_result": test_result,
        "diff_result": diff_result,
        "evidence_paths": [evidence_path],
    }
