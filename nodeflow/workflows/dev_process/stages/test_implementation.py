"""write_tests stage — optional test scaffolding after implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from nodeflow.workflows.dev_process.constants import EXEC_TIMEOUT_SECONDS
from nodeflow.workflows.dev_process.evidence import record_exec_evidence
from nodeflow.workflows.dev_process.hermetic_argv import implement_argv
from nodeflow.workflows.dev_process.workers import ExecWorker, resolve_exec_worker, run_exec


def run_test_implementation_stage(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    approved_spec: str,
    approved_plan: str,
    exec_argv: list[str] | None = None,
    exec_worker_kind: Optional[str] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    prompt = (
        "Add or update automated tests for the implementation.\n\n"
        f"## Spec\n{approved_spec}\n\n## Plan\n{approved_plan}\n"
    )
    cwd = str(repo_root)

    if body is not None:
        from nodeflow.workflows.dev_process.node_runner import run_node_exec

        execution_output, evidence_path, _rec = run_node_exec(
            body,
            node_name="write_tests",
            stage="test_implementation",
            prompt=prompt,
            cwd=cwd,
            run_id=run_id,
            artifact_root=artifact_root,
        )
    else:
        worker: ExecWorker = resolve_exec_worker(exec_worker_kind)
        argv = exec_argv if exec_argv is not None else implement_argv()
        execution_output = run_exec(
            worker, prompt=prompt, cwd=cwd, argv=argv, timeout=EXEC_TIMEOUT_SECONDS
        )
        evidence_path = record_exec_evidence(
            artifact_root=artifact_root,
            run_id=run_id,
            stage="test_implementation",
            invoker=worker.invoker,
            execution_output=execution_output,
            argv=argv,
            prompt=prompt,
            cwd=cwd,
        )

    tests_note = Path(artifact_root) / "test_implementation" / "tests_written.txt"
    tests_note.parent.mkdir(parents=True, exist_ok=True)
    tests_note.write_text(str(execution_output.get("stdout") or "tests stage ok"), encoding="utf-8")
    return {
        "status": "completed",
        "evidence_paths": [evidence_path],
        "tests_artifact": str(tests_note),
    }
