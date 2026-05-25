"""write_spec node."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import EXEC_TIMEOUT_SECONDS
from nodeflow.workflows.dev_process.evidence import record_exec_evidence
from nodeflow.workflows.dev_process.exec_policy import default_argv_for_worker
from nodeflow.workflows.dev_process.paths import assert_path_under_run_dir
from nodeflow.workflows.dev_process.reuse import collect_repo_context
from nodeflow.workflows.dev_process.spec_prompt import build_spec_prompt
from nodeflow.workflows.dev_process.workers import ExecWorker, resolve_exec_worker, run_exec


def _parse_spec_stdout(stdout: str) -> str:
    try:
        parsed = json.loads(stdout.strip())
    except json.JSONDecodeError as e:
        raise NodeExecutionFailure(f"spec stdout must be JSON object: {e}") from e
    if not isinstance(parsed, dict):
        raise NodeExecutionFailure("spec stdout must be a JSON object")
    spec = parsed.get("spec")
    if not isinstance(spec, str) or not spec.strip():
        raise NodeExecutionFailure("spec JSON must include non-empty string field 'spec'")
    return spec.strip()


def run_spec_stage(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    task_prompt: str,
    base_revision: str,
    exec_argv: list[str] | None = None,
    revision_context: str | None = None,
    notes: str | None = None,
    reference_materials: list[dict[str, Any]] | None = None,
    previous_spec: str | None = None,
    exec_worker_kind: Optional[str] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    repo_context = collect_repo_context(
        repo_root=repo_root,
        task_prompt=task_prompt,
        base_revision=base_revision,
        revision_context=revision_context,
    )
    prompt_text = build_spec_prompt(
        task_prompt=task_prompt,
        repo_context=repo_context,
        notes=notes,
        revision_context=revision_context,
        reference_materials=reference_materials,
        previous_spec=previous_spec,
    )
    cwd = str(repo_root)

    if body is not None:
        from nodeflow.workflows.dev_process.node_runner import run_node_exec

        execution_output, evidence_path, _rec = run_node_exec(
            body,
            node_name="write_spec",
            stage="spec",
            prompt=prompt_text,
            cwd=cwd,
            run_id=run_id,
            artifact_root=artifact_root,
        )
    else:
        worker: ExecWorker = resolve_exec_worker(exec_worker_kind)
        argv = exec_argv if exec_argv is not None else default_argv_for_worker(worker.kind)
        execution_output = run_exec(
            worker, prompt=prompt_text, cwd=cwd, argv=argv, timeout=EXEC_TIMEOUT_SECONDS
        )
        evidence_path = record_exec_evidence(
            artifact_root=artifact_root,
            run_id=run_id,
            stage="spec",
            invoker=worker.invoker,
            execution_output=execution_output,
            argv=argv,
            prompt=prompt_text,
            cwd=cwd,
        )

    spec_text = _parse_spec_stdout(str(execution_output.get("stdout") or ""))
    spec_dir = Path(artifact_root) / "spec"
    spec_dir.mkdir(parents=True, exist_ok=True)
    spec_path = spec_dir / "spec.md"
    spec_path.write_text(spec_text, encoding="utf-8")
    assert_path_under_run_dir(artifact_root, str(spec_path))
    return {
        "status": "completed",
        "spec_artifact": str(spec_path),
        "evidence_paths": [evidence_path],
    }
