"""spec_plan stage — collect context + codex + checkpoint."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.evidence import record_exec_evidence
from nodeflow.workflows.dev_process.hermetic_argv import spec_plan_argv
from nodeflow.workflows.dev_process.paths import assert_path_under_run_dir
from nodeflow.workflows.dev_process.reuse import collect_repo_context, write_stage_checkpoint
from nodeflow.workflows.dev_process.constants import EXEC_TIMEOUT_SECONDS
from nodeflow.workflows.dev_process.workers import ExecWorker, resolve_exec_worker, run_exec


def _parse_spec_plan_stdout(stdout: str) -> Tuple[str, str]:
    try:
        parsed = json.loads(stdout.strip())
    except json.JSONDecodeError as e:
        raise NodeExecutionFailure(f"spec_plan stdout must be JSON object: {e}") from e
    if not isinstance(parsed, dict):
        raise NodeExecutionFailure("spec_plan stdout must be a JSON object")
    spec = parsed.get("spec")
    plan = parsed.get("plan")
    if not isinstance(spec, str) or not spec.strip():
        raise NodeExecutionFailure("spec_plan JSON must include non-empty string field 'spec'")
    if not isinstance(plan, str) or not plan.strip():
        raise NodeExecutionFailure("spec_plan JSON must include non-empty string field 'plan'")
    return spec.strip(), plan.strip()


def run_spec_plan_stage(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    task_prompt: str,
    base_revision: str,
    exec_argv: list[str] | None = None,
    codex_argv: list[str] | None = None,
    revision_context: str | None = None,
    exec_worker_kind: Optional[str] = None,
) -> Dict[str, Any]:
    repo_context = collect_repo_context(
        repo_root=repo_root,
        task_prompt=task_prompt,
        base_revision=base_revision,
        revision_context=revision_context,
    )
    prompt_text = (
        "Draft a spec and plan for the following task. "
        'Respond with a single JSON object: {"spec": "...", "plan": "..."}.\n\n'
        f"Task:\n{task_prompt}\n\n"
        f"Repository context:\n{json.dumps(repo_context, ensure_ascii=False)[:12000]}"
    )
    if revision_context:
        prompt_text += f"\n\nRevision context:\n{revision_context}"

    worker: ExecWorker = resolve_exec_worker(exec_worker_kind)
    argv = exec_argv if exec_argv is not None else codex_argv
    argv = argv if argv is not None else spec_plan_argv()
    cwd = str(repo_root)
    execution_output = run_exec(worker, prompt=prompt_text, cwd=cwd, argv=argv, timeout=EXEC_TIMEOUT_SECONDS)
    evidence_path = record_exec_evidence(
        artifact_root=artifact_root,
        run_id=run_id,
        stage="spec_plan",
        invoker=worker.invoker,
        execution_output=execution_output,
        argv=argv,
        prompt=prompt_text,
        cwd=cwd,
    )

    stdout = execution_output.get("stdout") or ""
    spec_text, plan_text = _parse_spec_plan_stdout(str(stdout))
    spec_path = Path(artifact_root) / "spec_plan" / "spec.md"
    plan_path = Path(artifact_root) / "spec_plan" / "plan.md"
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text(spec_text, encoding="utf-8")
    plan_path.write_text(plan_text, encoding="utf-8")

    stage_cp_dir = str(Path(artifact_root) / "spec_plan")
    stage_result = write_stage_checkpoint(
        request={
            "stage": "spec_plan",
            "ok": True,
            "summary": "spec_plan completed",
            "next_action": "approve_spec",
            "artifacts": [],
        },
        checkpoint_dir=stage_cp_dir,
        run_id=run_id,
        stage="spec_plan",
        repo_root=repo_root,
        extra_inputs={"execution_output": execution_output},
        params={
            "next_action_default": "approve_spec",
            "write_spec_plan_candidate": True,
            "spec_plan_candidate_suffix": "approved_candidate",
        },
    )
    cp_path = None
    for art in stage_result.get("artifacts") or []:
        if isinstance(art, dict) and art.get("kind") == "checkpoint":
            cp_path = art.get("path")
            break
    candidate = stage_result.get("approved_candidate_path")

    if cp_path:
        assert_path_under_run_dir(artifact_root, cp_path)

    return {
        "status": "completed",
        "stage_checkpoint_path": cp_path,
        "approved_candidate_path": candidate,
        "spec_artifact": str(spec_path),
        "plan_artifact": str(plan_path),
        "stage_result": stage_result,
        "evidence_paths": [evidence_path],
    }
