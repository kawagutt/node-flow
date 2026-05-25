"""plan.write stage."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import EXEC_TIMEOUT_SECONDS
from nodeflow.workflows.dev_process.evidence import record_exec_evidence
from nodeflow.workflows.dev_process.hermetic_argv import plan_argv
from nodeflow.workflows.dev_process.paths import assert_path_under_run_dir
from nodeflow.workflows.dev_process.plan_prompt import build_plan_prompt
from nodeflow.workflows.dev_process.workers import resolve_exec_worker, run_exec


def _parse_plan_stdout(stdout: str) -> str:
    try:
        parsed = json.loads(stdout.strip())
    except json.JSONDecodeError as e:
        raise NodeExecutionFailure(f"plan stdout must be JSON object: {e}") from e
    if not isinstance(parsed, dict):
        raise NodeExecutionFailure("plan stdout must be a JSON object")
    plan = parsed.get("plan")
    if not isinstance(plan, str) or not plan.strip():
        raise NodeExecutionFailure("plan JSON must include non-empty string field 'plan'")
    return plan.strip()


def run_plan_stage(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    task_prompt: str,
    approved_spec: str,
    exec_argv: list[str] | None = None,
    revision_context: str | None = None,
    previous_plan: str | None = None,
    exec_worker_kind: Optional[str] = None,
) -> Dict[str, Any]:
    prompt_text = build_plan_prompt(
        task_prompt=task_prompt,
        approved_spec=approved_spec,
        revision_context=revision_context,
        previous_plan=previous_plan,
    )
    worker = resolve_exec_worker(exec_worker_kind)
    argv = exec_argv if exec_argv is not None else plan_argv()
    cwd = str(repo_root)
    execution_output = run_exec(
        worker,
        prompt=prompt_text,
        cwd=cwd,
        argv=argv,
        timeout=EXEC_TIMEOUT_SECONDS,
    )
    evidence_path = record_exec_evidence(
        artifact_root=artifact_root,
        run_id=run_id,
        stage="plan",
        invoker=worker.invoker,
        execution_output=execution_output,
        argv=argv,
        prompt=prompt_text,
        cwd=cwd,
    )
    plan_text = _parse_plan_stdout(str(execution_output.get("stdout") or ""))
    plan_dir = Path(artifact_root) / "plan"
    plan_dir.mkdir(parents=True, exist_ok=True)
    plan_path = plan_dir / "plan.md"
    plan_path.write_text(plan_text, encoding="utf-8")
    assert_path_under_run_dir(artifact_root, str(plan_path))
    return {
        "status": "completed",
        "plan_artifact": str(plan_path),
        "evidence_paths": [evidence_path],
    }
