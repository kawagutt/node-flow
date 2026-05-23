"""spec_plan stage — collect context + codex + checkpoint."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, Tuple

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.nodes.exec.codex_exec import CodexExecNode
from nodeflow.workflows.dev_process.evidence import record_exec_evidence
from nodeflow.workflows.dev_process.paths import assert_path_under_run_dir
from nodeflow.workflows.dev_process.reuse import collect_repo_context, write_stage_checkpoint


def _hermetic_codex_argv() -> list[str]:
    script = (
        "import json; "
        'print(json.dumps({"spec": "# Spec\\n\\nTask spec.", "plan": "# Plan\\n\\nTask plan."}))'
    )
    return [sys.executable, "-c", script]


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
    codex_argv: list[str] | None = None,
    revision_context: str | None = None,
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

    argv = codex_argv if codex_argv is not None else _hermetic_codex_argv()
    cwd = str(repo_root)
    codex = CodexExecNode()
    exec_out = codex.execute(
        {"prompt": prompt_text},
        {"argv": argv, "timeout": 120, "cwd": cwd},
    )
    execution_output = exec_out.get("execution_output") or {}
    if not execution_output.get("ok"):
        raise NodeExecutionFailure(
            f"spec_plan codex_exec failed: {execution_output.get('stderr') or execution_output}"
        )
    evidence_path = record_exec_evidence(
        artifact_root=artifact_root,
        run_id=run_id,
        stage="spec_plan",
        invoker="codex_exec",
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
