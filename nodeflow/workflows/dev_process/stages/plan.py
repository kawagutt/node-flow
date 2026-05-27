"""write_plan node."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Optional

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.artifact_versions import (
    PLAN_VERSION_STATUS_COMMITTED,
    mark_plan_draft_pending_contract_validation,
    write_plan_latest_only,
    write_versioned_plan,
)
from nodeflow.workflows.dev_process.constants import EXEC_TIMEOUT_SECONDS
from nodeflow.workflows.dev_process.contract_check import merge_continuation_plan
from nodeflow.workflows.dev_process.evidence import record_exec_evidence
from nodeflow.workflows.dev_process.exec_policy import default_argv_for_worker
from nodeflow.workflows.dev_process.paths import assert_path_under_run_dir
from nodeflow.workflows.dev_process.plan_phases import (
    PlanData,
    PlanParseError,
    parse_continuation_plan,
    parse_new_plan,
    save_plan_json,
)
from nodeflow.workflows.dev_process.plan_prompt import (
    build_continuation_plan_prompt,
    build_plan_prompt,
    format_planning_mode_context,
)
from nodeflow.workflows.dev_process.workers import resolve_exec_worker, run_exec

MAX_PLAN_PARSE_RETRIES = 3


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


def _save_invalid_attempt(artifact_root: str, attempt: int, plan_text: str, error_msg: str) -> None:
    inv_dir = Path(artifact_root) / "plan" / "invalid_attempts"
    inv_dir.mkdir(parents=True, exist_ok=True)
    inv_dir.joinpath(f"attempt_{attempt:03d}.md").write_text(plan_text, encoding="utf-8")
    inv_dir.joinpath(f"attempt_{attempt:03d}_error.txt").write_text(error_msg, encoding="utf-8")


def _run_plan_generation(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    task_prompt: str,
    approved_spec: str,
    revision_context: str | None = None,
    previous_plan: str | None = None,
    exec_argv: list[str] | None = None,
    exec_worker_kind: Optional[str] = None,
    body: Optional[Dict[str, Any]] = None,
    completed_phases: list[Dict[str, Any]] | None = None,
    parse_error_feedback: str | None = None,
    continuation_findings: list[Dict[str, Any]] | None = None,
) -> tuple[str, str | None]:
    """Run plan generation agent and return (raw plan text, evidence_path)."""
    dp = (body or {}).get("dev_process") if body else None
    mode_prefix = format_planning_mode_context(dp) if isinstance(dp, dict) else ""

    if continuation_findings is not None:
        prompt_text = build_continuation_plan_prompt(
            task_prompt=task_prompt,
            approved_spec=approved_spec,
            completed_phases=completed_phases or [],
            findings=continuation_findings,
            previous_plan=previous_plan,
            parse_error_feedback=parse_error_feedback,
            revision_context=revision_context,
        )
    else:
        prompt_text = build_plan_prompt(
            task_prompt=task_prompt,
            approved_spec=approved_spec,
            revision_context=revision_context,
            previous_plan=previous_plan,
            completed_phases=completed_phases,
            parse_error_feedback=parse_error_feedback,
        )
    if mode_prefix:
        prompt_text = mode_prefix + "\n" + prompt_text
    cwd = str(repo_root)
    evidence_path: str | None = None

    if body is not None:
        from nodeflow.workflows.dev_process.node_runner import run_node_exec

        execution_output, evidence_path, _rec = run_node_exec(
            body,
            node_name="write_plan",
            stage="plan",
            prompt=prompt_text,
            cwd=cwd,
            run_id=run_id,
            artifact_root=artifact_root,
        )
    else:
        worker = resolve_exec_worker(exec_worker_kind)
        argv = exec_argv if exec_argv is not None else default_argv_for_worker(worker.kind)
        execution_output = run_exec(
            worker, prompt=prompt_text, cwd=cwd, argv=argv, timeout=EXEC_TIMEOUT_SECONDS
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

    return _parse_plan_stdout(str(execution_output.get("stdout") or "")), evidence_path


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
    body: Optional[Dict[str, Any]] = None,
    completed_phases: list[Dict[str, Any]] | None = None,
    continuation_findings: list[Dict[str, Any]] | None = None,
    continuation_start_index: int = 0,
    existing_plan: PlanData | None = None,
    existing_plan_text: str | None = None,
    defer_plan_version_commit: bool = False,
) -> Dict[str, Any]:
    parse_error_feedback: str | None = None
    evidence_paths: list[str] = []
    is_continuation = continuation_findings is not None

    for attempt in range(1, MAX_PLAN_PARSE_RETRIES + 1):
        plan_text, ev_path = _run_plan_generation(
            repo_root=repo_root,
            artifact_root=artifact_root,
            run_id=run_id,
            task_prompt=task_prompt,
            approved_spec=approved_spec,
            revision_context=revision_context,
            previous_plan=previous_plan,
            exec_argv=exec_argv,
            exec_worker_kind=exec_worker_kind,
            body=body,
            completed_phases=completed_phases,
            parse_error_feedback=parse_error_feedback,
            continuation_findings=continuation_findings,
        )
        if ev_path:
            evidence_paths.append(ev_path)
        plan_dir = Path(artifact_root) / "plan"
        plan_dir.mkdir(parents=True, exist_ok=True)

        try:
            if is_continuation:
                if existing_plan is None:
                    raise NodeExecutionFailure(
                        "continuation plan requires existing_plan (merged executable plan)"
                    )
                continuation_plan = parse_continuation_plan(
                    plan_text, start_index=continuation_start_index
                )
                from nodeflow.workflows.dev_process.plan_phases import (
                    renumber_continuation_headings,
                )

                display_cont_md = renumber_continuation_headings(
                    plan_text, start_index=continuation_start_index
                )
                merged_text = (
                    (existing_plan_text or existing_plan.raw_text).rstrip()
                    + "\n\n---\n\n"
                    + "## Continuation plan\n\n"
                    + display_cont_md.strip()
                )
                plan_data = merge_continuation_plan(
                    existing_plan,
                    continuation_plan,
                    completed_count=continuation_start_index,
                )
                plan_data = PlanData(
                    phases=plan_data.phases,
                    raw_text=merged_text,
                    plan_sha256=hashlib.sha256(merged_text.encode()).hexdigest(),
                )
                continuation_raw_md = plan_text
            else:
                continuation_plan = None
                continuation_raw_md = None
                plan_data = parse_new_plan(plan_text)

            dp = (body or {}).get("dev_process") if body else None
            if dp is not None:
                if defer_plan_version_commit:
                    version_info = write_plan_latest_only(artifact_root, plan_data)
                else:
                    version_info = write_versioned_plan(
                        artifact_root,
                        plan_data,
                        dp,
                        continuation_raw_md=continuation_raw_md,
                        continuation_plan=continuation_plan,
                    )
                plan_path = version_info["latest_path"]
                plan_json_path = version_info["latest_json_path"]
                cont_json_path = version_info.get("continuation_json_path", "")
            else:
                plan_path = str(plan_dir / "plan.md")
                Path(plan_path).write_text(plan_data.raw_text, encoding="utf-8")
                assert_path_under_run_dir(artifact_root, plan_path)
                plan_json_path = save_plan_json(plan_data, str(plan_dir))
                cont_json_path = ""
                if is_continuation and continuation_plan is not None:
                    cont_dir = plan_dir / "continuations"
                    cont_dir.mkdir(parents=True, exist_ok=True)
                    cont_count = len(list(cont_dir.glob("continuation_*.json"))) + 1
                    cont_json_path = str(cont_dir / f"continuation_{cont_count:03d}.json")
                    (cont_dir / f"continuation_{cont_count:03d}.md").write_text(
                        plan_text, encoding="utf-8"
                    )
                    Path(cont_json_path).write_text(
                        json.dumps(continuation_plan.to_dict(), indent=2, ensure_ascii=False),
                        encoding="utf-8",
                    )

            result_dict: Dict[str, Any] = {
                "status": "completed",
                "plan_artifact": str(plan_path),
                "plan_json_path": plan_json_path,
                "phase_count": len(plan_data.phases),
                "phase_ids": [p.id for p in plan_data.phases],
                "parse_attempts": attempt,
                "plan_sha256": plan_data.plan_sha256,
                "evidence_paths": evidence_paths,
            }
            if dp is not None:
                result_dict["plan_version"] = dp.get("current_plan_version", "")
                if defer_plan_version_commit:
                    mark_plan_draft_pending_contract_validation(dp, result_dict)
                else:
                    result_dict["plan_version_status"] = PLAN_VERSION_STATUS_COMMITTED
                    dp["plan_version_status"] = PLAN_VERSION_STATUS_COMMITTED
                    dp.pop("draft_plan_pending_contract_validation", None)
            if is_continuation and continuation_plan is not None:
                result_dict["continuation"] = True
                result_dict["continuation_start_index"] = continuation_start_index
                if cont_json_path:
                    result_dict["continuation_json_path"] = cont_json_path
                result_dict["continuation_phase_ids"] = [p.id for p in continuation_plan.phases]
            return result_dict
        except PlanParseError as e:
            _save_invalid_attempt(artifact_root, attempt, plan_text, str(e))
            parse_error_feedback = str(e)

    raise NodeExecutionFailure(
        f"Plan parse failed after {MAX_PLAN_PARSE_RETRIES} attempts; "
        f"last error: {parse_error_feedback}"
    )
