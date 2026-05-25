"""Run a single dev-process job (1 exec = 1 session = 1 evidence)."""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from nodeflow.workflows.dev_process.argv_builder import resolve_job
from nodeflow.workflows.dev_process.constants import EXEC_TIMEOUT_SECONDS
from nodeflow.workflows.dev_process.evidence import record_exec_evidence
from nodeflow.workflows.dev_process.job_spec import JobRecord
from nodeflow.workflows.dev_process.session_store import new_session_id
from nodeflow.workflows.dev_process.workers import resolve_exec_worker, run_exec


def run_job(
    body: Dict[str, Any],
    *,
    job_key: str,
    stage: str,
    prompt: str,
    cwd: str,
    run_id: str,
    artifact_root: str,
    exec_worker_kind: Optional[str] = None,
) -> Tuple[Dict[str, Any], str, JobRecord]:
    worker_kind, model, argv = resolve_job(body, job_key)
    if exec_worker_kind:
        worker_kind = exec_worker_kind
    worker = resolve_exec_worker(worker_kind)
    jobs = body.setdefault("jobs", [])
    session_id = new_session_id(run_id=run_id, job_key=job_key, index=len(jobs))
    execution_output = run_exec(
        worker, prompt=prompt, cwd=cwd, argv=argv, timeout=EXEC_TIMEOUT_SECONDS
    )
    evidence_path = record_exec_evidence(
        artifact_root=artifact_root,
        run_id=run_id,
        stage=stage,
        invoker=worker.invoker,
        execution_output=execution_output,
        argv=argv,
        prompt=prompt,
        cwd=cwd,
    )
    record = JobRecord(
        job_key=job_key,
        stage=stage,
        worker=worker_kind,
        model=model,
        session_id=session_id,
        evidence_path=evidence_path,
        argv=argv,
    )
    jobs.append(record.to_dict())
    return execution_output, evidence_path, record
