"""Run a single dev-process node execution (1 exec = 1 logical session = 1 evidence).

P10: every Codex exec in the main path flows through ``run_node_exec``.

``model`` resolved from ``exec_policy_snapshot`` is recorded in both ``NodeRun``
and the evidence JSON as **audit metadata**.  It is *not* injected into worker argv
yet; model selection remains the responsibility of the argv preset.  A future phase
(P10.5 / P11) may add argv-level model injection per worker type.

``session_id`` is a **logical** identifier deterministically derived from
``(run_id, node_name, index)``.  Provider-level session isolation (e.g. Codex
session resume) is worker-dependent and not guaranteed by ``run_node_exec`` itself.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from nodeflow.workflows.dev_process.argv_builder import resolve_node_exec
from nodeflow.workflows.dev_process.constants import EXEC_TIMEOUT_SECONDS
from nodeflow.workflows.dev_process.evidence import record_exec_evidence
from nodeflow.workflows.dev_process.node_run import NODE_TYPE_PREFIX, NodeRun
from nodeflow.workflows.dev_process.session_store import new_session_id
from nodeflow.workflows.dev_process.workers import resolve_exec_worker, run_exec


def run_node_exec(
    body: Dict[str, Any],
    *,
    node_name: str,
    stage: str,
    prompt: str,
    cwd: str,
    run_id: str,
    artifact_root: str,
    invoker_override: Optional[str] = None,
    argv_override: Optional[List[str]] = None,
    timeout: int = EXEC_TIMEOUT_SECONDS,
) -> Tuple[Dict[str, Any], str, NodeRun]:
    """Execute one node and record it in ``body["node_runs"]``.

    *argv_override*, when given, replaces the argv resolved from
    ``exec_policy_snapshot``.  This is used for test hooks such as
    ``force_blocking`` review argv.

    Returns ``(execution_output, evidence_path, record)``.
    """
    worker_kind, model, argv = resolve_node_exec(body, node_name)
    if argv_override is not None:
        argv = list(argv_override)
    worker = resolve_exec_worker(worker_kind)
    node_runs = body.setdefault("node_runs", [])
    session_id = new_session_id(run_id=run_id, node_name=node_name, index=len(node_runs))
    execution_output = run_exec(worker, prompt=prompt, cwd=cwd, argv=argv, timeout=timeout)
    invoker = invoker_override or worker.invoker
    evidence_path = record_exec_evidence(
        artifact_root=artifact_root,
        run_id=run_id,
        stage=stage,
        invoker=invoker,
        execution_output=execution_output,
        argv=argv,
        prompt=prompt,
        cwd=cwd,
        node_name=node_name,
        session_id=session_id,
        model=model,
        worker=worker_kind,
    )
    node_type = f"{NODE_TYPE_PREFIX}.{node_name}"
    record = NodeRun(
        node_name=node_name,
        node_type=node_type,
        stage=stage,
        worker=worker_kind,
        model=model,
        session_id=session_id,
        evidence_path=evidence_path,
        argv=argv,
    )
    node_runs.append(record.to_dict())
    return execution_output, evidence_path, record
