"""Pluggable exec workers for dev_process stages (P4)."""

from __future__ import annotations

from typing import Any, Dict, Optional, Protocol, runtime_checkable

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.nodes.exec.codex_exec import CodexExecNode
from nodeflow.workflows.dev_process.constants import EXEC_WORKER_CODEX


@runtime_checkable
class ExecWorker(Protocol):
    kind: str
    invoker: str

    def run(
        self,
        *,
        prompt: str,
        cwd: str,
        argv: list[str],
        timeout: int = 120,
    ) -> Dict[str, Any]:
        """Return execution_output dict; raise NodeExecutionFailure when not ok."""


class CodexExecWorker:
    kind = EXEC_WORKER_CODEX
    invoker = "codex_exec"

    def run(
        self,
        *,
        prompt: str,
        cwd: str,
        argv: list[str],
        timeout: int = 120,
    ) -> Dict[str, Any]:
        out = CodexExecNode().execute(
            {"prompt": prompt},
            {"argv": argv, "timeout": timeout, "cwd": cwd},
        )
        execution_output = out.get("execution_output") or {}
        if not execution_output.get("ok"):
            raise NodeExecutionFailure(
                f"codex_exec failed: {execution_output.get('stderr') or execution_output}"
            )
        return execution_output


def resolve_exec_worker(kind: Optional[str]) -> ExecWorker:
    name = (kind or EXEC_WORKER_CODEX).strip() or EXEC_WORKER_CODEX
    if name == EXEC_WORKER_CODEX:
        return CodexExecWorker()
    raise NodeExecutionFailure(f"unsupported exec_worker kind: {name!r}")


def run_exec(
    worker: ExecWorker,
    *,
    prompt: str,
    cwd: str,
    argv: list[str],
    timeout: int = 120,
) -> Dict[str, Any]:
    return worker.run(prompt=prompt, cwd=cwd, argv=argv, timeout=timeout)
