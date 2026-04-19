"""PythonSummarizeResultNode — Part V §7.1 (input: execution_result port)."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict, List

from nodeflow.core.base_node import ExecutionContext
from nodeflow.nodes.base.python_action import PythonActionNode


class PythonSummarizeResultNode(PythonActionNode):
    role = "summarize_result"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        er = inputs.get("execution_result")
        if not isinstance(er, dict):
            er = {}

        stdout = er.get("stdout")
        stderr = er.get("stderr")
        summary_line = er.get("summary")
        raw = er.get("raw_response")
        ok = er.get("ok")

        text_parts: List[str] = []
        if isinstance(stdout, str) and stdout.strip():
            text_parts.append(stdout.strip()[:2000])
        if isinstance(stderr, str) and stderr.strip():
            text_parts.append("stderr: " + stderr.strip()[:500])
        if summary_line:
            text_parts.append(str(summary_line))
        if raw is not None and not text_parts:
            text_parts.append(str(raw)[:2000])

        blob = "\n".join(text_parts) if text_parts else "(no textual output)"
        short = blob[:280] + ("…" if len(blob) > 280 else "")
        key_findings: List[str] = []
        if ok is False:
            key_findings.append("execution reported ok=false")
        if isinstance(stderr, str) and stderr.strip():
            key_findings.append("non-empty stderr")
        if not key_findings:
            key_findings.append("completed")

        return {
            "summary": {
                "short": short,
                "key_findings": key_findings,
                "next_hint": er.get("next_hint"),
            }
        }
