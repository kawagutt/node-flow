"""PythonSummarizeResultNode — consumes execution_output port; emits summary + passthrough."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict, List

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import PythonActionNode


class PythonSummarizeResultNode(PythonActionNode):
    role = "summarize_result"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        eo = inputs.get("execution_output")
        if not isinstance(eo, dict) or not eo:
            return {}

        stdout = eo.get("stdout")
        stderr = eo.get("stderr")
        summary_line = eo.get("summary")
        raw = eo.get("raw_output")
        ok = eo.get("ok")

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

        summary_payload = {
            "short": short,
            "key_findings": key_findings,
            "next_hint": eo.get("next_hint"),
        }
        eo_out = dict(eo) if isinstance(eo, dict) else {}
        return {
            "summary": summary_payload,
            "execution_output": eo_out,
        }
