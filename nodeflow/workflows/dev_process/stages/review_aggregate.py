"""Shared helpers for stage-scoped review JSON (spec_review, plan_review)."""

from __future__ import annotations

import json
from typing import Any, Dict, List

from nodeflow.core.base_node import NodeExecutionFailure

REVIEW_JSON_OUTPUT_CONTRACT = (
    "Respond with a single JSON object:\n"
    "{\n"
    '  "ok": true | false,\n'
    '  "blocking_findings": [{"id": "...", "summary": "..."}],\n'
    '  "non_blocking_findings": [{"id": "...", "summary": "..."}]\n'
    "}"
)


def append_review_json_contract(prompt_text: str) -> str:
    return f"{prompt_text.rstrip()}\n\n{REVIEW_JSON_OUTPUT_CONTRACT}\n"


def parse_review_stdout(stdout: str) -> Dict[str, Any]:
    try:
        parsed = json.loads(stdout.strip())
    except json.JSONDecodeError as e:
        raise NodeExecutionFailure(f"review stdout must be JSON object: {e}") from e
    if not isinstance(parsed, dict):
        raise NodeExecutionFailure("review stdout must be a JSON object")
    return parsed


def aggregate_stage_review(
    execution_output: Dict[str, Any],
    *,
    stage: str,
) -> Dict[str, Any]:
    stdout = str(execution_output.get("stdout") or "")
    payload = parse_review_stdout(stdout)
    blocking: List[Any] = list(payload.get("blocking_findings") or [])
    ok_raw = payload.get("ok", True)
    if not isinstance(ok_raw, bool):
        raise NodeExecutionFailure(f"{stage} review JSON field 'ok' must be boolean")
    ok = ok_raw and not blocking
    decision = "pass" if ok else "fail"
    return {
        "stage": stage,
        "decision": decision,
        "ok": ok,
        "blocking_findings": blocking,
        "non_blocking_findings": list(payload.get("non_blocking_findings") or []),
        "blocking_count": len(blocking),
    }
