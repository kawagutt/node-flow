"""Parse structured JSON review output from CodexExec / CLI execution_result."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Tuple

REVIEW_JSON_CONTRACT_TEXT = """You MUST print exactly one JSON object on stdout (no markdown fences, no extra text). Schema:
{
  "ok": true or false,
  "blocking_findings": [
    {
      "id": "string",
      "area": "diff|spec|tests|other",
      "summary": "string",
      "suggested_fix": "optional string"
    }
  ],
  "non_blocking_findings": [ same shape as blocking_findings ],
  "spec_revision_needed": true or false
}
Use "ok": false when there are blocking issues; list them in "blocking_findings"."""


def _coalesce_text(er: Dict[str, Any]) -> str:
    parts: List[str] = []
    for k in ("stdout", "stderr"):
        v = er.get(k)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())
    return "\n".join(parts)


def _extract_json_object(text: str) -> str | None:
    """First JSON object in text using JSONDecoder.raw_decode (safe inside strings)."""
    if not text or not text.strip():
        return None
    s = text.strip()
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", s, re.IGNORECASE)
    if fence:
        s = fence.group(1).strip()

    decoder = json.JSONDecoder()
    for i, ch in enumerate(s):
        if ch != "{":
            continue
        try:
            _, end = decoder.raw_decode(s[i:])
        except json.JSONDecodeError:
            continue
        return s[i : i + end]
    return None


def parse_review_contract_from_execution_result(er: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """
    Parse LLM review JSON from execution_result.

    Returns (parsed_ok, payload). On parse failure payload is empty dict.
    """
    if not isinstance(er, dict):
        return False, {}
    text = _coalesce_text(er)
    blob = _extract_json_object(text)
    if not blob:
        return False, {}
    try:
        obj = json.loads(blob)
    except json.JSONDecodeError:
        return False, {}
    if not isinstance(obj, dict):
        return False, {}
    return True, obj


def validate_review_contract_payload(obj: Dict[str, Any]) -> bool:
    """Require contract keys and types so partial JSON cannot pass as success."""
    if not isinstance(obj.get("ok"), bool):
        return False
    if not isinstance(obj.get("blocking_findings"), list):
        return False
    if not isinstance(obj.get("non_blocking_findings"), list):
        return False
    if not isinstance(obj.get("spec_revision_needed"), bool):
        return False
    return True
