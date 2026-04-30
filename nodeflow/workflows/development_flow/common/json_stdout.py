"""Extract a single JSON object from CLI stdout (markdown fences, extra text)."""

from __future__ import annotations

import json
import re
from typing import Any


def extract_first_json_object(text: str) -> str | None:
    """First top-level JSON object using JSONDecoder.raw_decode (safe inside strings)."""
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


def loads_first_json_object(text: str) -> Any | None:
    blob = extract_first_json_object(text)
    if not blob:
        return None
    try:
        return json.loads(blob)
    except json.JSONDecodeError:
        return None
