"""Prompt builder for spec.write stage."""

from __future__ import annotations

import json
from typing import Any

REPO_CONTEXT_JSON_MAX_CHARS = 12_000


def build_spec_prompt(
    *,
    task_prompt: str,
    repo_context: dict[str, Any],
    notes: str | None = None,
    revision_context: str | None = None,
    reference_materials: list[dict[str, Any]] | None = None,
    previous_spec: str | None = None,
) -> str:
    prompt_text = (
        "Draft a specification for the following task. "
        'Respond with a single JSON object: {"spec": "..."}.\n\n'
        f"Task:\n{task_prompt}\n\n"
        f"Repository context:\n"
        f"{json.dumps(repo_context, ensure_ascii=False)[:REPO_CONTEXT_JSON_MAX_CHARS]}"
    )
    if previous_spec and previous_spec.strip():
        prompt_text += f"\n\n## Previous spec (revise in place)\n{previous_spec.strip()}"
    if notes and notes.strip():
        prompt_text += f"\n\nAdditional constraints or notes:\n{notes.strip()}"
    if reference_materials:
        lines = ["\n\n## Reference materials"]
        for mat in reference_materials:
            path = mat.get("path", "")
            lines.append(f"\n### {path}")
            if mat.get("text"):
                lines.append(str(mat["text"]))
            elif mat.get("binary_or_unsupported"):
                lines.append("(binary or unsupported — path only)")
        prompt_text += "\n".join(lines)
    if revision_context:
        prompt_text += f"\n\nRevision context:\n{revision_context}"
    return prompt_text
