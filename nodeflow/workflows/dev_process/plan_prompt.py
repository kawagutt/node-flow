"""Prompt builder for plan.write stage."""

from __future__ import annotations


def build_plan_prompt(
    *,
    task_prompt: str,
    approved_spec: str,
    revision_context: str | None = None,
    previous_plan: str | None = None,
) -> str:
    prompt_text = (
        "Draft an implementation plan from the approved specification. "
        'Respond with a single JSON object: {"plan": "..."}.\n\n'
        f"Task:\n{task_prompt}\n\n"
        f"## Approved spec\n{approved_spec}\n"
    )
    if previous_plan and previous_plan.strip():
        prompt_text += f"\n\n## Previous plan (revise in place)\n{previous_plan.strip()}"
    if revision_context:
        prompt_text += f"\n\nRevision context:\n{revision_context}"
    return prompt_text
