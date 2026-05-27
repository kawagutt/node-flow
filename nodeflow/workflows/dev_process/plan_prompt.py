"""Prompt builder for plan.write stage."""

from __future__ import annotations

from typing import Any, Dict

from nodeflow.workflows.dev_process.review_config import (
    KNOWN_PHASE_REVIEW_TARGETS,
    V1_PLAN_PROMPT_AGENTS,
)

_PHASE_FORMAT_RULES = """
## Plan format rules

The plan MUST use the following phase-based markdown format.
Each phase is a self-contained unit of work with its own review cycle.

### Phase heading format

```
## Phase N: <short title>
```

Where N is an integer starting from 1.

The **title** after the colon is display-only. Phase identity in the workflow is
``phase_NNN`` (order-based). ``contract_sha256`` does **not** include the title —
only Goal, Scope, Test plan, Review plan, Review checklist, and Acceptance criteria.

### Required sections per phase

Each phase MUST contain the following required sections in this order:

1. **Goal:** — one-paragraph description of what this phase achieves
2. **Scope:** — bullet list of what is included
3. **Test plan:** — bullet list of test scenarios
4. **Review plan:** — MUST contain exactly two sub-items (see below):
   - targets: what to inspect (scope hint for reviewers)
   - agents: who inspects (which reviewers actually run)
5. **Review checklist:** — bullet list of review criteria
6. **Acceptance criteria:** — bullet list of success criteria

Optional sections (may appear between Scope and Test plan):
- **Excluded:** — bullet list of what is NOT included

### Review plan: targets vs agents

Phase review uses **two different roles** — do not treat ``targets`` as the list of
reviewers to execute:

| Sub-item | Role | Runtime effect |
|----------|------|----------------|
| **targets** | *What to inspect* — which aspects of the change matter for this phase | Appended to the review prompt as a supplement (e.g. ``Review targets: implementation_phase``). Does **not** select which reviewer nodes run. |
| **agents** | *Who inspects* — which reviewer personas run | Determines the actual reviewers executed for this phase (one run per listed agent). |

If **agents** is omitted at runtime, the workflow falls back to the global
``review_depth_preset`` reviewer set — but the plan should always specify **agents**
explicitly for each phase.

**targets** must be chosen from the allowed list below (validation only).
**agents** must be chosen from the allowed agent list below (execution).

### Allowed values for ``targets`` (what to inspect)
{allowed_targets}

### Allowed values for ``agents`` (who inspects)
{allowed_agents}

### Phase granularity rules
- Each phase should be independently implementable and testable.
- Phases are executed sequentially — phase N+1 starts only after phase N passes review.
- Keep phases small enough that review diffs are manageable.
- Order phases so that foundational changes come first.

### Example phase

```markdown
## Phase 1: Add state model

**Goal:**
Add phase tracking to the checkpoint state.

**Scope:**
- Add phase metadata fields to checkpoint.
- Initialize phase_index from plan data.

**Excluded:**
- Do not change review agent behavior yet.

**Test plan:**
- Verify single-phase plans still work.
- Verify multi-phase plans initialize phase_index = 0.

**Review plan:**
- targets: implementation_phase
- agents: architecture, checklist_compliance

**Review checklist:**
- Phase state is persisted in checkpoint.
- Phase advance happens only after review OK.

**Acceptance criteria:**
- Current phase can be restored after resume.
- Phase passes implementation, test, and review independently.
```

In the example **Review plan**, ``targets: implementation_phase`` is *what to inspect*
(prompt supplement only); ``agents: architecture, checklist_compliance`` is *who inspects*
(the reviewers that actually run).
""".format(
    allowed_targets=", ".join(sorted(KNOWN_PHASE_REVIEW_TARGETS)),
    allowed_agents=", ".join(sorted(V1_PLAN_PROMPT_AGENTS)),
)

_COMPLETED_PHASE_PROTECTION = """
### Completed phase protection (plan rework only)

The following phases are already completed and their **contracts** are IMMUTABLE.
You MUST NOT change contract fields in these phases: goal, scope (include/exclude),
test plan, review plan (targets and agents), review checklist, or acceptance criteria.
You may only change phases that come AFTER the completed ones.

Phase **titles** are display-only and are not part of the contract hash — do not
rewrite completed phase bodies, but a title wording tweak alone would not change
the protected contract.

Completed phases (do not modify):
{completed_phases}
"""


_CONTINUATION_INSTRUCTIONS = """
## Continuation plan instructions

The previous phases have already been implemented according to the approved plan.
Do NOT modify or rewrite those completed phase contracts.
The final review found that the implemented result still does not fully satisfy the spec.

Create a **continuation plan** that starts from the current implementation state
and adds only the remaining work needed to satisfy the spec.

- Number your new phases starting from 1 (they will be reindexed automatically).
- Do NOT repeat or reference completed phase contracts.
- The new phases will be appended after the completed ones.
- Implementation will start from the current HEAD (no git reset).

Completed phases (already implemented, do not modify):
{completed_phases}

Final review findings to address:
{findings}
"""


def format_planning_mode_context(dp: dict) -> str:
    """Human-readable planning mode block for plan revision prompts."""
    mode = dp.get("planning_mode", "")
    if mode != "continuation_from_head":
        return ""
    lines = [
        "## Planning mode: continuation_from_head",
        "",
        "This is NOT a normal plan rework that rewrites pending phase contracts.",
        "Completed phases are immutable historical records.",
        "Generate a continuation plan only: append new phases starting from the current HEAD.",
        f"- continuation_count: {dp.get('continuation_count', 0)}",
        f"- continuation_start_phase: {dp.get('continuation_start_phase', '')}",
        f"- current_plan_version: {dp.get('current_plan_version', '')}",
        f"- current_spec_version: {dp.get('current_spec_version', '')}",
    ]
    return "\n".join(lines) + "\n"


def build_continuation_plan_prompt(
    *,
    task_prompt: str,
    approved_spec: str,
    completed_phases: list[Dict[str, Any]],
    findings: list[Dict[str, Any]] | None = None,
    previous_plan: str | None = None,
    parse_error_feedback: str | None = None,
    revision_context: str | None = None,
) -> str:
    """Build prompt for generating a continuation plan after final review NG(plan)."""
    phase_lines = []
    for cp in completed_phases:
        phase_lines.append(
            f"- {cp['id']}: {cp.get('title', '?')} (contract_sha256: {cp['contract_sha256']})"
        )

    findings_lines = []
    for f in findings or []:
        desc = f.get("description") or f.get("summary") or str(f)
        findings_lines.append(f"- {desc}")

    prompt_text = (
        "Create a continuation plan to address remaining work after final review. "
        'Respond with a single JSON object: {"plan": "..."}.\n'
        + _PHASE_FORMAT_RULES
        + _CONTINUATION_INSTRUCTIONS.format(
            completed_phases="\n".join(phase_lines) or "(none)",
            findings="\n".join(findings_lines) or "(none)",
        )
        + f"\nTask:\n{task_prompt}\n\n"
        f"## Approved spec\n{approved_spec}\n"
    )
    if previous_plan and previous_plan.strip():
        prompt_text += f"\n\n## Original plan (for reference only)\n{previous_plan.strip()}"
    if parse_error_feedback:
        prompt_text += (
            f"\n\n## Parse error from previous attempt (fix these issues)\n{parse_error_feedback}"
        )
    if revision_context:
        prompt_text += f"\n\nRevision context:\n{revision_context}"
    return prompt_text


def build_plan_prompt(
    *,
    task_prompt: str,
    approved_spec: str,
    revision_context: str | None = None,
    previous_plan: str | None = None,
    completed_phases: list[Dict[str, Any]] | None = None,
    parse_error_feedback: str | None = None,
) -> str:
    prompt_text = (
        "Draft an implementation plan from the approved specification. "
        'Respond with a single JSON object: {"plan": "..."}.\n'
        + _PHASE_FORMAT_RULES
        + f"\nTask:\n{task_prompt}\n\n"
        f"## Approved spec\n{approved_spec}\n"
    )
    if completed_phases:
        phase_lines = []
        for cp in completed_phases:
            phase_lines.append(
                f"- {cp['id']}: {cp.get('title', '?')} (contract_sha256: {cp['contract_sha256']})"
            )
        prompt_text += _COMPLETED_PHASE_PROTECTION.format(completed_phases="\n".join(phase_lines))
    if previous_plan and previous_plan.strip():
        prompt_text += f"\n\n## Previous plan (revise in place)\n{previous_plan.strip()}"
    if parse_error_feedback:
        prompt_text += (
            f"\n\n## Parse error from previous attempt (fix these issues)\n{parse_error_feedback}"
        )
    if revision_context:
        prompt_text += f"\n\nRevision context:\n{revision_context}"
    return prompt_text
