"""Plan phase parser: extract phases from plan markdown, produce plan.json.

Phase identity is ``phase_NNN`` (derived from order), not the markdown heading title.
``contract_sha256`` fingerprints the **executable contract** only:

- goal, scope (include/exclude), test plan, review plan (targets/agents),
  review checklist, acceptance criteria

The phase **title** (``## Phase N: <title>``) is display-only and is **not** part of
``contract_sha256``. Completed-phase protection in plan rework compares
``contract_sha256`` only — renaming a completed phase title does not invalidate it.

After plan rework, ``phase_results[phase_id].title`` for **completed** phases keeps the
**historical execution title** (not updated from the new plan). Status / CLI display
may differ from ``plan.md`` for completed phases until a full re-run.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List


class PlanParseError(Exception):
    """Raised when plan markdown cannot be parsed into valid phases."""


@dataclass(frozen=True)
class PlanPhase:
    index: int
    id: str
    title: str
    goal: str
    scope_include: list[str]
    scope_exclude: list[str]
    test_plan: list[str]
    review_targets: list[str]
    review_agents: list[str]
    review_checklist: list[str]
    acceptance_criteria: list[str]
    contract_sha256: str
    source_heading: str


@dataclass(frozen=True)
class PlanData:
    phases: list[PlanPhase]
    raw_text: str
    plan_sha256: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_sha256": self.plan_sha256,
            "total_phases": len(self.phases),
            "phases": [_phase_to_dict(p) for p in self.phases],
        }


def _phase_to_dict(p: PlanPhase) -> Dict[str, Any]:
    return {
        "index": p.index,
        "id": p.id,
        "title": p.title,
        "goal": p.goal,
        "scope_include": p.scope_include,
        "scope_exclude": p.scope_exclude,
        "test_plan": p.test_plan,
        "review_targets": p.review_targets,
        "review_agents": p.review_agents,
        "review_checklist": p.review_checklist,
        "acceptance_criteria": p.acceptance_criteria,
        "contract_sha256": p.contract_sha256,
        "source_heading": p.source_heading,
    }


def _compute_contract_sha256(phase: Dict[str, Any]) -> str:
    """Hash the phase contract fields (goal/scope/tests/review/acceptance).

    Excludes ``title`` and ``source_heading`` — those are display/metadata only.
    Used by ``validate_rework_contracts()`` to detect changes to completed phases.
    """
    # review_targets / review_agents are sorted so contract hash is order-independent
    # (reviewer run order is not part of the executable contract).
    payload = {
        "goal": phase["goal"].strip(),
        "scope_include": [x.strip() for x in phase["scope_include"]],
        "scope_exclude": [x.strip() for x in phase["scope_exclude"]],
        "test_plan": [x.strip() for x in phase["test_plan"]],
        "review_targets": sorted(phase["review_targets"]),
        "review_agents": sorted(phase["review_agents"]),
        "review_checklist": [x.strip() for x in phase["review_checklist"]],
        "acceptance_criteria": [x.strip() for x in phase["acceptance_criteria"]],
    }
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


_PHASE_HEADING_RE = re.compile(r"^##\s+Phase\s+(\d+)\s*:\s*(.+)$", re.MULTILINE)

_REQUIRED_SECTIONS = frozenset(
    {"Goal", "Scope", "Test plan", "Review plan", "Review checklist", "Acceptance criteria"}
)

_SECTION_RE = re.compile(r"^\*\*(.+?)\*\*\s*$", re.MULTILINE)


def renumber_continuation_headings(text: str, *, start_index: int) -> str:
    """Rewrite ``## Phase N:`` headings for display in merged plan.

    LLM continuation plans use Phase 1, 2, ...; map them to 1-based human phase
    numbers starting at ``start_index + 1`` (e.g. start_index=3 → Phase 4, 5, ...).
    """

    def repl(match: re.Match[str]) -> str:
        llm_num = int(match.group(1))
        display_num = start_index + (llm_num - 1) + 1
        return f"## Phase {display_num}: {match.group(2)}"

    return _PHASE_HEADING_RE.sub(repl, text)


def _split_phase_blocks(text: str) -> List[tuple[str, str, str]]:
    """Split plan text into (number, title, body) tuples per phase heading."""
    matches = list(_PHASE_HEADING_RE.finditer(text))
    if not matches:
        raise PlanParseError("No '## Phase N: ...' headings found in plan")
    blocks: List[tuple[str, str, str]] = []
    for i, m in enumerate(matches):
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        blocks.append((m.group(1), m.group(2).strip(), text[start:end]))
    return blocks


def _parse_bullet_list(text: str) -> list[str]:
    items: list[str] = []
    for line in text.strip().splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            items.append(stripped[2:].strip())
        elif stripped.startswith("* "):
            items.append(stripped[2:].strip())
    return items


def _extract_sections(body: str) -> Dict[str, str]:
    """Extract **Section:** blocks from a phase body."""
    parts = _SECTION_RE.split(body)
    sections: Dict[str, str] = {}
    i = 1
    while i < len(parts) - 1:
        key = parts[i].rstrip(":").strip()
        val = parts[i + 1]
        sections[key] = val
        i += 2
    return sections


def _normalize_phase_review_target(target: str) -> str:
    """Canonicalize phase review target names (``test`` → ``test_phase``)."""
    if target == "test":
        return "test_phase"
    return target


def _parse_review_plan(text: str) -> tuple[list[str], list[str]]:
    """Parse review plan section into (targets, agents)."""
    targets: list[str] = []
    agents: list[str] = []
    for line in text.strip().splitlines():
        stripped = line.strip()
        if stripped.lower().startswith("- targets:") or stripped.lower().startswith("- target:"):
            raw = stripped.split(":", 1)[1].strip()
            targets = [
                _normalize_phase_review_target(t.strip()) for t in raw.split(",") if t.strip()
            ]
        elif stripped.lower().startswith("- agents:") or stripped.lower().startswith("- agent:"):
            raw = stripped.split(":", 1)[1].strip()
            agents = [a.strip() for a in raw.split(",") if a.strip()]
    return targets, agents


def _validate_review_config(
    targets: list[str],
    agents: list[str],
    phase_heading: str,
    *,
    allowed_targets: frozenset[str],
    allowed_agents: frozenset[str],
) -> None:
    for t in targets:
        if t not in allowed_targets:
            raise PlanParseError(
                f"{phase_heading}: unknown review target {t!r}; "
                f"allowed: {sorted(allowed_targets)}"
            )
    for a in agents:
        if a not in allowed_agents:
            raise PlanParseError(
                f"{phase_heading}: unknown review agent {a!r}; "
                f"allowed: {sorted(allowed_agents)}"
            )
    if len(targets) != len(set(targets)):
        raise PlanParseError(f"{phase_heading}: duplicate review targets: {targets!r}")
    if len(agents) != len(set(agents)):
        raise PlanParseError(f"{phase_heading}: duplicate review agents: {agents!r}")


def _parse_single_phase(
    index: int,
    title: str,
    body: str,
    heading: str,
    *,
    allowed_targets: frozenset[str],
    allowed_agents: frozenset[str],
) -> PlanPhase:
    sections = _extract_sections(body)

    for req in _REQUIRED_SECTIONS:
        found = False
        for key in sections:
            if key.lower() == req.lower():
                found = True
                break
        if not found:
            raise PlanParseError(f"{heading}: missing required section '**{req}:**'")

    def _get(name: str) -> str:
        for key, val in sections.items():
            if key.lower() == name.lower():
                return val
        return ""

    goal_text = _get("Goal").strip()
    if not goal_text:
        raise PlanParseError(f"{heading}: **Goal:** must not be empty")

    scope_items = _parse_bullet_list(_get("Scope"))
    if not scope_items:
        raise PlanParseError(f"{heading}: **Scope:** must contain at least one bullet item")
    excluded_items = _parse_bullet_list(_get("Excluded"))
    test_plan_items = _parse_bullet_list(_get("Test plan"))
    if not test_plan_items:
        raise PlanParseError(f"{heading}: **Test plan:** must contain at least one bullet item")
    review_checklist_items = _parse_bullet_list(_get("Review checklist"))
    if not review_checklist_items:
        raise PlanParseError(
            f"{heading}: **Review checklist:** must contain at least one bullet item"
        )
    acceptance_criteria_items = _parse_bullet_list(_get("Acceptance criteria"))
    if not acceptance_criteria_items:
        raise PlanParseError(
            f"{heading}: **Acceptance criteria:** must contain at least one bullet item"
        )

    review_text = _get("Review plan")
    targets, agents_list = _parse_review_plan(review_text)
    if not targets:
        raise PlanParseError(f"{heading}: **Review plan:** must include non-empty targets")
    if not agents_list:
        raise PlanParseError(f"{heading}: **Review plan:** must include non-empty agents")
    _validate_review_config(
        targets,
        agents_list,
        heading,
        allowed_targets=allowed_targets,
        allowed_agents=allowed_agents,
    )

    phase_id = f"phase_{index:03d}"
    phase_dict = {
        "goal": goal_text,
        "scope_include": scope_items,
        "scope_exclude": excluded_items,
        "test_plan": test_plan_items,
        "review_targets": targets,
        "review_agents": agents_list,
        "review_checklist": review_checklist_items,
        "acceptance_criteria": acceptance_criteria_items,
    }
    contract = _compute_contract_sha256(phase_dict)

    return PlanPhase(
        index=index,
        id=phase_id,
        title=title,
        goal=goal_text,
        scope_include=scope_items,
        scope_exclude=excluded_items,
        test_plan=test_plan_items,
        review_targets=targets,
        review_agents=agents_list,
        review_checklist=review_checklist_items,
        acceptance_criteria=acceptance_criteria_items,
        contract_sha256=contract,
        source_heading=heading,
    )


def parse_new_plan(text: str) -> PlanData:
    """Parse a new phase-formatted plan. Strict: requires ## Phase headings."""
    from nodeflow.workflows.dev_process.review_config import (
        KNOWN_PHASE_REVIEW_TARGETS,
        KNOWN_REVIEW_AGENTS,
    )

    blocks = _split_phase_blocks(text)
    phases: list[PlanPhase] = []
    for i, (num, title, body) in enumerate(blocks):
        expected_num = i + 1
        try:
            actual_num = int(num)
        except ValueError:
            raise PlanParseError(f"Phase heading number must be an integer, got {num!r}")
        if actual_num != expected_num:
            raise PlanParseError(
                f"Phase heading numbers must be sequential starting from 1: "
                f"expected {expected_num}, got {actual_num}"
            )
        heading = f"## Phase {num}: {title}"
        phase = _parse_single_phase(
            index=i,
            title=title,
            body=body,
            heading=heading,
            allowed_targets=KNOWN_PHASE_REVIEW_TARGETS,
            allowed_agents=KNOWN_REVIEW_AGENTS,
        )
        phases.append(phase)

    if not phases:
        raise PlanParseError("No phases found in plan")

    plan_sha = hashlib.sha256(text.encode()).hexdigest()
    return PlanData(phases=phases, raw_text=text, plan_sha256=plan_sha)


def parse_continuation_plan(text: str, *, start_index: int) -> PlanData:
    """Parse a continuation plan, reindexing phases from ``start_index``.

    The LLM generates headings as ``## Phase 1:``, ``## Phase 2:``, etc.
    These are reindexed to ``phase_{start_index:03d}``, ``phase_{start_index+1:03d}``, etc.
    """
    from nodeflow.workflows.dev_process.review_config import (
        KNOWN_PHASE_REVIEW_TARGETS,
        KNOWN_REVIEW_AGENTS,
    )

    blocks = _split_phase_blocks(text)
    phases: list[PlanPhase] = []
    for i, (num, title, body) in enumerate(blocks):
        expected_num = i + 1
        try:
            actual_num = int(num)
        except ValueError:
            raise PlanParseError(f"Phase heading number must be an integer, got {num!r}")
        if actual_num != expected_num:
            raise PlanParseError(
                f"Continuation phase heading numbers must be sequential starting from 1: "
                f"expected {expected_num}, got {actual_num}"
            )
        real_index = start_index + i
        heading = f"## Phase {num}: {title}"
        phase = _parse_single_phase(
            index=real_index,
            title=title,
            body=body,
            heading=heading,
            allowed_targets=KNOWN_PHASE_REVIEW_TARGETS,
            allowed_agents=KNOWN_REVIEW_AGENTS,
        )
        phases.append(phase)

    if not phases:
        raise PlanParseError("No phases found in continuation plan")

    plan_sha = hashlib.sha256(text.encode()).hexdigest()
    return PlanData(phases=phases, raw_text=text, plan_sha256=plan_sha)


OLD_NON_PHASE_PLAN_MESSAGE = (
    "This checkpoint has an old non-phase plan format. "
    "Please regenerate the plan with revise-plan or restart the dev_process run."
)


def assert_strict_phase_plan(plan_data: PlanData, *, plan_text: str = "") -> None:
    """Reject legacy or non-phase plans when resuming from checkpoint artifacts."""
    from nodeflow.core.base_node import NodeExecutionFailure

    del plan_text  # validated at write time via parse_new_plan; JSON is the resume SOT

    if not plan_data.phases:
        raise NodeExecutionFailure(OLD_NON_PHASE_PLAN_MESSAGE)
    if any(p.source_heading == "(legacy)" for p in plan_data.phases):
        raise NodeExecutionFailure(OLD_NON_PHASE_PLAN_MESSAGE)
    for phase in plan_data.phases:
        if (
            not phase.scope_include
            or not phase.test_plan
            or not phase.review_checklist
            or not phase.acceptance_criteria
        ):
            raise NodeExecutionFailure(OLD_NON_PHASE_PLAN_MESSAGE)


def save_plan_json(plan_data: PlanData, plan_dir: str) -> str:
    """Write plan.json and return its path."""
    from pathlib import Path

    out_dir = Path(plan_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "plan.json"
    path.write_text(
        json.dumps(plan_data.to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return str(path)
