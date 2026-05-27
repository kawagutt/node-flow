"""Tests for plan_phases parser and review_config."""

from __future__ import annotations

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.plan_phases import (
    PlanData,
    PlanParseError,
    assert_strict_phase_plan,
    parse_continuation_plan,
    parse_new_plan,
    renumber_continuation_headings,
)
from nodeflow.workflows.dev_process.review_config import (
    KNOWN_FINAL_REVIEW_TARGETS,
    KNOWN_PHASE_REVIEW_TARGETS,
    KNOWN_REVIEW_AGENTS,
    KNOWN_REVIEW_TARGETS,
)


def _make_phase_md(
    num: int = 1,
    title: str = "Add feature X",
    goal: str = "Implement feature X",
    scope: str = "- Add module X.\n- Wire it up.",
    excluded: str = "",
    test_plan: str = "- Unit tests for X.",
    review_targets: str = "implementation_phase",
    review_agents: str = "architecture, checklist_compliance",
    review_checklist: str = "- Code is clean.",
    acceptance_criteria: str = "- Feature works end to end.",
) -> str:
    lines = [
        f"## Phase {num}: {title}",
        "",
        "**Goal:**",
        goal,
        "",
        "**Scope:**",
        scope,
        "",
    ]
    if excluded:
        lines += ["**Excluded:**", excluded, ""]
    else:
        lines += ["**Excluded:**", "- Nothing.", ""]
    lines += [
        "**Test plan:**",
        test_plan,
        "",
        "**Review plan:**",
        f"- targets: {review_targets}",
        f"- agents: {review_agents}",
        "",
        "**Review checklist:**",
        review_checklist,
        "",
        "**Acceptance criteria:**",
        acceptance_criteria,
    ]
    return "\n".join(lines)


def _multi_phase_md(count: int = 3) -> str:
    parts = []
    for i in range(1, count + 1):
        parts.append(
            _make_phase_md(
                num=i,
                title=f"Phase {i} title",
                goal=f"Goal for phase {i}",
            )
        )
    return "\n\n".join(parts)


class TestParseNewPlan:
    def test_single_phase_success(self) -> None:
        md = _make_phase_md()
        data = parse_new_plan(md)
        assert len(data.phases) == 1
        p = data.phases[0]
        assert p.index == 0
        assert p.id == "phase_000"
        assert p.title == "Add feature X"
        assert p.goal == "Implement feature X"
        assert p.scope_include == ["Add module X.", "Wire it up."]
        assert "architecture" in p.review_agents
        assert p.contract_sha256

    def test_multi_phase_success(self) -> None:
        md = _multi_phase_md(3)
        data = parse_new_plan(md)
        assert len(data.phases) == 3
        for i, p in enumerate(data.phases):
            assert p.index == i
            assert p.id == f"phase_{i:03d}"
            assert p.title == f"Phase {i + 1} title"

    def test_phase_id_independent_of_title(self) -> None:
        md = _multi_phase_md(2)
        data = parse_new_plan(md)
        assert data.phases[0].id == "phase_000"
        assert data.phases[1].id == "phase_001"

    def test_contract_sha256_excludes_title(self) -> None:
        md_a = _make_phase_md(title="Title A")
        md_b = _make_phase_md(title="Title B")
        data_a = parse_new_plan(md_a)
        data_b = parse_new_plan(md_b)
        assert data_a.phases[0].contract_sha256 == data_b.phases[0].contract_sha256

    def test_duplicate_review_agents_rejected(self) -> None:
        md = _make_phase_md(review_agents="architecture, architecture")
        with pytest.raises(PlanParseError, match="duplicate review agents"):
            parse_new_plan(md)

    def test_duplicate_review_targets_rejected(self) -> None:
        md = _make_phase_md().replace(
            "implementation_phase",
            "implementation_phase, implementation_phase",
        )
        with pytest.raises(PlanParseError, match="duplicate review targets"):
            parse_new_plan(md)

    def test_contract_sha256_review_order_invariant(self) -> None:
        md_a = _make_phase_md(review_agents="architecture, checklist_compliance")
        md_b = _make_phase_md(review_agents="checklist_compliance, architecture")
        data_a = parse_new_plan(md_a)
        data_b = parse_new_plan(md_b)
        assert data_a.phases[0].contract_sha256 == data_b.phases[0].contract_sha256

    def test_missing_required_section_raises(self) -> None:
        md = "## Phase 1: X\n\n**Goal:**\nDo X\n"
        with pytest.raises(PlanParseError, match="missing required section"):
            parse_new_plan(md)

    def test_no_phase_headings_raises(self) -> None:
        md = "# Plan\n\nJust some text without phases."
        with pytest.raises(PlanParseError, match="No.*Phase.*headings"):
            parse_new_plan(md)

    def test_unknown_review_agent_raises(self) -> None:
        md = _make_phase_md(review_agents="unknown_agent")
        with pytest.raises(PlanParseError, match="unknown review agent"):
            parse_new_plan(md)

    def test_unknown_review_target_raises(self) -> None:
        md = _make_phase_md(review_targets="unknown_target")
        with pytest.raises(PlanParseError, match="unknown review target"):
            parse_new_plan(md)

    def test_final_diff_in_phase_review_raises(self) -> None:
        md = _make_phase_md(review_targets="final_diff")
        with pytest.raises(PlanParseError, match="unknown review target"):
            parse_new_plan(md)

    def test_plan_data_to_dict(self) -> None:
        md = _multi_phase_md(2)
        data = parse_new_plan(md)
        d = data.to_dict()
        assert d["total_phases"] == 2
        assert len(d["phases"]) == 2
        assert d["phases"][0]["id"] == "phase_000"
        assert "plan_sha256" in d

    def test_empty_goal_raises(self) -> None:
        md = _make_phase_md(goal="")
        with pytest.raises(PlanParseError, match="Goal.*must not be empty"):
            parse_new_plan(md)

    def test_empty_scope_raises(self) -> None:
        md = _make_phase_md(scope="")
        with pytest.raises(PlanParseError, match="Scope.*must contain"):
            parse_new_plan(md)

    def test_empty_test_plan_raises(self) -> None:
        md = _make_phase_md(test_plan="")
        with pytest.raises(PlanParseError, match="Test plan.*must contain"):
            parse_new_plan(md)

    def test_empty_review_checklist_raises(self) -> None:
        md = _make_phase_md(review_checklist="")
        with pytest.raises(PlanParseError, match="Review checklist.*must contain"):
            parse_new_plan(md)

    def test_empty_acceptance_criteria_raises(self) -> None:
        md = _make_phase_md(acceptance_criteria="")
        with pytest.raises(PlanParseError, match="Acceptance criteria.*must contain"):
            parse_new_plan(md)

    def test_empty_review_targets_raises(self) -> None:
        md = _make_phase_md(review_targets="")
        with pytest.raises(PlanParseError, match="Review plan.*must include non-empty targets"):
            parse_new_plan(md)

    def test_empty_review_agents_raises(self) -> None:
        md = _make_phase_md(review_agents="")
        with pytest.raises(PlanParseError, match="Review plan.*must include non-empty agents"):
            parse_new_plan(md)


class TestStrictPhasePlanOnly:
    def test_empty_phases_rejected(self) -> None:
        data = PlanData(phases=[], raw_text="Implement everything.", plan_sha256="x")
        with pytest.raises(NodeExecutionFailure, match="old non-phase plan format"):
            assert_strict_phase_plan(data)

    def test_weak_legacy_contract_rejected(self) -> None:
        from nodeflow.workflows.dev_process.plan_phases import PlanPhase

        data = PlanData(
            phases=[
                PlanPhase(
                    index=0,
                    id="phase_000",
                    title="T",
                    goal="g",
                    scope_include=[],
                    scope_exclude=[],
                    test_plan=[],
                    review_targets=["implementation_phase"],
                    review_agents=["architecture"],
                    review_checklist=[],
                    acceptance_criteria=[],
                    contract_sha256="sha",
                    source_heading="## Phase 1: T",
                )
            ],
            raw_text="",
            plan_sha256="x",
        )
        with pytest.raises(NodeExecutionFailure, match="old non-phase plan format"):
            assert_strict_phase_plan(data)

    def test_legacy_source_heading_rejected(self) -> None:
        from nodeflow.workflows.dev_process.plan_phases import PlanPhase

        data = PlanData(
            phases=[
                PlanPhase(
                    index=0,
                    id="phase_000",
                    title="T",
                    goal="g",
                    scope_include=["s"],
                    scope_exclude=[],
                    test_plan=["t"],
                    review_targets=["implementation_phase"],
                    review_agents=["architecture"],
                    review_checklist=["c"],
                    acceptance_criteria=["a"],
                    contract_sha256="sha",
                    source_heading="(legacy)",
                )
            ],
            raw_text="",
            plan_sha256="x",
        )
        with pytest.raises(NodeExecutionFailure, match="old non-phase plan format"):
            assert_strict_phase_plan(data)


class TestRenumberContinuationHeadings:
    def test_renumbers_from_start_index(self) -> None:
        md = _make_phase_md(1, title="A") + "\n" + _make_phase_md(2, title="B")
        out = renumber_continuation_headings(md, start_index=3)
        assert "## Phase 4: A" in out
        assert "## Phase 5: B" in out
        assert "## Phase 1:" not in out

    def test_merged_plan_has_no_duplicate_phase_one(self) -> None:
        existing = "## Phase 1: Orig\n\n**Goal:**\ng\n"
        cont = _make_phase_md(1, title="Add validation")
        display = renumber_continuation_headings(cont, start_index=3)
        merged = existing.rstrip() + "\n\n---\n\n## Continuation plan\n\n" + display
        assert merged.count("## Phase 1:") == 1


class TestParseContinuationPlan:
    def test_reindexes_from_start_index(self) -> None:
        md = _make_phase_md(num=1, title="Validation") + "\n" + _make_phase_md(num=2, title="Tests")
        data = parse_continuation_plan(md, start_index=3)
        assert len(data.phases) == 2
        assert data.phases[0].id == "phase_003"
        assert data.phases[0].index == 3
        assert data.phases[0].title == "Validation"
        assert data.phases[1].id == "phase_004"
        assert data.phases[1].index == 4
        assert data.phases[1].title == "Tests"

    def test_single_phase_continuation(self) -> None:
        md = _make_phase_md(num=1, title="Fix issue")
        data = parse_continuation_plan(md, start_index=5)
        assert len(data.phases) == 1
        assert data.phases[0].id == "phase_005"

    def test_no_headings_raises(self) -> None:
        with pytest.raises(PlanParseError, match="No.*Phase.*headings"):
            parse_continuation_plan("No phases here", start_index=0)

    def test_non_sequential_headings_raise(self) -> None:
        md = _make_phase_md(num=2, title="Skipped")
        with pytest.raises(PlanParseError, match="sequential"):
            parse_continuation_plan(md, start_index=0)

    def test_contract_sha256_is_computed(self) -> None:
        md = _make_phase_md(num=1, title="A")
        data = parse_continuation_plan(md, start_index=0)
        assert data.phases[0].contract_sha256
        assert len(data.phases[0].contract_sha256) == 64


class TestReviewTargetNormalization:
    def test_old_test_target_normalized_to_test_phase(self) -> None:
        md = _make_phase_md(review_targets="test")
        data = parse_new_plan(md)
        assert data.phases[0].review_targets == ["test_phase"]


class TestReviewConfig:
    def test_phase_targets_subset_of_known(self) -> None:
        assert KNOWN_PHASE_REVIEW_TARGETS <= KNOWN_REVIEW_TARGETS

    def test_final_targets_subset_of_known(self) -> None:
        assert KNOWN_FINAL_REVIEW_TARGETS <= KNOWN_REVIEW_TARGETS

    def test_no_overlap_phase_final(self) -> None:
        assert not (KNOWN_PHASE_REVIEW_TARGETS & KNOWN_FINAL_REVIEW_TARGETS)

    def test_agents_are_nonempty(self) -> None:
        assert len(KNOWN_REVIEW_AGENTS) > 0
