"""dev-process flow constants."""

from __future__ import annotations

SCHEMA_VERSION = "dev_process.flow.v2"

# Legacy v1 (tests referencing old names may import these aliases)
STATE_AWAITING_SPEC = "awaiting_spec_human_gate"

STATE_INITIALIZED = "initialized"
STATE_AWAITING_SPEC_REVISION = "awaiting_spec_revision"
STATE_AWAITING_SPEC_HUMAN_GATE = "awaiting_spec_human_gate"
STATE_AWAITING_PLAN_REVISION = "awaiting_plan_revision"
STATE_AWAITING_IMPLEMENTATION = "awaiting_implementation"
STATE_AWAITING_IMPLEMENTATION_REWORK = "awaiting_implementation_rework"
STATE_AWAITING_TEST_REWORK = "awaiting_test_rework"
STATE_AWAITING_REWORK_DECISION = "awaiting_rework_decision"
STATE_AWAITING_REVIEW = "awaiting_rework_decision"  # alias for finalize helpers
STATE_AWAITING_FINAL = "awaiting_final_approval"  # review merge_ok; human approve_final pending
STATE_AWAITING_MERGE = "awaiting_merge"  # after approve_final; merge pending

STATE_MERGED = "merged"
STATE_FAILED = "failed"

ACTION_START = "start"
ACTION_REVISE_SPEC = "revise_spec"
ACTION_REQUEST_SPEC_REVISION = "request_spec_revision"
ACTION_APPROVE_SPEC = "approve_spec"
ACTION_REVISE_PLAN = "revise_plan"
ACTION_CONTINUE_IMPLEMENTATION = "continue_implementation"
ACTION_REWORK = "rework_implementation"
ACTION_MERGE = "merge"
ACTION_APPROVE_FINAL = "approve_final"
ACTION_REJECT_SPEC = "reject_spec"
ACTION_REJECT_FINAL = "reject_final"

TERMINAL_STATES = frozenset({STATE_MERGED, STATE_FAILED})

WORKSPACE_STRATEGY_CURRENT_REPO = "current_repo"
WORKSPACE_STRATEGY_GIT_WORKTREE = "git_worktree"

EXEC_WORKER_CODEX = "codex"

EXEC_TIMEOUT_SECONDS = 300

MERGE_POLICY_RECORD_ONLY = "record_only"
MERGE_POLICY_GIT_MERGE_BRANCH = "git_merge_branch"

# Stale downstream keys per upstream stage
STALE_DOWNSTREAM: dict[str, tuple[str, ...]] = {
    "spec": ("plan", "plan_review", "implementation", "test_implementation", "review"),
    "plan": ("plan_review", "implementation", "test_implementation", "review"),
    "implementation": ("test_implementation", "review"),
    "test_implementation": ("review",),
}

# v2 checkpoint stage keys (no spec_plan / implement)
V2_CHECKPOINT_STAGES: tuple[str, ...] = (
    "spec",
    "spec_review",
    "plan",
    "plan_review",
    "implementation",
    "test_implementation",
    "run_tests",
    "review",
)

MERGE_GATE_STAGES: tuple[str, ...] = V2_CHECKPOINT_STAGES

EVIDENCE_STAGE_KEYS: tuple[str, ...] = (
    "spec",
    "spec_review",
    "plan",
    "plan_review",
    "implementation",
    "test_implementation",
    "review",
)
