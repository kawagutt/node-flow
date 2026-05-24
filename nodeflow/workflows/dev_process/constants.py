"""dev-process flow constants."""

from __future__ import annotations

SCHEMA_VERSION = "dev_process.flow.v1"

STATE_INITIALIZED = "initialized"
STATE_AWAITING_SPEC = "awaiting_spec_approval"
STATE_AWAITING_REVIEW = "awaiting_review_decision"
# Post-approve_final: human approved; merge is the remaining gate.
STATE_AWAITING_FINAL = "awaiting_merge"

STATE_MERGED = "merged"
STATE_FAILED = "failed"

ACTION_START = "start"
ACTION_APPROVE_SPEC = "approve_spec"
ACTION_REVISE_SPEC = "revise_spec"
ACTION_REWORK = "rework_implementation"
ACTION_MERGE = "merge"
ACTION_APPROVE_FINAL = "approve_final"
ACTION_REJECT_SPEC = "reject_spec"
ACTION_REJECT_FINAL = "reject_final"

TERMINAL_STATES = frozenset({STATE_MERGED, STATE_FAILED})

WORKSPACE_STRATEGY_CURRENT_REPO = "current_repo"
WORKSPACE_STRATEGY_GIT_WORKTREE = "git_worktree"

EXEC_WORKER_CODEX = "codex"
