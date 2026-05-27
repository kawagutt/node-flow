"""Node execution record types for dev-process runs.

A *Node* is a processing unit in the dev-process graph (e.g. ``write_spec``,
``review_spec``).  A *NodeRun* is the record of one execution of that node.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

NODE_TYPE_PREFIX = "dev_process"


@dataclass(frozen=True)
class NodeRun:
    node_name: str
    node_type: str
    stage: str
    kind: str  # "llm" | "local" | "aggregate" | "skipped"
    worker: str
    model: Optional[str]
    session_id: Optional[str]
    evidence_path: str
    argv: List[str]
    skipped: bool = False
    skip_reason: Optional[str] = None
    constraints: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
