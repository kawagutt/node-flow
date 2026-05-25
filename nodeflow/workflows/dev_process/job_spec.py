"""Job record types for dev-process runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class JobRecord:
    job_key: str
    stage: str
    worker: str
    model: Optional[str]
    session_id: str
    evidence_path: str
    argv: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
