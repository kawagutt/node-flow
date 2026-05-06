"""Wire-level source reference for port delivery (executable PipeSpec, core Runner)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class SourceRef:
    """Declares where an input port or pipe output reads from (whole-port payloads only).

    Nested field selection is not part of the core model; reshape payloads upstream so each
    delivery remains one dict per port.
    """

    kind: Literal["input", "node"]
    port_name: str
    node_id: str | None = None
