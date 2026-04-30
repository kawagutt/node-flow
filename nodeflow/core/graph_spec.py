"""Immutable description of a PipeNode child graph."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple

from nodeflow.core.base_node import BaseNode

InputBinding = Tuple[str, ...]


@dataclass(frozen=True)
class GraphSpec:
    nodes: Dict[str, BaseNode]
    order: list[str]
    bindings: Dict[str, Dict[str, InputBinding]]
    params: Dict[str, Dict[str, Any]]
    final: str
