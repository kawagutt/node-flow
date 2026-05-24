"""Registry of named pipes for ``nodeflow --pipe <name>``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List


@dataclass(frozen=True)
class NamedPipeEntry:
    """Handler invoked with remaining CLI argv (after ``--pipe <name>``)."""

    name: str
    invoke: Callable[[List[str]], int]


_REGISTRY: Dict[str, NamedPipeEntry] = {}


def register_named_pipe(entry: NamedPipeEntry, *, override: bool = False) -> None:
    if entry.name in _REGISTRY and not override:
        raise ValueError(f"named pipe {entry.name!r} already registered")
    _REGISTRY[entry.name] = entry


def get_named_pipe(name: str) -> NamedPipeEntry | None:
    return _REGISTRY.get(name)


def list_named_pipes() -> List[str]:
    return sorted(_REGISTRY.keys())


def _register_builtins() -> None:
    from nodeflow.pipes.dev_process_pipe import dev_process_entry

    register_named_pipe(dev_process_entry(), override=True)


_register_builtins()
