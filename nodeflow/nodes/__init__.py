"""Reusable concrete building-block nodes (routing, exec, summarize, …).

Built-in registry wiring lives in the package-root ``builtins`` module (loaded by ``import nodeflow``).
This subpackage intentionally does **not** trigger registration on import.
"""

from __future__ import annotations
