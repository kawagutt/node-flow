"""Pipeline loading — YAML 1.5 removed.

These names are **not** exported from :mod:`nodeflow.core` until a real loader exists (Phase 7).
Import from ``nodeflow.core.loader`` only when you need the transitional stubs.

**Next:** ``load_pipeline`` as **v1.6 JSON → executable PipeSpec** (same names) or
``load_json_pipe_spec`` plus narrowed exports; delete ``NotImplementedError`` stubs once shipped.
"""

from __future__ import annotations


class VersionMismatchError(Exception):
    """Raised for removed YAML version checks."""


def load_pipeline(_workspace_dir: str, _file_path: str):
    raise NotImplementedError(
        "YAML 1.5 pipeline loading was removed from nodeflow.core. "
        "Use PipeNode + pipe_spec() (executable PipeSpec); JSON loader in a follow-up phase."
    )


def load_node_pipeline(_file_path: str) -> dict:
    raise NotImplementedError(
        "YAML 1.5 load_node_pipeline was removed. Use executable PipeSpec / JSON loader phase."
    )
