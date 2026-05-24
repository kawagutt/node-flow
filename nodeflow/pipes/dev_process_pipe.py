"""Named pipe handler for ``nodeflow --pipe dev-process``."""

from __future__ import annotations

from typing import List

from nodeflow.pipes.registry import NamedPipeEntry


def _invoke_dev_process(argv: List[str]) -> int:
    from nodeflow.workflows.dev_process.cli import main as dev_process_main

    try:
        dev_process_main.main(args=argv, prog_name="nodeflow", standalone_mode=True)
    except SystemExit as exc:
        code = exc.code
        if code is None:
            return 0
        if isinstance(code, int):
            return code
        return 1
    return 0


def dev_process_entry() -> NamedPipeEntry:
    return NamedPipeEntry(name="dev-process", invoke=_invoke_dev_process)
