"""Dispatch ``nodeflow --pipe <name>`` to registered handlers."""

from __future__ import annotations

import sys
from typing import List

import click

from nodeflow.pipes.registry import get_named_pipe, list_named_pipes


def has_named_pipe(argv: List[str]) -> bool:
    return "--pipe" in argv


def _split_pipe_argv(argv: List[str]) -> tuple[str, List[str]]:
    """Return (pipe_name, remaining_argv) after ``--pipe NAME``."""
    out: List[str] = []
    i = 0
    pipe_name: str | None = None
    while i < len(argv):
        if argv[i] == "--pipe":
            if i + 1 >= len(argv):
                raise click.ClickException("--pipe requires a pipe name")
            pipe_name = argv[i + 1]
            i += 2
            continue
        out.append(argv[i])
        i += 1
    if pipe_name is None:
        raise click.ClickException("--pipe requires a pipe name")
    return pipe_name, out


def dispatch_named_pipe(argv: List[str]) -> int:
    pipe_name, rest = _split_pipe_argv(argv)
    entry = get_named_pipe(pipe_name)
    if entry is None:
        known = ", ".join(list_named_pipes()) or "(none)"
        raise click.ClickException(f"unknown pipe {pipe_name!r}; known pipes: {known}")
    return entry.invoke(rest)


def main_dispatch_from_sys_argv() -> int:
    try:
        return dispatch_named_pipe(sys.argv[1:])
    except click.ClickException as e:
        click.echo(f"Error: {e.message}", err=True)
        return 1
