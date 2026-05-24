"""NodeFlow CLI — run v1.7 JSON PipeSpec pipelines and named pipes."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.core.run import load_and_kick_pipeline
from nodeflow.pipes.dispatch import dispatch_named_pipe, has_named_pipe


def _parse_cli_value(value: str) -> Any:
    s = value.strip()
    if s.startswith("[") or s.startswith("{"):
        try:
            return json.loads(s)
        except json.JSONDecodeError as e:
            raise click.ClickException(f"invalid JSON value {value!r}: {e}") from e
    return value


def _parse_kv_pairs(items: tuple[str, ...]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in items:
        if "=" not in item:
            raise click.ClickException(f"expected key=value, got {item!r}")
        key, value = item.split("=", 1)
        out[key.strip()] = _parse_cli_value(value)
    return out


@click.command()
@click.argument("pipeline", type=click.Path(exists=False))
@click.option(
    "--workspace",
    "-w",
    default=".",
    help="Workspace directory (resolves relative paths in PipeSpec)",
)
@click.option("--input", "-i", "input_", multiple=True, help="Pipe input port (key=value)")
def pipeline_main(pipeline: str, workspace: str, input_: tuple) -> None:
    """Run a v1.7 JSON PipeSpec (``*.json``) to completion and print pipe outputs."""
    try:
        workspace_dir = str(Path(workspace).resolve())
        initial_inputs = _parse_kv_pairs(input_)
        result = load_and_kick_pipeline(workspace_dir, pipeline, initial_inputs=initial_inputs)
        click.echo("Pipeline execution completed.")
        click.echo(json.dumps(result, indent=2, ensure_ascii=False))
    except NodeExecutionFailure as e:
        raise click.ClickException(str(e)) from e
    except NotImplementedError as e:
        raise click.ClickException(str(e)) from e
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def main(args: list[str] | None = None) -> int:
    """Console script entrypoint: named pipe dispatch or JSON PipeSpec execution."""
    argv = list(sys.argv[1:] if args is None else args)
    try:
        if has_named_pipe(argv):
            return dispatch_named_pipe(argv)
        pipeline_main.main(args=argv, standalone_mode=False)
    except click.ClickException as e:
        click.echo(f"Error: {e.message}", err=True)
        return 1
    except SystemExit as exc:
        code = exc.code
        if code is None:
            return 0
        if isinstance(code, int):
            return code
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
