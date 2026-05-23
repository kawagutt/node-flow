"""NodeFlow CLI — run v1.7 JSON PipeSpec pipelines."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.core.run import load_and_kick_pipeline


def _parse_kv_pairs(items: tuple[str, ...]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise click.ClickException(f"expected key=value, got {item!r}")
        key, value = item.split("=", 1)
        out[key.strip()] = value
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
def main(pipeline: str, workspace: str, input_: tuple) -> None:
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


if __name__ == "__main__":
    main()
