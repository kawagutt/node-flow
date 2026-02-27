"""
NodeFlow v1.2 CLI — kick pipeline execute, display output. Resume is not provided (§7).
"""

import sys
from pathlib import Path

import click

from .runner import load_and_kick_pipeline


@click.command()
@click.argument("pipeline", type=click.Path(exists=True))
@click.option("--workspace", "-w", default=".", help="Workspace directory")
@click.option(
    "--input", "-i", "input_", multiple=True, help="Initial inputs (key=value)"
)
def main(pipeline: str, workspace: str, input_: tuple) -> None:
    """NodeFlow v1.2 — Run pipeline. Resume is program API only."""
    try:
        workspace_dir = str(Path(workspace).resolve())
        initial_inputs = {}
        for item in input_:
            if "=" in item:
                key, value = item.split("=", 1)
                initial_inputs[key] = value
        result = load_and_kick_pipeline(
            workspace_dir, pipeline, initial_inputs=initial_inputs
        )
        click.echo("Pipeline execution completed.")
        click.echo(f"Output: {result}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
