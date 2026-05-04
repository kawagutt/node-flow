"""NodeFlow CLI — compatibility stub after legacy YAML graph pipeline removal.

Public v1.6 wiring is **JSON PipeSpec** only: use :func:`nodeflow.core.loader.load_pipeline`
with a ``*.json`` file, or construct a :class:`~nodeflow.core.node_kinds.PipeNode` and call
:meth:`~nodeflow.core.base_node.BaseNode.execute`. This CLI still calls
:func:`nodeflow.core.run.load_and_kick_pipeline`, which **always raises** ``NotImplementedError``.
"""

import sys
from pathlib import Path

import click

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.core.run import load_and_kick_pipeline


@click.command()
@click.argument("pipeline", type=click.Path(exists=False))
@click.option("--workspace", "-w", default=".", help="Workspace directory")
@click.option("--input", "-i", "input_", multiple=True, help="Initial inputs (key=value)")
def main(pipeline: str, workspace: str, input_: tuple) -> None:
    """Removed entrypoint: does not load YAML or JSON. Always delegates to the removed YAML kick.

    For v1.6, load a ``*.json`` PipeSpec via ``nodeflow.core.loader.load_pipeline`` or use a
    ``PipeNode`` programmatically (see ``doc/nodeflow_spec.md``). The ``pipeline`` argument is
    retained only for historical CLI shape.
    """
    try:
        workspace_dir = str(Path(workspace).resolve())
        initial_inputs = {}
        for item in input_:
            if "=" in item:
                key, value = item.split("=", 1)
                initial_inputs[key] = value
        result = load_and_kick_pipeline(workspace_dir, pipeline, initial_inputs=initial_inputs)
        click.echo("Pipeline execution completed.")
        click.echo(f"Output: {result}")
    except NodeExecutionFailure as e:
        raise click.ClickException(str(e)) from e
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
