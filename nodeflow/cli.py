"""
CLI エントリーポイント
"""
import click
import sys
from pathlib import Path
from .runner import Runner


@click.command()
@click.argument("pipeline", type=click.Path(exists=True))
@click.option("--workspace", "-w", default=".", help="ワークスペースディレクトリ")
@click.option("--input", "-i", multiple=True, help="初期 inputs（key=value 形式）")
@click.option("--flag", "-f", multiple=True, help="flags（key=value 形式）")
def main(pipeline: str, workspace: str, input: tuple, flag: tuple):
    """NodeFlow CLI - Pipeline を実行"""
    try:
        workspace_dir = Path(workspace).resolve()
        runner = Runner(str(workspace_dir))
        
        initial_inputs = {}
        for item in input:
            if "=" in item:
                key, value = item.split("=", 1)
                initial_inputs[key] = value
        
        flags = {}
        for item in flag:
            if "=" in item:
                key, value = item.split("=", 1)
                flags[key] = value
        
        context = runner.run(pipeline, initial_inputs=initial_inputs)
        
        click.echo("Pipeline execution completed successfully.")
        click.echo(f"Final context: {context.snapshot()}")
        
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
