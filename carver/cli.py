import os
import sys

import click

from .backends import sb

@click.group()
@click.pass_context
def cli(ctx):
    """Carver CLI - Manage your content entities, sources, items, and artifacts."""
    ctx.ensure_object(dict)


# Register sub-commands
cli.add_command(sb)

if __name__ == '__main__':
    cli()
