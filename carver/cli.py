import os
import sys

import click

from .backends.supabase.cli import sb
from .generators.cli import generator

@click.group()
@click.pass_context
def cli(ctx):
    """Carver CLI - Manage your content entities, sources, items, and artifacts."""
    ctx.ensure_object(dict)


# Register sub-commands
cli.add_command(sb)
cli.add_command(generator)

if __name__ == '__main__':
    cli()
