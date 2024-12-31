import os
import sys
import logging

import click

from .backends.supabase.cli import sb
from .generators.cli import generator

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

@click.group()
@click.pass_context
def cli(ctx):
    """Carver CLI - Manage your content projects, sources, items, and artifacts."""
    ctx.ensure_object(dict)


# Register sub-commands
cli.add_command(sb)
cli.add_command(generator)

if __name__ == '__main__':
    cli()
