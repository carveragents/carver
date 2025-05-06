import os
import sys
import logging

import click


try:
    # Relative import for when run as a module
    from .backends.supabase.cli import sb
    from .generators.cli import generator
except ImportError:
    # Fallback to absolute import for direct script execution
    from backends.supabase.cli import sb
    from generators.cli import generator


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
