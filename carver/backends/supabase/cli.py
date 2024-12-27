from . import commands
from . import utils

from .utils import SupabaseClient

from .commands import *

@click.group()
@click.pass_context
def sb(ctx):
    """
    Manage data in Supabase
    """
    ctx.obj['supabase'] = SupabaseClient()

sb.add_command(project)
sb.add_command(source)
sb.add_command(post)
sb.add_command(artifact)
