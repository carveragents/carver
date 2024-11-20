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

sb.add_command(entity)
sb.add_command(source)
sb.add_command(item)
sb.add_command(artifact)
