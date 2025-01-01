import os
import sys
import json
import traceback

from typing import Optional, List, Dict, Any
from datetime import datetime
from pathlib import Path

import click

from tabulate import tabulate

from ..utils.helpers import *
from ..utils import get_spec_config
from carver.utils import format_datetime, parse_date_filter

thisdir = os.path.dirname(__file__)

####################################
# Template manager
####################################
@click.group()
@click.pass_context
def template(ctx):
    """Manage templates"""
    pass

@template.command('list')
@click.option('--show-content', is_flag=True, help='Show detailed template contents')
@click.pass_context
def list_templates(ctx, show_content):
    """List available specification templates."""
    templates_dir = Path(__file__).parent.parent / "templates"

    if not templates_dir.exists():
        click.echo("Templates directory not found")
        return

    template_files = list(templates_dir.glob("*.json"))
    template_files.extend(templates_dir.glob("*.py"))

    if not template_files:
        click.echo("No template files found")
        return

    table_data = []
    headers = ["Model", "Name", "Description"]

    for file_path in template_files:
        try:
            template = get_spec_config(str(file_path), raw=True)
            name = file_path.stem
            model = name.split('_')[0] if '_' in name else ''
            template_name = '_'.join(name.split('_')[1:]) if '_' in name else name
            table_data.append([
                model,
                template_name,
                template.get('description', 'No description')
            ])

        except Exception as e:
            click.echo(f"Error loading template {file_path}: {str(e)}", err=True)
            continue

    click.echo(tabulate(table_data, headers=headers, tablefmt="grid"))

@template.command('show')
@click.argument('template_name')
@click.pass_context
def show_template(ctx, template_name):
    """Display full content of a specific template."""
    try:
        template = load_template(template_name, raw=True)

        if 'specifications' in template:
            click.echo("\nDependency Tree:")
            tree = format_dependency_tree(template['specifications'])
            click.echo("\n".join(tree))
            click.echo("\nFull Template Configuration:")

        click.echo(json.dumps(template, indent=2))
    except Exception as e:
        click.echo(f"Error loading template: {str(e)}", err=True)

@template.command('init')
@click.pass_context
def init_template(ctx):
    """Create a sample template file that can be customized."""

    sample = '''
system_prompt = """
Extract key entities and relationships from the following transcript to construct a knowledge graph. Focus on:
{{var1}}
{{var2}}

Return the structured data in the exact format shown in the knowledge graph visualization.
"""

def get_config(raw: bool = False):

    return {
        "name": "Sample Generator",
        "description": "Generate Samples from Content",
        "platforms": ["*", "YOUTUBE"],
        "specifications": [
            {
                "id": 2001,
                "name": "Technical Knowledge Graph",
                "description": "Extract technical concepts and relationships",
                "config": {
                    "generator": "knowledge_graph",
                    "dependencies": ["Transcription"],
                    "system_prompt": system_prompt,
                    "max_triplets_per_chunk": 20,
                    "include_embeddings": True,
                    "min_confidence": 0.6,
                }
            }
        ]
    }
    '''

    print(sample)

