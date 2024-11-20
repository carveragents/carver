import os
import sys
import json
import traceback

from typing import List, Dict, Optional

import click

from tabulate import tabulate

from .base import BaseArtifactGenerator

thisdir = os.path.dirname(__file__)

@click.group()
def generator():
    """
    View and manage generators
    """
    pass

@generator.command()
@click.option('--name', help='Filter by generator name')
@click.option('--format', 'output_format',
              type=click.Choice(['table', 'grid', 'pipe', 'orgtbl', 'rst', 'mediawiki', 'html']),
              default='table',
              help='Output format')
@click.pass_context
def list(ctx, name: Optional[str], output_format: str):
    """List available artifact generators and their capabilities."""
    try:
        # Get all generator subclasses
        generators = BaseArtifactGenerator.__subclasses__()

        if name:
            generators = [g for g in generators if name.lower() in g.name.lower()]

        if not generators:
            click.echo("No generators found")
            return

        headers = ['Name', 'Description', 'Platforms', 'Source Types', 'Required Config']
        rows = []

        for generator_class in generators:
            info = generator_class.get_info()
            description = info['description'] or 'N/A'
            if len(description) > 20:
                description = description[:20] + "..."

            rows.append([
                info['name'],

                ', '.join(info['supported_platforms']) or 'N/A',
                ', '.join(info['supported_source_types']) or 'N/A',
                ', '.join(info['required_config']) or 'None'
            ])

        click.echo(tabulate(rows, headers=headers, tablefmt=output_format))
        click.echo(f"\nTotal generators: {len(generators)}")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error listing generators: {str(e)}", err=True)

# Add to artifact.py in the content group

@generator.command()
@click.argument('name')
@click.option('--format', 'output_format',
              type=click.Choice(['table', 'grid', 'pipe', 'orgtbl', 'rst', 'mediawiki', 'html']),
              default='rst',
              help='Output format')
@click.pass_context
def show(ctx, name: str, output_format: str):
    """Show detailed information about a specific generator."""
    try:
        # Get all generator subclasses
        generators = BaseArtifactGenerator.__subclasses__()
        generator = next((g for g in generators if g.name.lower() == name.lower()), None)

        if not generator:
            click.echo(f"No generator found with name: {name}")
            return

        info = generator.get_info()

        # Basic Information
        click.echo("\n=== Generator Information ===")
        click.echo(f"Name: {info['name']}")
        click.echo(f"Description: {info['description']}")

        # Supported Platforms
        click.echo("\n=== Supported Platforms ===")
        if info['supported_platforms']:
            for platform in info['supported_platforms']:
                click.echo(f"- {platform}")
        else:
            click.echo("No platform restrictions specified")

        # Supported Source Types
        click.echo("\n=== Supported Source Types ===")
        if info['supported_source_types']:
            for source_type in info['supported_source_types']:
                click.echo(f"- {source_type}")
        else:
            click.echo("No source type restrictions specified")

        # Configuration Requirements
        click.echo("\n=== Required Configuration ===")
        if info['required_config']:
            headers = ['Parameter', 'Description']
            rows = []
            for param in info['required_config']:
                # You could enhance this by adding parameter descriptions to the generator class
                rows.append([param, "Required parameter"])
            click.echo(tabulate(rows, headers=headers, tablefmt=output_format))
        else:
            click.echo("No configuration required")

        # Sample Configuration
        click.echo("\n=== Sample Configuration ===")
        sample_config = {
            param: f"<{param}>" for param in info['required_config']
        }
        click.echo(json.dumps(sample_config, indent=2))

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error showing generator details: {str(e)}", err=True)

@generator.command()
@click.option('--name', required=True, help='Name for the new generator')
@click.option('--output', type=click.Path(), help='Output file path (defaults to stdout)')
@click.pass_context
def sample(ctx, name: str, output: Optional[str]):
    """Generate a sample generator class template."""

    output = open(os.path.join(thisdir, 'sample.py')).read()
    output = output.replace("__NAME__", name.capitalize())
    if output:
        # Write to file
        try:
            with open(output, 'w') as f:
                f.write(sample)
            click.echo(f"Sample generator written to: {output}")
        except Exception as e:
            click.echo(f"Error writing sample generator: {str(e)}", err=True)
    else:
        # Print to stdout
        click.echo(sample)
