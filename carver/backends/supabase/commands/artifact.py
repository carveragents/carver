import os
import sys
import json
import traceback

from typing import Optional, List, Dict
from datetime import datetime
from pathlib import Path

import click

from tabulate import tabulate

from ..utils.helpers import topological_sort
from ..utils import format_datetime, parse_date_filter, get_spec_config
from .artifact_manager import ArtifactManager

thisdir = os.path.dirname(__file__)

####################################
# Artifact Commands
####################################
@click.group()
@click.pass_context
def artifact(ctx):
    """Manage artifacts and specifications in the system."""
    ctx.obj['manager'] = ArtifactManager(ctx.obj['supabase'])

@artifact.command()
@click.option('--spec-id', required=True, type=int, help='Specification ID')
@click.option('--posts', required=True, help='Comma-separated list of post IDs')
@click.option('--generator-name', required=True, help='Generator name to use')
@click.pass_context
def generate(ctx, spec_id: int, posts: str, generator_name: str):
    """Generate artifacts for specified posts using a specification."""
    manager = ctx.obj['manager']
    try:
        post_ids = [int(i.strip()) for i in posts.split(',')]
        results = manager.artifact_bulk_create_from_spec(spec_id, post_ids, generator_name)
        click.echo(f"Successfully generated {len(results)} artifacts")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error generating artifacts: {str(e)}", err=True)

@artifact.command()
@click.option('--spec-id', required=False, type=int, help='Specification ID')
@click.option('--source-id', required=False, type=int, help='Source ID')
@click.option('--last', type=str, help='Filter posts by time (e.g. "1d", "2h", "30m")')
@click.option('--generator-name', required=False, help='Generator name to use')
@click.option('--offset', default=0, type=int, help='Offset for search results')
@click.option('--limit', default=50, type=int, help='Maximum number of posts to fetch')
@click.pass_context
def bulk_generate(ctx, spec_id: int, source_id: int, last: Optional[str], generator_name: str, offset: int, limit: int):
    """Bulk generate artifacts for posts from a source that don't have active artifacts."""
    manager = ctx.obj['manager']
    db = ctx.obj['supabase']

    try:
        if not spec_id and not source_id:
            click.echo("Specify one of spec_id or source_id", err=True)
            return

        specs = db.specification_search(source_id=source_id,
                                        spec_id=spec_id,
                                        active=True)
        if not specs:
            click.echo(f"No active specifications found")
            return

        if not source_id:
            source_id = specs[0]['source_id']

        # Get posts needing artifacts
        time_filter = parse_date_filter(last) if last else None
        posts = db.post_search_with_artifacts(
            source_id=source_id,
            modified_after=time_filter,
            limit=limit,
            offset=offset,
        )

        if not posts:
            click.echo("No posts found requiring artifact generation")
            return

        click.echo(f"Found {len(posts)} posts to process for artifact generation")

        # Process each specification
        for spec in specs:
            if ((generator_name is not None) and
                (spec['config'].get('generator') != generator_name)):
                continue

            try:
                results = manager.artifact_bulk_create_from_spec(spec, posts, generator_name)
                click.echo(f"Successfully generated {len(results)} artifacts using specification {spec['id']}")
            except Exception as e:
                traceback.print_exc()
                click.echo(f"Error generating artifacts for spec {spec['id']}: {str(e)}", err=True)

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error during bulk generation: {str(e)}", err=True)


@artifact.command()
@click.option('--spec-id', type=int, help='Filter by specification ID')
@click.option('--post-id', type=int, help='Filter by post ID')
@click.option('--artifact-type', help='Filter by artifact type')
@click.option('--status', help='Filter by status')
@click.option('--active/--inactive', default=True, help='Filter by active status')
@click.option('--last', type=str, help='Filter by time window (e.g. "1d", "2h", "30m")')
@click.option('--offset', default=0, type=int, help='Offset for search results')
@click.option('--limit', default=50, type=int, help='Maximum number of posts to fetch')
@click.option('--format', 'output_format',
              type=click.Choice(['table', 'grid', 'pipe', 'orgtbl', 'rst', 'mediawiki', 'html']),
              default='table',
              help='Output format for display')
@click.option('--dump', 'dump_format',
              type=click.Choice(['text', 'csv', 'json']),
              help='Dump results to file in specified format')
@click.option('--output', '-o', type=click.Path(), help='Output file path for dump')
@click.pass_context
def search(ctx, spec_id: Optional[int], post_id: Optional[int],
           artifact_type: Optional[str], status: Optional[str],
           last: Optional[str], offset: int, limit: int,
           active: Optional[bool], output_format: str,
           dump_format: Optional[str], output: Optional[str]):
    """Search artifacts with optional data dump."""
    db = ctx.obj['supabase']
    try:
        # Parse time window if provided
        time_filter = parse_date_filter(last) if last else None

        artifacts = db.artifact_search(
            spec_id=spec_id,
            post_id=post_id,
            artifact_type=artifact_type,
            status=status,
            active=active,
            modified_after=time_filter,
            offset=offset,
            limit=limit
        )

        if not artifacts:
            click.echo("No artifacts found")
            return

        # Prepare data for display and dump
        rows = []
        for art in artifacts:
            row = {
                'id': art['id'],
                'specification': art['carver_artifact_specification']['name'],
                'post_id': art['post_id'],
                'post_name': art['carver_post']['name'],
                'post_description': art['carver_post']['description'],
                'post_url': art['carver_post']['url'],
                'artifact_type': art['artifact_type'],
                'generator': f"{art['generator_name']}:{art['generator_id']}",
                'title': art['title'],
                'content': art['content'],
                'status': art['status'],
                'version': art['version'],
                'embedding': "Y" if art['content_embedding'] is not None else "N",
                'active': art['active'],
                'created_at': format_datetime(art['created_at'])
            }
            rows.append(row)

        # Display results in table format
        if not dump_format:
            click.echo("Notes: V=version, A=Active, Type=Artifact Type, E=Embedding")
            table_rows = [[
                r['id'],
                r['specification'],
                r['post_id'],
                r['artifact_type'],
                r['generator'],
                r['title'][:20] + ('...' if len(r['title']) > 20 else ''),
                r['status'],
                r['version'],
                r['embedding'],
                '✓' if r['active'] else '✗',
                r['created_at']
            ] for r in rows]

            headers = ['ID', 'Spec', "PostID", 'Type', "Generator:ID",
                       'Title', 'Status', 'V', 'E','A', 'Created']

            click.echo(tabulate(table_rows, headers=headers, tablefmt=output_format))
            click.echo(f"\nTotal artifacts: {len(artifacts)}")
            return

        # Handle data dump
        output_file = output or f"artifacts_dump_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        if dump_format == 'json':
            output_file = f"{output_file}.json" if not output else output
            with open(output_file, 'w') as f:
                json.dump(rows, f, indent=2)

        elif dump_format == 'csv':
            import csv
            output_file = f"{output_file}.csv" if not output else output
            with open(output_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

        elif dump_format == 'text':
            output_file = f"{output_file}.txt" if not output else output
            marker = "==================================="
            header = f"\n\n\n{marker}\nArtifact\n{marker}\n\n"
            with open(output_file, 'w') as f:
                for row in rows:
                    f.write(header)
                    for key, value in row.items():
                        label = key.replace("_", " ").title()
                        if len(str(value)) > 50:
                            newline = "\n"
                            lines = str(value).split("\n")
                            value = ""
                            for l in lines:
                                value += "    " + l + "\n"
                        else:
                            newline = " "
                        f.write(f"[{label.upper()[:20]:15}] {newline}{value}\n")
                    f.write("\n")

        click.echo(f"Successfully dumped {len(artifacts)} artifacts to {output_file}")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@artifact.command('update-status')
@click.option('--spec-id', required=True, type=int, help='Specification ID')
@click.option('--artifacts', required=True, help='Comma-separated list of artifact IDs')
@click.option('--status', type=click.Choice(['draft', 'in_review', 'published', 'archived', 'failed']),
              required=True, help='New status')
@click.pass_context
def update_status(ctx, spec_id: int, artifacts: str, status: str):
    """Update status for multiple artifacts."""
    manager = ctx.obj['manager']
    try:
        artifact_ids = [int(i.strip()) for i in artifacts.split(',')]
        updated = manager.artifact_bulk_status_update(spec_id, artifact_ids, status)
        click.echo(f"Updated status for {len(updated)} artifacts")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error updating status: {str(e)}", err=True)

@artifact.command()
@click.option('--spec-id', required=True, type=int, help='Specification ID')
@click.option('--artifacts', required=False, help='Comma-separated list of artifact IDs')
@click.pass_context
def activate(ctx, spec_id: int, artifacts: str):
    """Activate multiple artifacts."""
    manager = ctx.obj['manager']
    try:
        if artifacts:
            artifacts = [int(i.strip()) for i in artifacts.split(',')]
        updated = manager.artifact_bulk_activate(spec_id, artifacts)
        click.echo(f"Activated {len(updated)} artifacts")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error activating artifacts: {str(e)}", err=True)

@artifact.command()
@click.option('--spec-id', required=True, type=int, help='Specification ID')
@click.option('--artifacts', required=False, help='Comma-separated list of artifact IDs')
@click.pass_context
def deactivate(ctx, spec_id: int, artifacts: str):
    """Deactivate multiple artifacts."""
    manager = ctx.obj['manager']
    try:

        if artifacts is not None:
            artifacts = [int(i.strip()) for i in artifacts.split(',')]
        updated = manager.artifact_bulk_deactivate(spec_id, artifacts)
        click.echo(f"Deactivated {len(updated)} artifacts")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error deactivating artifacts: {str(e)}", err=True)
