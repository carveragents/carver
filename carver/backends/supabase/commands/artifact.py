import os
import sys
import json
import traceback

from typing import Optional, List, Dict
from datetime import datetime

import click

from tabulate import tabulate

from ..utils import format_datetime, parse_date_filter
from .artifact_manager import ArtifactManager

@click.group()
def artifact():
    """Manage artifacts and specifications in the system."""
    pass

####################################
# Artifact Specification Commands
####################################
@artifact.group()
@click.pass_context
def spec(ctx):
    """Manage artifact specifications."""
    ctx.obj['manager'] = ArtifactManager(ctx.obj['supabase'])

@spec.command()
@click.option('--source-id', required=True, type=int, help='Source ID')
@click.option('--name', required=True, help='Specification name')
@click.option('--description', help='Specification description')
@click.option('--config', required=True, type=click.Path(exists=True), help='Path to JSON config file')
@click.pass_context
def add(ctx, source_id: int, name: str, description: Optional[str], config: str):
    """Add a new artifact specification."""
    manager = ctx.obj['manager']
    try:
        with open(config, 'r') as f:
            config_data = json.load(f)

        db = ctx.obj['supabase']
        source = db.source_get(source_id)
        if not source:
            click.echo(f"Source with ID {source_id} not found", err=True)
            return

        spec = manager.specification_create(source, {
            'source_id': source_id,
            'name': name,
            'description': description,
            'config': config_data
        })
        click.echo(f"Created specification {spec['id']}: {spec['name']}")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error creating specification: {str(e)}", err=True)

@spec.command()
@click.option('--source-id', type=int, help='Filter by source ID')
@click.option('--name', help='Filter by name (partial match)')
@click.option('--active/--inactive', default=None, help='Filter by active status')
@click.option('--format', 'output_format',
              type=click.Choice(['table', 'grid', 'pipe', 'orgtbl', 'rst', 'mediawiki', 'html']),
              default='table',
              help='Output format')
@click.pass_context
def search(ctx, source_id: Optional[int], name: Optional[str],
           active: Optional[bool], output_format: str):
    """Search artifact specifications."""
    db = ctx.obj['supabase']
    try:
        specs = db.specification_search(
            source_id=source_id,
            name=name,
            active=active
        )

        if specs:
            headers = ['ID', 'Source', 'Name', 'Generator', 'Active', 'Updated']
            rows = []
            for spec in specs:
                source_name = spec['carver_source']['name'] if spec.get('carver_source') else 'N/A'
                rows.append([
                    spec['id'],
                    f"{source_name[:20]} ({spec['source_id']})",
                    spec['name'],
                    spec['config'].get('generator'),
                    '✓' if spec['active'] else '✗',
                    format_datetime(spec['updated_at'])
                ])

            click.echo(tabulate(rows, headers=headers, tablefmt=output_format))
            click.echo(f"\nTotal specifications: {len(specs)}")
        else:
            click.echo("No specifications found")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@spec.command()
@click.argument('spec_id', type=int)
@click.pass_context
def show(ctx, spec_id: int):
    """Show detailed information about a specification."""
    db = ctx.obj['supabase']
    try:
        spec = db.specification_get(spec_id)
        if not spec:
            click.echo(f"Specification {spec_id} not found", err=True)
            return

        click.echo("\n=== Specification Information ===")
        click.echo(f"ID: {spec['id']}")
        click.echo(f"Name: {spec['name']}")
        click.echo(f"Description: {spec.get('description', 'N/A')}")
        click.echo(f"Source ID: {spec['source_id']}")
        click.echo(f"Active: {'Yes' if spec['active'] else 'No'}")
        click.echo(f"Created: {format_datetime(spec['created_at'])}")
        click.echo(f"Updated: {format_datetime(spec['updated_at'])}")

        click.echo("\n=== Configuration ===")
        click.echo(json.dumps(spec['config'], indent=2))
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@spec.command()
@click.argument('spec_id', type=int)
@click.option('--source-id', type=int, help='Source ID')
@click.option('--name', help='New specification name')
@click.option('--description', help='New specification description')
@click.option('--config', type=click.Path(exists=True), help='Path to new JSON config file')
@click.option('--active/--inactive', default=None, type=bool, help='Update active status')
@click.pass_context
def update(ctx, spec_id: int, source_id: Optional[int],
           name: Optional[str], description: Optional[str],
           config: Optional[str], active: Optional[bool]):
    """Update an existing artifact specification."""

    db = ctx.obj['supabase']
    manager = ctx.obj['manager']

    try:
        update_data = {}

        if source_id:
            db = ctx.obj['supabase']
            source = db.source_get(source_id)
            if not source:
                click.echo(f"Source with ID {source_id} not found", err=True)
                return
            update_data['source_id'] = source['id']

        # Build update data from provided options
        if name is not None:
            update_data['name'] = name
        if description is not None:
            update_data['description'] = description
        if active is not None:
            update_data['active'] = active
        if config:
            with open(config, 'r') as f:
                config_data = json.load(f)
            update_data['config'] = config_data

        if not update_data:
            click.echo("No updates provided")
            return

        spec = manager.specification_update(spec_id, update_data)
        click.echo(f"Updated specification {spec['id']}: {spec['name']}")

        # Show updated specification
        click.echo("\nUpdated Specification:")
        click.echo("======================")
        click.echo(f"ID: {spec['id']}")
        click.echo(f"Name: {spec['name']}")
        click.echo(f"Description: {spec.get('description', 'N/A')}")
        click.echo(f"Active: {'Yes' if spec['active'] else 'No'}")
        if config:
            click.echo("\nUpdated Configuration:")
            click.echo(json.dumps(spec['config'], indent=2))

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error updating specification: {str(e)}", err=True)

@spec.command()
@click.argument('specs')
@click.pass_context
def activate(ctx, specs: str):
    """
    Activate one or more specifications.

    Args:
       specs (str): Comma-separated list of specification IDs
    """
    db = ctx.obj['supabase']
    try:
        # Parse specification IDs
        spec_ids = [int(s.strip()) for s in specs.split(',')]

        # Validate specifications exist
        for spec_id in spec_ids:
            spec = db.specification_get(spec_id)
            if not spec:
                click.echo(f"Warning: Specification {spec_id} not found", err=True)
                continue

        # Prepare update data
        update_data = {
            'active': True,
            'updated_at': datetime.utcnow().isoformat()
        }

        # Update each specification
        updated = []
        for spec_id in spec_ids:
            try:
                result = db.specification_update(spec_id, update_data)
                if result:
                    updated.append(result)
            except Exception as e:
                click.echo(f"Error activating specification {spec_id}: {str(e)}", err=True)
                continue

        if updated:
            click.echo(f"Successfully activated {len(updated)} specifications:")
            for spec in updated:
                click.echo(f"- {spec['id']}: {spec['name']}")
        else:
            click.echo("No specifications were activated")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@spec.command()
@click.argument('specs')
@click.pass_context
def deactivate(ctx, specs: str):
    """
    Deactivate one or more specifications.

    Args:
       specs (str): Comma-separated list of specification IDs
    """
    db = ctx.obj['supabase']
    try:
        # Parse specification IDs
        spec_ids = [int(s.strip()) for s in specs.split(',')]

        # Validate specifications exist
        for spec_id in spec_ids:
            spec = db.specification_get(spec_id)
            if not spec:
                click.echo(f"Warning: Specification {spec_id} not found", err=True)
                continue

        # Prepare update data
        update_data = {
            'active': False,
            'updated_at': datetime.utcnow().isoformat()
        }

        # Update each specification
        updated = []
        for spec_id in spec_ids:
            try:
                result = db.specification_update(spec_id, update_data)
                if result:
                    updated.append(result)
            except Exception as e:
                click.echo(f"Error deactivating specification {spec_id}: {str(e)}", err=True)
                continue

        if updated:
            click.echo(f"Successfully deactivated {len(updated)} specifications:")
            for spec in updated:
                click.echo(f"- {spec['id']}: {spec['name']}")
        else:
            click.echo("No specifications were deactivated")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

####################################
# Artifact Commands
####################################
@artifact.group()
@click.pass_context
def content(ctx):
    """Manage content artifacts."""
    ctx.obj['manager'] = ArtifactManager(ctx.obj['supabase'])

@content.command()
@click.option('--spec-id', required=True, type=int, help='Specification ID')
@click.option('--items', required=True, help='Comma-separated list of item IDs')
@click.option('--generator-name', required=True, help='Generator name to use')
@click.pass_context
def generate(ctx, spec_id: int, items: str, generator_name: str):
    """Generate artifacts for specified items using a specification."""
    manager = ctx.obj['manager']
    try:
        item_ids = [int(i.strip()) for i in items.split(',')]
        results = manager.artifact_bulk_create_from_spec(spec_id, item_ids, generator_name)
        click.echo(f"Successfully generated {len(results)} artifacts")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error generating artifacts: {str(e)}", err=True)

@content.command()
@click.option('--spec-id', required=False, type=int, help='Specification ID')
@click.option('--source-id', required=False, type=int, help='Source ID')
@click.option('--last', type=str, help='Filter items by time (e.g. "1d", "2h", "30m")')
@click.option('--generator-name', required=True, help='Generator name to use')
@click.pass_context
def bulk_generate(ctx, spec_id: int, source_id: int, last: Optional[str], generator_name: str):
    """Bulk generate artifacts for items from a source that don't have active artifacts."""
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

        # Get items needing artifacts
        time_filter = parse_date_filter(last) if last else None
        items = db.item_search_with_artifacts(
            source_id=source_id,
            modified_after=time_filter,
            limit=1000,
        )

        if not items:
            click.echo("No items found requiring artifact generation")
            return

        click.echo(f"Found {len(items)} items to process for artifact generation")

        # Process each specification
        for spec in specs:
            if spec['config'].get('generator') != generator_name:
                continue

            try:
                results = manager.artifact_bulk_create_from_spec(spec, items, generator_name)
                click.echo(f"Generated {len(results)} artifacts using specification {spec['id']}")
            except Exception as e:
                traceback.print_exc()
                click.echo(f"Error generating artifacts for spec {spec['id']}: {str(e)}", err=True)

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error during bulk generation: {str(e)}", err=True)


@content.command()
@click.option('--spec-id', type=int, help='Filter by specification ID')
@click.option('--item-id', type=int, help='Filter by item ID')
@click.option('--artifact-type', help='Filter by artifact type')
@click.option('--status', help='Filter by status')
@click.option('--active/--inactive', default=None, help='Filter by active status')
@click.option('--offset', default=0, type=int, help='Offset for search results')
@click.option('--limit', default=50, type=int, help='Maximum number of items to fetch')
@click.option('--format', 'output_format',
              type=click.Choice(['table', 'grid', 'pipe', 'orgtbl', 'rst', 'mediawiki', 'html']),
              default='table',
              help='Output format')
@click.pass_context
def search(ctx, spec_id: Optional[int], item_id: Optional[int],
           artifact_type: Optional[str], status: Optional[str],
           offset: int, limit: int,
           active: Optional[bool], output_format: str):
    """Search artifacts."""
    db = ctx.obj['supabase']
    try:
        artifacts = db.artifact_search(
            spec_id=spec_id,
            item_id=item_id,
            artifact_type=artifact_type,
            status=status,
            active=active,
            offset=offset,
            limit=limit
        )

        if artifacts:
            click.echo("Notes: V=version, A=Active, Type=Artifact Type")
            headers = ['ID', 'SourceID', "ItemID", 'Type', "Generator:ID",
                       'Title', 'Status', 'V', 'A', 'Created']
            rows = []
            for art in artifacts:
                rows.append([
                    art['id'],
                    art['carver_artifact_specification']['name'],
                    art['item_id'],
                    art['artifact_type'],
                    art['generator_name'] + ":" + art['generator_id'],
                    art['title'][:30] + ('...' if len(art['title']) > 30 else ''),
                    art['status'],
                    art['version'],
                    '✓' if art['active'] else '✗',
                    format_datetime(art['created_at'])
                ])

            click.echo(tabulate(rows, headers=headers, tablefmt=output_format))
            click.echo(f"\nTotal artifacts: {len(artifacts)}")
        else:
            click.echo("No artifacts found")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@content.command('update-status')
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

@content.command()
@click.option('--spec-id', required=True, type=int, help='Specification ID')
@click.option('--artifacts', required=True, help='Comma-separated list of artifact IDs')
@click.pass_context
def activate(ctx, spec_id: int, artifacts: str):
    """Activate multiple artifacts."""
    manager = ctx.obj['manager']
    try:
        artifact_ids = [int(i.strip()) for i in artifacts.split(',')]
        updated = manager.artifact_bulk_active_update(spec_id, artifact_ids, True)
        click.echo(f"Activated {len(updated)} artifacts")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error activating artifacts: {str(e)}", err=True)

@content.command()
@click.option('--spec-id', required=True, type=int, help='Specification ID')
@click.option('--artifacts', required=True, help='Comma-separated list of artifact IDs')
@click.pass_context
def deactivate(ctx, spec_id: int, artifacts: str):
    """Deactivate multiple artifacts."""
    manager = ctx.obj['manager']
    try:
        artifact_ids = [int(i.strip()) for i in artifacts.split(',')]
        updated = manager.artifact_bulk_active_update(spec_id, artifact_ids, False)
        click.echo(f"Deactivated {len(updated)} artifacts")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error deactivating artifacts: {str(e)}", err=True)
