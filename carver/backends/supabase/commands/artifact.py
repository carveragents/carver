import os
import sys
import json
import traceback

from typing import Optional, List, Dict
from datetime import datetime

import click

from tabulate import tabulate

from ..utils import format_datetime, parse_date_filter, get_spec_config
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
@click.option('--config', required=True, type=click.Path(exists=True), help='Path to JSON/py config file')
@click.pass_context
def add(ctx, source_id: int, name: str, description: Optional[str], config: str):
    """Add a new artifact specification."""
    manager = ctx.obj['manager']
    try:
        config_data = get_spec_config(config)

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
@click.option('--active/--inactive', default=True, help='Filter by active status')
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
            specmap = {spec['id']: spec for spec in specs}

            headers = ['ID', 'Source', 'Name', 'Generator', 'Dependencies', 'Active', 'Updated']
            rows = []
            for spec in specs:
                source_name = spec['carver_source']['name'] if spec.get('carver_source') else 'N/A'

                # Get dependencies
                deps = spec['config'].get('dependencies', [])
                if isinstance(deps, (int, str)):
                    deps = [deps]
                elif deps is None:
                    deps = []

                # Format dependencies string
                if deps:
                    # Get dependency names
                    dep_names = []
                    for dep_id in deps:
                        if dep_id not in specmap:
                            click.echo(f"Warning: Specification {dep_id} not found", err=True)
                            return
                        dep_spec = specmap[dep_id]
                        dep_names.append(f"{dep_id}:{dep_spec['name'][:20]}")
                    deps_str = ",\n".join(dep_names)
                else:
                    deps_str = "None"

                rows.append([
                    spec['id'],
                    f"{source_name[:20]} ({spec['source_id']})",
                    spec['name'],
                    spec['config'].get('generator'),
                    deps_str,
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
            config_data = get_spec_config(config)
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

@spec.command()
@click.argument('spec_id', type=int)
@click.option('--depends-on', required=True, help='Comma-separated list of specification IDs that this spec depends on')
@click.pass_context
def update_dependencies(ctx, spec_id: int, depends_on: str):
    """Update dependencies of a specification, replacing any existing dependencies."""
    db = ctx.obj['supabase']
    try:
        # Get the specification
        spec = db.specification_get(spec_id)
        if not spec:
            click.echo(f"Specification {spec_id} not found", err=True)
            return

        # Parse dependency IDs
        try:
            dependency_ids = [int(s.strip()) for s in depends_on.split(',')]
        except ValueError:
            click.echo("Error: Dependencies must be comma-separated integers", err=True)
            return

        # Validate all dependency specifications exist
        for dep_id in dependency_ids:
            dep_spec = db.specification_get(dep_id)
            if not dep_spec:
                click.echo(f"Warning: Specification {dep_id} not found", err=True)
                return

        # Update the config with new dependencies
        config = spec.get('config', {})
        config['dependencies'] = dependency_ids
        update_data = {
            'config': config,
            'updated_at': datetime.utcnow().isoformat()
        }

        # Update the specification
        updated_spec = db.specification_update(spec_id, update_data)
        if updated_spec:
            click.echo(f"Successfully updated dependencies for specification {spec_id}")
            click.echo("\nNew dependencies:")
            for dep_id in dependency_ids:
                dep_spec = db.specification_get(dep_id)
                if dep_spec:
                    click.echo(f"- {dep_id}: {dep_spec['name']}")
        else:
            click.echo("Error updating specification", err=True)

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
@click.option('--generator-name', required=False, help='Generator name to use')
@click.option('--offset', default=0, type=int, help='Offset for search results')
@click.option('--limit', default=50, type=int, help='Maximum number of items to fetch')
@click.pass_context
def bulk_generate(ctx, spec_id: int, source_id: int, last: Optional[str], generator_name: str, offset: int, limit: int):
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
            limit=limit,
            offset=offset,
        )

        if not items:
            click.echo("No items found requiring artifact generation")
            return

        click.echo(f"Found {len(items)} items to process for artifact generation")

        # Process each specification
        for spec in specs:
            if ((generator_name is not None) and
                (spec['config'].get('generator') != generator_name)):
                continue

            try:
                results = manager.artifact_bulk_create_from_spec(spec, items, generator_name)
                click.echo(f"Successfully generated {len(results)} artifacts using specification {spec['id']}")
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
@click.option('--active/--inactive', default=True, help='Filter by active status')
@click.option('--last', type=str, help='Filter by time window (e.g. "1d", "2h", "30m")')
@click.option('--offset', default=0, type=int, help='Offset for search results')
@click.option('--limit', default=50, type=int, help='Maximum number of items to fetch')
@click.option('--format', 'output_format',
              type=click.Choice(['table', 'grid', 'pipe', 'orgtbl', 'rst', 'mediawiki', 'html']),
              default='table',
              help='Output format for display')
@click.option('--dump', 'dump_format',
              type=click.Choice(['text', 'csv', 'json']),
              help='Dump results to file in specified format')
@click.option('--output', '-o', type=click.Path(), help='Output file path for dump')
@click.pass_context
def search(ctx, spec_id: Optional[int], item_id: Optional[int],
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
            item_id=item_id,
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
                'item_id': art['item_id'],
                'item_name': art['carver_item']['name'],
                'item_description': art['carver_item']['description'],
                'item_url': art['carver_item']['url'],
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
                r['item_id'],
                r['artifact_type'],
                r['generator'],
                r['title'][:20] + ('...' if len(r['title']) > 20 else ''),
                r['status'],
                r['version'],
                r['embedding'],
                '✓' if r['active'] else '✗',
                r['created_at']
            ] for r in rows]

            headers = ['ID', 'Spec', "ItemID", 'Type', "Generator:ID",
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

@content.command()
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
