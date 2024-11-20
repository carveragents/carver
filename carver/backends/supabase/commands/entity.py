import os
import sys
import json
import traceback

from typing import Optional
from datetime import datetime

import click

from tabulate import tabulate

from ..utils import *

@click.group()
@click.pass_context
def entity(ctx):
    """Manage entities in the system."""
    pass

@entity.command()
@click.option('--name', required=True, help='Name of the entity')
@click.option('--description', help='Description of the entity')
@click.option('--owner', required=True, help='Owner of the entity')
@click.option('--entity-type', required=True,
              type=click.Choice(['PERSON', 'ORGANIZATION', 'PROJECT']),
              help='Type of the entity')
@click.option('--config', type=str, help='JSON configuration for the entity')
@click.option('--metadata', type=str, help='JSON metadata for the entity')
@click.pass_context
def add(ctx, name: str, description: Optional[str], owner: str, entity_type: str,
        config: Optional[str], metadata: Optional[str]):
    """Add a new entity to the system."""
    db = ctx.obj['supabase']

    try:
        config_json = json.loads(config) if config else {}
        metadata_json = json.loads(metadata) if metadata else {}

        now = datetime.utcnow()

        data = {
            'active': True,
            'name': name,
            'description': description,
            'owner': owner,
            'entity_type': entity_type,
            'config': config_json,
            'metadata': metadata_json,
            'created_at': now.isoformat(),
            'updated_at': now.isoformat()
        }

        entity = db.entity_create(data)

        if entity:
            click.echo(f"Successfully created entity: {name} (ID: {entity['id']})")
        else:
            click.echo("Error creating entity", err=True)

    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON format in config or metadata", err=True)
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@entity.command()
@click.argument('entity_id', type=int)
@click.option('--activate', is_flag=True)
@click.option('--deactivate', is_flag=True)
@click.option('--name', help='New name for the entity')
@click.option('--description', help='New description for the entity')
@click.option('--owner', help='New owner for the entity')
@click.option('--entity-type',
              type=click.Choice(['PERSON', 'ORGANIZATION', 'PROJECT']),
              help='New type for the entity')
@click.option('--config', help='New JSON configuration for the entity')
@click.option('--metadata', help='New JSON metadata for the entity')
@click.pass_context
def update(ctx, entity_id: int, activate: bool, deactivate: bool,
           name: Optional[str], description: Optional[str],
           owner: Optional[str], entity_type: Optional[str],
           config: Optional[str], metadata: Optional[str]):
    """Update an existing entity."""
    db = ctx.obj['supabase']

    try:
        update_data = {'updated_at': datetime.utcnow().isoformat()}

        if activate:
            update_data['active'] = True
        if deactivate:
            update_data['active'] = False
        if name:
            update_data['name'] = name
        if description:
            update_data['description'] = description
        if owner:
            update_data['owner'] = owner
        if entity_type:
            update_data['entity_type'] = entity_type
        if config:
            update_data['config'] = json.loads(config)
        if metadata:
            update_data['metadata'] = json.loads(metadata)

        entity = db.entity_update(entity_id, update_data)

        if entity:
            click.echo(f"Successfully updated entity ID: {entity_id}")
        else:
            click.echo("Error updating entity or entity not found", err=True)

    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON format in config or metadata", err=True)
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@entity.command()
@click.option('--active/--inactive', default=None, help='Filter by active status')
@click.option('--entity-type',
              type=click.Choice(['PERSON', 'ORGANIZATION', 'PROJECT']),
              help='Filter by entity type')
@click.option('--owner', help='Filter by owner')
@click.option('--search', help='Search in entity names')
@click.option('--created-since', help='Show entities created since (ISO date or relative like "1d", "1w")')
@click.option('--updated-since', help='Show entities updated since (ISO date or relative like "1d", "1w")')
@click.option('--format', 'output_format',
              type=click.Choice(['table', 'grid', 'pipe', 'orgtbl', 'rst', 'mediawiki', 'html']),
              default='table',
              help='Output format for the table')
@click.pass_context
def search(ctx, active: Optional[bool], entity_type: Optional[str], owner: Optional[str],
           search: Optional[str], created_since: Optional[str], updated_since: Optional[str],
           output_format: str):
    """List entities with optional filters."""
    db = ctx.obj['supabase']

    try:
        # Parse date filters
        created_since_dt = parse_date_filter(created_since) if created_since else None
        updated_since_dt = parse_date_filter(updated_since) if updated_since else None

        # Query entities
        entities = db.entity_search(
            active=active,
            entity_type=entity_type,
            owner=owner,
            name=search,
            created_since=created_since_dt,
            updated_since=updated_since_dt
        )

        if entities:
            # Prepare table data
            headers = ['ID', 'Name', 'Type', 'Owner', 'Active', 'Created', 'Updated', 'Description']
            rows = []

            for entity in entities:
                rows.append([
                    entity['id'],
                    entity['name'],
                    entity['entity_type'],
                    entity['owner'],
                    '✓' if entity['active'] else '✗',
                    format_datetime(entity['created_at']),
                    format_datetime(entity['updated_at']),
                    (entity.get('description') or '')[:50] + ('...' if entity.get('description', '') and len(entity['description']) > 50 else '')
                ])

            # Print table
            click.echo(tabulate(rows, headers=headers, tablefmt=output_format))
            click.echo(f"\nTotal entities: {len(entities)}")
        else:
            click.echo("No entities found")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@entity.command()
@click.argument('entity_id', type=int)
@click.pass_context
def show(ctx, entity_id: int):
    """Show detailed information about a specific entity."""
    db = ctx.obj['supabase']

    try:
        entity = db.entity_get(entity_id)
        if not entity:
            click.echo(f"Entity with ID {entity_id} not found", err=True)
            return

        # Basic information
        click.echo("\n=== Entity Information ===")
        click.echo(f"ID: {entity['id']}")
        click.echo(f"Name: {entity['name']}")
        click.echo(f"Type: {entity['entity_type']}")
        click.echo(f"Owner: {entity['owner']}")
        click.echo(f"Active: {'Yes' if entity['active'] else 'No'}")

        if entity['description']:
            click.echo(f"\nDescription: {entity['description']}")

        # Timestamps
        click.echo("\n=== Timestamps ===")
        click.echo(f"Created: {format_datetime(entity['created_at'])}")
        click.echo(f"Updated: {format_datetime(entity['updated_at'])}")

        # Configuration
        if entity.get('config'):
            click.echo("\n=== Configuration ===")
            click.echo(json.dumps(entity['config'], indent=2))

        # Metadata
        if entity.get('metadata'):
            click.echo("\n=== Metadata ===")
            click.echo(json.dumps(entity['metadata'], indent=2))

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)
