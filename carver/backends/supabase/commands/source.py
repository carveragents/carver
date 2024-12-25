import os
import sys
import json
import traceback

from typing import Optional
from datetime import datetime

import click
from tabulate import tabulate

from .item_manager import ItemManager
from ..utils import *

PLATFORM_CHOICES = ['TWITTER', 'GITHUB', 'YOUTUBE', 'RSS', 'WEB', 'SUBSTACK']
SOURCE_TYPE_CHOICES = ['FEED', 'PROFILE', 'CHANNEL', 'REPOSITORY', 'PAGE', "NEWSLETTER"]

@click.group()
@click.pass_context
def source(ctx):
    """Manage sources in the system."""
    ctx.obj['item_manager'] = ItemManager(ctx.obj['supabase'])

@source.command()
@click.option('--url', required=True, help='URL of the source')
@click.option('--entity-id', required=True, type=int, help='ID of the parent entity')
@click.option('--name', help='Override the automatically inferred name')
@click.option('--description', help='Description of the source')
@click.option('--config', type=str, help='Additional JSON configuration to merge')
@click.pass_context
def add(ctx, url: str, entity_id: int, name: Optional[str],
        description: Optional[str], config: Optional[str]):
    """Add a new source to the system. Source details will be inferred from the URL."""
    db = ctx.obj['supabase']

    try:
        # Parse the URL
        source_info = SourceURLParser.parse_url(url)
        if not source_info:
            click.echo(f"Error: Could not determine source type from URL: {url}", err=True)
            return

        # Allow override of inferred name
        if name:
            source_info['name'] = name

        # Add description if provided
        if description:
            source_info['description'] = description

        # Merge additional config if provided
        if config:
            config_json = json.loads(config)
            source_info['config'] = {**source_info['config'], **config_json}

        # Add required fields
        now = datetime.utcnow()
        source_info.update({
            'active': True,
            'entity_id': entity_id,
            'analysis_metadata': {},
            'created_at': now.isoformat(),
            'updated_at': now.isoformat()
        })

        # Create the source
        source = db.source_create(source_info)

        if source:
            click.echo(f"Successfully created source: {source_info['name']} (ID: {source['id']})")
            click.echo("\nInferred source details:")
            click.echo(f"Platform: {source_info['platform']}")
            click.echo(f"Type: {source_info['source_type']}")
            click.echo(f"Identifier: {source_info['source_identifier']}")
        else:
            click.echo("Error creating source", err=True)

    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON format in config", err=True)
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@source.command()
@click.argument('source_id', type=int)
@click.option('--activate', is_flag=True, help='Activate the source')
@click.option('--deactivate', is_flag=True, help='Deactivate the source')
@click.option('--name', help='New name for the source')
@click.option('--description', help='New description for the source')
@click.option('--platform', type=click.Choice(PLATFORM_CHOICES))
@click.option('--source-type', type=click.Choice(SOURCE_TYPE_CHOICES))
@click.option('--source-identifier', help='New source identifier')
@click.option('--url', help='New URL')
@click.option('--config', help='New JSON configuration')
@click.option('--metadata', help='JSON metadata to update/add')
@click.pass_context
def update(ctx, source_id: int, activate: bool, deactivate: bool,
           name: Optional[str], description: Optional[str],
           platform: Optional[str], source_type: Optional[str],
           source_identifier: Optional[str], url: Optional[str],
           config: Optional[str], metadata: Optional[str]):
    """Update an existing source."""
    db = ctx.obj['supabase']

    try:
        update_data = {'updated_at': datetime.utcnow().isoformat()}

        if activate and deactivate:
            click.echo("Error: Cannot both activate and deactivate", err=True)
            return

        if activate:
            update_data['active'] = True
        if deactivate:
            update_data['active'] = False
        if name:
            update_data['name'] = name
        if description:
            update_data['description'] = description
        if platform:
            update_data['platform'] = platform.strip().upper()
        if source_type:
            update_data['source_type'] = source_type.strip().upper()
        if source_identifier:
            update_data['source_identifier'] = source_identifier
        if url:
            update_data['url'] = url
        if config:
            update_data['config'] = json.loads(config)

        source = db.source_update(source_id, update_data)

        # Handle metadata update separately if provided
        if metadata:
            metadata_json = json.loads(metadata)
            source = db.source_update_metadata(source_id, metadata_json)

        if source:
            click.echo(f"Successfully updated source ID: {source_id}")
        else:
            click.echo("Error updating source or source not found", err=True)

    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON format in config or metadata", err=True)
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@source.command()
@click.option('--active/--inactive', default=None, help='Filter by active status')
@click.option('--entity-id', type=int, help='Filter by entity ID')
@click.option('--platform', type=click.Choice(PLATFORM_CHOICES))
@click.option('--source-type', type=click.Choice(SOURCE_TYPE_CHOICES))
@click.option('--search', help='Search in source names')
@click.option('--updated-since', help='Show sources updated since (ISO date or relative like "1d", "1w")')
@click.option('--crawled-since', help='Show sources crawled since (ISO date or relative like "1d", "1w")')
@click.option('--format', 'output_format',
              type=click.Choice(['table', 'grid', 'pipe', 'orgtbl', 'rst', 'mediawiki', 'html']),
              default='table',
              help='Output format for the table')
@click.pass_context
def search(ctx, active: Optional[bool], entity_id: Optional[int],
           platform: Optional[str], source_type: Optional[str],
           search: Optional[str], updated_since: Optional[str],
           crawled_since: Optional[str], output_format: str):
    """List sources with optional filters."""
    db = ctx.obj['supabase']

    try:
        # Parse date filters
        updated_since_dt = parse_date_filter(updated_since) if updated_since else None
        crawled_since_dt = parse_date_filter(crawled_since) if crawled_since else None

        # Query sources
        sources = db.source_search(
            active=active,
            entity_id=entity_id,
            platform=platform,
            source_type=source_type,
            name=search,
            updated_since=updated_since_dt,
            crawled_since=crawled_since_dt
        )

        if sources:
            # Prepare table data
            headers = ['ID', 'Name', 'Entity', 'Platform', 'Type', 'Active', 'Last Crawled', 'Updated']
            rows = []

            for source in sources:
                entity_name = source['carver_entity']['name'] if source['carver_entity'] else 'N/A'
                rows.append([
                    source['id'],
                    source['name'][:30] + ('...' if len(source['name']) > 30 else ''),
                    f"{entity_name} ({source['entity_id']})",
                    source['platform'],
                    source['source_type'],
                    '✓' if source['active'] else '✗',
                    format_datetime(source['last_crawled']) if source.get('last_crawled') else 'Never',
                    format_datetime(source['updated_at'])
                ])

            # Print table
            click.echo(tabulate(rows, headers=headers, tablefmt=output_format))
            click.echo(f"\nTotal sources: {len(sources)}")
        else:
            click.echo("No sources found")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@source.command()
@click.argument('source_id', type=int)
@click.pass_context
def show(ctx, source_id: int):
    """Show detailed information about a specific source."""
    db = ctx.obj['supabase']

    try:
        source = db.source_get(source_id)
        if not source:
            click.echo(f"Source with ID {source_id} not found", err=True)
            return

        entity = source['carver_entity']

        # Basic information
        click.echo("\n=== Source Information ===")
        click.echo(f"ID: {source['id']}")
        click.echo(f"Name: {source['name']}")
        click.echo(f"Active: {'Yes' if source['active'] else 'No'}")
        click.echo(f"Platform: {source['platform']}")
        click.echo(f"Type: {source['source_type']}")
        click.echo(f"Identifier: {source['source_identifier']}")
        click.echo(f"URL: {source['url']}")

        if source['description']:
            click.echo(f"\nDescription: {source['description']}")

        # Entity information
        click.echo("\n=== Parent Entity ===")
        click.echo(f"ID: {entity['id']}")
        click.echo(f"Name: {entity['name']}")
        click.echo(f"Type: {entity['entity_type']}")
        click.echo(f"Owner: {entity['owner']}")

        # Timestamps
        click.echo("\n=== Timestamps ===")
        click.echo(f"Created: {format_datetime(source['created_at'])}")
        click.echo(f"Updated: {format_datetime(source['updated_at'])}")
        if source.get('last_crawled'):
            click.echo(f"Last Crawled: {format_datetime(source['last_crawled'])}")

        # Configuration
        if source.get('config'):
            click.echo("\n=== Configuration ===")
            click.echo(json.dumps(source['config'], indent=2))

        # Analysis Metadata
        if source.get('analysis_metadata'):
            click.echo("\n=== Analysis Metadata ===")
            click.echo(json.dumps(source['analysis_metadata'], indent=2))

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@source.command()
@click.argument('source_id', type=int)
@click.option('--fields', help='Comma-separated list of fields to sync')
@click.option('--max-results', type=int, help='Maximum number of items to fetch')
@click.pass_context
def sync_items(ctx, source_id: int, fields: Optional[str], max_results: Optional[int]):
    """Sync items from a specific source."""
    db = ctx.obj['supabase']
    item_manager = ctx.obj['item_manager']

    try:
        # Verify source exists and is active
        source = db.source_get(source_id)
        if not source:
            click.echo(f"Source {source_id} not found", err=True)
            return

        if not source['active']:
            click.echo(f"Warning: Source {source_id} is not active", err=True)
            if not click.confirm("Continue anyway?"):
                return

        click.echo(f"\nSyncing items for source: {source['name']} (ID: {source_id})")

        field_list = fields.split(',') if fields else None
        try:
            added, updated = item_manager.sync_items(source_id, field_list, max_results)
            click.echo(f"Successfully synced items:")
            click.echo(f"- Added: {added}")
            click.echo(f"- Updated: {updated}")
        except Exception as e:
            click.echo(f"Error syncing items: {str(e)}", err=True)

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@source.command()
@click.argument('source_id', type=int)
@click.pass_context
def update_analytics(ctx, source_id: int):
    """Update analytics metadata for a source."""
    db = ctx.obj['supabase']

    try:
        # Verify source exists
        source = db.source_get(source_id)
        if not source:
            click.echo(f"Source {source_id} not found", err=True)
            return

        click.echo(f"\nComputing analytics for source: {source['name']} (ID: {source_id})")

        # Update analytics using the new method
        updated_source = db.update_source_analytics(source_id)

        if updated_source and updated_source.get('analysis_metadata'):
            metrics = updated_source['analysis_metadata']['metrics']
            click.echo("\nAnalytics updated successfully:")
            click.echo(f"- Active Items: {metrics['counts']['items']}")
            click.echo(f"- Active Artifacts: {metrics['counts']['artifacts']}")
            click.echo(f"- Active Specifications: {metrics['counts']['specifications']}")

            click.echo("\nArtifact Type Distribution:")
            for artifact_type, count in metrics['distribution']['artifact_type'].items():
                click.echo(f"- {artifact_type}: {count}")

            click.echo("\nArtifact Status Distribution:")
            for status, count in metrics['distribution']['artifact_status'].items():
                click.echo(f"- {status}: {count}")
        else:
            click.echo("Error updating source analytics", err=True)

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)
