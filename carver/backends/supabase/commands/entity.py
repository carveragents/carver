import os
import sys
import json
import traceback

from typing import Optional
from datetime import datetime
from collections import defaultdict

import click

from tabulate import tabulate

from carver.feeds.youtube import YouTubePlaylistDiscovery

from .item_manager import ItemManager
from .artifact_manager import ArtifactManager
from ..utils import *

@click.group()
@click.pass_context
def entity(ctx):
    """Manage entities in the system."""
    ctx.obj['item_manager'] = ItemManager(ctx.obj['supabase'])
    ctx.obj['artifact_manager'] = ArtifactManager(ctx.obj['supabase'])

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
@click.option('--active/--inactive', default=True, help='Filter by active status')
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

@entity.command()
@click.argument('entity_id', type=int)
@click.option('--fields', help='Comma-separated list of fields to sync for each source')
@click.option('--max-results', type=int, help='Maximum number of items to fetch per source')
@click.pass_context
def sync_items(ctx, entity_id: int, fields: Optional[str], max_results: Optional[int]):
    """Sync items from all active sources for an entity."""
    db = ctx.obj['supabase']
    item_manager = ctx.obj['item_manager']

    try:
        # Get all active sources for the entity
        sources = db.source_search(
            active=True,
            entity_id=entity_id
        )

        if not sources:
            click.echo(f"No active sources found for entity {entity_id}")
            return

        total_added = 0
        total_updated = 0
        field_list = fields.split(',') if fields else None

        # Process each source
        for source in sources:
            click.echo(f"\nProcessing source: {source['name']} (ID: {source['id']})")
            try:
                added, updated = item_manager.sync_items(
                    source['id'],
                    field_list,
                    max_results
                )
                total_added += added
                total_updated += updated
                click.echo(f"- Added: {added}, Updated: {updated}")
            except Exception as e:
                click.echo(f"Error processing source {source['id']}: {str(e)}", err=True)
                continue

        click.echo(f"\nSync completed for {len(sources)} sources")
        click.echo(f"Total items: {total_added} added, {total_updated} updated")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@entity.command()
@click.argument('entity_id', type=int)
@click.option('--max-retries', type=int, default=3,
              help='Maximum number of retries for dependency resolution')
@click.option('--last', type=str, help='Filter items by time (e.g. "1d", "2h", "30m")')
@click.option('--offset', default=0, type=int, help='Offset for search results')
@click.option('--limit', default=50, type=int, help='Maximum number of items to fetch')
@click.pass_context
def generate_bulk(ctx, entity_id: int, max_retries: int, last: Optional[str],
                 offset: int, limit: int):
    """Generate bulk content for all active specifications of an entity in dependency order."""
    db = ctx.obj['supabase']
    artifact_manager = ctx.obj['artifact_manager']

    try:
        # Get all active specifications for the entity
        specs = db.specification_search(
            entity_id=entity_id,
            active=True
        )

        if not specs:
            click.echo(f"No active specifications found for entity {entity_id}")
            return

        click.echo(f"Found {len(specs)} specs")

        # Sort specifications by dependencies
        try:
            sorted_specs_ids = topological_sort(specs)
        except ValueError as e:
            click.echo(f"Error in dependency resolution: {str(e)}", err=True)
            return

        # Group specifications by source
        source_specs = {}
        for spec in specs:
            source_id = spec['source_id']
            if source_id not in source_specs:
                source_specs[source_id] = {}
            source_specs[source_id][spec['id']] = spec

        click.echo(f"Found {len(source_specs)} sources")

        # Process each source
        total_generated = 0
        failed_specs = []

        for source_id, source_specs in source_specs.items():

            sample_spec = list(source_specs.values())[0]
            source = sample_spec['carver_source']
            label = f"[{source_id}] {source['name']}"
            click.echo(f"\n{label}: Started processing")

            # Get items needing artifacts
            time_filter = parse_date_filter(last) if last else None
            items = db.item_search_with_artifacts(
                source_id=source_id,
                modified_after=time_filter,
                offset=offset,
                limit=limit
            )

            if not items:
                click.echo(f"No items found requiring artifact generation for source {source_id}")
                continue

            click.echo(f"{label}: Found {len(items)} items with artifacts")

            # Process specifications for this source
            for spec_id in sorted_specs_ids:
                if spec_id not in source_specs:
                    continue

                spec = source_specs[spec_id]
                source = spec['carver_source']
                click.echo(f"\n{label}: Processing Specification [{spec_id}] {spec['name']}")

                retry_count = 0
                success = False

                while retry_count < max_retries and not success:
                    try:
                        results = artifact_manager.artifact_bulk_create_from_spec(spec,
                                                                                  items,
                                                                                  None)
                        total_generated += len(results)
                        click.echo(f"Generated {len(results)} artifacts")
                        success = True
                    except Exception as e:
                        retry_count += 1
                        if retry_count < max_retries:
                            click.echo(f"Retry {retry_count}/{max_retries} for spec {spec['id']}")
                        else:
                            click.echo(f"Failed to process spec {spec['id']} after {max_retries} attempts: {str(e)}", err=True)
                            failed_specs.append(spec['id'])

        click.echo(f"\nBulk generation completed")
        click.echo(f"Total artifacts generated: {total_generated}")
        if failed_specs:
            click.echo(f"Failed specifications: {failed_specs}")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)



def validate_choice(value):
    valid_options = ['y','n', 'e', 'q']
    if value.lower() not in valid_options:
        raise click.BadParameter(f"Invalid choice: {value}. Please select from {', '.join(valid_options)}.")
    return value.lower()

@entity.command()
@click.argument('entity_id', type=int)
@click.argument('keywords', nargs=-1, required=True)
@click.option('--what', required=True,
              default='playlist',
              type=click.Choice(['playlist', 'channel']))
@click.option('--max-results', '-m', default=10, help='Maximum number of playlists to discover')
@click.pass_context
def discover_playlists(ctx, entity_id: int, keywords: tuple, what: str,
                       max_results: int):
    """Discover YouTube playlists based on keywords and create sources for the entity."""
    db = ctx.obj['supabase']

    try:
        # Get entity
        entity = db.entity_get(entity_id)
        if not entity:
            click.echo(f"Entity with ID {entity_id} not found", err=True)
            return


        discovery = YouTubePlaylistDiscovery()
        query = ' '.join(keywords)

        click.echo(f"Searching for playlists matching: {query}")
        playlists = discovery.discover_playlists(query=query,
                                                 what=what,
                                                 max_results=max_results)

        if not playlists:
            click.echo("No playlists found matching your criteria")
            return

        click.echo(f"Found {len(playlists)} playlists")

        for playlist in playlists:

            prefix = "https://www.youtube.com"
            if what == 'playlist':
                url = f"{prefix}/playlist?list={playlist['id']}"
            else:
                url = f"{prefix}/channel/{playlist['id']}"

            click.echo("\n" + "="*50)
            click.echo(hyperlink(url, label=f"Playlist: {playlist['title']}"))
            click.echo(f"Channel: {playlist['channel_title']}")
            click.echo(f"Description: {playlist['description'][:200]}")
            click.echo(f"Published: {playlist['published_at']}")

            choice = click.prompt(
                "Choose an option (yes, NO, exit)",
                type=str,
                default='N',
                value_proc=validate_choice
            )
            if choice in ["e", "q", "quit", "exit"]:
                break

            if choice in ["y", "yes"]:
                source_data = {
                    'name': playlist['title'],
                    'description': playlist['description'],
                    'platform': 'youtube',
                    'entity_id': entity_id,
                    'active': True,
                    'url': url,
                    'source_type': 'playlist',
                    'source_identifier': playlist['id'],
                    'config': {
                    },
                    'analysis_metadata': {
                        'channel': playlist['channel_title'],
                        'published_at': playlist['published_at'],
                        'thumbnail_url': playlist['thumbnail_url']
                    }
                }

                # Create the source
                source = db.source_create(source_data)
                if source:
                    click.echo(f"Created source ID: {source['id']} for playlist: {playlist['title']}")
                else:
                    click.echo("Error creating source", err=True)

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@entity.command("search-similar")
@click.argument('entity_id', type=int)
@click.argument('query', type=str)
@click.option('--threshold', type=float, default=0.7, help='Similarity threshold')
@click.option('--limit', type=int, default=10, help='Maximum results to return')
@click.pass_context
def search_similar(ctx, entity_id: int, query: str, threshold: float, limit: int):
    """Search for similar artifacts across all sources in an entity."""
    db = ctx.obj['supabase']
    artifact_manager = ctx.obj['artifact_manager']

    try:
        # Get all specs for the entity
        specs = db.specification_search(
            entity_id=entity_id,
            active=True
        )

        if not specs:
            click.echo("No active specifications found for entity")
            return

        specmap = {spec['id']: spec for spec in specs}

        # Search across all specs
        all_results = []
        for spec in specs:
            print(f"[{spec['name']}] Searching")
            results = artifact_manager.artifact_search_similar(
                query=query,
                match_threshold=threshold,
                match_count=limit,
                spec_id=spec['id']
            )
            all_results.extend(results)
            print(f"[{spec['name']}] Found {len(results)} Total {len(all_results)}")

        # Sort by similarity and limit
        all_results.sort(key=lambda x: x['similarity'], reverse=True)

        if all_results:
            headers = ['ID', 'Source', 'Spec', 'Title', 'Type', 'Similarity']
            rows = [[
                r['id'],
                specmap[r['spec_id']]['carver_source']['name'],
                specmap[r['spec_id']]['name'],
                r['title'],
                r['artifact_type'],
                f"{r['similarity']:.3f}"
            ] for r in all_results]

            click.echo(tabulate(rows, headers=headers, maxcolwidths=[None, 20, 20, 40,None, None]))
            click.echo(f"\nTotal results: {len(all_results)}")
        else:
            click.echo("No similar artifacts found")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@entity.command('update-embeddings')
@click.argument('entity_id', type=int)
@click.option('--batch-size', default=100, type=int, help='Number of artifacts to process in each batch')
@click.option('--status', help='Filter by artifact status')
@click.option('--force/--no-force', default=False, help='Update even if embedding exists')
@click.option('--dry-run/--no-dry-run', default=False, help='Show what would be updated without making changes')
@click.option('--last', type=str, help='Filter items by time (e.g. "1d", "2h", "30m")')
@click.option('--offset', default=0, type=int, help='Offset for search results')
@click.option('--limit', default=50, type=int, help='Maximum number of items to fetch')
@click.pass_context
def update_embeddings(ctx, entity_id: int, batch_size: int, status: Optional[str],
                      force: bool, dry_run: bool,
                      last: Optional[str], offset: int, limit: int):
    """
    Bulk update embeddings for all artifacts under an entity's sources.

    Example:
    carver entity update-embeddings 123 --batch-size 50 --status published
    """
    db = ctx.obj['supabase']
    artifact_manager = ctx.obj['artifact_manager']

    try:
        # Get the entity first
        entity = db.entity_get(entity_id)
        if not entity:
            click.echo(f"Entity {entity_id} not found", err=True)
            return

        click.echo(f"\nProcessing embeddings for entity: {entity['name']}")

        # Get all active specifications for the entity's sources
        specs = db.specification_search(
            entity_id=entity_id,
            active=True
        )

        if not specs:
            click.echo("No active specifications found for entity")
            return

        click.echo(f"Found {len(specs)} active specifications")

        total_processed = 0
        total_updated = 0
        total_errors = 0

        time_filter = parse_date_filter(last) if last else None

        # Process each specification
        for spec in specs:
            print(f"[{spec['name']}] Source: {spec['carver_source']['name']}")
            print(f"[{spec['name']}] Started processing")
            # Get artifacts without embeddings for this spec
            artifacts = db.artifact_search(
                    spec_id=spec['id'],
                    status=status,
                    active=True,
                    modified_after=time_filter,
                    offset=offset,
                    limit=limit,
                    has_embedding=False if not force else None
                )
            if len(artifacts) == 0:
                print("[{spec['name']}] Found no artifacts to process")
                continue

            print(f"[{spec['name']}] Updating {len(artifacts)}")

            if dry_run:
                print(f"[{spec['name']}] Dry-run. Done")
                continue

            try:
                # Update embeddings for this batch
                result = artifact_manager.artifact_bulk_update_embeddings(
                    artifacts=artifacts,
                    force_update=force,
                    batch_size=batch_size
                )

                total_processed += result['processed']
                total_updated += result['updated']
                total_errors += result['errors']

            except Exception as e:
                click.echo(f"Error processing batch: {str(e)}", err=True)
                total_errors += len(batch)

        # Print summary
        click.echo("\nEmbedding Update Summary")
        click.echo("=====================")
        click.echo(f"Total artifacts processed: {total_processed}")
        click.echo(f"Successfully updated: {total_updated}")
        click.echo(f"Errors: {total_errors}")

        if dry_run:
            click.echo("\nThis was a dry run - no changes were made")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error updating embeddings: {str(e)}", err=True)

@entity.command()
@click.argument('entity_id', type=int)
@click.pass_context
def update_analytics(ctx, entity_id: int):
    """Update analytics metadata for all sources of an entity."""
    db = ctx.obj['supabase']

    try:
        # Verify entity exists
        entity = db.entity_get(entity_id)
        if not entity:
            click.echo(f"Entity {entity_id} not found", err=True)
            return

        click.echo(f"\nUpdating analytics for entity: {entity['name']} (ID: {entity_id})")

        # Get all sources for this entity
        sources = db.source_search(
            entity_id=entity_id,
            active=True,
            fields=['id', 'name']
        )

        if not sources:
            click.echo("No sources found for this entity")
            return

        click.echo(f"Found {len(sources)} sources to process")

        entity_metrics = {
            'last_update': datetime.utcnow().isoformat(),
            'sources_count': len(sources),
            'sources': {},
            'totals': {
                'items': 0,
                'artifacts': 0,
                'specifications': 0
            }
        }

        # Process each source
        with click.progressbar(sources, label='Processing sources') as source_list:
            for source in source_list:
                # Update analytics for this source
                updated_source = db.update_source_analytics(source['id'])

                if updated_source and updated_source.get('analysis_metadata'):
                    metrics = updated_source['analysis_metadata']['metrics']

                    # Accumulate totals for entity-level metrics
                    entity_metrics['totals']['items'] += metrics['counts']['items']
                    entity_metrics['totals']['artifacts'] += metrics['counts']['artifacts']
                    entity_metrics['totals']['specifications'] += metrics['counts']['specifications']

                    # Store summarized metrics for this source
                    entity_metrics['sources'][source['id']] = {
                        'name': source['name'],
                        'counts': metrics['counts']
                    }

        # Update entity metadata
        entity_analytics = {
            'metrics': entity_metrics,
        }

        entity = db.entity_update_metadata(entity_id, entity_analytics)

        if entity:
            # Print summary
            click.echo("\nAnalytics update completed:")
            click.echo(f"- Total Sources: {entity_metrics['sources_count']}")
            click.echo(f"- Total Items: {entity_metrics['totals']['items']}")
            click.echo(f"- Total Artifacts: {entity_metrics['totals']['artifacts']}")
            click.echo(f"- Total Specifications: {entity_metrics['totals']['specifications']}")
        else:
            click.echo("Error updating entity analytics", err=True)

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

