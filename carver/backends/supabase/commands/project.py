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
from carver.utils import *

from .post_manager import PostManager
from .artifact_manager import ArtifactManager
from ..utils import *

@click.group()
@click.pass_context
def project(ctx):
    """Manage projects in the system."""
    ctx.obj['post_manager'] = PostManager(ctx.obj['supabase'])
    ctx.obj['artifact_manager'] = ArtifactManager(ctx.obj['supabase'])

@project.command()
@click.option('--name', required=True, help='Name of the project')
@click.option('--description', help='Description of the project')
@click.option('--owner', required=True, help='Owner of the project')
@click.option('--project-type', required=True,
              type=click.Choice(['PERSON', 'ORGANIZATION', 'PROJECT']),
              help='Type of the project')
@click.option('--config', type=str, help='JSON configuration for the project')
@click.option('--metadata', type=str, help='JSON metadata for the project')
@click.pass_context
def add(ctx, name: str, description: Optional[str], owner: str, project_type: str,
        config: Optional[str], metadata: Optional[str]):
    """Add a new project to the system."""
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
            'project_type': project_type,
            'config': config_json,
            'metadata': metadata_json,
            'created_at': now.isoformat(),
            'updated_at': now.isoformat()
        }

        project = db.project_create(data)

        if project:
            click.echo(f"Successfully created project: {name} (ID: {project['id']})")
        else:
            click.echo("Error creating project", err=True)

    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON format in config or metadata", err=True)
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@project.command()
@click.argument('project_id', type=int)
@click.option('--activate', is_flag=True)
@click.option('--deactivate', is_flag=True)
@click.option('--name', help='New name for the project')
@click.option('--description', help='New description for the project')
@click.option('--owner', help='New owner for the project')
@click.option('--project-type',
              type=click.Choice(['PERSON', 'ORGANIZATION', 'PROJECT']),
              help='New type for the project')
@click.option('--config', help='New JSON configuration for the project')
@click.option('--metadata', help='New JSON metadata for the project')
@click.pass_context
def update(ctx, project_id: int, activate: bool, deactivate: bool,
           name: Optional[str], description: Optional[str],
           owner: Optional[str], project_type: Optional[str],
           config: Optional[str], metadata: Optional[str]):
    """Update an existing project."""
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
        if project_type:
            update_data['project_type'] = project_type
        if config:
            update_data['config'] = json.loads(config)
        if metadata:
            update_data['metadata'] = json.loads(metadata)

        project = db.project_update(project_id, update_data)

        if project:
            click.echo(f"Successfully updated project ID: {project_id}")
        else:
            click.echo("Error updating project or project not found", err=True)

    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON format in config or metadata", err=True)
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@project.command()
@click.option('--active/--inactive', default=True, help='Filter by active status')
@click.option('--project-type',
              type=click.Choice(['PERSON', 'ORGANIZATION', 'PROJECT']),
              help='Filter by project type')
@click.option('--owner', help='Filter by owner')
@click.option('--search', help='Search in project names')
@click.option('--created-since', help='Show projects created since (ISO date or relative like "1d", "1w")')
@click.option('--updated-since', help='Show projects updated since (ISO date or relative like "1d", "1w")')
@click.option('--format', 'output_format',
              type=click.Choice(['table', 'grid', 'pipe', 'orgtbl', 'rst', 'mediawiki', 'html']),
              default='table',
              help='Output format for the table')
@click.pass_context
def search(ctx, active: Optional[bool], project_type: Optional[str], owner: Optional[str],
           search: Optional[str], created_since: Optional[str], updated_since: Optional[str],
           output_format: str):
    """List projects with optional filters."""
    db = ctx.obj['supabase']

    try:
        # Parse date filters
        created_since_dt = parse_date_filter(created_since) if created_since else None
        updated_since_dt = parse_date_filter(updated_since) if updated_since else None

        # Query projects
        projects = db.project_search(
            active=active,
            project_type=project_type,
            owner=owner,
            name=search,
            created_since=created_since_dt,
            updated_since=updated_since_dt
        )

        if projects:
            # Prepare table data
            headers = ['ID', 'Name', 'Type', 'Owner', 'Active', 'Created', 'Updated', 'Description']
            rows = []

            for project in projects:
                rows.append([
                    project['id'],
                    project['name'],
                    project['project_type'],
                    project['owner'],
                    '✓' if project['active'] else '✗',
                    format_datetime(project['created_at']),
                    format_datetime(project['updated_at']),
                    (project.get('description') or '')[:50] + ('...' if project.get('description', '') and len(project['description']) > 50 else '')
                ])

            # Print table
            click.echo(tabulate(rows, headers=headers, tablefmt=output_format))
            click.echo(f"\nTotal projects: {len(projects)}")
        else:
            click.echo("No projects found")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@project.command()
@click.argument('project_id', type=int)
@click.pass_context
def show(ctx, project_id: int):
    """Show detailed information about a specific project."""
    db = ctx.obj['supabase']

    try:
        project = db.project_get(project_id)
        if not project:
            click.echo(f"Project with ID {project_id} not found", err=True)
            return

        # Basic information
        click.echo("\n=== Project Information ===")
        click.echo(f"ID: {project['id']}")
        click.echo(f"Name: {project['name']}")
        click.echo(f"Type: {project['project_type']}")
        click.echo(f"Owner: {project['owner']}")
        click.echo(f"Active: {'Yes' if project['active'] else 'No'}")

        if project['description']:
            click.echo(f"\nDescription: {project['description']}")

        # Timestamps
        click.echo("\n=== Timestamps ===")
        click.echo(f"Created: {format_datetime(project['created_at'])}")
        click.echo(f"Updated: {format_datetime(project['updated_at'])}")

        # Configuration
        if project.get('config'):
            click.echo("\n=== Configuration ===")
            click.echo(json.dumps(project['config'], indent=2))

        # Metadata
        if project.get('metadata'):
            click.echo("\n=== Metadata ===")
            click.echo(json.dumps(project['metadata'], indent=2))

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@project.command()
@click.argument('project_id', type=int)
@click.option('--fields', help='Comma-separated list of fields to sync for each source')
@click.option('--max-results', type=int, help='Maximum number of posts to fetch per source')
@click.pass_context
def sync_posts(ctx, project_id: int, fields: Optional[str], max_results: Optional[int]):
    """Sync posts from all active sources for an project."""
    db = ctx.obj['supabase']
    post_manager = ctx.obj['post_manager']

    try:
        # Get all active sources for the project
        sources = db.source_search(
            active=True,
            project_id=project_id
        )

        if not sources:
            click.echo(f"No active sources found for project {project_id}")
            return

        total_added = 0
        total_updated = 0
        field_list = fields.split(',') if fields else None

        # Process each source
        for source in sources:
            click.echo(f"\nProcessing source: {source['name']} (ID: {source['id']})")
            try:
                added, updated = post_manager.sync_posts(
                    source['id'],
                    field_list,
                    max_results
                )
                total_added += added
                total_updated += updated
                click.echo(f"- Added: {added}, Updated: {updated}")
            except Exception as e:
                traceback.print_exc()
                click.echo(f"Error processing source {source['id']}: {str(e)}", err=True)
                ctx.exit(1)

        click.echo(f"\nSync completed for {len(sources)} sources")
        click.echo(f"Total posts: {total_added} added, {total_updated} updated")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@project.command()
@click.argument('project_id', type=int)
@click.option('--max-retries', type=int, default=3,
              help='Maximum number of retries for dependency resolution')
@click.option('--last', type=str, help='Filter posts by time (e.g. "1d", "2h", "30m")')
@click.option('--offset', default=0, type=int, help='Offset for search results')
@click.option('--limit', default=50, type=int, help='Maximum number of posts to fetch')
@click.pass_context
def generate_bulk(ctx, project_id: int, max_retries: int, last: Optional[str],
                 offset: int, limit: int):
    """Generate bulk content for all active specifications of an project in dependency order."""
    db = ctx.obj['supabase']
    artifact_manager = ctx.obj['artifact_manager']

    try:
        # Get all active specifications for the project
        specs = db.specification_search(
            project_id=project_id,
            active=True
        )

        if not specs:
            click.echo(f"No active specifications found for project {project_id}")
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

            # Get posts needing artifacts
            time_filter = parse_date_filter(last) if last else None
            posts = db.post_search_with_artifacts(
                source_id=source_id,
                modified_after=time_filter,
                offset=offset,
                limit=limit
            )

            if not posts:
                click.echo(f"No posts found requiring artifact generation for source {source_id}")
                continue

            click.echo(f"{label}: Found {len(posts)} posts with artifacts")

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
                                                                                  posts,
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

@project.command()
@click.argument('project_id', type=int)
@click.argument('keywords', nargs=-1, required=True)
@click.option('--what', required=True,
              default='playlist',
              type=click.Choice(['playlist', 'channel']))
@click.option('--max-results', '-m', default=10, help='Maximum number of playlists to discover')
@click.pass_context
def discover_playlists(ctx, project_id: int, keywords: tuple, what: str,
                       max_results: int):
    """Discover YouTube playlists based on keywords and create sources for the project."""
    db = ctx.obj['supabase']

    try:
        # Get project
        project = db.project_get(project_id)
        if not project:
            click.echo(f"Project with ID {project_id} not found", err=True)
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
                    'project_id': project_id,
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

@project.command("search-similar")
@click.argument('project_id', type=int)
@click.argument('query', type=str)
@click.option('--threshold', type=float, default=0.7, help='Similarity threshold')
@click.option('--limit', type=int, default=10, help='Maximum results to return')
@click.pass_context
def search_similar(ctx, project_id: int, query: str, threshold: float, limit: int):
    """Search for similar artifacts across all sources in an project."""
    db = ctx.obj['supabase']
    artifact_manager = ctx.obj['artifact_manager']

    try:
        # Get all specs for the project
        specs = db.specification_search(
            project_id=project_id,
            active=True
        )

        if not specs:
            click.echo("No active specifications found for project")
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

@project.command('update-embeddings')
@click.argument('project_id', type=int)
@click.option('--batch-size', default=100, type=int, help='Number of artifacts to process in each batch')
@click.option('--status', help='Filter by artifact status')
@click.option('--force/--no-force', default=False, help='Update even if embedding exists')
@click.option('--dry-run/--no-dry-run', default=False, help='Show what would be updated without making changes')
@click.option('--last', type=str, help='Filter posts by time (e.g. "1d", "2h", "30m")')
@click.option('--offset', default=0, type=int, help='Offset for search results')
@click.option('--limit', default=50, type=int, help='Maximum number of posts to fetch')
@click.pass_context
def update_embeddings(ctx, project_id: int, batch_size: int, status: Optional[str],
                      force: bool, dry_run: bool,
                      last: Optional[str], offset: int, limit: int):
    """
    Bulk update embeddings for all artifacts under an project's sources.

    Example:
    carver project update-embeddings 123 --batch-size 50 --status published
    """
    db = ctx.obj['supabase']
    artifact_manager = ctx.obj['artifact_manager']

    try:
        # Get the project first
        project = db.project_get(project_id)
        if not project:
            click.echo(f"Project {project_id} not found", err=True)
            return

        click.echo(f"\nProcessing embeddings for project: {project['name']}")

        # Get all active specifications for the project's sources
        specs = db.specification_search(
            project_id=project_id,
            active=True
        )

        if not specs:
            click.echo("No active specifications found for project")
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
                print(f"[{spec['name']}] Found no artifacts to process")
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

@project.command()
@click.argument('project_id', type=int)
@click.pass_context
def update_analytics(ctx, project_id: int):
    """Update analytics metadata for all sources of an project."""
    db = ctx.obj['supabase']

    try:
        # Verify project exists
        project = db.project_get(project_id)
        if not project:
            click.echo(f"Project {project_id} not found", err=True)
            return

        click.echo(f"\nUpdating analytics for project: {project['name']} (ID: {project_id})")

        # Get all sources for this project
        sources = db.source_search(
            project_id=project_id,
            active=True,
            fields=['id', 'name']
        )

        if not sources:
            click.echo("No sources found for this project")
            return

        click.echo(f"Found {len(sources)} sources to process")

        project_metrics = {
            'last_update': datetime.utcnow().isoformat(),
            'sources_count': len(sources),
            'sources': {},
            'totals': {
                'posts': 0,
                'artifacts': 0,
                'specifications': 0
            }
        }

        # Process each source
        with click.progressbar(sources, label='Processing sources') as source_list:
            for source in source_list:
                # Update analytics for this source
                updated_source = db.source_update_analytics(source['id'])

                if updated_source and updated_source.get('analysis_metadata'):
                    metrics = updated_source['analysis_metadata']['metrics']

                    # Accumulate totals for project-level metrics
                    project_metrics['totals']['posts'] += metrics['counts']['posts']
                    project_metrics['totals']['artifacts'] += metrics['counts']['artifacts']
                    project_metrics['totals']['specifications'] += metrics['counts']['specifications']

                    # Store summarized metrics for this source
                    project_metrics['sources'][source['id']] = {
                        'name': source['name'],
                        'counts': metrics['counts']
                    }

        # Update project metadata
        project_analytics = {
            'metrics': project_metrics,
        }

        project = db.project_update_metadata(project_id, project_analytics)

        if project:
            # Print summary
            click.echo("\nAnalytics update completed:")
            click.echo(f"- Total Sources: {project_metrics['sources_count']}")
            click.echo(f"- Total Posts: {project_metrics['totals']['posts']}")
            click.echo(f"- Total Artifacts: {project_metrics['totals']['artifacts']}")
            click.echo(f"- Total Specifications: {project_metrics['totals']['specifications']}")
        else:
            click.echo("Error updating project analytics", err=True)

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

