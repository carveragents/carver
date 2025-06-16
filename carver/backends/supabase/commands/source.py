import os
import sys
import json
import traceback

from typing import Optional
from datetime import datetime

import click
from tabulate import tabulate

from .post_manager import PostManager
from .artifact_manager import ArtifactManager
from .source_manager import SourceManager

from ..utils import *
from carver.utils import *

PLATFORM_CHOICES = ['TWITTER', 'GITHUB', 'YOUTUBE', 'RSS', 'WEB', 'SUBSTACK', "EXA"]
SOURCE_TYPE_CHOICES = ['FEED', 'PROFILE', 'CHANNEL', 'REPOSITORY', 'PAGE', "NEWSLETTER", "SEARCH"]

@click.group()
@click.pass_context
def source(ctx):
    """Manage sources in the system."""
    ctx.obj['post_manager'] = PostManager(ctx.obj['supabase'])
    ctx.obj['artifact_manager'] = ArtifactManager(ctx.obj['supabase'])
    ctx.obj['source_manager'] = SourceManager(ctx.obj['supabase'])

@source.command()
@click.option('--url', required=True, help='URL of the source')
@click.option('--project-id', required=True, type=int, help='ID of the parent project')
@click.option('--name', help='Override the automatically inferred name')
@click.option('--description', help='Description of the source')
@click.option('--config', type=str, help='Additional JSON configuration to merge')
@click.pass_context
def add(ctx, url: str, project_id: int, name: Optional[str],
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
            'project_id': project_id,
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

@source.command('add-with-template')
@click.argument('project_id', type=int)
@click.argument('template_name')
@click.option('--name', help='Override the automatically inferred name')
@click.option('--description', help='Description of the source')
@click.pass_context
def add_with_template(ctx, project_id: int, template_name: str,
                     name: Optional[str], description: Optional[str]):
    """Add a new source using a template."""
    db = ctx.obj['supabase']

    try:

        # Load template
        template = load_template(model="source", name=template_name)

        source = template['specifications'][0]
        source.pop('id',None)

        # Allow override of inferred name
        if name:
            source['name'] = name

        # Add description if provided
        if description:
            source['description'] = description

        # Add required fields
        now = datetime.utcnow()
        source.update({
            'active': True,
            'project_id': project_id,
            'analysis_metadata': {},
            'created_at': now.isoformat(),
            'updated_at': now.isoformat()
        })

        # Create the source
        source = db.source_create(source)

        if source:
            click.echo(f"Successfully created source: {source['name']} (ID: {source['id']})")
            click.echo("\nSource details:")
            click.echo(f"Platform: {source['platform']}")
            click.echo(f"Type: {source['source_type']}")
            click.echo(f"Template: {template_name}")
        else:
            click.echo("Error creating source", err=True)

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
@click.option('--project-id', required=True, type=int, help='ID of the parent project')
@click.option('--url', help='New URL')
@click.option('--config', help='New JSON configuration')
@click.option('--metadata', help='JSON metadata to update/add')
@click.pass_context
def update(ctx, source_id: int, activate: bool, deactivate: bool,
           name: Optional[str], description: Optional[str],
           project_id: Optional[int],
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
        if project_id:
            update_data['project_id'] = project_id
        if source_type:
            update_data['source_type'] = source_type.strip().upper()
        if source_identifier:
            update_data['source_identifier'] = source_identifier
        if url:
            update_data['url'] = url
        if config:
            try:
                update_data['config'] = json.load(open(config))
            except:
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
@click.option('--project-id', type=int, help='Filter by project ID')
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
def search(ctx, active: Optional[bool], project_id: Optional[int],
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
            project_id=project_id,
            platform=platform,
            source_type=source_type,
            name=search,
            updated_since=updated_since_dt,
            crawled_since=crawled_since_dt
        )

        if sources:
            # Prepare table data
            headers = ['ID', 'Name', 'Project', 'Platform', 'Type', 'Active', 'Last Crawled', 'Updated']
            rows = []

            for source in sources:
                project_name = source['carver_project']['name'] if source['carver_project'] else 'N/A'
                rows.append([
                    source['id'],
                    source['name'][:30] + ('...' if len(source['name']) > 30 else ''),
                    f"{project_name} ({source['project_id']})",
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

        project = source['carver_project']

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

        # Project information
        click.echo("\n=== Parent Project ===")
        click.echo(f"ID: {project['id']}")
        click.echo(f"Name: {project['name']}")
        click.echo(f"Type: {project['project_type']}")
        click.echo(f"Owner: {project['owner']}")

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
@click.option('--max-results', type=int, help='Maximum number of posts to fetch')
@click.pass_context
def sync_posts(ctx, source_id: int, fields: Optional[str], max_results: Optional[int]):
    """Sync posts from a specific source."""
    db = ctx.obj['supabase']
    post_manager = ctx.obj['post_manager']

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

        click.echo(f"\nSyncing posts for source: {source['name']} (ID: {source_id})")

        field_list = fields.split(',') if fields else None
        try:
            added, updated = post_manager.sync_posts(source_id, field_list, max_results)
            click.echo(f"Successfully synced posts:")
            click.echo(f"- Added: {added}")
            click.echo(f"- Updated: {updated}")
        except Exception as e:
            traceback.print_exc()
            click.echo(f"Error syncing posts: {str(e)}", err=True)

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
        updated_source = db.source_update_analytics(source_id)

        if updated_source and updated_source.get('analysis_metadata'):
            metrics = updated_source['analysis_metadata']['metrics']
            click.echo("\nAnalytics updated successfully:")
            click.echo(f"- Active Posts: {metrics['counts']['posts']}")
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

@source.command()
@click.argument('source_id', type=int)
@click.option('--max-retries', type=int, default=3,
              help='Maximum number of retries for dependency resolution')
@click.option('--last', type=str, help='Filter posts by time (e.g. "1d", "2h", "30m")')
@click.option('--offset', default=0, type=int, help='Offset for search results')
@click.option('--limit', default=50, type=int, help='Maximum number of posts to fetch')
@click.option('--generator-name', type=str, help='Optional generator name to filter specifications')
@click.pass_context
def generate_bulk(ctx, source_id: int, max_retries: int, last: Optional[str],
                 offset: int, limit: int, generator_name: Optional[str]):
    """Generate bulk content for all active specifications of a source in dependency order."""
    db = ctx.obj['supabase']
    artifact_manager = ctx.obj['artifact_manager']

    try:
        # Get source details
        source = db.source_get(source_id)
        if not source:
            click.echo(f"Source {source_id} not found")
            return

        # Get all active specifications for the source
        specs = db.specification_search(
            source_id=source_id,
            active=True
        )

        if not specs:
            click.echo("No active specifications found for source")
            return

        # Filter specs by generator if specified
        if generator_name:
            specs = [s for s in specs if s['config'].get('generator') == generator_name]
            if not specs:
                click.echo(f"No active specifications found for generator {generator_name}")
                return

        click.echo(f"Found {len(specs)} active specifications")

        # Sort specifications by dependencies
        try:
            sorted_specs_ids = topological_sort(specs)
        except ValueError as e:
            click.echo(f"Error in dependency resolution: {str(e)}", err=True)
            return

        # Create a map of spec ID to spec data
        spec_map = {spec['id']: spec for spec in specs}

        # Get posts needing artifacts
        time_filter = parse_date_filter(last) if last else None
        posts = db.post_search_with_artifacts(
            source_id=source_id,
            modified_after=time_filter,
            offset=offset,
            limit=limit
        )

        if not posts:
            click.echo("No posts found requiring artifact generation")
            return

        click.echo(f"Found {len(posts)} posts to process")

        # Process specifications in dependency order
        total_generated = 0
        failed_specs = []

        label = f"[{source['name']}]"

        for spec_id in sorted_specs_ids:
            if spec_id not in spec_map:
                continue

            spec = spec_map[spec_id]
            click.echo(f"\n{label} Processing Specification [{spec_id}] {spec['name']}")

            retry_count = 0
            success = False

            while retry_count < max_retries and not success:
                try:
                    results = artifact_manager.artifact_bulk_create_from_spec(
                        spec,
                        posts,
                        None  # Use default generator from spec
                    )
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

@source.command()
@click.argument('source_id', type=int)
@click.option('--batch-size', default=50, type=int,
              help='Number of posts to process per batch')
@click.option('--last', type=str,
              help='Filter posts by time (e.g. "1d", "2h", "30m")')
@click.option('--spec-id', type=int,
              help='Specific knowledge graph specification ID to use')
@click.pass_context
def generate_knowledge_graph(ctx, source_id: int, batch_size: int,
                           last: Optional[str], spec_id: Optional[int]):
    """Generate knowledge graph from source content."""
    source_manager = ctx.obj['source_manager']

    try:
        # Get source details
        source = source_manager.db.source_get(source_id)
        if not source:
            click.echo(f"Source {source_id} not found")
            return

        click.echo(f"\nProcessing knowledge graph for source: {source['name']}")

        # Parse time filter if provided
        time_filter = parse_date_filter(last) if last else None

        print("Calling generate knowledge graph")

        # Generate graph
        result = source_manager.generate_knowledge_graph(
            source_id=source_id,
            batch_size=batch_size,
            last_modified=time_filter,
            spec_id=spec_id
        )

        if result['status'] == 'success':
            click.echo("\nKnowledge graph generated successfully!")
            click.echo("\nStats:")
            click.echo(f"- Nodes: {result['stats']['nodes']}")
            click.echo(f"- Edges: {result['stats']['edges']}")
            click.echo(f"- Documents: {result['stats']['documents']}")
            click.echo(f"- Using specification: {result['spec_id']}")
        else:
            click.echo(f"\n{result['message']}")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)


@source.command()
@click.argument('source_ids', nargs=-1, type=int, required=True)
@click.pass_context
def activate(ctx, source_ids):
    """Activate one or more sources by their IDs."""
    source_manager = ctx.obj['source_manager']

    db = ctx.obj['supabase']

    try:
        updated = db.source_bulk_update_flag(list(source_ids),
                                                  active=True)
        click.echo(f"Successfully activated {updated} sources")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error activating sources: {str(e)}", err=True)

@source.command()
@click.argument('source_ids', nargs=-1, type=int, required=True)
@click.pass_context
def deactivate(ctx, source_ids):
    """Deactivate one or more sources by their IDs."""

    db = ctx.obj['supabase']
    try:
        updated = db.source_bulk_update_flag(list(source_ids),
                                             active=False)
        click.echo(f"Successfully deactivated {updated} sources")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error deactivating sources: {str(e)}", err=True)
