import os
import sys
import json
import click
from typing import Optional
from datetime import datetime
import traceback
from tabulate import tabulate

from carver.utils import format_datetime, parse_date_filter
from .post_manager import PostManager

@click.group()
@click.pass_context
def post(ctx):
    """Manage posts in the system."""
    ctx.obj['post_manager'] = PostManager(ctx.obj['supabase'])

@post.command()
@click.option('--source-id', required=True, type=int, help='Source ID')
@click.option('--fields', help='Comma-separated list of fields to sync')
@click.option('--max-results', type=int, help='Maximum number of posts to fetch')
@click.pass_context
def sync(ctx, source_id: int, fields: Optional[str], max_results: Optional[int]):
    """Sync posts from source feed"""
    manager = ctx.obj['post_manager']

    try:
        field_list = fields.split(',') if fields else None
        added, updated = manager.sync_posts(source_id, field_list, max_results)
        click.echo(f"Successfully synced posts: {added} added, {updated} updated")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error syncing posts: {str(e)}", err=True)

@post.command()
@click.option('--source-id', type=int, required=True, help='Source ID')
@click.option('--identifiers', required=True, help='Comma-separated list of content identifiers')
@click.pass_context
def activate_by_content(ctx, source_id: int, identifiers: str):
    """Activate specific posts by their content identifiers"""
    manager = ctx.obj['post_manager']

    try:
        id_list = [i.strip() for i in identifiers.split(',')]
        updated = manager.bulk_activate_by_content(source_id, id_list)
        click.echo(f"Successfully activated {updated} posts")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error activating posts: {str(e)}", err=True)

@post.command()
@click.option('--source-id', type=int, required=True, help='Source ID')
@click.option('--identifiers', required=True, help='Comma-separated list of content identifiers')
@click.pass_context
def deactivate_by_content(ctx, source_id: int, identifiers: str):
    """Deactivate specific posts by their content identifiers"""
    manager = ctx.obj['post_manager']

    try:
        id_list = [i.strip() for i in identifiers.split(',')]
        updated = manager.bulk_deactivate_by_content(source_id, id_list)
        click.echo(f"Successfully deactivated {updated} posts")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error deactivating posts: {str(e)}", err=True)

@post.command()
@click.option('--source-id', type=int, help='Filter by source ID')
@click.option('--content-type', help='Filter by content type')
@click.option('--author', help='Filter by author')
@click.option('--active/--inactive', default=None, help='Filter by active status')
@click.option('--processed/--unprocessed', default=None, help='Filter by processed status')
@click.option('--published-since', help='Show posts published since (ISO date or relative like "1d", "1w")')
@click.option('--acquired-since', help='Show posts acquired since (ISO date or relative like "1d", "1w")')
@click.option('--title-search', help='Search in titles')
@click.option('--tags-search', help='Search in tags')
@click.option('--limit', type=int, default=20, help='Number of posts to return')
@click.option('--offset', type=int, default=0, help='Number of posts to skip')
@click.option('--format', 'output_format',
              type=click.Choice(['table', 'grid', 'pipe', 'orgtbl', 'rst', 'mediawiki', 'html']),
              default='table',
              help='Output format for the table')
@click.pass_context
def search(ctx, source_id: Optional[int], content_type: Optional[str],
           author: Optional[str], active: Optional[bool], processed: Optional[bool],
           published_since: Optional[str], acquired_since: Optional[str],
           title_search: Optional[str], tags_search: Optional[str],
           limit: int, offset: int, output_format: str):
    """Search posts with various filters."""
    db = ctx.obj['supabase']

    try:
        # Parse date filters
        published_since_dt = parse_date_filter(published_since) if published_since else None
        acquired_since_dt = parse_date_filter(acquired_since) if acquired_since else None

        # Search posts
        posts = db.post_search(
            source_id=source_id,
            content_type=content_type,
            author=author,
            active=active,
            is_processed=processed,
            published_since=published_since_dt,
            acquired_since=acquired_since_dt,
            title_search=title_search,
            tags_search=tags_search,
            limit=limit,
            offset=offset
        )

        if posts:
            # Prepare table data
            headers = ['ID',
                       # "Content ID",
                       'Source', 'Title', 'Type', 'Author',
                       'A', 'P', 'Published', 'Updated']
            rows = []

            for post in posts:
                source_name = post['carver_source']['name'] if post.get('carver_source') else 'N/A'
                rows.append([
                    post['id'],
                    #post['content_identifier'][-20:],
                    f"{source_name[:12]} ({post['source_id']})",
                    post['title'][:30] + ('...' if len(post.get('title', '')) > 50 else ''),
                    post['content_type'],
                    post.get('author', 'N/A')[:20],
                    '✓' if post['active'] else '✗',
                    '✓' if post['is_processed'] else '✗',
                    format_datetime(post['published_at']) if post.get('published_at') else 'N/A',
                    format_datetime(post['updated_at'])
                ])

            # Print table
            click.echo("Note: A = Active, P=Processed")
            click.echo(tabulate(rows, headers=headers, tablefmt=output_format))
            click.echo(f"\nTotal posts: {len(posts)}")
            if len(posts) == limit:
                click.echo(f"Note: Result limit reached. Use --offset {offset + limit} to see more.")
        else:
            click.echo("No posts found")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@post.command()
@click.argument('post_id', type=int)
@click.pass_context
def show(ctx, post_id: int):
    """Show detailed information about a specific post."""
    db = ctx.obj['supabase']

    try:
        post = db.post_get(post_id)
        if not post:
            click.echo(f"Post with ID {post_id} not found", err=True)
            return

        source = post.get('carver_source', {})

        # Basic information
        click.echo("\n=== Post Information ===")
        click.echo(f"ID: {post['id']}")
        click.echo(f"Source: {source.get('name', 'N/A')} (ID: {post['source_id']})")
        click.echo(f"Content Type: {post['content_type']}")
        click.echo(f"Content Identifier: {post['content_identifier']}")
        click.echo(f"Title: {post['title']}")
        click.echo(f"Author: {post.get('author', 'N/A')}")
        click.echo(f"URL: {post['url']}")
        click.echo(f"Active: {'Yes' if post['active'] else 'No'}")
        click.echo(f"Processed: {'Yes' if post['is_processed'] else 'No'}")

        if post.get('content'):
            click.echo(f"\nContent:")
            click.echo(post['content'])

        if post.get('description'):
            click.echo(f"\nDescription:")
            click.echo(post['description'])

        # Media information
        if post.get('media_type') or post.get('media_url') or post.get('thumbnail_url'):
            click.echo("\n=== Media Information ===")
            if post.get('media_type'):
                click.echo(f"Media Type: {post['media_type']}")
            if post.get('media_url'):
                click.echo(f"Media URL: {post['media_url']}")
            if post.get('thumbnail_url'):
                click.echo(f"Thumbnail URL: {post['thumbnail_url']}")
            if post.get('duration'):
                click.echo(f"Duration: {post['duration']}")

        # Timestamps
        click.echo("\n=== Timestamps ===")
        click.echo(f"Published: {format_datetime(post['published_at']) if post.get('published_at') else 'N/A'}")
        click.echo(f"Last Updated: {format_datetime(post['last_updated_at']) if post.get('last_updated_at') else 'N/A'}")
        click.echo(f"Acquired: {format_datetime(post['acquired_at'])}")
        click.echo(f"Updated: {format_datetime(post['updated_at'])}")

        # Content metrics
        if post.get('content_metrics'):
            click.echo("\n=== Content Metrics ===")
            click.echo(json.dumps(post['content_metrics'], indent=2))

        # Analysis metadata
        if post.get('analysis_metadata'):
            click.echo("\n=== Analysis Metadata ===")
            click.echo(json.dumps(post['analysis_metadata'], indent=2))

        # Tags and categories
        if post.get('tags') or post.get('categories'):
            click.echo("\n=== Classification ===")
            if post.get('tags'):
                click.echo(f"Tags: {post['tags']}")
            if post.get('categories'):
                click.echo(f"Categories: {post['categories']}")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@post.command()
@click.option('--source-id', required=True, type=int, help='Source ID')
@click.pass_context
def bulk_deactivate_by_source(ctx, source_id: int):
    """Deactivate all posts for a source"""
    manager = ctx.obj['post_manager']

    try:
        # Get all active posts for the source
        posts = manager.db.post_search(
            source_id=source_id,
            active=True,
            fields=['id'],
            limit=1000,
        )

        if not posts:
            click.echo(f"No active posts found for source {source_id}")
            return

        # Create update data for all posts
        post_ids = [post['id'] for post in posts]
        updated = manager.db.post_bulk_update_flag(post_ids, active=False)

        click.echo(f"Successfully deactivated {len(updated)} posts")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error deactivating posts: {str(e)}", err=True)

