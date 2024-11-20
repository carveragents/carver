import os
import sys
import json
import click
from typing import Optional
from datetime import datetime
import traceback
from tabulate import tabulate

from ..utils import format_datetime, parse_date_filter
from .item_manager import ItemManager

@click.group()
@click.pass_context
def item(ctx):
    """Manage items in the system."""
    ctx.obj['manager'] = ItemManager(ctx.obj['supabase'])

@item.command()
@click.option('--source-id', required=True, type=int, help='Source ID')
@click.option('--fields', help='Comma-separated list of fields to sync')
@click.option('--max-results', type=int, help='Maximum number of items to fetch')
@click.pass_context
def sync(ctx, source_id: int, fields: Optional[str], max_results: Optional[int]):
    """Sync items from source feed"""
    manager = ctx.obj['manager']

    try:
        field_list = fields.split(',') if fields else None
        added, updated = manager.sync_items(source_id, field_list, max_results)
        click.echo(f"Successfully synced items: {added} added, {updated} updated")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error syncing items: {str(e)}", err=True)

@item.command()
@click.option('--source-id', type=int, required=True, help='Source ID')
@click.option('--identifiers', required=True, help='Comma-separated list of content identifiers')
@click.pass_context
def activate(ctx, source_id: int, identifiers: str):
    """Activate specific items by their content identifiers"""
    manager = ctx.obj['manager']

    try:
        id_list = [i.strip() for i in identifiers.split(',')]
        updated = manager.bulk_activate(source_id, id_list)
        click.echo(f"Successfully activated {updated} items")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error activating items: {str(e)}", err=True)

@item.command()
@click.option('--source-id', type=int, required=True, help='Source ID')
@click.option('--identifiers', required=True, help='Comma-separated list of content identifiers')
@click.pass_context
def deactivate(ctx, source_id: int, identifiers: str):
    """Deactivate specific items by their content identifiers"""
    manager = ctx.obj['manager']

    try:
        id_list = [i.strip() for i in identifiers.split(',')]
        updated = manager.bulk_deactivate(source_id, id_list)
        click.echo(f"Successfully deactivated {updated} items")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error deactivating items: {str(e)}", err=True)

@item.command()
@click.option('--source-id', type=int, help='Filter by source ID')
@click.option('--content-type', help='Filter by content type')
@click.option('--author', help='Filter by author')
@click.option('--active/--inactive', default=None, help='Filter by active status')
@click.option('--processed/--unprocessed', default=None, help='Filter by processed status')
@click.option('--published-since', help='Show items published since (ISO date or relative like "1d", "1w")')
@click.option('--acquired-since', help='Show items acquired since (ISO date or relative like "1d", "1w")')
@click.option('--title-search', help='Search in titles')
@click.option('--tags-search', help='Search in tags')
@click.option('--limit', type=int, default=100, help='Number of items to return')
@click.option('--offset', type=int, default=0, help='Number of items to skip')
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
    """Search items with various filters."""
    db = ctx.obj['supabase']

    try:
        # Parse date filters
        published_since_dt = parse_date_filter(published_since) if published_since else None
        acquired_since_dt = parse_date_filter(acquired_since) if acquired_since else None

        # Search items
        items = db.item_search(
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

        if items:
            # Prepare table data
            headers = ['ID', "Content ID", 'Source', 'Title', 'Type', 'Author', 'A', 'P', 'Published', 'Updated']
            rows = []

            for item in items:
                source_name = item['carver_source']['name'] if item.get('carver_source') else 'N/A'
                rows.append([
                    item['id'],
                    item['content_identifier'][-20:],
                    f"{source_name[:20]} ({item['source_id']})",
                    item['title'][:30] + ('...' if len(item.get('title', '')) > 50 else ''),
                    item['content_type'],
                    item.get('author', 'N/A')[:20],
                    '✓' if item['active'] else '✗',
                    '✓' if item['is_processed'] else '✗',
                    format_datetime(item['published_at']) if item.get('published_at') else 'N/A',
                    format_datetime(item['updated_at'])
                ])

            # Print table
            click.echo("Note: A = Active, P=Processed")
            click.echo(tabulate(rows, headers=headers, tablefmt=output_format))
            click.echo(f"\nTotal items: {len(items)}")
            if len(items) == limit:
                click.echo(f"Note: Result limit reached. Use --offset {offset + limit} to see more.")
        else:
            click.echo("No items found")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@item.command()
@click.argument('item_id', type=int)
@click.pass_context
def show(ctx, item_id: int):
    """Show detailed information about a specific item."""
    db = ctx.obj['supabase']

    try:
        item = db.item_get(item_id)
        if not item:
            click.echo(f"Item with ID {item_id} not found", err=True)
            return

        source = item.get('carver_source', {})

        # Basic information
        click.echo("\n=== Item Information ===")
        click.echo(f"ID: {item['id']}")
        click.echo(f"Source: {source.get('name', 'N/A')} (ID: {item['source_id']})")
        click.echo(f"Content Type: {item['content_type']}")
        click.echo(f"Content Identifier: {item['content_identifier']}")
        click.echo(f"Title: {item['title']}")
        click.echo(f"Author: {item.get('author', 'N/A')}")
        click.echo(f"URL: {item['url']}")
        click.echo(f"Active: {'Yes' if item['active'] else 'No'}")
        click.echo(f"Processed: {'Yes' if item['is_processed'] else 'No'}")

        if item.get('content'):
            click.echo(f"\nContent:")
            click.echo(item['content'])

        if item.get('description'):
            click.echo(f"\nDescription:")
            click.echo(item['description'])

        # Media information
        if item.get('media_type') or item.get('media_url') or item.get('thumbnail_url'):
            click.echo("\n=== Media Information ===")
            if item.get('media_type'):
                click.echo(f"Media Type: {item['media_type']}")
            if item.get('media_url'):
                click.echo(f"Media URL: {item['media_url']}")
            if item.get('thumbnail_url'):
                click.echo(f"Thumbnail URL: {item['thumbnail_url']}")
            if item.get('duration'):
                click.echo(f"Duration: {item['duration']}")

        # Timestamps
        click.echo("\n=== Timestamps ===")
        click.echo(f"Published: {format_datetime(item['published_at']) if item.get('published_at') else 'N/A'}")
        click.echo(f"Last Updated: {format_datetime(item['last_updated_at']) if item.get('last_updated_at') else 'N/A'}")
        click.echo(f"Acquired: {format_datetime(item['acquired_at'])}")
        click.echo(f"Updated: {format_datetime(item['updated_at'])}")

        # Content metrics
        if item.get('content_metrics'):
            click.echo("\n=== Content Metrics ===")
            click.echo(json.dumps(item['content_metrics'], indent=2))

        # Analysis metadata
        if item.get('analysis_metadata'):
            click.echo("\n=== Analysis Metadata ===")
            click.echo(json.dumps(item['analysis_metadata'], indent=2))

        # Tags and categories
        if item.get('tags') or item.get('categories'):
            click.echo("\n=== Classification ===")
            if item.get('tags'):
                click.echo(f"Tags: {item['tags']}")
            if item.get('categories'):
                click.echo(f"Categories: {item['categories']}")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)
