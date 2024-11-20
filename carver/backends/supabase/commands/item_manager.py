import os
import sys
import json

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from dateutil import parser as dateparser

from carver.feeds.base import FeedReader

class ItemManager:
    """Manages item operations including sync with feeds"""

    def __init__(self, db_client):
        self.db = db_client

    def sync_items(self, source_id: int,
                  fields: Optional[List[str]] = None,
                  max_results: Optional[int] = None) -> Tuple[int, int]:
        """
        Sync items for a source
        Returns tuple of (items_added, items_updated)
        """
        # Get source information
        source = self.db.source_get(source_id)
        if not source:
            raise ValueError(f"Source {source_id} not found")

        # Get appropriate reader with max_results
        reader = FeedReader.get_reader(source, max_results)

        # Get existing items
        existing_items = self.db.item_search(
            source_id=source_id,
            limit=10000,
            active=True,
            fields=fields or ['id', 'content_identifier', 'updated_at', 'title', 'description', 'content', 'published_at']
        )

        # Create lookup of existing items
        existing_map = {
            item['content_identifier']: item
            for item in existing_items
        }

        print("Existing map", len(existing_map))

        # Read feed
        new_items = reader.read()

        # Split into updates and creates
        to_create = []
        to_update = []

        seen = {}

        for item in new_items:

            # Fix missing columns
            item['name'] = item['title']

            # Now continue
            content_id = item['content_identifier']

            # Ensure that there are no duplicates
            if content_id in seen:
                print("Seen", content_id)
                continue
            seen[content_id] = 1

            if content_id in existing_map:
                d1 = dateparser.parse(item['published_at'])
                d2 = dateparser.parse(existing_map[content_id]['published_at'])
                change = ((item['title'] != existing_map[content_id]['title']) or
                          (item['description'] != existing_map[content_id]['description']) or
                          (d1 != d2))
                if change:
                    item['id'] = existing_map[content_id]['id']
                    to_update.append(item)
            else:
                # New item
                to_create.append(item)

        print(f"To create: {len(to_create)}")
        print(f"To update: {len(to_update)}")

        # Perform bulk operations
        created = self.db.item_bulk_create(to_create)
        updated = self.db.item_bulk_update(to_update)

        # Update source last_crawled timestamp
        reader.update_source_metadata(self.db)

        return len(created), len(updated)

    def bulk_activate(self, source_id: int, content_identifiers: List[str]) -> int:
        """Activate multiple items by their content identifiers"""
        items = self.db.item_search(
            source_id=source_id,
            fields=['id', 'content_identifier']
        )

        # Filter items to update
        to_update = [
            {'id': item['id'], 'active': True}
            for item in items
            if item['content_identifier'] in content_identifiers
        ]

        updated = self.db.item_bulk_update(to_update)
        return len(updated)

    def bulk_deactivate(self, source_id: int, content_identifiers: List[str]) -> int:
        """Deactivate multiple items by their content identifiers"""
        items = self.db.item_search(
            source_id=source_id,
            fields=['id', 'content_identifier']
        )

        # Filter items to update
        to_update = [
            {'id': item['id'], 'active': False}
            for item in items
            if item['content_identifier'] in content_identifiers
        ]

        updated = self.db.item_bulk_update(to_update)
        return len(updated)
