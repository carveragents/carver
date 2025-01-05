import os
import sys
import json

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from dateutil import parser as dateparser

from carver.feeds.base import FeedReader

class PostManager:
    """Manages post operations including sync with feeds"""

    def __init__(self, db_client):
        self.db = db_client

    def sync_posts(self, source_id: int,
                  fields: Optional[List[str]] = None,
                  max_results: Optional[int] = None) -> Tuple[int, int]:
        """
        Sync posts for a source
        Returns tuple of (posts_added, posts_updated)
        """

        now = datetime.utcnow().isoformat()

        # Get source information
        source = self.db.source_get(source_id)
        if not source:
            raise ValueError(f"Source {source_id} not found")

        # Get appropriate reader with max_results
        reader = FeedReader.get_reader(source, max_results)

        # Get existing posts
        existing_posts = self.db.post_search(
            source_id=source_id,
            limit=10000,
            active=True,
            fields=fields or ['id', 'content_identifier', 'updated_at', 'title', 'description', 'published_at']
        )

        # Create lookup of existing posts
        existing_map = {
            post['content_identifier']: post
            for post in existing_posts
        }

        print("Existing map", len(existing_map))

        # Read feed
        new_posts = reader.read()

        # Split into updates and creates
        to_create = []
        to_update = []

        seen = {}

        for post in new_posts:

            if 'created_at' not in post:
                post['created_at'] = now

            if 'updated_at' not in post:
                post['updated_at'] = now

            # Fix missing columns
            if 'name' not in post:
                post['name'] = post['title']
            post['name'] = post['name'][:255]
            post['title'] = post['title'][:500]

            # Ensure that there are no duplicates
            content_id = post['content_identifier']
            if content_id in seen:
                continue
            seen[content_id] = 1

            # Now continue
            if 'author' in post and post['author'] and len(post['author']) > 255:
                post['author'] = post['author'][:255]

            if content_id in existing_map:
                d1 = dateparser.parse(post['published_at'])
                d2 = dateparser.parse(existing_map[content_id]['published_at'])
                change = ((post['title'] != existing_map[content_id]['title']) or
                          #(post['description'] != existing_map[content_id]['description']) or
                          (d1 != d2))
                #print(post['title'],
                #       existing_map[content_id]['title'],
                #       (post['title'] != existing_map[content_id]['title']))
                # print(d1, d2, (d1 != d2))
                if change:
                    post['id'] = existing_map[content_id]['id']
                    to_update.append(post)
            else:
                # New post
                to_create.append(post)

        print(f"To create: {len(to_create)}")
        print(f"To update: {len(to_update)}")

        # Perform bulk operations
        created = self.db.post_bulk_create(to_create)
        updated = self.db.post_bulk_update(to_update)

        # Update source last_crawled timestamp
        reader.update_source_metadata(self.db)

        return len(created), len(updated)

    def bulk_activate_by_content(self, source_id: int, content_identifiers: List[str]) -> int:
        """Activate multiple posts by their content identifiers"""
        posts = self.db.post_search(
            source_id=source_id,
            fields=['id', 'content_identifier']
        )

        # Filter posts to update
        to_update = [
            {'id': post['id'], 'active': True}
            for post in posts
            if post['content_identifier'] in content_identifiers
        ]

        updated = self.db.post_bulk_update(to_update)
        return len(updated)

    def bulk_deactivate_by_content(self, source_id: int, content_identifiers: List[str]) -> int:
        """Deactivate multiple posts by their content identifiers"""
        posts = self.db.post_search(
            source_id=source_id,
            fields=['id', 'content_identifier']
        )

        # Filter posts to update
        to_update = [
            {'id': post['id'], 'active': False}
            for post in posts
            if post['content_identifier'] in content_identifiers
        ]

        updated = self.db.post_bulk_update(to_update)
        return len(updated)

    def bulk_deactivate_by_source(self, source_id: int) -> int:
        """Deactivate all posts for a source"""
        posts = self.db.post_search(
            source_id=source_id,
            active=True,
            fields=['id']
        )

        if not posts:
            return 0

        post_ids = [post['id'] for post in posts]
        return self.db.post_bulk_update_flag(post_ids, active=False)

