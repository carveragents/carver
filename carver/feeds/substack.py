import logging
import traceback
from typing import List, Dict, Any, Optional
from datetime import datetime

from newspaper import fulltext
from substack_api import newsletter
from dateutil import parser

from .base import FeedReader

logger = logging.getLogger(__name__)

class SubstackReader(FeedReader):
    """Reader for Substack newsletters"""

    def get_content_identifier(self, item: Dict[str, Any]) -> str:
        """Get unique identifier for a Substack post"""
        # Use the slug as the unique identifier
        return item.get('slug', '')

    def prepare_item(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Substack post to database item"""
        base_item = super().prepare_item(raw_item)

        # Get full content if available
        content = self._get_content(raw_item)
        summary = raw_item.get('description', '') or raw_item.get('subtitle', '')

        # Extract post metrics
        duration = raw_item.get('podcast_duration', 0)
        has_audio = ((duration is not None) and (int(duration)> 0))
        metrics = {
            'word_count': raw_item.get('word_count', 0),
            'like_count': raw_item.get('like_count', 0),
            'comment_count': raw_item.get('comment_count', 0),
            'is_premium': raw_item.get('audience', '') == 'only_paid',
            'has_audio': has_audio
        }

        base_item.update({
            'content_type': 'POST',
            'title': raw_item.get('title', 'Untitled'),
            'description': summary,
            'content': content,
            'author': raw_item.get('author', {}).get('name', ''),
            'published_at': self._parse_date(raw_item.get('post_date', '')),
            'url': f"https://{raw_item['subdomain']}.substack.com/p/{raw_item['slug']}",
            'language': raw_item.get('language', 'en'),
            'content_metrics': metrics,
            'tags': ','.join(raw_item.get('tags', []))
        })

        return base_item

    def read(self) -> List[Dict[str, Any]]:
        """Read Substack newsletters"""
        try:

            name = self.source['source_identifier']

            items = []
            try:
                # Get metadata for posts
                items = newsletter.get_newsletter_post_metadata(
                    name,
                    start_offset=0,
                    end_offset=self.max_results if self.max_results else 30
                )

                # Add subdomain to each post for URL construction
                for post in items:
                    post['subdomain'] = name

            except Exception as e:
                logger.error(f"Error reading Substack newsletter {substack_name}: {str(e)}")
                raise

            # Sort by publication date and limit results
            items.sort(key=lambda x: x.get('post_date', ''), reverse=True)
            if self.max_results:
                items = items[:self.max_results]

            return [self.prepare_item(item) for item in items]

        except Exception as e:
            logger.error(f"Error reading Substack feeds: {str(e)}")
            raise

    def _get_content(self, item: Dict[str, Any]) -> str:
        """Extract and clean content from Substack post"""
        try:
            # Get full HTML content
            html = newsletter.get_post_contents(
                item['subdomain'],
                item['slug'],
                html_only=True
            )

            # Extract text content
            content = fulltext(html)

            return content

        except Exception as e:
            traceback.print_exc()
            logger.warning(f"Error getting full content for {item.get('slug', '')}: {str(e)}")
            # Fall back to description/subtitle if available
            return item.get('description', '') or item.get('subtitle', '')

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to ISO format"""
        if not date_str:
            return None

        try:
            return parser.parse(date_str).isoformat()
        except Exception as e:
            logger.warning(f"Could not parse date: '{date_str}'")
            return None
