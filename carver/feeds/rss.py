import os
import sys
import json
import logging
import traceback

from typing import List, Dict, Any, Optional
from datetime import datetime

import feedparser
import requests

from bs4 import BeautifulSoup
from dateutil import parser

from .base import FeedReader

logger = logging.getLogger(__name__)

class RSSReader(FeedReader):
    """Reader for standard RSS feeds"""

    def get_content_identifier(self, item: Dict[str, Any]) -> str:

        """Get unique identifier for an item"""
        # Try different possible unique identifiers
        cid =  item.get('title', '')  # Fallback to title if nothing else available
        if 'id' in item:
            cid = item['id']
        elif 'guid' in item:
            cid = item['guid']
        elif 'link' in item:
            cid = item['link']

        cid = cid.replace("https://reddit.com","")
        cid = cid.replace("https://www.reddit.com","")
        return cid

    def prepare_item(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert RSS entry to database item"""
        base_item = super().prepare_item(raw_item)

        # Extract content and clean it
        content = self._get_content(raw_item)
        summary = self._get_summary(raw_item)

        # Detect language from content
        language = self._detect_language(raw_item)

        base_item.update({
            'content_type': 'POST',
            'title': raw_item.get('title', 'Untitled'),
            'description': summary,
            'content': content,
            'author': self._get_author(raw_item),
            'published_at': self._parse_date(raw_item.get('published', raw_item.get('updated', ''))),
            'url': raw_item.get('link', ''),
            'language': language,
            'content_metrics': self._get_metrics(raw_item, content),
            'tags': self._get_tags(raw_item)
        })

        return base_item

    def read(self) -> List[Dict[str, Any]]:
        """Read RSS feed"""

        try:

            headers = {
                "User-Agent": "Mozilla/5.0 (compatible; MyRSSParser/1.0; +https://django.org)"
            }

            url = self.source['url']
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Parse the feed content
            feed = feedparser.parse(response.text)

            if feed.bozo and feed.bozo_exception:
                logger.error(f"Feed parsing error: {str(feed.bozo_exception)}")

            items = feed.entries

            # Respect max_results if specified
            if self.max_results:
                items = items[:self.max_results]

            return [self.prepare_item(item) for item in items]

        except Exception as e:
            logger.error(f"Error reading RSS feed: {str(e)}")
            raise

    def _get_content(self, item: Dict[str, Any]) -> str:
        """Extract and clean content from RSS item"""
        content = ''

        # Try different content fields
        if 'content' in item:
            content = item.content[0].value
        elif 'summary_detail' in item:
            content = item.summary_detail.value
        elif 'description' in item:
            content = item.description
        elif 'summary' in item:
            content = item.summary

        # Clean HTML if present
        if content:
            try:
                soup = BeautifulSoup(content, 'html.parser')
                content = soup.get_text(separator=' ', strip=True)
            except Exception as e:
                logger.warning(f"Error cleaning HTML content: {str(e)}")

        return content

    def _get_summary(self, item: Dict[str, Any]) -> str:
        """Get or generate item summary"""
        summary = item.get('summary', '')
        if not summary:
            content = self._get_content(item)
            summary = content[:500] + '...' if len(content) > 500 else content
        return summary

    def _get_author(self, item: Dict[str, Any]) -> str:
        """Extract author information"""
        if 'author_detail' in item:
            return item.author_detail.get('name', item.author_detail.get('email', ''))
        return item.get('author', '')

    def _get_metrics(self, item: Dict[str, Any], content: str) -> Dict[str, Any]:
        """Calculate content metrics"""
        return {
            'word_count': len(content.split()),
            'char_count': len(content),
            'has_images': 'img' in content.lower(),
            'has_links': 'href' in content.lower()
        }

    def _get_tags(self, item: Dict[str, Any]) -> str:
        """Extract tags from item"""
        tags = []

        # Try different tag fields
        if 'tags' in item:
            tags.extend([tag.term for tag in item.tags])
        if 'categories' in item:
            tags.extend(item.categories)

        return ','.join(set(tags))

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to ISO format"""
        if not date_str:
            return None

        try:
            return feedparser._parse_date(date_str)
        except:
            try:
                # Fallback to dateutil parser
                return parser.parse(date_str).isoformat()
            except:
                traceback.print_exc()
                logger.warning(f"Could not parse date: '{date_str}'")
                return None

    def _detect_language(self, item: Dict[str, Any]) -> str:
        """Detect content language"""
        # Try to get language from feed
        if 'language' in item:
            return item.language

        # You could integrate with langdetect here for more accurate detection
        return 'en'  # Default to English
