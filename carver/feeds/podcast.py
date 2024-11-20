import logging

from typing import List, Dict, Any, Optional
from datetime import datetime

import feedparser
import requests
import isodate

from dateutil import parser
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

from .base import FeedReader

logger = logging.getLogger(__name__)

class PodcastReader(FeedReader):
    """Reader for podcast RSS feeds with enhanced metadata support"""

    NAMESPACES = {
        'itunes': 'http://www.itunes.com/dtds/podcast-1.0.dtd',
        'content': 'http://purl.org/rss/1.0/modules/content/',
        'media': 'http://search.yahoo.com/mrss/',
        'spotify': 'http://www.spotify.com/ns/rss',
        'googleplay': 'http://www.google.com/schemas/play-podcasts/1.0'
    }

    def get_content_identifier(self, item: Dict[str, Any]) -> str:
        """Get unique identifier for an episode"""
        # Try multiple possible identifiers in order of preference
        for field in ['guid', 'id', 'link', 'enclosure_url']:
            if field in item and item[field]:
                return str(item[field])
        return item.get('title', '')  # Fallback to title if nothing else available

    def prepare_item(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert podcast episode to database item"""
        base_item = super().prepare_item(raw_item)

        # Get episode details
        media_info = self._get_media_info(raw_item)
        transcript = self._get_transcript(raw_item)

        base_item.update({
            'content_type': 'EPISODE',
            'title': raw_item.get('title', 'Untitled Episode'),
            'description': self._get_description(raw_item),
            'content': transcript,
            'summary': raw_item.get('summary', raw_item.get('subtitle', '')),
            'author': self._get_author(raw_item),
            'published_at': self._parse_date(raw_item.get('pubDate', '')),
            'url': raw_item.get('link', media_info.get('url', '')),
            'media_type': 'audio',
            'media_url': media_info.get('url'),
            'thumbnail_url': self._get_image(raw_item),
            'duration': media_info.get('duration'),
            'language': self._get_language(raw_item),
            'content_metrics': {
                'duration_seconds': media_info.get('duration_seconds', 0),
                'file_size': media_info.get('file_size', 0),
                'mime_type': media_info.get('mime_type', ''),
                'has_transcript': bool(transcript),
                'episode_type': raw_item.get('itunes_episodetype', 'full'),
                'episode_number': raw_item.get('itunes_episode'),
                'season_number': raw_item.get('itunes_season')
            },
            'analysis_metadata': {
                'explicit': raw_item.get('itunes_explicit', False),
                'block': raw_item.get('itunes_block', False),
                'keywords': raw_item.get('itunes_keywords', '').split(','),
                'categories': self._get_categories(raw_item)
            }
        })

        return base_item

    def read(self) -> List[Dict[str, Any]]:
        """Read podcast feed"""
        try:
            feed = feedparser.parse(self.source['url'])

            if feed.bozo and feed.bozo_exception:
                logger.error(f"Feed parsing error: {str(feed.bozo_exception)}")

            items = feed.entries

            # Update source metadata
            self._update_feed_metadata(feed.feed)

            # Respect max_results if specified
            if self.max_results and len(items) > self.max_results:
                items = items[:self.max_results]

            return [self.prepare_item(item) for item in items]

        except Exception as e:
            logger.error(f"Error reading podcast feed: {str(e)}")
            raise

    def _get_media_info(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Extract media information from episode"""
        info = {
            'url': None,
            'duration': None,
            'duration_seconds': 0,
            'file_size': 0,
            'mime_type': None
        }

        # Try to get enclosure information
        if hasattr(item, 'enclosures') and item.enclosures:
            enclosure = item.enclosures[0]
            info.update({
                'url': enclosure.get('href', enclosure.get('url')),
                'file_size': int(enclosure.get('length', 0)),
                'mime_type': enclosure.get('type')
            })

        # Get duration in different formats
        duration = item.get('itunes_duration', item.get('duration'))
        if duration:
            info['duration'] = duration
            try:
                # Handle different duration formats (HH:MM:SS, MM:SS, or seconds)
                if ':' in str(duration):
                    info['duration_seconds'] = sum(
                        x * int(t) for x, t in zip([3600, 60, 1], duration.split(':')[::-1])
                    )
                else:
                    info['duration_seconds'] = int(float(duration))
            except:
                logger.warning(f"Could not parse duration: {duration}")

        return info

    def _get_description(self, item: Dict[str, Any]) -> str:
        """Get episode description with HTML cleaning"""
        # Try different description fields
        description = item.get('content', [{'value': ''}])[0].get('value', '')
        if not description:
            description = item.get('description', '')

        # Clean HTML if present
        if description:
            try:
                soup = BeautifulSoup(description, 'html.parser')
                description = soup.get_text(separator='\n', strip=True)
            except Exception as e:
                logger.warning(f"Error cleaning HTML description: {str(e)}")

        return description

    def _get_transcript(self, item: Dict[str, Any]) -> Optional[str]:
        """Get episode transcript if available"""
        # Check for transcript in content:encoded or other fields
        transcript = None

        if hasattr(item, 'content_encoded'):
            transcript = item.content_encoded
        elif 'transcript_url' in item:
            try:
                response = requests.get(item['transcript_url'])
                response.raise_for_status()
                transcript = response.text
            except:
                logger.warning(f"Could not fetch transcript from {item.get('transcript_url')}")

        return transcript

    def _get_image(self, item: Dict[str, Any]) -> Optional[str]:
        """Get episode image URL"""
        # Try different image sources
        if hasattr(item, 'image'):
            return item.image.get('href')

        if hasattr(item, 'itunes_image'):
            return item.itunes_image.get('href')

        # Fallback to feed image if available
        feed = self.source.get('config', {})
        return feed.get('image_url')

    def _get_author(self, item: Dict[str, Any]) -> str:
        """Get episode author"""
        # Try different author fields
        for field in ['itunes_author', 'author', 'creator']:
            if hasattr(item, field) and getattr(item, field):
                return getattr(item, field)
        return ''

    def _get_language(self, item: Dict[str, Any]) -> str:
        """Get episode language"""
        # Try item-specific language first
        if hasattr(item, 'language'):
            return item.language

        # Fallback to feed language
        feed = self.source.get('config', {})
        return feed.get('language', 'en')

    def _get_categories(self, item: Dict[str, Any]) -> List[str]:
        """Get episode categories"""
        categories = []

        # Get iTunes categories
        if hasattr(item, 'itunes_categories'):
            categories.extend(item.itunes_categories)

        # Get regular categories
        if hasattr(item, 'tags'):
            categories.extend([tag.term for tag in item.tags])

        return list(set(categories))

    def _update_feed_metadata(self, feed: Dict[str, Any]) -> None:
        """Update source metadata with feed information"""
        try:
            metadata = {
                'feed_title': feed.get('title'),
                'feed_author': feed.get('itunes_author', feed.get('author')),
                'feed_description': feed.get('description'),
                'feed_language': feed.get('language', 'en'),
                'feed_explicit': feed.get('itunes_explicit', False),
                'feed_categories': feed.get('itunes_categories', []),
                'feed_owner': {
                    'name': feed.get('itunes_owner_name'),
                    'email': feed.get('itunes_owner_email')
                },
                'feed_type': feed.get('itunes_type', 'episodic'),
                'feed_copyright': feed.get('rights', feed.get('copyright')),
                'feed_updated': feed.get('updated', feed.get('published')),
                'total_episodes': len(feed.get('entries', [])),
                'last_checked': datetime.utcnow().isoformat()
            }

            self.source['config'].update(metadata)

        except Exception as e:
            logger.error(f"Error updating feed metadata: {str(e)}")
