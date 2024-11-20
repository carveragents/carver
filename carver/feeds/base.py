from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime

from decouple import Config

from ..utils import get_config

class FeedReader(ABC):
    """Base class for all feed readers"""

    def __init__(self, source: Dict[str, Any],
                 max_results: Optional[int] = None,
                 config: Config = None):
        self.source = source
        if config is None:
            config = get_config()
        self.config = config
        self.max_results = max_results

    @abstractmethod
    def read(self) -> List[Dict[str, Any]]:
        """Read feed and return list of items"""
        pass

    @abstractmethod
    def get_content_identifier(self, item: Dict[str, Any]) -> str:
        """Get unique identifier for an item"""
        pass

    def prepare_item(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare raw item data for database storage"""
        now = datetime.utcnow().isoformat()

        return {
            'source_id': self.source['id'],
            'active': True,
            'content_identifier': self.get_content_identifier(raw_item),
            'acquired_at': now,
            'updated_at': now,
            'is_processed': False,
            'analysis_metadata': {},
            'content_metrics': {}
            # Language will be set by specific readers based on content
        }

    @classmethod
    def get_reader(cls, source: Dict[str, Any], max_results: Optional[int] = None) -> 'FeedReader':
        """Factory method to get appropriate reader for source"""

        from .youtube import YouTubeChannelReader, YouTubePlaylistReader
        from .github import GithubRepositoryReader
        from .podcast import PodcastReader
        from .rss import RSSReader

        platform = source.get('platform', '').upper()
        source_type = source.get('source_type', '').upper()

        reader_map = {
            ('YOUTUBE', 'CHANNEL'): YouTubeChannelReader,
            ('YOUTUBE', 'FEED'): YouTubePlaylistReader,
            ('YOUTUBE', 'PLAYLIST'): YouTubePlaylistReader,
            ('GITHUB', 'REPOSITORY'): GithubRepositoryReader,
            ('REDDIT', 'FEED'): RSSReader,
            ('RSS', 'FEED'): RSSReader,
            ('RSS', 'PODCAST'): PodcastReader
        }

        reader_class = reader_map.get((platform, source_type))
        if not reader_class:
            raise ValueError(f"No reader found for platform {platform} and type {source_type}")

        return reader_class(source, max_results)

    def update_source_metadata(self, db_client) -> Dict[str, Any]:
        """Update source metadata after successful feed read"""
        metadata_update = {
            'id': self.source['id'],
            'last_crawled': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }
        return db_client.source_update(self.source['id'], metadata_update)
