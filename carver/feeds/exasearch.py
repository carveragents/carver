import os
import sys
import json
import logging
import traceback

from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from dataclasses import dataclass, asdict

from exa_py import Exa
from exa_py.api import Result

from carver.utils import get_config, parse_date_filter
from .base import FeedReader

logger = logging.getLogger(__name__)

class ExaReader(FeedReader):
    """Base class for Exa.ai readers"""

    def __init__(self, source: Dict[str, Any], max_results: Optional[int] = None):
        super().__init__(source, max_results)
        api_key = self.config.get('EXA_API_KEY')
        if not api_key:
            raise ValueError("Exa API key not found in source config")

        self.exa = Exa(api_key=api_key)

    def get_content_identifier(self, item: Dict[str, Any]) -> str:
        """Get unique identifier for an item"""
        return item['id']

    def prepare_item(self, raw_item: Result) -> Dict[str, Any]:
        """Convert Exa API response to database item"""

        base_item = super().prepare_item(asdict(raw_item))

        # Extract content information
        base_item.update({
            'content_type': 'ARTICLE',
            'title': raw_item.title,
            'description': raw_item.summary,
            'summary': raw_item.summary,
            'author': raw_item.author,
            'content': raw_item.text,
            'url': raw_item.url,
            'published_at': raw_item.published_date,
            # 'thumbnail_url': raw_item.get('image', None),
            'media_type': 'text',
            #'language': raw_item.get('language', 'en'),
            "language": "en",
            'content_metrics': {
                'text_length': len(raw_item.text),
                'score': raw_item.score,
            }
        })

        return base_item

class ExaSearchReader(ExaReader):
    """Reader for Exa.ai search results"""

    def read(self) -> List[Dict[str, Any]]:

        query = self.source['config'].get('query')
        if query is None:
            raise Exception("Invalid query")

        if isinstance(query, list):
            query = " ".join(query)
        elif not isinstance(query, str):
            query = str(query)

        # Get date range from source config or default to last 7 days
        date_filter = self.source['config'].get('date_filter', '3d')
        start_date = parse_date_filter(date_filter)
        end_date = datetime.now(timezone.utc)

        # Calculate max results (default to 50 if not specified)
        max_results = min(self.max_results or 25, 25)

        try:
            response = self.exa.search_and_contents(
                query,
                type="neural",  # Use neural search as specified
                start_published_date=start_date.isoformat(),
                end_published_date=end_date.isoformat(),
                text=True,
                summary=True,
                num_results=max_results,
                livecrawl="always"
            )

            # Process and return results
            return [self.prepare_item(item) for item in response.results]

        except Exception as e:
            traceback.print_exc()
            logger.error(f"Error performing Exa search for query '{query}': {str(e)}")
            return []

# Add to FeedReader.get_reader() method in base.py:
# ('EXA', 'SEARCH'): ExaSearchReader,
