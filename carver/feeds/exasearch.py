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
            'author': raw_item.author,
            'url': raw_item.url,
            'published_at': raw_item.published_date,
            # 'thumbnail_url': raw_item.get('image', None),
            'media_type': 'text',
            #'language': raw_item.get('language', 'en'),
            "language": "en",
            'content_metrics': {
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
        max_results = self.source['config'].get('num_results', 25)
        if self.max_results and max_results != self.max_results:
            print(f"Overriding default num_results ({max_results}) with {self.max_results}")
            max_results = min(self.max_results, 30)

        extra = {}
        if 'category' in self.source['config']:
            extra['category'] = self.source['config']['category']

        _type = self.source['config'].get('type', 'neural')
        print("_type", _type)
        try:
            response = self.exa.search(
                query,
                type=_type,
                start_published_date=start_date.isoformat(),
                end_published_date=end_date.isoformat(),
                num_results=max_results,
                **extra
            )

            # Process and return results
            items = [self.prepare_item(item) for item in response.results]

            default_domain_exclude = ['twitter.com', 'x.com']
            domain_exclude = self.source['config'].get('domain_exclude',
                                                      default_domain_exclude)

            skipped = 0
            final_items = []
            for item in items:
                if any([ex in item['url'] for ex in domain_exclude]):
                    skipped += 1
                    continue
                final_items.append(item)

            if skipped > 0:
                print(f"Skipped {skipped} items due overlap with {domain_exclude}")

            return final_items

        except Exception as e:
            traceback.print_exc()
            logger.error(f"Error performing Exa search for query '{query}': {str(e)}")
            return []

# Add to FeedReader.get_reader() method in base.py:
# ('EXA', 'SEARCH'): ExaSearchReader,
