import os
import sys
import json
import re

from typing import List, Dict, Any, Optional, Type
from datetime import datetime
from abc import ABC, abstractmethod

from exa_py import Exa
from exa_py.api import Result

from .base import BaseArtifactGenerator

from carver.utils import get_config

class ExaContentGenerator(BaseArtifactGenerator):
    name = "exa_content"
    description = "Extracts and processes content from Exa"
    supported_platforms = ['EXA']
    supported_source_types = ['SEARCH']
    required_config = []

    def __init__(self):
        super().__init__()
        api_key = get_config()('EXA_API_KEY')
        if not api_key:
            raise ValueError("Exa API key not found in config")
        self.exa = Exa(api_key=api_key)

    def get_ids(self, config: Dict[str, Any]):
        return ['en']

    def get_content(self, url: str) -> Dict[str, Any]:
        try:
            result = self.exa.get_contents([url], text=True)
            if not result.results:
                raise Exception(f"No content found for {url}")
            body = abstract = result.results[0]

            if (("arxiv" in url) and ("/abs/" in url)):
                url1 = url.replace("/abs/", "/html/")
                result = self.exa.get_contents([url1], text=True)
                if result.results:
                    body = result.results[0]

            response = {
                "generator_name": self.name,
                "generator_id": "en",
                "format": "text",
                "language": "en",
                "description": abstract.text,
                "content": body.text,
                "analysis_metadata": {
                    "word_count": len(body.text.split()),
                    "source_url": abstract.url,
                    "author": abstract.author,
                    "published_date": abstract.published_date
                }
            }

            return response

        except Exception as e:
            print(f"[{self.name}] Error getting content: {str(e)}")
            raise

    def generate(self, post: Dict[str, Any],
                spec: Dict[str, Any],
                existing: List[Dict[str, Any]]) -> Dict[str, Any]:

        for e in existing:
            if (e.get('generator_name') == self.name and
                e.get('generator_id') == "en"):
                return []

        artifact = self.get_content(post['content_identifier'])
        artifact.update({
            "artifact_type": "CONTENT",
            "title": f"Content: {post.get('title', 'Untitled')}"
        })

        return [artifact]

    def generate_bulk(self, posts: List[Dict[str, Any]],
                     config: Dict[str, Any],
                     existing_map: Dict[int, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Generate content for multiple posts in bulk"""
        all_artifacts = []

        # Group URLs to process in chunks of 10
        urls = []
        url_to_post = {}
        for post in posts:
            url = post['content_identifier']
            existing = existing_map.get(post['id'], [])

            # Skip if content exists
            if any(e.get('generator_name') == self.name and
                  e.get('generator_id') == "content" for e in existing):
                continue

            urls.append(url)
            url_to_post[url] = post

        # Process in chunks of 10
        for i in range(0, len(urls), 10):
            chunk = urls[i:i+10]
            try:
                results = self.exa.get_contents(chunk, text=True)
                for content in results.results:
                    post = url_to_post[content.url]
                    artifact = {
                        "generator_name": self.name,
                        "generator_id": "content",
                        "format": "text",
                        "language": content.language or "en",
                        "content": content.text,
                        "artifact_type": "CONTENT",
                        "title": f"Content: {post.get('title', 'Untitled')}",
                        "analysis_metadata": {
                            "word_count": len(content.text.split()),
                            "source_url": content.url,
                            "author": content.author,
                            "published_date": content.published_date
                        }
                    }
                    all_artifacts.append(artifact)
            except Exception as e:
                print(f"[{self.name}] Error in bulk processing chunk: {str(e)}")
                continue

        return all_artifacts
