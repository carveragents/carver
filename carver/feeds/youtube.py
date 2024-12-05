import os
import sys
import json
import logging
import traceback

from typing import List, Dict, Any, Optional
from datetime import datetime

from ..utils import get_config

import googleapiclient.discovery

logger = logging.getLogger(__name__)

from .base import FeedReader

class YouTubeReader(FeedReader):
    """Base class for YouTube readers"""

    def __init__(self, source: Dict[str, Any], max_results: Optional[int] = None):
        super().__init__(source, max_results)
        api_key = self.config.get('youtube_api_key')
        if not api_key:
            raise ValueError("YouTube API key not found in source config")

        self.youtube = googleapiclient.discovery.build(
            'youtube', 'v3', developerKey=api_key)

    def get_video_details(self, video_ids: List[str]) -> List[Dict[str, Any]]:
        """Get detailed information for a list of video IDs"""
        details = []

        # Process in chunks of 50 (YouTube API limit)
        for i in range(0, len(video_ids), 50):
            chunk = video_ids[i:i + 50]
            try:
                response = self.youtube.videos().list(
                    part='snippet,contentDetails,statistics,localizations',
                    id=','.join(chunk)
                ).execute()
                details.extend(response.get('items', []))
            except Exception as e:
                logger.error(f"Error fetching video details: {str(e)}")

        return details

    def get_content_identifier(self, item: Dict[str, Any]) -> str:

        # Could be a playlist or channel search result..
        if (('contentDetails' in item) and ('videoId' in item['contentDetails'])):
            return item['contentDetails']['videoId']
        elif 'videoId' in item['id']:
            return item['id']['videoId']

        return item['id']

    def prepare_item(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert YouTube API response to database item"""
        base_item = super().prepare_item(raw_item)

        # Extract snippet information
        snippet = raw_item['snippet']
        content_details = raw_item.get('contentDetails', {})
        statistics = raw_item.get('statistics', {})

        # Get language from defaultLanguage or detect from title/description
        language = snippet.get('defaultLanguage',
                             snippet.get('defaultAudioLanguage',
                                       self._detect_language(snippet.get('title', ''))))

        try:
            thumbnail_url = snippet['thumbnails']['default']['url']
        except:
            thumbnail_url = None

        base_item.update({
            'content_type': 'VIDEO',
            'title': snippet['title'],
            'description': snippet['description'],
            'author': snippet['channelTitle'],
            'published_at': snippet['publishedAt'],
            'url': f"https://www.youtube.com/watch?v={self.get_content_identifier(raw_item)}",
            'thumbnail_url': thumbnail_url,
            'media_type': 'video',
            'language': language,
            'content_metrics': {
                'duration': content_details.get('duration'),
                'dimension': content_details.get('dimension'),
                'definition': content_details.get('definition'),
                'caption': content_details.get('caption', 'false'),
                'view_count': int(statistics.get('viewCount', 0)),
                'like_count': int(statistics.get('likeCount', 0)),
                'comment_count': int(statistics.get('commentCount', 0))
            }
        })

        # Add available localizations
        if 'localizations' in raw_item:
            base_item['analysis_metadata']['localizations'] = list(raw_item['localizations'].keys())

        return base_item

    def _detect_language(self, text: str) -> str:
        """Simple language detection - implement more sophisticated detection if needed"""
        # You could integrate with langdetect or other language detection libraries
        return 'en'  # Default to English for now

class YouTubeSearchReader(YouTubeReader):
    """Reader for YouTube search results. Supports multiple search phrases using YouTube's boolean operators."""


    def read(self, details=False) -> List[Dict[str, Any]]:

        query = self.source['source_identifier']

        page_token = None
        items = []
        remaining_results = self.max_results

        while True:
            try:
                # Calculate how many results to request
                request_max = min(50, remaining_results) if remaining_results else 50

                request = self.youtube.search().list(
                    part='id,snippet',
                    q=query,
                    order='date',  # Default to date ordering like other readers
                    type='video',  # Only get videos
                    maxResults=request_max,
                    pageToken=page_token
                )

                response = request.execute()
                current_items = response.get('items', [])

                if not current_items:
                    break

                # Get detailed information for these videos if requested
                if details:
                    video_ids = [item['id']['videoId'] for item in current_items]
                    detailed_items = self.get_video_details(video_ids)
                    items.extend(detailed_items)
                else:
                    items.extend(current_items)

                # Update remaining results
                if remaining_results is not None:
                    remaining_results -= len(current_items)
                    if remaining_results <= 0:
                        break

                # Get next page token
                page_token = response.get('nextPageToken')
                if not page_token:
                    break

            except Exception as e:
                logger.error(f"Error performing YouTube search for query '{query}': {str(e)}")
                break

        return [self.prepare_item(item) for item in items]

class YouTubeChannelReader(YouTubeReader):
    """Reader for YouTube channels"""

    def read(self, details=False) -> List[Dict[str, Any]]:
        channel_id = self.source['source_identifier']
        page_token = None
        items = []
        remaining_results = self.max_results

        while True:
            try:
                # Calculate how many results to request
                request_max = min(50, remaining_results) if remaining_results else 50

                request = self.youtube.search().list(
                    part='id,snippet,contentDetails',
                    channelId=channel_id,
                    order='date',
                    type='video',  # Only get videos
                    maxResults=request_max,
                    pageToken=page_token
                )

                response = request.execute()
                current_items = response.get('items', [])

                if not current_items:
                    break

                # Get detailed information for these videos
                if details:
                    video_ids = [item['id']['videoId'] for item in current_items]
                    detailed_items = self.get_video_details(video_ids)
                    items.extend(detailed_items)
                else:
                    items.extend(current_items)

                # Update remaining results
                if remaining_results is not None:
                    remaining_results -= len(current_items)
                    if remaining_results <= 0:
                        break

                # Get next page token
                page_token = response.get('nextPageToken')
                if not page_token:
                    break

            except Exception as e:
                logger.error(f"Error reading YouTube channel: {str(e)}")
                break

        return [self.prepare_item(item) for item in items]

class YouTubePlaylistReader(YouTubeReader):
    """Reader for YouTube playlists"""

    def read(self, details=False) -> List[Dict[str, Any]]:
        playlist_id = self.source['source_identifier']
        page_token = None
        items = []
        remaining_results = self.max_results

        while True:
            try:
                # Calculate how many results to request
                request_max = min(50, remaining_results) if remaining_results else 50

                request = self.youtube.playlistItems().list(
                    part='snippet,contentDetails',
                    playlistId=playlist_id,
                    maxResults=request_max,
                    pageToken=page_token
                )

                response = request.execute()
                current_items = response.get('items', [])

                if not current_items:
                    break

                if details:
                    # Get video IDs from playlist items
                    video_ids = [item['contentDetails']['videoId'] for item in current_items]
                    detailed_items = self.get_video_details(video_ids)

                    items.extend(detailed_items)
                else:
                    items.extend(current_items)

                # Update remaining results
                if remaining_results is not None:
                    remaining_results -= len(current_items)
                    if remaining_results <= 0:
                        break

                # Get next page token
                page_token = response.get('nextPageToken')
                if not page_token:
                    break

            except Exception as e:
                logger.error(f"Error reading YouTube playlist: {str(e)}")
                break

        return [self.prepare_item(item) for item in items]


class YouTubePlaylistDiscovery:
    """Discovers YouTube playlists based on search criteria"""

    def __init__(self):
        config = get_config()
        api_key = config.get('youtube_api_key')
        if not api_key:
            raise ValueError("YouTube API key not found in source config")
        self.youtube = googleapiclient.discovery.build(
            'youtube', 'v3', developerKey=api_key)

    def discover_playlists(self, query: str,
                           what: Optional[str] = "playlist",
                           max_results: Optional[int] = 10) -> List[Dict[str, Any]]:
        """Search for playlists matching query"""
        try:

            assert what in ['playlist', 'channel']

            request = self.youtube.search().list(
                part='id,snippet',
                q=query,
                type=what,
                order='date',
                maxResults=min(max_results, 50)
            )

            response = request.execute()
            playlists = []

            for item in response.get('items', []):
                _id = item['id'].get('playlistId', item['id'].get('channelId'))
                playlist = {
                    'id': _id,
                    'title': item['snippet']['title'],
                    'description': item['snippet']['description'],
                    'channel_title': item['snippet']['channelTitle'],
                    'published_at': item['snippet']['publishedAt'],
                    'thumbnail_url': item['snippet']['thumbnails']['default']['url']
                }
                playlists.append(playlist)

            return playlists

        except Exception as e:
            traceback.print_exc()
            logger.error(f"Error discovering playlists: {str(e)}")
            return []
