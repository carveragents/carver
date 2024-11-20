import re
from urllib.parse import urlparse, parse_qs
import requests
from typing import Dict, Optional, Tuple
import feedparser
import json
from pytube import YouTube, Playlist, Channel
import xml.etree.ElementTree as ET
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class SourceURLParser:
    """Parse various URLs to extract source information with rich metadata"""

    YOUTUBE_PATTERNS = {
        'channel': r'(?:https?:\/\/)?(?:www\.)?youtube\.com\/(?:c\/|channel\/|user\/)?([a-zA-Z0-9_-]+)',
        'playlist': r'(?:https?:\/\/)?(?:www\.)?youtube\.com\/(?:playlist\?list=|watch\?v=[a-zA-Z0-9_-]+&list=)([a-zA-Z0-9_-]+)',
    }

    GITHUB_PATTERNS = {
        'repository': r'(?:https?:\/\/)?(?:www\.)?github\.com\/([a-zA-Z0-9_-]+)\/([a-zA-Z0-9_-]+)\/?$'
    }

    REDDIT_PATTERNS = {
        'subreddit': r'(?:https?:\/\/)?(?:www\.)?reddit\.com\/r\/([a-zA-Z0-9_-]+)\/?$',
        'user': r'(?:https?:\/\/)?(?:www\.)?reddit\.com\/user\/([a-zA-Z0-9_-]+)\/?$'
    }

    @classmethod
    def parse_url(cls, url: str) -> Optional[Dict]:
        """
        Parse URL and return source details with rich metadata
        Returns None if URL type cannot be determined
        """
        try:
            # Normalize URL
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url

            parsed = urlparse(url)

            # Try different parsers in order
            parsers = [
                cls._parse_youtube,
                cls._parse_github,
                cls._parse_reddit,
                cls._parse_podcast,
                cls._parse_rss
            ]

            for parser in parsers:
                try:
                    result = parser(url, parsed)
                    if result:
                        return result
                except Exception as e:
                    logger.warning(f"Error in {parser.__name__}: {str(e)}")
                    continue

            return None
        except Exception as e:
            logger.error(f"Error parsing URL {url}: {str(e)}")
            return None

    @classmethod
    def _parse_youtube(cls, url: str, parsed_url: urlparse) -> Optional[Dict]:
        """Parse YouTube URLs with rich metadata"""
        if 'youtube.com' not in parsed_url.netloc:
            return None

        # Try parsing as channel
        if '/channel/' in url or '/c/' in url or '/user/' in url:
            try:
                channel = Channel(url)
                channel_info = {
                    'platform': 'YOUTUBE',
                    'source_type': 'CHANNEL',
                    'name': channel.channel_name,
                    'source_identifier': channel.channel_id,
                    'url': channel.channel_url,
                    'config': {
                        'type': 'channel',
                        'channel_name': channel.channel_name,
                        'channel_id': channel.channel_id,
                        'channel_url': channel.channel_url,
                        'fetched_at': datetime.utcnow().isoformat()
                    }
                }

                # Try to get channel description and other metadata
                if hasattr(channel, 'initial_data'):
                    channel_info['description'] = channel.initial_data.get('description', '')
                    channel_info['config'].update({
                        'subscriber_count': channel.initial_data.get('subscriberCount', 0),
                        'video_count': channel.initial_data.get('videoCount', 0)
                    })

                return channel_info
            except Exception as e:
                logger.error(f"YouTube channel parsing error: {str(e)}")
                return None

        # Try parsing as playlist
        playlist_match = re.search(cls.YOUTUBE_PATTERNS['playlist'], url)
        if playlist_match:
            try:
                # Extract the playlist ID from either format
                playlist_id = playlist_match.group(1)

                # Construct canonical playlist URL
                playlist_url = f'https://www.youtube.com/playlist?list={playlist_id}'

                playlist = Playlist(playlist_url)

                # Sometimes playlist title might not be immediately available
                # In such cases, try to fetch it from the first video
                title = playlist.title
                description = playlist.description
                if not title and playlist.video_urls:
                    try:
                        first_video = YouTube(playlist.video_urls[0])
                        title = first_video.playlist_title or f"Playlist: {playlist_id}"
                        description = first_video.playlist_description or ""
                    except Exception as e:
                        logger.warning(f"Error fetching playlist details from video: {str(e)}")
                        title = f"Playlist: {playlist_id}"
                        description = ""

                return {
                    'platform': 'YOUTUBE',
                    'source_type': 'PLAYLIST',
                    'name': title,
                    'description': description,
                    'source_identifier': playlist_id,
                    'url': playlist_url,  # Use canonical URL
                    'config': {
                        'type': 'playlist',
                        'playlist_id': playlist_id,
                        'playlist_title': title,
                        'playlist_description': description,
                        'author': playlist.owner,
                        'video_count': len(playlist.video_urls),
                        'first_video_url': playlist.video_urls[0] if playlist.video_urls else None,
                        'fetched_at': datetime.utcnow().isoformat()
                    }
                }
            except Exception as e:
                logger.error(f"YouTube playlist parsing error: {str(e)}")
                return None

        return None

    @classmethod
    def _parse_github(cls, url: str, parsed_url: urlparse) -> Optional[Dict]:
        """Parse GitHub repository URLs with API metadata"""
        if 'github.com' not in parsed_url.netloc:
            return None

        match = re.match(cls.GITHUB_PATTERNS['repository'], url)
        if match:
            username, repo = match.group(1), match.group(2)
            api_url = f"https://api.github.com/repos/{username}/{repo}"

            try:
                response = requests.get(api_url)
                if response.status_code == 200:
                    repo_data = response.json()

                    return {
                        'platform': 'GITHUB',
                        'source_type': 'REPOSITORY',
                        'name': repo_data['full_name'],
                        'description': repo_data['description'],
                        'source_identifier': str(repo_data['id']),
                        'url': repo_data['html_url'],
                        'config': {
                            'type': 'repository',
                            'repo_id': repo_data['id'],
                            'full_name': repo_data['full_name'],
                            'owner': username,
                            'default_branch': repo_data['default_branch'],
                            'language': repo_data['language'],
                            'created_at': repo_data['created_at'],
                            'updated_at': repo_data['updated_at'],
                            'pushed_at': repo_data['pushed_at'],
                            'size': repo_data['size'],
                            'stars': repo_data['stargazers_count'],
                            'forks': repo_data['forks_count'],
                            'open_issues': repo_data['open_issues_count'],
                            'topics': repo_data.get('topics', []),
                            'homepage': repo_data.get('homepage', ''),
                            'has_wiki': repo_data.get('has_wiki', False),
                            'fetched_at': datetime.utcnow().isoformat()
                        }
                    }
            except Exception as e:
                logger.error(f"GitHub API error: {str(e)}")

        return None

    @classmethod
    def _parse_reddit(cls, url: str, parsed_url: urlparse) -> Optional[Dict]:
        """Parse Reddit URLs using RSS feeds"""

        if 'reddit.com' not in parsed_url.netloc:
            return None

        # Try matching subreddit or user patterns
        subreddit_match = re.match(cls.REDDIT_PATTERNS['subreddit'], url)
        user_match = re.match(cls.REDDIT_PATTERNS['user'], url)

        if subreddit_match:
            subreddit = subreddit_match.group(1)
            rss_url = f'https://www.reddit.com/r/{subreddit}/.rss'
            source_type = 'subreddit'
        elif user_match:
            username = user_match.group(1)
            rss_url = f'https://www.reddit.com/user/{username}/.rss'
            source_type = 'user'
        else:
            return None

        try:
            feed = feedparser.parse(rss_url)
            if feed.get('feed') and feed.feed.get('title'):
                return {
                    'platform': 'REDDIT',
                    'source_type': 'FEED',
                    'name': feed.feed.title,
                    'description': feed.feed.get('description', ''),
                    'source_identifier': parsed_url.path.strip('/'),
                    'url': rss_url,
                    'config': {
                        'type': 'reddit',
                        'reddit_type': source_type,
                        'feed_title': feed.feed.title,
                        'feed_link': feed.feed.get('link', ''),
                        'feed_updated': feed.feed.get('updated', ''),
                        'feed_language': feed.feed.get('language', ''),
                        'recent_items': len(feed.entries),
                        'last_entry_date': feed.entries[0].get('published', '') if feed.entries else '',
                        'fetched_at': datetime.utcnow().isoformat()
                    }
                }
        except Exception as e:
            logger.error(f"Reddit RSS error: {str(e)}")

        return None

    @classmethod
    def _parse_podcast(cls, url: str, parsed_url: urlparse) -> Optional[Dict]:
        """Parse podcast XML feed URLs"""
        try:
            # Try to fetch and parse as podcast XML
            response = requests.get(url, timeout=10)
            root = ET.fromstring(response.content)

            # Check if it's a podcast feed by looking for typical podcast elements
            channel = root.find('channel')
            if channel is None:
                return None

            # Look for podcast-specific tags
            itunes_ns = {'itunes': 'http://www.itunes.com/dtds/podcast-1.0.dtd'}
            is_podcast = (
                channel.find('itunes:summary', itunes_ns) is not None or
                channel.find('itunes:author', itunes_ns) is not None or
                channel.find('itunes:category', itunes_ns) is not None
            )

            if not is_podcast:
                return None

            # Extract podcast information
            title = channel.find('title').text if channel.find('title') is not None else 'Unknown Podcast'
            description = channel.find('description').text if channel.find('description') is not None else ''

            # Get iTunes specific metadata
            author = channel.find('itunes:author', itunes_ns)
            author = author.text if author is not None else None

            category = channel.find('itunes:category', itunes_ns)
            category = category.get('text') if category is not None else None

            return {
                'platform': 'RSS',
                'source_type': 'FEED',
                'name': title,
                'description': description,
                'source_identifier': parsed_url.netloc + parsed_url.path,
                'url': url,
                'config': {
                    'type': 'podcast',
                    'category': category,
                    'language': channel.find('language').text if channel.find('language') is not None else None,
                    'copyright': channel.find('copyright').text if channel.find('copyright') is not None else None,
                    'last_build_date': channel.find('lastBuildDate').text if channel.find('lastBuildDate') is not None else None,
                    'image_url': channel.find('image/url').text if channel.find('image/url') is not None else None,
                    'explicit': channel.find('itunes:explicit', itunes_ns).text if channel.find('itunes:explicit', itunes_ns) is not None else None,
                }
            }

        except ET.ParseError:
            return None
        except Exception as e:
            logger.error(f"Podcast parsing error: {str(e)}")
            return None


    @classmethod
    def _parse_rss(cls, url: str, parsed_url: urlparse) -> Optional[Dict]:
        """Parse standard RSS feeds"""
        try:
            feed = feedparser.parse(url)

            if feed.get('feed') and feed.feed.get('title'):
                # Check if it's already been identified as a podcast
                if cls._looks_like_podcast(feed):
                    return None

                # Build config with comprehensive feed information
                config = {
                    'type': 'rss',
                    'feed_title': feed.feed.title,
                    'feed_link': feed.feed.get('link', ''),
                    'feed_subtitle': feed.feed.get('subtitle', ''),
                    'feed_updated': feed.feed.get('updated', ''),
                    'feed_language': feed.feed.get('language', 'en'),
                    'feed_author': feed.feed.get('author', ''),
                    'feed_generator': feed.feed.get('generator', ''),
                    'fetched_at': datetime.utcnow().isoformat()
                }

                # Add any additional feed metadata if available
                if hasattr(feed.feed, 'publisher'):
                    config['feed_publisher'] = feed.feed.publisher
                if hasattr(feed.feed, 'rights'):
                    config['feed_rights'] = feed.feed.rights
                if hasattr(feed.feed, 'image'):
                    config['feed_image'] = {
                        'url': feed.feed.image.get('url', ''),
                        'title': feed.feed.image.get('title', ''),
                        'link': feed.feed.image.get('link', '')
                    }

                # Try to determine update frequency if multiple entries available
                if len(feed.entries) > 1:
                    try:
                        dates = [
                            parser.parse(entry.published)
                            for entry in feed.entries[:5]
                            if hasattr(entry, 'published')
                        ]
                        if len(dates) > 1:
                            deltas = [(dates[i] - dates[i+1]).total_seconds() / 3600
                                    for i in range(len(dates)-1)]
                            avg_hours_between_posts = sum(deltas) / len(deltas)
                            config['estimated_update_frequency_hours'] = round(avg_hours_between_posts, 1)
                    except Exception as e:
                        logger.warning(f"Could not determine update frequency: {str(e)}")

                return {
                    'platform': 'RSS',
                    'source_type': 'FEED',
                    'name': feed.feed.title,
                    'description': feed.feed.get('description', '') or feed.feed.get('subtitle', ''),
                    'source_identifier': parsed_url.netloc + parsed_url.path,
                    'url': url,
                    'config': config
                }

        except Exception as e:
            logger.error(f"RSS parsing error for {url}: {str(e)}")
            return None

    @staticmethod
    def _looks_like_podcast(feed) -> bool:
        """Check if a feedparser feed looks like a podcast"""
        if not feed.entries:
            return False

        # Check for podcast-specific indicators
        if hasattr(feed.feed, 'itunes_type'):
            return True

        # Check for enclosures or media content in the first few entries
        for entry in feed.entries[:3]:
            if (hasattr(entry, 'enclosures') and entry.enclosures) or \
               (hasattr(entry, 'media_content') and entry.media_content):
                return True

        # Check for common podcast RSS namespaces
        podcast_namespaces = [
            'http://www.itunes.com/dtds/podcast-1.0.dtd',
            'http://www.google.com/schemas/play-podcasts/1.0'
        ]

        feed_namespaces = getattr(feed, 'namespaces', {}).values()
        return any(ns in feed_namespaces for ns in podcast_namespaces)
