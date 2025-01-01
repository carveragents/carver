import traceback
import re
import json
import logging

from urllib.parse import urlparse, parse_qs
from typing import Dict, Optional, Tuple
import xml.etree.ElementTree as ET
from datetime import datetime

import requests
import feedparser

from pytube import YouTube, Playlist, Channel

logger = logging.getLogger(__name__)

class SourceURLParser:
    """Parse various URLs to extract source information with rich metadata"""

    YOUTUBE_PATTERNS = {
        'channel': r'(?:https?:\/\/)?(?:www\.)?youtube\.com\/(?:c\/|channel\/|user\/)?([a-zA-Z0-9_-]+)',
        'playlist': r'(?:https?:\/\/)?(?:www\.)?youtube\.com\/(?:playlist\?list=|watch\?v=[a-zA-Z0-9_-]+&list=)([a-zA-Z0-9_-]+)',
        'search': r'(?:https?:\/\/)?(?:www\.)?youtube\.com\/results\?(?:[^&]*&)*search_query=([^&]+)'
    }

    GITHUB_PATTERNS = {
        'repository': r'(?:https?:\/\/)?(?:www\.)?github\.com\/([a-zA-Z0-9_-]+)\/([a-zA-Z0-9_-]+)\/?$'
    }

    REDDIT_PATTERNS = {
        'subreddit': r'(?:https?:\/\/)?(?:www\.)?reddit\.com\/r\/([a-zA-Z0-9_-]+)\/?$',
        'user': r'(?:https?:\/\/)?(?:www\.)?reddit\.com\/user\/([a-zA-Z0-9_-]+)\/?$'
    }

    SUBSTACK_PATTERNS = {
        'newsletter': r'(?:https?:\/\/)?([a-zA-Z0-9-]+)\.substack\.com\/?$',
        'post': r'(?:https?:\/\/)?([a-zA-Z0-9-]+)\.substack\.com\/p\/([a-zA-Z0-9-]+)'
    }

    EXA_PATTERNS = {
        'search': r'(?:https?:\/\/)?([a-zA-Z0-9-]+)\.exa\.ai\/?$',
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
                cls._parse_substack,
                cls._parse_reddit,
                cls._parse_podcast,
                cls._parse_rss,
                cls._parse_exa
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

        if '/results' in url:
            try:
                # Parse query parameters
                query_params = parse_qs(parsed_url.query)

                if 'search_query' in query_params:
                    search_query = query_params['search_query'][0]

                    # Build search config
                    search_config = {
                        'type': 'search',
                        'query': search_query,
                        'fetched_at': datetime.utcnow().isoformat()
                    }

                    # Add additional search parameters if present
                    for param in ['sp', 'order']:
                        if param in query_params:
                            search_config[param] = query_params[param][0]

                    return {
                        'platform': 'YOUTUBE',
                        'source_type': 'SEARCH',
                        'name': f'YouTube Search: {search_query}',
                        'description': f'YouTube search results for: {search_query}',
                        'source_identifier': search_query,
                        'url': url,
                        'config': search_config
                    }
            except Exception as e:
                logger.error(f"YouTube search parsing error: {str(e)}")
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

                try:
                    title = playlist.title
                except:
                    title = "Playlist {playlist_id}"

                try:
                    description = playlist.description
                except:
                    description = ""

                # Sometimes playlist title might not be immediately available
                # In such cases, try to fetch it from the first video
                if not title and playlist.video_urls:
                    try:
                        first_video = YouTube(playlist.video_urls[0])
                        title = first_video.playlist_title or f"Playlist: {playlist_id}"
                        description = first_video.playlist_description or ""
                    except Exception as e:
                        traceback.print_exc()
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
                traceback.print_exc()
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
                        'recent_posts': len(feed.entries),
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

    @classmethod
    def _parse_exa(cls, url: str, parsed_url: urlparse) -> Optional[Dict]:
        """Parse Substack URLs with metadata"""
        if 'exa.ai' not in parsed_url.netloc:
            return None

        return {
            'platform': 'EXA',
            'source_type': 'SEARCH',
            "name": "Exa Search",
            "description": "Exa Search",
            "source_identifier": "",
            'url': url,
            'config': {
                "query": "to be filled"
            }
        }
    @classmethod
    def _parse_substack(cls, url: str, parsed_url: urlparse) -> Optional[Dict]:
        """Parse Substack URLs with metadata"""
        if 'substack.com' not in parsed_url.netloc:
            return None

        try:
            # Try matching newsletter homepage pattern
            newsletter_name = None
            newsletter_match = re.match(cls.SUBSTACK_PATTERNS['newsletter'], url)
            if newsletter_match:
                newsletter_name = newsletter_match.group(1)
            else:
                post_match = re.match(cls.SUBSTACK_PATTERNS['post'], url)
                if post_match:
                    newsletter_name = post_match.group(1)

            if newsletter_name is None:
                logger.error(f"Substack URL parsing error")
                return None

            metadata = newsletter.get_newsletter_post_metadata(
                newsletter_name,
                start_offset=0,
                end_offset=1  # Just get one post to get newsletter info
            )

            if metadata:
                latest_post = metadata[0]
                return {
                    'platform': 'SUBSTACK',
                    'source_type': 'NEWSLETTER',
                    'name': latest_post.get('publication_name', newsletter_name),
                    'description': latest_post.get('publication_description', ''),
                    'source_identifier': newsletter_name,
                    'url': url,
                    'config': {
                        'type': 'newsletter',
                        'subdomain': newsletter_name,
                        'newsletter_name': newsletter_name,
                        'author': latest_post.get('author', {}).get('name', ''),
                        'fetched_at': datetime.utcnow().isoformat()
                    }
                }
        except Exception as e:
            # Fall back to basic info if API fails
            return {
                'platform': 'SUBSTACK',
                'source_type': 'NEWSLETTER',
                'name': newsletter_name,
                'description': '',
                'source_identifier': newsletter_name,
                'url': url,
                'config': {
                    'type': 'newsletter',
                    'newsletter_name': newsletter_name,
                    'subdomain': newsletter_name,
                    'fetched_at': datetime.utcnow().isoformat()
                }
            }

        except Exception as e:
            logger.error(f"Substack parsing error: {str(e)}")
            return None

        return None
