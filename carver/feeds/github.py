import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import requests
from .base import FeedReader

logger = logging.getLogger(__name__)

class GithubRepositoryReader(FeedReader):
    """Reader for GitHub repositories"""

    def __init__(self, source: Dict[str, Any], max_results: Optional[int] = None):
        super().__init__(source, max_results)
        self.github_token = self.config.get('github_token')
        self.headers = {
            'Accept': 'application/vnd.github.v3+json'
        }
        if self.github_token:
            self.headers['Authorization'] = f'token {self.github_token}'

    def get_content_identifier(self, item: Dict[str, Any]) -> str:
        return str(item['id'])

    def prepare_item(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert GitHub API response to database item"""
        base_item = super().prepare_item(raw_item)

        # Determine the item type and prepare accordingly
        item_type = self._determine_item_type(raw_item)

        base_item.update({
            'content_type': item_type,
            'title': self._get_title(raw_item, item_type),
            'description': self._get_description(raw_item, item_type),
            'author': raw_item.get('user', {}).get('login'),
            'published_at': raw_item.get('created_at'),
            'last_updated_at': raw_item.get('updated_at'),
            'url': raw_item.get('html_url'),
            'language': raw_item.get('language', 'en'),
            'content_metrics': self._get_metrics(raw_item, item_type),
            'tags': self._get_tags(raw_item, item_type)
        })

        return base_item

    def read(self) -> List[Dict[str, Any]]:
        """Read repository activity"""
        repo_info = self.config
        owner = repo_info['owner']
        repo = repo_info['full_name'].split('/')[-1]

        items = []

        try:
            # Get releases
            releases = self._get_releases(owner, repo)
            items.extend(releases)

            # Get issues and pull requests
            issues_prs = self._get_issues_and_prs(owner, repo)
            items.extend(issues_prs)

            # Get commits
            commits = self._get_commits(owner, repo)
            items.extend(commits)

            # Respect max_results if specified
            if self.max_results:
                items.sort(key=lambda x: x.get('created_at', ''), reverse=True)
                items = items[:self.max_results]

            return [self.prepare_item(item) for item in items]

        except Exception as e:
            logger.error(f"Error reading GitHub repository: {str(e)}")
            raise

    def _get_releases(self, owner: str, repo: str) -> List[Dict[str, Any]]:
        """Get repository releases"""
        url = f"https://api.github.com/repos/{owner}/{repo}/releases"
        releases = []

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            for release in response.json():
                release['item_type'] = 'RELEASE'
                releases.append(release)

        except Exception as e:
            logger.warning(f"Error fetching releases: {str(e)}")

        return releases

    def _get_issues_and_prs(self, owner: str, repo: str) -> List[Dict[str, Any]]:
        """Get repository issues and pull requests"""
        url = f"https://api.github.com/repos/{owner}/{repo}/issues"
        params = {'state': 'all'}
        items = []

        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()

            for item in response.json():
                item['item_type'] = 'PULL_REQUEST' if 'pull_request' in item else 'ISSUE'
                items.append(item)

        except Exception as e:
            logger.warning(f"Error fetching issues and PRs: {str(e)}")

        return items

    def _get_commits(self, owner: str, repo: str) -> List[Dict[str, Any]]:
        """Get repository commits"""
        url = f"https://api.github.com/repos/{owner}/{repo}/commits"
        commits = []

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            for commit in response.json():
                commit['item_type'] = 'COMMIT'
                commits.append(commit)

        except Exception as e:
            logger.warning(f"Error fetching commits: {str(e)}")

        return commits

    def _determine_item_type(self, item: Dict[str, Any]) -> str:
        """Determine the type of GitHub item"""
        return item.get('item_type', 'UNKNOWN')

    def _get_title(self, item: Dict[str, Any], item_type: str) -> str:
        """Get appropriate title based on item type"""
        if item_type == 'RELEASE':
            return item.get('name', item.get('tag_name', 'Unnamed Release'))
        elif item_type in ['ISSUE', 'PULL_REQUEST']:
            return item.get('title', 'Unnamed Issue/PR')
        elif item_type == 'COMMIT':
            return item.get('commit', {}).get('message', '').split('\n')[0][:100]
        return 'Unnamed Item'

    def _get_description(self, item: Dict[str, Any], item_type: str) -> str:
        """Get appropriate description based on item type"""
        if item_type == 'RELEASE':
            return item.get('body', '')
        elif item_type in ['ISSUE', 'PULL_REQUEST']:
            return item.get('body', '')
        elif item_type == 'COMMIT':
            return item.get('commit', {}).get('message', '')
        return ''

    def _get_metrics(self, item: Dict[str, Any], item_type: str) -> Dict[str, Any]:
        """Get metrics based on item type"""
        metrics = {}

        if item_type == 'RELEASE':
            metrics.update({
                'download_count': sum(asset.get('download_count', 0) for asset in item.get('assets', []))
            })
        elif item_type in ['ISSUE', 'PULL_REQUEST']:
            metrics.update({
                'comments': item.get('comments', 0),
                'state': item.get('state', ''),
                'labels': [label['name'] for label in item.get('labels', [])]
            })
        elif item_type == 'COMMIT':
            stats = item.get('stats', {})
            metrics.update({
                'additions': stats.get('additions', 0),
                'deletions': stats.get('deletions', 0),
                'total_changes': stats.get('total', 0)
            })

        return metrics

    def _get_tags(self, item: Dict[str, Any], item_type: str) -> str:
        """Get tags based on item type"""
        tags = [item_type.lower()]

        if item_type in ['ISSUE', 'PULL_REQUEST']:
            tags.extend([label['name'] for label in item.get('labels', [])])
            tags.append(item.get('state', '').lower())

        return ','.join(tags)
