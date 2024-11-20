import pytest
import tempfile
import os
from pathlib import Path
from youtube_feed.storage import SQLiteStorage

@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname

@pytest.fixture
def db_path(temp_dir):
    return os.path.join(temp_dir, "test.db")

@pytest.fixture
def storage(db_path):
    return SQLiteStorage(db_path)

@pytest.fixture
def sample_video_data():
    return {
        'youtube_id': 'test123',
        'url': 'https://youtube.com/watch?v=test123',
        'filename': 'test_video.mp4',
        'title': 'Test Video',
        'description': 'Test Description',
        'summary': 'Test Summary',
        'duration': 120,
        'author': 'Test Author',
        'publish_date': '2024-01-01T00:00:00',
        'view_count': 1000,
        'thumb_url': 'https://img.youtube.com/test123',
        'playlist_id': 'playlist123',
        'channel_id': 'channel123',
        'tags': ['test', 'video'],
        'categories': ['Education']
    }
