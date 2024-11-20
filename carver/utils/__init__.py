import os
import sys
from pathlib import Path

from decouple import Config, RepositoryIni

__all__ = [
    'get_config',
]

def get_config():
    config = Config(RepositoryIni(str(Path.home() / '.carver' / 'client.ini')))
    return config

