import os
import sys
from pathlib import Path

from decouple import Config, RepositoryIni

__all__ = [
    'get_config',
    'flatten'
]

def get_config():
    config = Config(RepositoryIni(str(Path.home() / '.carver' / 'client.ini')))
    return config


def flatten(data):

    text = ""
    if isinstance(data, list):
        for d in data:
            text += flatten(d) + "\n"
    elif isinstance(data, dict):
        for key, value in data.items():
            text += key + "\n-------\n" + flatten(value) + "\n"
    else:
        text = str(data)

    return text
