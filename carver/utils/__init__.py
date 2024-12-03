import os
import sys
import json

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

class SafeEncoder(json.JSONEncoder):

    def default(self, obj):
        try:
            if isinstance(obj, (datetime, date)):
                result = obj.isoformat()
            elif isinstance(obj, (tuple)):
                result = super().default(list(obj))
            elif hasattr(obj, 'to_dict'):
                return super().default(obj.to_dict())
            elif hasattr(obj, 'to_json'):
                return super().default(obj.to_json())
            else:
                result = super().default(obj)
        except:
            result = str(obj)

        return result
