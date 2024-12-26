import os
import sys
import json

from pathlib import Path

from decouple import Config, RepositoryIni

__all__ = [
    'get_config',
    'flatten'
]

# Configuration file locations to search
CONFIG_LOCATIONS = []
ENV_LOCATIONS = []
for parent in [
        Path('/etc/carver-db/'),          # System-wide
        Path('/etc/carver/'),          # System-wide
        Path('/etc/secrets/'),          # System-wide
        Path.home() / '.carver-db',
        Path.home() / '.carver',
        Path.home() / '.secrets'
]:
    CONFIG_LOCATIONS.append( parent / 'carver.ini')
    CONFIG_LOCATIONS.append( parent / '.carver.ini')
    CONFIG_LOCATIONS.append( parent / 'carver-db.ini')
    CONFIG_LOCATIONS.append( parent / '.carver-db.ini')
    CONFIG_LOCATIONS.append( parent / 'server.ini')
    CONFIG_LOCATIONS.append( parent / 'client.ini')
    ENV_LOCATIONS.append( parent / '.env')

def get_config():
    config = None

    # First try INI files
    for config_path in CONFIG_LOCATIONS:
        if config_path.is_file():
            config = Config(RepositoryIni(str(config_path)))
            break

    # Then try ENV files
    if not config:
        for env_path in ENV_LOCATIONS:
            if env_path.is_file():
                config = Config(RepositoryEnv(str(env_path)))
                break

    # If no config file is found, use environment variables
    if not config:
        config = Config(RepositoryEnv())

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
