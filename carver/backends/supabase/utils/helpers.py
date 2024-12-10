import os
import sys
import json

from typing import List, Dict, Any
from collections import defaultdict
from datetime import datetime, timedelta
import importlib.util

from supabase import create_client, Client
from dateutil import parser

from carver.utils import get_config

__all__ = [
    'get_supabase_client',
    'format_datetime',
    'parse_date_filter',
    'chunks',
    'topological_sort',
    'hyperlink',
    'get_spec_config'
]

def get_supabase_client() -> Client:
    """Initialize Supabase client using credentials from config file."""

    config = get_config()
    supabase_url = config('SUPABASE_URL')
    supabase_key = config('SUPABASE_KEY')

    return create_client(supabase_url, supabase_key)

def format_datetime(dt_str: str) -> str:
    """Format datetime string for display"""
    dt = parser.parse(dt_str)
    return dt.strftime('%Y-%m-%d %H:%M')

def parse_date_filter(date_str: str) -> datetime:
    """Parse date filter string into datetime object"""
    if date_str.endswith('h'):
        hours = int(date_str[:-1])
        return datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(hours=hours)
    elif date_str.endswith('d'):
        days = int(date_str[:-1])
        return datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(days=days)
    elif date_str.endswith('w'):
        weeks = int(date_str[:-1])
        return datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(weeks=weeks)
    elif date_str.endswith('m'):
        months = int(date_str[:-1])
        return datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(weeks=months*4)
    else:
        return parser.parse(date_str)

def chunks(lst: List[Any], n: int) -> List[List[Any]]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def build_dependency_graph(specs):
    """Build a graph of specification dependencies."""
    graph = defaultdict(list)

    specs = sorted(specs, key=lambda x: x['id'])
    for spec in specs:
        spec_id = spec['id']
        dependencies = spec.get('config', {}).get('dependencies', [])
        if isinstance(dependencies, int):
            dependencies = [dependencies]
        elif isinstance(dependencies, str):
            dependencies = [int(dependencies)]
        graph[spec_id] = dependencies

    return graph

def topological_sort(specs):
    """Sort specifications based on dependencies."""

    graph = build_dependency_graph(specs)

    def visit(node, visited, temp_mark, order, graph):
        if node in temp_mark:
            raise ValueError(f"Circular dependency detected involving spec {node}")
        if node not in visited:
            temp_mark.add(node)
            for neighbor in graph[node]:
                visit(neighbor, visited, temp_mark, order, graph)
            temp_mark.remove(node)
            visited.add(node)
            order.append(node)

    visited = set()
    temp_mark = set()
    order = []
    for node in graph:
        if node not in visited:
            visit(node, visited, temp_mark, order, graph)

    return order


def hyperlink(uri, label=None):
    if label is None:
        label = uri
    parameters = ''

    # OSC 8 ; params ; URI ST <name> OSC 8 ;; ST
    escape_mask = '\033]8;{};{}\033\\{}\033]8;;\033\\'

    return escape_mask.format(parameters, uri, label)


def get_spec_config(path: str) -> Any:
    """
    Load a Python/json file and return config

    Args:
        file_path: Path to the Python file

    Returns:
        The loaded module
    """

    if path is None or not os.path.exists(path):
        raise Exception("Invalid/missing config file: {path}")

    # Handle json
    if path.lower().endswith(".json"):
        return json.load(open(path))

    if not path.lower().endswith(".py"):
        raise Exception("Unsupported file format")

    # Get absolute path
    abs_path = os.path.abspath(path)

    # Get module name from file name
    module_name = os.path.splitext(os.path.basename(path))[0]

    # Load the module specification
    spec = importlib.util.spec_from_file_location(module_name, abs_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for module {module_name} from {abs_path}")

    # Create the module
    module = importlib.util.module_from_spec(spec)

    # Execute the module
    spec.loader.exec_module(module)

    return module.get_config()
