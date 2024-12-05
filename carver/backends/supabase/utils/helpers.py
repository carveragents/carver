import os
import sys
import json

from typing import List, Dict, Any
from collections import defaultdict
from datetime import datetime, timedelta

from supabase import create_client, Client
from dateutil import parser

from carver.utils import get_config

__all__ = [
    'get_supabase_client',
    'format_datetime',
    'parse_date_filter',
    'chunks',
    'topological_sort'
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


