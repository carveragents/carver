import os
import sys

from typing import List, Dict, Any
from datetime import datetime, timedelta

from supabase import create_client, Client
from dateutil import parser

from carver.utils import get_config

__all__ = [

    'get_supabase_client',
    'format_datetime',
    'parse_date_filter',
    'chunks'
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
