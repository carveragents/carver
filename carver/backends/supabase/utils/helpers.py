import os
import sys
import json
import glob

from typing import List, Dict, Any
from collections import defaultdict
from datetime import datetime, timedelta
import importlib.util

from supabase import create_client, Client

from carver.utils import get_config, parse_date_filter, chunks, format_datetime

thisdir = os.path.dirname(__file__)

__all__ = [
    'get_supabase_client',
    'topological_sort',
    'hyperlink',
    'get_spec_config',
    'load_template',
    'format_dependency_tree'
]

def get_supabase_client() -> Client:
    """Initialize Supabase client using credentials from config file."""

    config = get_config()
    supabase_url = config('SUPABASE_URL')
    supabase_key = config('SUPABASE_KEY')

    return create_client(supabase_url, supabase_key)

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
    for node in list(graph.keys()):
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


def get_spec_config(path: str, raw=False, show=False) -> Any:
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

    return module.get_config(raw=raw, show=show)

def load_template(name: str,
                  model: str = "",
                  raw: bool = False,
                  show: bool = False,
                  ) -> Dict:
    """Load and validate the template file"""

    parentdirs = [
        os.path.join(thisdir, "..", 'templates'),
        "."
    ]
    if ((model not in ['', None]) and
        (not name.startswith(f"{model}_"))):
        name = f"{model}_{name}"

    alternatives = []
    for parentdir in parentdirs:
        patterns = [f"{parentdir}/*{name}.json",
                    f"{parentdir}/*{name}.py"]
        for pattern in patterns:
            alternatives.extend(glob.glob(pattern))

    if len(alternatives) == 0:
        raise ValueError(f"Template missing required fields: {name}")

    template_path = alternatives[0]
    print("Using template from:", template_path)

    # Could be json or py
    template = get_spec_config(template_path, raw=raw, show=show)

    return template

def format_dependency_tree(specs: list, indent: str = "") -> list:
    """Format specifications as a dependency tree"""
    spec_map = {spec['id']: spec for spec in specs}
    formatted = []

    def format_spec(spec, indent):
        deps = spec['config'].get('dependencies', [])
        if isinstance(deps, (int, str)):
            deps = [int(deps)]
        elif deps is None:
            deps = []

        lines = [
            f"{indent}├── Name: {spec['name']}",
            f"{indent}│   ID: {spec['id']}",
            f"{indent}│   Generator: {spec['config'].get('generator', 'N/A')}",
            f"{indent}│   Dependencies: {deps if deps else 'None'}"
        ]
        return lines

    # Start with specs that have no dependencies
    processed = set()
    def process_spec(spec_id, current_indent):
        if spec_id in processed:
            return []

        spec = spec_map[spec_id]
        formatted.extend(format_spec(spec, current_indent))
        processed.add(spec_id)

        # See whose parent is this spec_id
        deps = []
        for spec in specs:
            if spec['id'] in processed:
                continue
            if spec_id in spec['config'].get('dependencies', []):
                deps.append(spec['id'])

        for dep in deps:
            if dep in spec_map:
                formatted.append(f"{current_indent}│")
                process_spec(dep, current_indent + "    ")
            else:
                print(f"{dep} not in spec_map")

    # Find root specs (those with no dependencies)
    root_specs = []
    for spec in specs:
        dependencies = spec['config'].get('dependencies', [])
        dependencies = [d for d in dependencies if isinstance(d, int)]
        if len(dependencies) == 0:
            root_specs.append(spec)

    for spec in root_specs:
        process_spec(spec['id'], "")
        formatted.append("")

    return formatted

