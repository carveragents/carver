import os
import sys
import json
import traceback

from typing import Optional, List, Dict, Any
from datetime import datetime
from pathlib import Path

import click

from tabulate import tabulate

from ..utils.helpers import topological_sort
from ..utils import get_spec_config
from carver.utils import format_datetime, parse_date_filter
from .artifact_manager import ArtifactManager

thisdir = os.path.dirname(__file__)

####################################
# Artifact Specification Commands
####################################
@click.group()
@click.pass_context
def spec(ctx):
    """Manage artifact specifications."""
    ctx.obj['manager'] = ArtifactManager(ctx.obj['supabase'])

@spec.command()
@click.option('--source-id', required=True, type=int, help='Source ID')
@click.option('--name', required=True, help='Specification name')
@click.option('--description', help='Specification description')
@click.option('--config', required=True, type=click.Path(exists=True), help='Path to JSON/py config file')
@click.pass_context
def add(ctx, source_id: int, name: str, description: Optional[str], config: str):
    """Add a new artifact specification."""
    manager = ctx.obj['manager']
    try:
        config_data = get_spec_config(config)

        db = ctx.obj['supabase']
        source = db.source_get(source_id)
        if not source:
            click.echo(f"Source with ID {source_id} not found", err=True)
            return

        spec = manager.specification_create(source, {
            'source_id': source_id,
            'name': name,
            'description': description,
            'config': config_data
        })
        click.echo(f"Created specification {spec['id']}: {spec['name']}")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error creating specification: {str(e)}", err=True)

@spec.command()
@click.option('--source-id', type=int, help='Filter by source ID')
@click.option('--name', help='Filter by name (partial match)')
@click.option('--active/--inactive', default=True, help='Filter by active status')
@click.option('--format', 'output_format',
              type=click.Choice(['table', 'grid', 'pipe', 'orgtbl', 'rst', 'mediawiki', 'html']),
              default='table',
              help='Output format')
@click.pass_context
def search(ctx, source_id: Optional[int], name: Optional[str],
           active: Optional[bool], output_format: str):
    """Search artifact specifications."""

    db = ctx.obj['supabase']
    try:
        specs = db.specification_search(
            source_id=source_id,
            name=name,
            active=active
        )

        if specs:
            specmap = {spec['id']: spec for spec in specs}

            headers = ['ID', 'Source', 'Name', 'Generator', 'Dependencies', 'Active', 'Updated']
            rows = []
            for spec in specs:
                source_name = spec['carver_source']['name'] if spec.get('carver_source') else 'N/A'

                # Get dependencies
                deps = spec['config'].get('dependencies', [])
                if isinstance(deps, (int, str)):
                    deps = [deps]
                elif deps is None:
                    deps = []

                # Format dependencies string
                if deps:
                    # Get dependency names
                    dep_names = []
                    for dep_id in deps:
                        if dep_id not in specmap:
                            click.echo(f"Warning: Specification {dep_id} not found", err=True)
                            return
                        dep_spec = specmap[dep_id]
                        dep_names.append(f"{dep_id}:{dep_spec['name'][:20]}")
                    deps_str = ",\n".join(dep_names)
                else:
                    deps_str = "None"

                rows.append([
                    spec['id'],
                    f"{source_name[:20]} ({spec['source_id']})",
                    spec['name'],
                    spec['config'].get('generator'),
                    deps_str,
                    '✓' if spec['active'] else '✗',
                    format_datetime(spec['updated_at'])
                ])

            click.echo(tabulate(rows, headers=headers, tablefmt=output_format))
            click.echo(f"\nTotal specifications: {len(specs)}")
        else:
            click.echo("No specifications found")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@spec.command()
@click.argument('spec_id', type=int)
@click.pass_context
def show(ctx, spec_id: int):
    """Show detailed information about a specification."""
    db = ctx.obj['supabase']
    try:
        spec = db.specification_get(spec_id)
        if not spec:
            click.echo(f"Specification {spec_id} not found", err=True)
            return

        click.echo("\n=== Specification Information ===")
        click.echo(f"ID: {spec['id']}")
        click.echo(f"Name: {spec['name']}")
        click.echo(f"Description: {spec.get('description', 'N/A')}")
        click.echo(f"Source ID: {spec['source_id']}")
        click.echo(f"Active: {'Yes' if spec['active'] else 'No'}")
        click.echo(f"Created: {format_datetime(spec['created_at'])}")
        click.echo(f"Updated: {format_datetime(spec['updated_at'])}")

        click.echo("\n=== Configuration ===")
        click.echo(json.dumps(spec['config'], indent=2))
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@spec.command()
@click.argument('spec_id', type=int)
@click.option('--source-id', type=int, help='Source ID')
@click.option('--name', help='New specification name')
@click.option('--description', help='New specification description')
@click.option('--config', type=click.Path(exists=True), help='Path to new JSON config file')
@click.option('--active/--inactive', default=None, type=bool, help='Update active status')
@click.pass_context
def update(ctx, spec_id: int, source_id: Optional[int],
           name: Optional[str], description: Optional[str],
           config: Optional[str], active: Optional[bool]):
    """Update an existing artifact specification."""

    db = ctx.obj['supabase']
    manager = ctx.obj['manager']

    try:
        update_data = {}

        if source_id:
            db = ctx.obj['supabase']
            source = db.source_get(source_id)
            if not source:
                click.echo(f"Source with ID {source_id} not found", err=True)
                return
            update_data['source_id'] = source['id']

        # Build update data from provided options
        if name is not None:
            update_data['name'] = name
        if description is not None:
            update_data['description'] = description
        if active is not None:
            update_data['active'] = active
        if config:
            config_data = get_spec_config(config)
            update_data['config'] = config_data

        if not update_data:
            click.echo("No updates provided")
            return

        spec = manager.specification_update(spec_id, update_data)
        click.echo(f"Updated specification {spec['id']}: {spec['name']}")

        # Show updated specification
        click.echo("\nUpdated Specification:")
        click.echo("======================")
        click.echo(f"ID: {spec['id']}")
        click.echo(f"Name: {spec['name']}")
        click.echo(f"Description: {spec.get('description', 'N/A')}")
        click.echo(f"Active: {'Yes' if spec['active'] else 'No'}")
        if config:
            click.echo("\nUpdated Configuration:")
            click.echo(json.dumps(spec['config'], indent=2))

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error updating specification: {str(e)}", err=True)

@spec.command()
@click.argument('specs')
@click.pass_context
def activate(ctx, specs: str):
    """
    Activate one or more specifications.

    Args:
       specs (str): Comma-separated list of specification IDs
    """
    db = ctx.obj['supabase']
    try:
        # Parse specification IDs
        spec_ids = [int(s.strip()) for s in specs.split(',')]

        # Validate specifications exist
        for spec_id in spec_ids:
            spec = db.specification_get(spec_id)
            if not spec:
                click.echo(f"Warning: Specification {spec_id} not found", err=True)
                continue

        # Prepare update data
        update_data = {
            'active': True,
            'updated_at': datetime.utcnow().isoformat()
        }

        # Update each specification
        updated = []
        for spec_id in spec_ids:
            try:
                result = db.specification_update(spec_id, update_data)
                if result:
                    updated.append(result)
            except Exception as e:
                click.echo(f"Error activating specification {spec_id}: {str(e)}", err=True)
                continue

        if updated:
            click.echo(f"Successfully activated {len(updated)} specifications:")
            for spec in updated:
                click.echo(f"- {spec['id']}: {spec['name']}")
        else:
            click.echo("No specifications were activated")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@spec.command()
@click.argument('specs')
@click.pass_context
def deactivate(ctx, specs: str):
    """
    Deactivate one or more specifications.

    Args:
       specs (str): Comma-separated list of specification IDs
    """
    db = ctx.obj['supabase']
    try:
        # Parse specification IDs
        spec_ids = [int(s.strip()) for s in specs.split(',')]

        # Validate specifications exist
        for spec_id in spec_ids:
            spec = db.specification_get(spec_id)
            if not spec:
                click.echo(f"Warning: Specification {spec_id} not found", err=True)
                continue

        # Prepare update data
        update_data = {
            'active': False,
            'updated_at': datetime.utcnow().isoformat()
        }

        # Update each specification
        updated = []
        for spec_id in spec_ids:
            try:
                result = db.specification_update(spec_id, update_data)
                if result:
                    updated.append(result)
            except Exception as e:
                click.echo(f"Error deactivating specification {spec_id}: {str(e)}", err=True)
                continue

        if updated:
            click.echo(f"Successfully deactivated {len(updated)} specifications:")
            for spec in updated:
                click.echo(f"- {spec['id']}: {spec['name']}")
        else:
            click.echo("No specifications were deactivated")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@spec.command()
@click.argument('spec_id', type=int)
@click.option('--depends-on', required=True, help='Comma-separated list of specification IDs that this spec depends on')
@click.pass_context
def update_dependencies(ctx, spec_id: int, depends_on: str):
    """Update dependencies of a specification, replacing any existing dependencies."""
    db = ctx.obj['supabase']
    try:
        # Get the specification
        spec = db.specification_get(spec_id)
        if not spec:
            click.echo(f"Specification {spec_id} not found", err=True)
            return

        # Parse dependency IDs
        try:
            dependency_ids = [int(s.strip()) for s in depends_on.split(',')]
        except ValueError:
            click.echo("Error: Dependencies must be comma-separated integers", err=True)
            return

        # Validate all dependency specifications exist
        for dep_id in dependency_ids:
            dep_spec = db.specification_get(dep_id)
            if not dep_spec:
                click.echo(f"Warning: Specification {dep_id} not found", err=True)
                return

        # Update the config with new dependencies
        config = spec.get('config', {})
        config['dependencies'] = dependency_ids
        update_data = {
            'config': config,
            'updated_at': datetime.utcnow().isoformat()
        }

        # Update the specification
        updated_spec = db.specification_update(spec_id, update_data)
        if updated_spec:
            click.echo(f"Successfully updated dependencies for specification {spec_id}")
            click.echo("\nNew dependencies:")
            for dep_id in dependency_ids:
                dep_spec = db.specification_get(dep_id)
                if dep_spec:
                    click.echo(f"- {dep_id}: {dep_spec['name']}")
        else:
            click.echo("Error updating specification", err=True)

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)



def update_spec_dependencies(spec: Dict, ref_to_new_map: Dict[int, int]) -> Dict:
    """Update specification dependencies using the mapping from reference to new IDs"""

    now = datetime.utcnow().isoformat()
    updated_spec = {
        'name': spec['name'],
        'description': spec['description'],
        'created_at': now,
        'updated_at': now
    }

    config = spec['config'].copy()
    deps = config.get('dependencies', [])
    if isinstance(deps, (int, str)):
        deps = [int(deps)]
    elif deps is None:
        deps = []
    else:
        deps = [int(d) for d in deps]

    # Update dependencies using the mapping
    new_deps = [ref_to_new_map[dep] for dep in deps if dep in ref_to_new_map]
    config['dependencies'] = new_deps
    updated_spec['config'] = config

    return updated_spec

@spec.command()
@click.option('--reference-source', required=True, type=int, help='Reference source ID')
@click.option('--target-source', required=True, type=int, help='Target source ID')
@click.option('--auto-approve', is_flag=True, help='Automatically approve all additions')
@click.pass_context
def sync_specs(ctx, reference_source: int, target_source: int, auto_approve: bool):
    """Sync specifications from reference source to target source."""
    db = ctx.obj['supabase']
    manager = ctx.obj['manager']

    try:

        if reference_source == target_source:
            click.echo("Source and target should be different")
            return

        # Get reference source specifications
        ref_specs = db.specification_search(source_id=reference_source)
        if not ref_specs:
            click.echo(f"No specifications found for reference source {reference_source}")
            return

        # Get target source specifications
        target_specs = db.specification_search(source_id=target_source)

        # Get source details
        ref_source = db.source_get(reference_source)
        target_source = db.source_get(target_source)

        if not ref_source or not target_source:
            click.echo("Invalid source IDs provided")
            return

        click.echo(f"\nSyncing specifications from {ref_source['name']} to {target_source['name']}")

        # Get sorted specification IDs using helper function
        sorted_spec_ids = topological_sort(ref_specs)

        # Create a map of spec ID to spec data
        spec_map = {spec['id']: spec for spec in ref_specs}

        click.echo(f"Sorted reference spec ids {sorted_spec_ids}")

        # Track mapping from reference spec IDs to new spec IDs
        ref_to_new_map = {}

        # Process specifications in dependency order
        for spec_id in sorted_spec_ids:
            ref_spec = spec_map[spec_id]

            # Check if similar spec exists in target
            existing_spec = next(
                (s for s in target_specs
                 if s['name'] == ref_spec['name'] and
                 s['config'].get('generator') == ref_spec['config'].get('generator')),
                None
            )

            if existing_spec:
                click.echo(f"\nSkipping '{ref_spec['name']}' - already exists (ID: {existing_spec['id']})")
                ref_to_new_map[ref_spec['id']] = existing_spec['id']
                continue

            # Show spec details and confirm addition
            click.echo(f"\nReference Specification to add:")
            click.echo("=" * 40)
            click.echo(f"Name: {ref_spec['name']}")
            click.echo(f"Description: {ref_spec.get('description', 'N/A')}")
            click.echo(f"Config: {json.dumps(ref_spec['config'], indent=4)}")

            # Update dependencies in config
            new_spec_data = update_spec_dependencies(ref_spec, ref_to_new_map)
            new_spec_data['source_id'] = target_source['id']

            if auto_approve or click.confirm("Add this specification?"):
                try:
                    print(json.dumps(new_spec_data, indent=4))

                    new_spec = manager.specification_create(target_source, new_spec_data)
                    click.echo(f"Created specification {new_spec['id']}")
                    ref_to_new_map[ref_spec['id']] = new_spec['id']
                except Exception as e:
                    click.echo(f"Error creating specification: {str(e)}", err=True)
                    if not click.confirm("Continue with remaining specifications?"):
                        return
            else:
                click.echo("Skipped")

        click.echo("\nSpecification sync complete")
        click.echo(f"Added/Mapped {len(ref_to_new_map)} specifications")

    except Exception as e:
        click.echo(f"Error during sync: {str(e)}", err=True)
        raise

def load_template(name: str) -> Dict:
    """Load and validate the template file"""

    templatedir = os.path.join(thisdir, 'templates')
    alternatives = [
        name,
        os.path.join(templatedir, name),
        os.path.join(templatedir, name + ".py"),
        os.path.join(templatedir, name + ".json")
    ]

    template_path = None
    for path in alternatives:
        if os.path.exists(path):
            template_path = path
            break

    if template_path is None:
        raise ValueError(f"Template missing required fields: {name}")

    # Could be json or py
    template = get_spec_config(template_path)

    required_fields = {'name', 'description', 'specifications'}
    if not all(field in template for field in required_fields):
        raise ValueError(f"Template missing required fields: {required_fields}")

    for spec in template['specifications']:
        spec_required = {'id', 'name', 'description', 'config'}
        if not all(field in spec for field in spec_required):
            raise ValueError(f"Specification missing required fields: {spec_required}")

    return template

def update_template_dependencies(spec: Dict, dummy_to_real_map: Dict[int, int], existing_specs: List[Dict[str, Any]]) -> Dict:
    """Update specification dependencies using the mapping from dummy to real IDs"""

    now = datetime.utcnow().isoformat()
    updated_spec = {
        'name': spec['name'],
        'description': spec['description'],
        'created_at': now,
        'updated_at': now
    }

    config = spec['config'].copy()

    # Dependencies will look like this. transcription is an existing spec
    # ['transcription', 2001]

    deps = config.get('dependencies', [])
    resolved_deps = []
    unresolved_deps = []
    if isinstance(deps, int):
        resolved_deps = [deps]
    elif isinstance(deps, str) and deps.isdigit():
        resolved_deps = [int(deps)]
    elif deps is None:
        pass
    else:
        for d in deps:
            if isinstance(d, int):
                resolved_deps.append(d)
            elif isinstance(d, str) and d.isdigit():
                resolved_deps.append(int(d))
            else:
                unresolved_deps.append(d)

    # Update dependencies using the mapping
    new_resolved_deps = [dummy_to_real_map[dep] for dep in resolved_deps if dep in dummy_to_real_map]

    # Look through the existing
    new_unresoved_deps = []
    for d in unresolved_deps:
        found = False
        for s in existing_specs:
            if s['name'] == d:
                new_unresoved_deps.append(s['id'])
                print(f"Matched dependency: {d} -> {s['name']}:{s['id']}")
                found = True
                break
        if not found:
            raise Exception(f"Unable to resolve dependency: {d}")

    config['dependencies'] = new_resolved_deps + new_unresoved_deps
    updated_spec['config'] = config

    return updated_spec

@spec.command('add-from-template')
@click.option('--source-id', required=True, type=int, help='Target source ID')
@click.option('--template', required=True, help='Path to template JSON/py file')
@click.option('--auto-approve', is_flag=True, help='Automatically approve all additions')
@click.pass_context
def add_from_template(ctx, source_id: int, template: str, auto_approve: bool):
    """Add specifications to a source using a template file."""
    db = ctx.obj['supabase']
    manager = ctx.obj['manager']

    try:
        # Load and validate template
        template_data = load_template(template)
        click.echo(f"\nLoaded template: {template_data['name']}")

        # Get source details
        source = db.source_get(source_id)
        if not source:
            click.echo("Invalid source ID provided")
            return

        valid_platforms = template_data.get('platforms', [])
        if (("*" not in valid_platforms) and
            (source['platform'] not in valid_platforms)):
            click.echo(f"Source platform {source['platform']} not in valid platforms for this template: {valid_platforms}")
            return

        # Get existing source specifications
        existing_specs = db.specification_search(source_id=source_id,
                                                 active=True)

        print([[s['id'], s['active']] for s in existing_specs])

        # Get sorted specification IDs from template
        sorted_spec_ids = topological_sort(template_data['specifications'])

        # Create a map of template spec ID to spec data
        spec_map = {spec['id']: spec for spec in template_data['specifications']}

        print("Spec Map", json.dumps(spec_map, indent=4))
        print("Sorted specs", sorted_spec_ids)

        # Track mapping from dummy IDs to real spec IDs
        dummy_to_real_map = {}

        # Process specifications in dependency order
        for spec_id in sorted_spec_ids:

            if not isinstance(spec_id, int):
                continue

            template_spec = spec_map[spec_id]

            # Check if similar spec exists
            existing_spec = next(
                (s for s in existing_specs
                 if s['name'] == template_spec['name'] and
                 s['config'].get('generator') == template_spec['config'].get('generator')),
                None
            )

            if existing_spec:
                click.echo(f"\nSkipping '{template_spec['name']}' - already exists (ID: {existing_spec['id']})")
                dummy_to_real_map[template_spec['id']] = existing_spec['id']
                continue

            # Show spec details and confirm addition
            click.echo(f"\nSpecification to add:")
            click.echo("=" * 40)
            click.echo(f"Name: {template_spec['name']}")
            click.echo(f"Description: {template_spec.get('description', 'N/A')}")
            click.echo(f"Generator: {template_spec['config'].get('generator', 'N/A')}")

            # Update dependencies in config
            new_spec_data = update_template_dependencies(template_spec, dummy_to_real_map, existing_specs)
            # Remove template ID and add source ID

            if 'id' in new_spec_data:
                del new_spec_data['id']
            new_spec_data['source_id'] = source_id

            if auto_approve or click.confirm("Add this specification?"):
                try:
                    new_spec = manager.specification_create(source, new_spec_data)
                    click.echo(f"Created specification {new_spec['id']}")
                    dummy_to_real_map[template_spec['id']] = new_spec['id']
                except Exception as e:
                    traceback.print_exc()
                    click.echo(f"Error creating specification: {str(e)}", err=True)
                    if not click.confirm("Continue with remaining specifications?"):
                        return
            else:
                click.echo("Skipped")

        click.echo("\nTemplate processing complete")
        click.echo(f"Added/Mapped {len(dummy_to_real_map)} specifications")

    except Exception as e:
        click.echo(f"Error processing template: {str(e)}", err=True)
        raise

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
    root_specs = [spec for spec in specs if not spec['config'].get('dependencies')]
    for spec in root_specs:
        process_spec(spec['id'], "")
        formatted.append("")

    return formatted

@spec.command('list-templates')
@click.option('--show-content', is_flag=True, help='Show detailed template contents')
@click.pass_context
def list_templates(ctx, show_content):
    """List available specification templates."""
    # Get templates directory
    templates_dir = Path(__file__).parent / "templates"

    print(templates_dir)
    if not templates_dir.exists():
        click.echo("Templates directory not found")
        return

    # Find template files
    template_files = list(templates_dir.glob("*.json"))
    template_files.extend(templates_dir.glob("*.py"))

    if not template_files:
        click.echo("No template files found")
        return

    # Display template information
    for file_path in template_files:
        click.echo("\n" + "=" * 50)
        click.echo(f"Template: {file_path.name}")
        click.echo("=" * 50)

        try:
            template = get_spec_config(str(file_path))
            click.echo(f"Name: {template.get('name', 'Unnamed')}")
            click.echo(f"Description: {template.get('description', 'No description')}")
            click.echo(f"\nSpecifications: {len(template['specifications'])}")

            click.echo("\nDependency Tree:")
            tree = format_dependency_tree(template['specifications'])
            click.echo("\n".join(tree))

            if show_content:
                click.echo("\nDetailed Configuration:")
                click.echo(json.dumps(template, indent=2))

        except Exception as e:
            traceback.print_exc()
            click.echo(f"Error loading template: {str(e)}", err=True)
            continue

        click.echo("\n")


@spec.command()
@click.argument('spec_id', type=int)
@click.option('--max-retries', type=int, default=1,
              help='Maximum number of retries for artifact generation')
@click.option('--last', type=str, help='Filter posts by time (e.g. "1d", "2h", "30m")')
@click.option('--offset', default=0, type=int, help='Offset for search results')
@click.option('--limit', default=50, type=int, help='Maximum number of posts to fetch')
@click.option('--force/--no-force', default=False, help='Force regeneration even if artifacts exist')
@click.pass_context
def generate_bulk(ctx, spec_id: int, max_retries: int, last: Optional[str],
                 offset: int, limit: int, force: bool):
    """Generate bulk content for a specification."""
    db = ctx.obj['supabase']
    artifact_manager = ArtifactManager(db)

    try:
        # Get specification details
        spec = db.specification_get(spec_id)
        if not spec:
            click.echo(f"Specification {spec_id} not found")
            return

        if not spec['active']:
            click.echo(f"Specification {spec_id} is not active")
            return

        source = spec['carver_source']
        if not source:
            click.echo(f"Source not found for specification {spec_id}")
            return

        label = f"[{source['name']}:{spec['name']}]"
        click.echo(f"\n{label} Starting bulk generation")

        # Validate dependencies are met
        deps = spec['config'].get('dependencies', [])
        if isinstance(deps, (int, str)):
            deps = [int(deps)]
        elif deps is None:
            deps = []

        if deps:
            click.echo(f"{label} Checking dependencies: {deps}")
            for dep_id in deps:
                dep_spec = db.specification_get(dep_id)
                if not dep_spec:
                    click.echo(f"Dependency specification {dep_id} not found", err=True)
                    return
                if not dep_spec['active']:
                    click.echo(f"Dependency specification {dep_id} is not active", err=True)
                    return

        # Get posts needing artifacts
        time_filter = parse_date_filter(last) if last else None
        # If force is true, get all posts regardless of artifacts
        posts = db.post_search_with_artifacts(
            source_id=source['id'],
            modified_after=time_filter,
            offset=offset,
            limit=limit
        )

        if not posts:
            click.echo(f"{label} No posts found requiring artifact generation")
            return

        click.echo(f"{label} Found {len(posts)} posts to process")

        # Generate artifacts
        total_generated = 0
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                results = artifact_manager.artifact_bulk_create_from_spec(
                    spec,
                    posts,
                    None  # Use default generator from spec
                )
                total_generated += len(results)
                success = True
                click.echo(f"{label} Generated {len(results)} artifacts")
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    click.echo(f"{label} Retry {retry_count}/{max_retries}")
                else:
                    click.echo(f"{label} Failed after {max_retries} attempts: {str(e)}", err=True)

        click.echo(f"\n{label} Bulk generation completed")
        click.echo(f"Total artifacts generated: {total_generated}")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)
