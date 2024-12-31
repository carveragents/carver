import os
import sys
import json
import traceback

from typing import Optional, List, Dict
from datetime import datetime
from pathlib import Path

import click

from tabulate import tabulate

from ..utils.helpers import topological_sort
from ..utils import format_datetime, parse_date_filter, get_spec_config
from .artifact_manager import ArtifactManager

thisdir = os.path.dirname(__file__)

####################################
# Artifact Commands
####################################
@click.group()
@click.pass_context
def artifact(ctx):
    """Manage artifacts and specifications in the system."""
    ctx.obj['manager'] = ArtifactManager(ctx.obj['supabase'])

@artifact.command()
@click.option('--spec-id', required=True, type=int, help='Specification ID')
@click.option('--posts', required=True, help='Comma-separated list of post IDs')
@click.option('--generator-name', required=True, help='Generator name to use')
@click.pass_context
def generate(ctx, spec_id: int, posts: str, generator_name: str):
    """Generate artifacts for specified posts using a specification."""
    manager = ctx.obj['manager']
    try:
        post_ids = [int(i.strip()) for i in posts.split(',')]
        results = manager.artifact_bulk_create_from_spec(spec_id, post_ids, generator_name)
        click.echo(f"Successfully generated {len(results)} artifacts")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error generating artifacts: {str(e)}", err=True)

@artifact.command()
@click.option('--spec-id', required=False, type=int, help='Specification ID')
@click.option('--source-id', required=False, type=int, help='Source ID')
@click.option('--last', type=str, help='Filter posts by time (e.g. "1d", "2h", "30m")')
@click.option('--generator-name', required=False, help='Generator name to use')
@click.option('--offset', default=0, type=int, help='Offset for search results')
@click.option('--limit', default=50, type=int, help='Maximum number of posts to fetch')
@click.pass_context
def bulk_generate(ctx, spec_id: int, source_id: int, last: Optional[str], generator_name: str, offset: int, limit: int):
    """Bulk generate artifacts for posts from a source that don't have active artifacts."""
    manager = ctx.obj['manager']
    db = ctx.obj['supabase']

    try:
        if not spec_id and not source_id:
            click.echo("Specify one of spec_id or source_id", err=True)
            return

        specs = db.specification_search(source_id=source_id,
                                        spec_id=spec_id,
                                        active=True)
        if not specs:
            click.echo(f"No active specifications found")
            return

        if not source_id:
            source_id = specs[0]['source_id']

        # Get posts needing artifacts
        time_filter = parse_date_filter(last) if last else None
        posts = db.post_search_with_artifacts(
            source_id=source_id,
            modified_after=time_filter,
            limit=limit,
            offset=offset,
        )

        if not posts:
            click.echo("No posts found requiring artifact generation")
            return

        click.echo(f"Found {len(posts)} posts to process for artifact generation")

        # Process each specification
        for spec in specs:
            if ((generator_name is not None) and
                (spec['config'].get('generator') != generator_name)):
                continue

            try:
                results = manager.artifact_bulk_create_from_spec(spec, posts, generator_name)
                click.echo(f"Successfully generated {len(results)} artifacts using specification {spec['id']}")
            except Exception as e:
                traceback.print_exc()
                click.echo(f"Error generating artifacts for spec {spec['id']}: {str(e)}", err=True)

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error during bulk generation: {str(e)}", err=True)


@artifact.command()
@click.option('--spec-id', type=int, help='Filter by specification ID')
@click.option('--post-id', type=int, help='Filter by post ID')
@click.option('--artifact-type', help='Filter by artifact type')
@click.option('--status', help='Filter by status')
@click.option('--active/--inactive', default=True, help='Filter by active status')
@click.option('--last', type=str, help='Filter by time window (e.g. "1d", "2h", "30m")')
@click.option('--offset', default=0, type=int, help='Offset for search results')
@click.option('--limit', default=50, type=int, help='Maximum number of posts to fetch')
@click.option('--format', 'output_format',
              type=click.Choice(['table', 'grid', 'pipe', 'orgtbl', 'rst', 'mediawiki', 'html']),
              default='table',
              help='Output format for display')
@click.option('--dump', 'dump_format',
              type=click.Choice(['text', 'csv', 'json']),
              help='Dump results to file in specified format')
@click.option('--output', '-o', type=click.Path(), help='Output file path for dump')
@click.pass_context
def search(ctx, spec_id: Optional[int], post_id: Optional[int],
           artifact_type: Optional[str], status: Optional[str],
           last: Optional[str], offset: int, limit: int,
           active: Optional[bool], output_format: str,
           dump_format: Optional[str], output: Optional[str]):
    """Search artifacts with optional data dump."""
    db = ctx.obj['supabase']
    try:
        # Parse time window if provided
        time_filter = parse_date_filter(last) if last else None

        artifacts = db.artifact_search(
            spec_id=spec_id,
            post_id=post_id,
            artifact_type=artifact_type,
            status=status,
            active=active,
            modified_after=time_filter,
            offset=offset,
            limit=limit
        )

        if not artifacts:
            click.echo("No artifacts found")
            return

        # Prepare data for display and dump
        rows = []
        for art in artifacts:
            row = {
                'id': art['id'],
                'specification': art['carver_artifact_specification']['name'],
                'post_id': art['post_id'],
                'post_name': art['carver_post']['name'],
                'post_description': art['carver_post']['description'],
                'post_url': art['carver_post']['url'],
                'artifact_type': art['artifact_type'],
                'generator': f"{art['generator_name']}:{art['generator_id']}",
                'title': art['title'],
                'content': art['content'],
                'status': art['status'],
                'version': art['version'],
                'embedding': "Y" if art['content_embedding'] is not None else "N",
                'active': art['active'],
                'created_at': format_datetime(art['created_at'])
            }
            rows.append(row)

        # Display results in table format
        if not dump_format:
            click.echo("Notes: V=version, A=Active, Type=Artifact Type, E=Embedding")
            table_rows = [[
                r['id'],
                r['specification'],
                r['post_id'],
                r['artifact_type'],
                r['generator'],
                r['title'][:20] + ('...' if len(r['title']) > 20 else ''),
                r['status'],
                r['version'],
                r['embedding'],
                '✓' if r['active'] else '✗',
                r['created_at']
            ] for r in rows]

            headers = ['ID', 'Spec', "PostID", 'Type', "Generator:ID",
                       'Title', 'Status', 'V', 'E','A', 'Created']

            click.echo(tabulate(table_rows, headers=headers, tablefmt=output_format))
            click.echo(f"\nTotal artifacts: {len(artifacts)}")
            return

        # Handle data dump
        output_file = output or f"artifacts_dump_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        if dump_format == 'json':
            output_file = f"{output_file}.json" if not output else output
            with open(output_file, 'w') as f:
                json.dump(rows, f, indent=2)

        elif dump_format == 'csv':
            import csv
            output_file = f"{output_file}.csv" if not output else output
            with open(output_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

        elif dump_format == 'text':
            output_file = f"{output_file}.txt" if not output else output
            marker = "==================================="
            header = f"\n\n\n{marker}\nArtifact\n{marker}\n\n"
            with open(output_file, 'w') as f:
                for row in rows:
                    f.write(header)
                    for key, value in row.items():
                        label = key.replace("_", " ").title()
                        if len(str(value)) > 50:
                            newline = "\n"
                            lines = str(value).split("\n")
                            value = ""
                            for l in lines:
                                value += "    " + l + "\n"
                        else:
                            newline = " "
                        f.write(f"[{label.upper()[:20]:15}] {newline}{value}\n")
                    f.write("\n")

        click.echo(f"Successfully dumped {len(artifacts)} artifacts to {output_file}")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error: {str(e)}", err=True)

@artifact.command('update-status')
@click.option('--spec-id', required=True, type=int, help='Specification ID')
@click.option('--artifacts', required=True, help='Comma-separated list of artifact IDs')
@click.option('--status', type=click.Choice(['draft', 'in_review', 'published', 'archived', 'failed']),
              required=True, help='New status')
@click.pass_context
def update_status(ctx, spec_id: int, artifacts: str, status: str):
    """Update status for multiple artifacts."""
    manager = ctx.obj['manager']
    try:
        artifact_ids = [int(i.strip()) for i in artifacts.split(',')]
        updated = manager.artifact_bulk_status_update(spec_id, artifact_ids, status)
        click.echo(f"Updated status for {len(updated)} artifacts")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error updating status: {str(e)}", err=True)

@artifact.command()
@click.option('--spec-id', required=True, type=int, help='Specification ID')
@click.option('--artifacts', required=False, help='Comma-separated list of artifact IDs')
@click.pass_context
def activate(ctx, spec_id: int, artifacts: str):
    """Activate multiple artifacts."""
    manager = ctx.obj['manager']
    try:
        if artifacts:
            artifacts = [int(i.strip()) for i in artifacts.split(',')]
        updated = manager.artifact_bulk_activate(spec_id, artifacts)
        click.echo(f"Activated {len(updated)} artifacts")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error activating artifacts: {str(e)}", err=True)

@artifact.command()
@click.option('--spec-id', required=False, type=int, help='Specification ID')
@click.option('--artifacts', required=False, help='Comma-separated list of artifact IDs')
@click.pass_context
def deactivate(ctx, spec_id: int, artifacts: str):
    """Deactivate multiple artifacts."""
    manager = ctx.obj['manager']
    try:

        if artifacts is not None:
import os
import sys
import json
import traceback
import time
import logging

from typing import List, Dict, Any, Optional, Type
from datetime import datetime
from abc import ABC, abstractmethod

from carver.llm import get_embedding
from carver.generators import ArtifactGeneratorFactory

logger = logging.getLogger(__name__)

class ArtifactManager:
    """Manages artifact and specification operations"""

    def __init__(self, db_client):
        self.db = db_client

    ##########################################################
    # Specification Methods
    ##########################################################
    def specification_create(self,
                             source: Dict[str, Any],
                             data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new artifact specification"""

        generator = data.get('config', {}).get('generator')
        if not generator:
            raise ValueError("Generator name not specified in config")

        # Validate config for the generator
        generatorobj = ArtifactGeneratorFactory.get_generator(generator)
        if not generatorobj.validate_config(source, data['config']):
            raise ValueError(f"Invalid configuration for {generator} generator")

        create_data = {
            'active': True,  # Default value
            **data  # Allow override from input data
        }

        return self.db.specification_create(create_data)

    def specification_update(self, spec_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing specification"""

        current = self.db.specification_get(spec_id)
        if not current:
            raise ValueError(f"Specification {spec_id} not found")

        source = current['carver_source']

        # If config is being updated, validate it
        if 'config' in data:
            config = data['config']
            generator = ArtifactGeneratorFactory.get_generator(config['generator'])
            if not generator.validate_config(source, data['config']):
                raise ValueError(f"Invalid configuration for {artifact_type} generator")

        return self.db.specification_update(spec_id, data)

    ##########################################################
    # Artifact Generation Methods
    ##########################################################
    def artifact_bulk_create_from_spec(self, spec: Dict[str, Any],
                                       posts: List[Dict[str, Any]],
                                       generator_name: Optional[str],
                                       delay: int = 5, # seconds
                                       max_content_size: int = 8192,
                                       ) -> List[Dict[str, Any]]:
        """Generate artifacts for multiple posts using a specification"""

        if not spec:
            raise ValueError(f"Specification not found")

        if not spec['active']:
            raise ValueError(f"Specification {spec['id']} is not active")

        if generator_name is not None:
            if spec['config'].get('generator') != generator_name:
                raise ValueError(f"Specification {spec_id} does not use generator {generator_name}")
        else:
            generator_name = spec['config'].get('generator')

        # Now run the generator...
        generator = ArtifactGeneratorFactory.get_generator(generator_name)
        generator_ids = generator.get_ids(spec['config'])

        artifacts_to_create = []
        errors = []
        newartifacts = False
        completelist = set()
        created = []
        now = datetime.utcnow().isoformat()

        for idx, post in enumerate(posts):
            existing_artifacts = post['artifacts']
            post_id = post['id']
            for artifact_data in existing_artifacts:
                completelist.add((post['id'], spec['id'],
                                  artifact_data['generator_name'],
                                  artifact_data['generator_id']))

        for idx, post in enumerate(posts):
            try:

                # print(json.dumps(post, indent=4))

                existing_artifacts = post['artifacts']
                post_id = post['id']
                artifacts_data = generator.generate(post,
                                                    spec['config'],
                                                    existing_artifacts)

                if isinstance(artifacts_data, dict):
                    artifacts_data = [artifacts_data]

                if len(artifacts_data) == 0:
                    print(f"[{idx}] Skipping. No artifacts need to be created")
                    continue

                for artifact_data in artifacts_data:
                    rec = (post['id'], spec['id'], artifact_data['generator_name'],
                           artifact_data['generator_id'])
                    if rec in completelist:
                        print(f"[{idx}] Skipping. Duplicate", rec)
                        continue

                    completelist.add(rec)

                    # Generate embedding for content
                    try:
                        text = ""
                        if 'name' in artifact_data and artifact_data['name']:
                            text += artifact_data['name'] + "\n"

                        if 'title' in artifact_data and artifact_data['title'] not in text:
                            text += artifact_data['title'] + "\n"

                        text += artifact_data['content']
                        text = text[:max_content_size]
                        content_embedding = get_embedding(text)
                    except Exception as e:
                        traceback.print_exc()
                        print(f"Error generating embedding: {str(e)}")
                        content_embedding = None
                        continue

                    artifact = {
                        'active': True,
                        'spec_id': spec['id'],
                        'post_id': post['id'],
                        'generator_name': artifact_data['generator_name'],
                        'generator_id':   artifact_data['generator_id'],
                        'name':           artifact_data['title'],
                        'title':          artifact_data['title'],
                        'artifact_type':  artifact_data['artifact_type'],
                        'description':    artifact_data.get('description'),
                        'content':        artifact_data['content'],
                        'content_embedding': content_embedding,
                        'format':         artifact_data.get('format', 'text'),
                        'language':       artifact_data.get('language', 'en'),
                        'status':         'draft',
                        'version':        1,
                        'analysis_metadata': artifact_data.get('analysis_metadata'),
                        'artifact_metrics':  artifact_data.get('artifact_metrics'),
                        'created_at':      now,
                        'updated_at':      now,
                    }
                    artifacts_to_create.append(artifact)
                    newartifacts = True
                    print(f"[{idx}] Adding", rec, "Total", len(artifacts_to_create))

                if idx > 0 and idx % 10 == 0:
                    print(f"[posts: {idx}] New artifacts to create {len(artifacts_to_create)}")
                    if newartifacts:
                        inc_created = self.db.artifact_bulk_create(artifacts_to_create)
                        created += inc_created
                        print(f"[posts: {idx}] Created {len(inc_created)}")
                        artifacts_to_create = []
                        if delay > 0:
                            time.sleep(delay)
                        newartifacts = False

            except Exception as e:
                #traceback.print_exc()
                errors.append(f"Error processing post {post_id}: {str(e)}"[:100])

        if newartifacts:
            inc_created = self.db.artifact_bulk_create(artifacts_to_create)
            created += inc_created
            print(f"[posts: {idx}] Created {len(inc_created)}")

        if errors:
            print("Generation errors occurred:", errors)

        return created

    def artifact_regenerate(self, artifact_id: int) -> Dict[str, Any]:
        """Regenerate a single artifact"""
        # Get existing artifact
        artifact = self.db.artifact_get(artifact_id)
        if not artifact:
            raise ValueError(f"Artifact {artifact_id} not found")

        # Get specification and post
        spec = self.db.specification_get(artifact['spec_id'])
        post = self.db.post_get(artifact['post_id'])

        if not spec or not post:
            raise ValueError("Associated specification or post not found")

        # Get generator and regenerate
        config = spec['config']
        generator = ArtifactGeneratorFactory.get_generator(config['generator'])
        artifact_data = generator.generate(post, spec['config'])

        # Update artifact
        update_data = {
            'content': artifact_data['content'],
            'version': artifact['version'] + 1,
            'analysis_metadata': artifact_data.get('analysis_metadata'),
            'artifact_metrics': artifact_data.get('artifact_metrics'),
        }

        return self.db.artifact_update(artifact_id, update_data)

    ##########################################################
    # Artifact Update Methods
    ##########################################################
    def artifact_bulk_update_status(self, status: str,
                                    spec_id: Optional[int] = None,
                                    artifact_ids: Optional[List[int]] = None,
                                    metadata: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Update status and optional metadata for multiple artifacts
        Can target artifacts either by direct IDs or by specification ID

        Args:
            status: New status to set
            spec_id: Optional specification ID to target all its artifacts
            artifact_ids: Optional list of specific artifact IDs to update
            metadata: Optional metadata to update
        """
        try:
            # If no artifact_ids provided, get them from spec_id
            if not artifact_ids and spec_id:
                results = self.artifact_search(
                    spec_id=spec_id,
                    active=True,
                    fields=['id']
                )
                artifact_ids = [r['id'] for r in results]

            if not artifact_ids:
                logger.warning("No artifacts found to update status")
                return []

            update_data = {
                'status': status,
                'updated_at': datetime.utcnow().isoformat()
            }
            if metadata:
                update_data['analysis_metadata'] = metadata

            updates = [{
                'id': artifact_id,
                **update_data
            } for artifact_id in artifact_ids]

            return self.artifact_bulk_update(updates)
        except Exception as e:
            logger.error(f"Error in bulk update artifact status: {str(e)}")
            raise

    def artifact_bulk_activate(self,
                            spec_id: Optional[int] = None,
                            artifact_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """
        Activate multiple artifacts by specification ID or specific artifact IDs

        Args:
            spec_id: Optional specification ID to target all its artifacts
            artifact_ids: Optional list of specific artifact IDs to activate
        """
        try:
            # If no artifact_ids provided, get them from spec_id
            if not artifact_ids and spec_id:
                results = self.artifact_search(
                    spec_id=spec_id,
                    active=False,  # Only get inactive artifacts
                    fields=['id']
                )
                artifact_ids = [r['id'] for r in results]

            if not artifact_ids:
                logger.warning("No artifacts found to activate")
                return []

            updates = [{
                'id': artifact_id,
                'active': True,
                'updated_at': datetime.utcnow().isoformat()
            } for artifact_id in artifact_ids]

            return self.artifact_bulk_update(updates)
        except Exception as e:
            logger.error(f"Error in bulk activate artifacts: {str(e)}")
            raise

    def artifact_bulk_deactivate(self,
                              spec_id: Optional[int] = None,
                              artifact_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """
        Deactivate multiple artifacts by specification ID or specific artifact IDs

        Args:
            spec_id: Optional specification ID to target all its artifacts
            artifact_ids: Optional list of specific artifact IDs to deactivate
        """
        try:
            # If no artifact_ids provided, get them from spec_id
            if not artifact_ids and spec_id:
                results = self.db.artifact_search(
                    spec_id=spec_id,
                    active=True,  # Only get active artifacts
                    limit=1000,
                    fields=['id']
                )
                artifact_ids = [r['id'] for r in results]

            return self.db.artifact_bulk_update_flag(artifact_ids, False)

        except Exception as e:
            logger.error(f"Error in bulk deactivate artifacts: {str(e)}")
            raise

    def artifact_metrics_update(self, artifact_id: int, metrics: Dict[str, Any],
                              replace: bool = False) -> Dict[str, Any]:
        """Update metrics for an artifact"""
        return self.db.artifact_metrics_update(artifact_id, metrics, replace)

    def posts_without_artifacts(self, source_id: int,
                                artifact_type: str,
                                modified_after: Optional[datetime] = None,
                                max_results: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get posts that don't have active artifacts of specified type

        Args:
            source_id: Source ID to search within
            artifact_type: Type of artifact to check for
            modified_after: Optional filter for posts modified after this time
            max_results: Maximum number of posts to return
        """

        return self.db.post_search_without_artifacts(
            source_id=source_id,
            artifact_type=artifact_type,
            modified_after=modified_after,
            limit=max_results or 1000
        )


    def artifact_bulk_update_embeddings(self,
                                        artifacts: List[Dict[str, Any]],
                                        force_update: bool = False,
                                        max_content_size: int = 8192,
                                        batch_size: int = 100) -> Dict[str, int]:
        """
        Update embeddings for a list of artifacts

        Args:
            artifacts: List of artifacts to update
            force_update: If True, update even if embedding exists
            batch_size: Size of batches for updates
            max_content_size: Size of each piece of text

        Returns:
            Dict with counts of processed, updated, and error artifacts
        """
        try:
            if not artifacts:
                return {'processed': 0, 'updated': 0, 'errors': 0}

            total_processed = 0
            total_updated = 0
            total_errors = 0

            # Process in batches
            for i in range(0, len(artifacts), batch_size):
                batch = artifacts[i:i + batch_size]
                batch_updates = []

                for artifact in batch:
                    try:
                        # Skip if already has embedding and not force updating
                        if not force_update and artifact.get('content_embedding'):
                            continue

                        if not artifact.get('content'):
                            logger.warning(f"Artifact {artifact['id']} has no content to embed")
                            total_errors += 1
                            continue

                        # Generate embedding
                        text = ""
                        if artifact['name']:
                            text += artifact['name'] + "\n"

                        if artifact['title'] and artifact['title'] != artifact['name']:
                            text += artifact['title'] + "\n"
                        text += artifact['content']
                        text = text[:max_content_size]
                        embedding = get_embedding(text)

                        batch_updates.append({
                            'id': artifact['id'],
                            'content_embedding': embedding,
                            'updated_at': datetime.utcnow().isoformat()
                        })

                    except Exception as e:
                        logger.error(f"Error processing artifact {artifact['id']}: {str(e)}")
                        total_errors += 1
                        continue

                print("Computed. Posting batch", i, i+batch_size)
                # Bulk update the batch
                if batch_updates:
                    try:
                        self.db.artifact_bulk_update(batch_updates)
                        total_updated += len(batch_updates)
                    except Exception as e:
                        traceback.print_exc()
                        logger.error(f"Error updating batch: {str(e)}")
                        total_errors += len(batch_updates)

                total_processed += len(batch)
                print("Completed batch", i, i+batch_size)

            return {
                'processed': total_processed,
                'updated': total_updated,
                'errors': total_errors
            }

        except Exception as e:
            logger.error(f"Error in bulk update embeddings: {str(e)}")
            raise

    def artifact_search_similar(self,
                                query: str,
                                match_threshold: float = 0.7,
                                match_count: int = 10,
                                spec_id: Optional[int] = None,
                                status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search for similar artifacts using text query"""
        try:
            # Generate embedding for query
            query_embedding = get_embedding(query)

            # Search using embedding
            return self.db.artifact_search_similar(
                query_embedding,
                match_threshold=match_threshold,
                match_count=match_count,
                spec_id=spec_id,
                status=status
            )

        except Exception as e:
            print(f"Error in similarity search: {str(e)}")
            raise
            artifacts = [int(i.strip()) for i in artifacts.split(',')]
        updated = manager.artifact_bulk_deactivate(spec_id, artifacts)
        click.echo(f"Deactivated {len(updated)} artifacts")
    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error deactivating artifacts: {str(e)}", err=True)
