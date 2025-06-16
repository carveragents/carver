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

        print(json.dumps(data, indent=4))

        generator = data.get('config', {}).get('generator')
        if not generator:
            raise ValueError("Generator name not specified in config")


        # Validate config for the generator
        generatorobj = ArtifactGeneratorFactory.get_generator(generator)
        if not generatorobj.validate_config(source, data):
            raise ValueError(f"Invalid configuration for {generator} generator")

        now = datetime.utcnow().isoformat()
        create_data = {
            'active': True,  # Default value
            'created_at': now,
            'updated_at': now,
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
            generator = ArtifactGeneratorFactory.get_generator(config['generator'])
            if not generator.validate_config(source, data):
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
        postmap = {}
        created = []
        now = datetime.utcnow().isoformat()

        for idx, post in enumerate(posts):
            existing_artifacts = post['artifacts']
            post_id = post['id']
            postmap[post_id] = post # note this...
            for artifact_data in existing_artifacts:
                completelist.add((post['id'], spec['id'],
                                  artifact_data['generator_name'],
                                  artifact_data['generator_id']))

        for idx, post in enumerate(posts):
            try:

                # print(idx, post['id'], post['title'])
                # print(json.dumps(post, indent=4))

                existing_artifacts = post['artifacts']
                post_id = post['id']
                artifacts_data = generator.generate(post,
                                                    spec,
                                                    existing_artifacts)

                if isinstance(artifacts_data, dict):
                    artifacts_data = [artifacts_data]

                if len(artifacts_data) == 0:
                    print(f"[{idx}] {post_id} Spec {spec['id']} Skipping. No artifacts need to be created")
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

                    # print(artifact['content'])

                    artifacts_to_create.append(artifact)
                    newartifacts = True
                    print(f"[{idx}] Adding", rec, "Total", len(artifacts_to_create))

                if idx > 0 and idx % 10 == 0:
                    print(f"[posts: {idx}] New artifacts to create {len(artifacts_to_create)}")
                    if newartifacts:
                        inc_created = self.db.artifact_bulk_create(artifacts_to_create)
                        created += inc_created
                        print(f"[posts: {idx}] Created {len(inc_created)}")

                        #print(json.dumps(inc_created[0], indent=4))

                        # Update the artifacts...
                        for a in artifacts_to_create:
                            postmap[a['post_id']]['artifacts'].append(a)

                        artifacts_to_create = []
                        if delay > 0:
                            time.sleep(delay)
                        newartifacts = False
            except Exception as e:
                traceback.print_exc()
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
        generator = ArtifactGeneratorFactory.get_generator(config['generator'])
        artifact_data = generator.generate(post, spec)

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
