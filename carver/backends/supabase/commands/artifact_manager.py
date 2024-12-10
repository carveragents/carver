import os
import sys
import json
import traceback
import time
import logging

from typing import List, Dict, Any, Optional, Type
from datetime import datetime
from abc import ABC, abstractmethod

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
                                       items: List[Dict[str, Any]],
                                       generator_name: Optional[str],
                                       delay: int = 5, # seconds
                                       ) -> List[Dict[str, Any]]:
        """Generate artifacts for multiple items using a specification"""

        if not spec:
            raise ValueError(f"Specification {spec_id} not found")

        if not spec['active']:
            raise ValueError(f"Specification {spec_id} is not active")

        if generator_name is not None:
            if spec['config'].get('generator') != generator_name:
                raise ValueError(f"Specification {spec_id} does not use generator {generator_name}")
        else:
            generator_name = spec['config'].get('generator')

        # Now run the generator...
        generator = ArtifactGeneratorFactory.get_generator(generator_name)

        artifacts_to_create = []
        errors = []

        for idx, item in enumerate(items):
            try:

                existing_artifacts = item['artifacts']
                item_id = item['id']
                artifacts_data = generator.generate(item, spec['config'], existing_artifacts)
                if isinstance(artifacts_data, dict):
                    artifacts_data = [artifacts_data]

                for artifact_data in artifacts_data:
                    print(f"[{idx}] Adding", (item['id'], spec['id'], artifact_data['generator_name'],
                                     artifact_data['generator_id']))

                    artifact = {
                        'active': True,
                        'spec_id': spec['id'],
                        'item_id': item['id'],
                        'generator_name': artifact_data['generator_name'],
                        'generator_id':   artifact_data['generator_id'],
                        'name':           artifact_data['title'],
                        'title':          artifact_data['title'],
                        'artifact_type':  artifact_data['artifact_type'],
                        'description':    artifact_data.get('description'),
                        'content':        artifact_data['content'],
                        'format':         artifact_data.get('format', 'text'),
                        'language':       artifact_data.get('language', 'en'),
                        'status':         'draft',
                        'version':        1,
                        'analysis_metadata': artifact_data.get('analysis_metadata'),
                        'artifact_metrics':  artifact_data.get('artifact_metrics')
                    }
                    artifacts_to_create.append(artifact)

                if idx > 0 and idx % 10 == 0:
                    print(f"[items: {idx}] New artifacts to create {len(artifacts_to_create)}")
                    if delay > 0:
                        time.sleep(delay)


            except Exception as e:
                traceback.print_exc()
                errors.append(f"Error processing item {item_id}: {str(e)}")
                continue

        print("Final artifacts to create", len(artifacts_to_create))
        created = self.db.artifact_bulk_create(artifacts_to_create)

        if errors:
            print("Generation errors occurred:", errors)

        return created

    def artifact_regenerate(self, artifact_id: int) -> Dict[str, Any]:
        """Regenerate a single artifact"""
        # Get existing artifact
        artifact = self.db.artifact_get(artifact_id)
        if not artifact:
            raise ValueError(f"Artifact {artifact_id} not found")

        # Get specification and item
        spec = self.db.specification_get(artifact['spec_id'])
        item = self.db.item_get(artifact['item_id'])

        if not spec or not item:
            raise ValueError("Associated specification or item not found")

        # Get generator and regenerate
        config = spec['config']
        generator = ArtifactGeneratorFactory.get_generator(config['generator'])
        artifact_data = generator.generate(item, spec['config'])

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

    def items_without_artifacts(self, source_id: int,
                                artifact_type: str,
                                modified_after: Optional[datetime] = None,
                                max_results: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get items that don't have active artifacts of specified type

        Args:
            source_id: Source ID to search within
            artifact_type: Type of artifact to check for
            modified_after: Optional filter for items modified after this time
            max_results: Maximum number of items to return
        """

        return self.db.item_search_without_artifacts(
            source_id=source_id,
            artifact_type=artifact_type,
            modified_after=modified_after,
            limit=max_results or 1000
        )

