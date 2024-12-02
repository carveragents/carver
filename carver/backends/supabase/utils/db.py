import os
import sys
import json
import logging

from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path

from decouple import Config, RepositoryIni
from supabase import create_client, Client

from .helpers import get_supabase_client, chunks

logger = logging.getLogger(__name__)

__all__ = [
    'SupabaseClient'
]

class SupabaseClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize Supabase client using credentials from config file."""
        self.client = get_supabase_client()

    # Entity methods
    def entity_get(self, entity_id: int) -> Dict[str, Any]:
        """Get a single entity by ID"""
        result = self.client.table('carver_entity') \
            .select('*') \
            .eq('id', entity_id) \
            .execute()
        return result.data[0] if result.data else None

    def entity_search(self,
                     active: Optional[bool] = None,
                     entity_type: Optional[str] = None,
                     owner: Optional[str] = None,
                     name: Optional[str] = None,
                     created_since: Optional[datetime] = None,
                     updated_since: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Search entities with various filters"""
        query = self.client.table('carver_entity').select('*')

        if active is not None:
            query = query.eq('active', active)
        if entity_type:
            query = query.eq('entity_type', entity_type)
        if owner:
            query = query.ilike('owner', f'%{owner}%')
        if name:
            query = query.ilike('name', f'%{name}%')
        if created_since:
            query = query.gte('created_at', created_since.isoformat())
        if updated_since:
            query = query.gte('updated_at', updated_since.isoformat())

        result = query.execute()
        return result.data

    def entity_create(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new entity"""
        result = self.client.table('carver_entity').insert(data).execute()
        return result.data[0] if result.data else None

    def entity_update(self, entity_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing entity"""
        result = self.client.table('carver_entity') \
            .update(data) \
            .eq('id', entity_id) \
            .execute()
        return result.data[0] if result.data else None

    # Source methods
    def source_get(self, source_id: int) -> Dict[str, Any]:
        """Get a single source by ID"""
        result = self.client.table('carver_source') \
            .select('*, carver_entity(*)') \
            .eq('id', source_id) \
            .execute()
        return result.data[0] if result.data else None

    def source_search(self,
                     active: Optional[bool] = None,
                     entity_id: Optional[int] = None,
                     platform: Optional[str] = None,
                     source_type: Optional[str] = None,
                     name: Optional[str] = None,
                     updated_since: Optional[datetime] = None,
                     crawled_since: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Search sources with various filters"""
        query = self.client.table('carver_source') \
            .select('*, carver_entity(*)')

        if active is not None:
            query = query.eq('active', active)
        if entity_id:
            query = query.eq('entity_id', entity_id)
        if platform:
            query = query.eq('platform', platform)
        if source_type:
            query = query.eq('source_type', source_type)
        if name:
            query = query.ilike('name', f'%{name}%')
        if updated_since:
            query = query.gte('updated_at', updated_since.isoformat())
        if crawled_since:
            query = query.gte('last_crawled', crawled_since.isoformat())

        result = query.execute()
        return result.data

    def source_create(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new source"""
        result = self.client.table('carver_source').insert(data).execute()
        return result.data[0] if result.data else None

    def source_update(self, source_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing source"""
        result = self.client.table('carver_source') \
            .update(data) \
            .eq('id', source_id) \
            .execute()
        return result.data[0] if result.data else None

    def source_update_metadata(self, source_id: int, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Update source's analysis metadata"""
        current = self.source_get(source_id)
        if not current:
            return None

        existing_metadata = current.get('analysis_metadata', {}) or {}
        updated_metadata = {**existing_metadata, **metadata}

        return self.source_update(source_id, {
            'analysis_metadata': updated_metadata,
            'updated_at': datetime.utcnow().isoformat()
        })

    ##########################################################
    # Item Methods
    ##########################################################
    def item_get(self, item_id: int) -> Optional[Dict]:
        """Get a single item by ID with its source information"""
        try:
            result = self.client.table('carver_item') \
                .select('*, carver_source(*)') \
                .eq('id', item_id) \
                .execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Error getting item {item_id}: {str(e)}")
            raise

    def item_search(self,
                    source_id: Optional[int] = None,
                    content_type: Optional[str] = None,
                    content_identifier: Optional[Any] = None,
                    author: Optional[str] = None,
                    active: Optional[bool] = None,
                    is_processed: Optional[bool] = None,
                    published_since: Optional[datetime] = None,
                    acquired_since: Optional[datetime] = None,
                    title_search: Optional[str] = None,
                    tags_search: Optional[str] = None,
                    fields: Optional[List[str]] = None,
                    limit: int = 100,
                    offset: int = 0) -> List[Dict[str, Any]]:
        """
        Search items with various filters

        Args:
            fields: Optional list of specific fields to return
            source_id: Filter by source ID
            content_type: Filter by content type
            content_identifier: Filter by content identifier
            author: Filter by author (partial match)
            active: Filter by active status
            is_processed: Filter by processed status
            published_since: Filter by publish date
            acquired_since: Filter by acquisition date
            title_search: Search in titles (partial match)
            tags_search: Search in tags (partial match)
            limit: Maximum number of items to return
            offset: Number of items to skip
        """
        try:
            # Build the select statement
            if fields:
                # Always include id and ensure no duplicates
                required_fields = {'id', 'source_id', 'content_identifier'}
                all_fields = list(required_fields.union(fields))
                select_statement = ', '.join(all_fields)
                # Add source details if requested
                if 'carver_source' in fields:
                    select_statement += ', carver_source(*)'
            else:
                select_statement = '*, carver_source(*)'

            query = self.client.table('carver_item').select(select_statement)

            if source_id:
                query = query.eq('source_id', source_id)
            if content_type:
                query = query.eq('content_type', content_type)
            if content_identifier:
                if isinstance(content_identifier, list):
                    query = query.in_('content_identifier', content_identifier)
                else:
                    query = query.eq('content_identifier', content_identifier)
            if author:
                query = query.ilike('author', f'%{author}%')
            if active is not None:
                query = query.eq('active', active)
            if is_processed is not None:
                query = query.eq('is_processed', is_processed)
            if published_since:
                query = query.gte('published_at', published_since.isoformat())
            if acquired_since:
                query = query.gte('acquired_at', acquired_since.isoformat())
            if title_search:
                query = query.ilike('title', f'%{title_search}%')
            if tags_search:
                query = query.ilike('tags', f'%{tags_search}%')

            # Add sorting and pagination
            query = query.order('published_at', desc=True)
            query = query.range(offset, offset + limit - 1)

            result = query.execute()
            return result.data

        except Exception as e:
            logger.error(f"Error in item_search: {str(e)}")
            raise

    def item_get_by_identifiers(self, source_id: int, content_identifiers: List[str],
                              fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get items by their source_id and content identifiers

        Args:
            source_id: The source ID
            content_identifiers: List of content identifiers to fetch
            fields: Optional list of specific fields to return
        """
        return self.item_search(
            source_id=source_id,
            content_identifier=content_identifiers,
            fields=fields,
            limit=len(content_identifiers)
        )

    def item_bulk_create(self, items: List[Dict[str, Any]], chunk_size: int = 100) -> List[Dict[str, Any]]:
        """
        Bulk create items with automatic chunking to avoid request size limits.
        Returns list of created items.
        """
        created_items = []

        # Process in chunks to avoid request size limits
        for chunk in chunks(items, chunk_size):
            try:
                result = self.client.table('carver_item').insert(chunk).execute()
                if result.data:
                    created_items.extend(result.data)
            except Exception as e:
                logger.error(f"Error in bulk create: {str(e)}")
                # Continue with next chunk even if one fails
                continue

        return created_items

    def item_bulk_update(self, items: List[Dict[str, Any]], chunk_size: int = 100) -> List[Dict[str, Any]]:
        """
        Bulk update items with automatic chunking.
        Each item dict must contain 'id' field.
        Returns list of updated items.
        """
        updated_items = []

        # Validate all items have IDs
        if not all('id' in item for item in items):
            raise ValueError("All items must have 'id' field for bulk update")

        # Process in chunks
        for chunk in chunks(items, chunk_size):
            try:
                result = self.client.table('carver_item') \
                    .upsert(chunk) \
                    .execute()
                if result.data:
                    updated_items.extend(result.data)
            except Exception as e:
                logger.error(f"Error in bulk update: {str(e)}")
                continue

        return updated_items

    def item_bulk_activate(self, source_id: int, content_identifiers: List[str]) -> List[Dict[str, Any]]:
        """Activate items by their content identifiers"""
        try:
            # First get the items to update
            items = self.item_search(
                source_id=source_id,
                content_identifier=content_identifiers
            )

            updates = [{
                'id': item['id'],
                'active': True,
                'updated_at': datetime.utcnow().isoformat()
            } for item in items]

            return self.item_bulk_update(updates)
        except Exception as e:
            logger.error(f"Error in bulk activate: {str(e)}")
            raise

    def item_bulk_deactivate(self, source_id: int, content_identifiers: List[str]) -> List[Dict[str, Any]]:
        """Deactivate items by their content identifiers"""
        try:
            # First get the items to update
            items = self.item_search(
                source_id=source_id,
                content_identifier=content_identifiers
            )

            updates = [{
                'id': item['id'],
                'active': False,
                'updated_at': datetime.utcnow().isoformat()
            } for item in items]

            return self.item_bulk_update(updates)
        except Exception as e:
            logger.error(f"Error in bulk deactivate: {str(e)}")
            raise

    def item_bulk_update_metadata(self, items: List[Dict[str, Any]], chunk_size: int = 100) -> List[Dict[str, Any]]:
        """
        Bulk update items' analysis metadata.
        Each item should have 'id' and 'analysis_metadata' fields.
        Preserves existing metadata.
        """
        updates = []
        for item in items:
            if 'id' not in item or 'analysis_metadata' not in item:
                continue

            current = self.item_get(item['id'])
            if not current:
                continue

            existing_metadata = current.get('analysis_metadata', {}) or {}
            updated_metadata = {**existing_metadata, **item['analysis_metadata']}

            updates.append({
                'id': item['id'],
                'analysis_metadata': updated_metadata,
                'updated_at': datetime.utcnow().isoformat()
            })

        return self.item_bulk_update(updates, chunk_size)

    def item_bulk_set_processed(self, item_ids: List[int], processed: bool = True) -> List[Dict[str, Any]]:
        """Bulk update processed status for multiple items"""
        try:
            updates = [{
                'id': item_id,
                'is_processed': processed,
                'updated_at': datetime.utcnow().isoformat()
            } for item_id in item_ids]

            return self.item_bulk_update(updates)
        except Exception as e:
            logger.error(f"Error in bulk set processed: {str(e)}")
            raise

    def item_search_with_artifacts(self,
                                   source_id: int,
                                   generator_name: Optional[str] = None,
                                   modified_after: Optional[datetime] = None,
                                   offset: int = 0,
                                   limit: int = 10) -> Dict[int, Dict]:
        """
        Find items with their artifacts for a specific generator
        Returns a map of item_id -> {item: item_data, artifacts: [artifact_data]}
        """
        try:
            # Get active items from source
            query = self.client.table('carver_item') \
                               .select('*') \
                               .eq('source_id', source_id) \
                               .eq('active', True)

            if modified_after:
                query = query.gte('updated_at', modified_after.isoformat())

            query  = query.range(offset, offset + limit - 1)
            result = query.execute()
            items  = result.data

            if len(items) == 0:
                return []

            item_ids = [i['id'] for i in items]
            query = self.client.table('carver_artifact') \
                               .select('*') \
                               .in_('item_id', item_ids) \
                               .eq('active', True)
            if generator_name:
                query = query.eq('generator_name', generator_name)

            artifacts = query.execute()
            for item in items:
                item_artifacts = [a for a in artifacts.data if a['item_id'] == item['id']]
                item['artifacts'] = item_artifacts

            return items

        except Exception as e:
            logger.error(f"Error in item_search_with_artifacts: {str(e)}")
            raise

    def item_search_without_artifacts(self,
                                      source_id: int,
                                      generator_name: Optional[str] = None,
                                      modified_after: Optional[datetime] = None,
                                      offset: int = 0,
                                      limit: int = 1000) -> List[Dict[str, Any]]:

        """Find items without artifacts for a specific generator"""
        try:
            query = self.client.table('carver_artifact') \
                               .select('item_id, carver_item!inner(source_id)')\
                               .eq('active', True) \
                               .eq('carver_item.source_id', source_id)
            if generator_name:
                query = query.eq('generator_name', generator_name)

            items_with_artifacts = query.execute()

            item_ids_with_artifacts = [item['item_id'] for item in items_with_artifacts.data]

            query = self.client.table('carver_item') \
                               .select('*') \
                               .eq('source_id', source_id) \
                               .eq('active', True)

            if item_ids_with_artifacts:
                query = query.not_.in_('id', item_ids_with_artifacts)

            if modified_after:
                query = query.gte('updated_at', modified_after.isoformat())

            query  = query.range(offset, offset + limit - 1)
            result = query.execute()
            return result.data

        except Exception as e:
            logger.error(f"Error in item_search_without_artifacts: {str(e)}")
            raise


    ##########################################################
    # Specification Methods
    ##########################################################
    def specification_get(self, spec_id: int) -> Optional[Dict[str, Any]]:
        """Get a single artifact specification by ID with related data"""
        try:
            result = self.client.table('carver_artifact_specification') \
                .select('*, carver_source(*)') \
                .eq('id', spec_id) \
                .execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Error getting specification {spec_id}: {str(e)}")
            raise

    def specification_search(self,
                             source_id: Optional[int] = None,
                             spec_id: Optional[int] = None,
                             name: Optional[str] = None,
                             active: Optional[bool] = None,
                             created_since: Optional[datetime] = None,
                             updated_since: Optional[datetime] = None,
                             limit: int = 100,
                             offset: int = 0) -> List[Dict[str, Any]]:
        """Search artifact specifications with filters"""
        try:
            query = self.client.table('carver_artifact_specification') \
                .select('*, carver_source(*)')

            if spec_id:
                query = query.eq('id', spec_id)
            if source_id:
                query = query.eq('source_id', source_id)
            if name:
                query = query.or_(f'name.ilike.%{name}%,description.ilike.%{name}%')
            if active is not None:
                query = query.eq('active', active)
            if created_since:
                query = query.gte('created_at', created_since.isoformat())
            if updated_since:
                query = query.gte('updated_at', updated_since.isoformat())

            # Add sorting and pagination
            query = query.order('created_at', desc=True) \
                .range(offset, offset + limit - 1)

            result = query.execute()
            return result.data
        except Exception as e:
            logger.error(f"Error in specification search: {str(e)}")
            raise

    def specification_create(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new artifact specification"""
        try:
            # Ensure required fields
            required = {'source_id', 'name', 'description', 'config'}
            if not all(k in data for k in required):
                raise ValueError(f"Missing required fields: {required - set(data.keys())}")

            result = self.client.table('carver_artifact_specification') \
                .insert(data) \
                .execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Error creating specification: {str(e)}")
            raise

    def specification_update(self, spec_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing artifact specification"""
        try:

            result = self.client.table('carver_artifact_specification') \
                .update(data) \
                .eq('id', spec_id) \
                .execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Error updating specification: {str(e)}")
            raise

    def specification_bulk_activate(self, spec_ids: List[int]) -> List[Dict[str, Any]]:
        """Activate multiple specifications"""
        try:
            updates = [{
                'id': spec_id,
                'active': True,
            } for spec_id in spec_ids]

            result = self.client.table('carver_artifact_specification') \
                .upsert(updates) \
                .execute()
            return result.data
        except Exception as e:
            logger.error(f"Error in bulk activate specifications: {str(e)}")
            raise

    def specification_bulk_deactivate(self, spec_ids: List[int]) -> List[Dict[str, Any]]:
        """Deactivate multiple specifications"""
        try:
            updates = [{
                'id': spec_id,
                'active': False,
            } for spec_id in spec_ids]

            result = self.client.table('carver_artifact_specification') \
                .upsert(updates) \
                .execute()
            return result.data
        except Exception as e:
            logger.error(f"Error in bulk deactivate specifications: {str(e)}")
            raise

    ##########################################################
    # Artifact Methods
    ##########################################################
    def artifact_get(self, artifact_id: int) -> Optional[Dict[str, Any]]:
        """Get a single artifact by ID with related data"""
        try:
            result = self.client.table('carver_artifact') \
                .select('*, carver_artifact_specification(*), carver_item(*)') \
                .eq('id', artifact_id) \
                .execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Error getting artifact {artifact_id}: {str(e)}")
            raise

    def artifact_search(self,
                        spec_id: Optional[int] = None,
                        item_id: Optional[int] = None,
                        artifact_type: Optional[str] = None,
                        status: Optional[str] = None,
                        active: Optional[bool] = None,
                        format: Optional[str] = None,
                        language: Optional[str] = None,
                        modified_after: Optional[datetime] = None,  # Added time window filter
                        created_since: Optional[datetime] = None,
                        updated_since: Optional[datetime] = None,
                        artifact_ids: Optional[List[int]] = None,
                        limit: int = 100,
                        offset: int = 0) -> List[Dict[str, Any]]:
        """Search artifacts with various filters"""
        try:
            query = self.client.table('carver_artifact') \
                               .select('*, carver_artifact_specification(*), carver_item(name, title, description, content_type, content_identifier, url)')  # Added item info

            if spec_id:
                query = query.eq('spec_id', spec_id)
            if item_id:
                query = query.eq('item_id', item_id)
            if artifact_type:
                query = query.eq('artifact_type', artifact_type)
            if status:
                query = query.eq('status', status)
            if active is not None:
                query = query.eq('active', active)
            if format:
                query = query.eq('format', format)
            if language:
                query = query.eq('language', language)
            if created_since:
                query = query.gte('created_at', created_since.isoformat())
            if updated_since:
                query = query.gte('updated_at', updated_since.isoformat())
            if modified_after:  # Added time window filter handling
                query = query.gte('updated_at', modified_after.isoformat())
            if artifact_ids:
                query = query.in_('id', artifact_ids)

            # Add sorting and pagination
            query = query.order('created_at', desc=True) \
                         .range(offset, offset + limit - 1)

            result = query.execute()
            return result.data
        except Exception as e:
            logger.error(f"Error in artifact search: {str(e)}")
            raise


    def artifact_bulk_create(self, artifacts: List[Dict[str, Any]],
                          chunk_size: int = 100) -> List[Dict[str, Any]]:
        """Bulk create artifacts with automatic chunking"""
        created = []

        # Ensure required fields
        required = {'spec_id', 'item_id', 'title', 'content',
                    'generator_name', 'generator_id',
                    'artifact_type', 'format'}
        for artifact in artifacts:
            if not all(k in artifact for k in required):
                raise ValueError(f"Missing required fields in artifact: {required - set(artifact.keys())}")

        for chunk in chunks(artifacts, chunk_size):
            try:
                result = self.client.table('carver_artifact') \
                    .insert(chunk) \
                    .execute()
                if result.data:
                    created.extend(result.data)
            except Exception as e:
                logger.error(f"Error in bulk create artifacts: {str(e)}")
                continue

        return created

    def artifact_bulk_update(self, artifacts: List[Dict[str, Any]],
                          chunk_size: int = 100) -> List[Dict[str, Any]]:
        """Bulk update artifacts with automatic chunking"""
        updated = []

        # Validate all artifacts have IDs
        if not all('id' in a for a in artifacts):
            raise ValueError("All artifacts must have 'id' field for bulk update")

        for chunk in chunks(artifacts, chunk_size):
            try:
                result = self.client.table('carver_artifact') \
                    .upsert(chunk) \
                    .execute()
                if result.data:
                    updated.extend(result.data)
            except Exception as e:
                logger.error(f"Error in bulk update artifacts: {str(e)}")
                continue

        return updated

    def artifact_bulk_update_status(self, artifact_ids: List[int],
                                 status: str,
                                 metadata: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Update status and optional metadata for multiple artifacts"""
        try:
            update_data = {
                'status': status,
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

    def artifact_bulk_activate(self, artifact_ids: List[int]) -> List[Dict[str, Any]]:
        """Activate multiple artifacts"""
        try:
            updates = [{
                'id': artifact_id,
                'active': True,
            } for artifact_id in artifact_ids]

            return self.artifact_bulk_update(updates)
        except Exception as e:
            logger.error(f"Error in bulk activate artifacts: {str(e)}")
            raise

    def artifact_bulk_deactivate(self, artifact_ids: List[int]) -> List[Dict[str, Any]]:
        """Deactivate multiple artifacts"""
        try:
            updates = [{
                'id': artifact_id,
                'active': False,
            } for artifact_id in artifact_ids]

            return self.artifact_bulk_update(updates)
        except Exception as e:
            logger.error(f"Error in bulk deactivate artifacts: {str(e)}")
            raise

    def artifact_update_metrics(self, artifact_id: int,
                             metrics: Dict[str, Any],
                             replace: bool = False) -> Dict[str, Any]:
        """Update artifact metrics, either merging or replacing existing metrics"""
        try:
            current = self.artifact_get(artifact_id)
            if not current:
                raise ValueError(f"Artifact {artifact_id} not found")

            if replace or not current.get('artifact_metrics'):
                updated_metrics = metrics
            else:
                # Merge with existing metrics
                updated_metrics = {
                    **current['artifact_metrics'],
                    **metrics
                }

            update_data = {
                'artifact_metrics': updated_metrics,
            }

            result = self.client.table('carver_artifact') \
                .update(update_data) \
                .eq('id', artifact_id) \
                .execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Error updating artifact metrics: {str(e)}")
            raise
