import os
import sys
import json
import logging

from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path

from psycopg2.pool import SimpleConnectionPool

from carver.utils import get_config
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

    def open_connection(self):
        """Initialize database connection pool"""
        config = get_config()
        self.pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dbname=config('SUPABASE_DBNAME'),
            user=config('SUPABASE_USER'),
            password=config('SUPABASE_PASSWORD'),
            host=config('SUPABASE_HOST'),
            port=config('SUPABASE_PORT', default=5432)
        )

    def close_connection(self):
        """Clean up database connections"""
        if hasattr(self, 'pool'):
            self.pool.closeall()

    # Project methods
    def project_get(self, project_id: int) -> Dict[str, Any]:
        """Get a single project by ID"""
        result = self.client.table('carver_project') \
            .select('*') \
            .eq('id', project_id) \
            .execute()
        return result.data[0] if result.data else None

    def project_search(self,
                      active: Optional[bool] = None,
                      project_type: Optional[str] = None,
                      owner: Optional[str] = None,
                      name: Optional[str] = None,
                      created_since: Optional[datetime] = None,
                      updated_since: Optional[datetime] = None,
                      fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Search projects with various filters"""
        select_statement = ', '.join(fields) if fields else '*'
        query = self.client.table('carver_project').select(select_statement)

        if active is not None:
            query = query.eq('active', active)
        if project_type:
            query = query.eq('project_type', project_type)
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

    def project_create(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new project"""
        result = self.client.table('carver_project').insert(data).execute()
        return result.data[0] if result.data else None

    def project_update(self, project_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing project"""
        result = self.client.table('carver_project') \
            .update(data) \
            .eq('id', project_id) \
            .execute()
        return result.data[0] if result.data else None

    def project_update_metadata(self, project_id: int, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Update project's metadata"""
        current = self.project_get(project_id)
        if not current:
            return None

        existing_metadata = current.get('metadata', {}) or {}
        updated_metadata = {**existing_metadata, **metadata}

        return self.project_update(project_id, {
            'metadata': updated_metadata,
            'updated_at': datetime.utcnow().isoformat()
        })

    # Source methods
    def source_get(self, source_id: int) -> Dict[str, Any]:
        """Get a single source by ID"""
        result = self.client.table('carver_source') \
            .select('*, carver_project!inner(*)') \
            .eq('id', source_id) \
            .execute()
        return result.data[0] if result.data else None

    def source_search(self,
                      active: Optional[bool] = None,
                      project_id: Optional[int] = None,
                      platform: Optional[str] = None,
                      source_type: Optional[str] = None,
                      name: Optional[str] = None,
                      updated_since: Optional[datetime] = None,
                      crawled_since: Optional[datetime] = None,
                      fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Search sources with various filters"""
        # Build the select statement
        if fields:
            # If fields are specified but 'carver_project' isn't in them, add it with basic fields
            field_list = fields.copy()
            if 'carver_project' not in field_list:
                field_list.append('carver_project(id, name)')
            select_statement = ', '.join(field_list)
        else:
            select_statement = '*, carver_project!inner(*)'

        query = self.client.table('carver_source').select(select_statement)

        if active is not None:
            query = query.eq('active', active)
        if project_id:
            query = query.eq('project_id', project_id)
        if platform:
            query = query.ilike('platform', platform)
        if source_type:
            query = query.ilike('source_type', source_type)
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

    def source_bulk_update(self, sources: List[Dict[str, Any]],
                           chunk_size: int = 100) -> List[Dict[str, Any]]:
        """
        Bulk update posts with automatic chunking.
        """
        updated_sources = []

        # Validate all posts have IDs
        if not all('id' in source for source in sources):
            raise ValueError("All sources must have 'id' field for bulk update")

        # Process in chunks
        for chunk in chunks(sources, chunk_size):
            try:
                result = self.client.table('carver_source') \
                    .upsert(chunk) \
                    .execute()
                if result.data:
                    updated_sources.extend(result.data)
            except Exception as e:
                logger.error(f"Error in bulk update: {str(e)}")
                continue

        return updated_sources

    def source_bulk_update_flag(self, source_ids: List[int],
                                active: bool) -> List[Dict[str, Any]]:

        data = {'active': active }

        response = self.client.table('carver_source')\
                              .update(data)\
                              .in_('id', source_ids)\
                              .execute()
        return response.data

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

    def source_update_analytics(self, source_id: int) -> Optional[Dict[str, Any]]:
        """
        Update analytics metadata for a source.

        Args:
            source_id: ID of the source to update

        Returns:
            Updated source if successful, None otherwise
        """
        try:
            # Get source analytics
            result = self.client.rpc('get_source_analytics', {
                'source_id_param': source_id
            }).execute()

            if not result.data:
                return None

            metrics = result.data[0]

            # Prepare analytics metadata
            analytics_metadata = {
                "metrics": {
                    'counts': {
                        'posts': metrics['active_posts_count'],
                        'artifacts': metrics['active_artifacts_count'],
                        'specifications': metrics['active_specs_count']
                    },
                    'distribution': {
                        'artifact_spec': metrics['artifact_spec_distribution'],
                        'artifact_type': metrics['artifact_type_distribution'],
                        'artifact_status': metrics['artifact_status_distribution']
                    },
                    'last_update': datetime.utcnow().isoformat()
                }
            }

            # Update source metadata
            return self.source_update_metadata(source_id, analytics_metadata)

        except Exception as e:
            logger.error(f"Error updating analytics for source {source_id}: {str(e)}")
            raise
    ##########################################################
    # Item Methods
    ##########################################################
    def post_get(self, post_id: int) -> Optional[Dict]:
        """Get a single item by ID with its source information"""
        try:
            result = self.client.table('carver_post') \
                .select('*, carver_source!inner(*)') \
                .eq('id', post_id) \
                .execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Error getting item {post_id}: {str(e)}")
            raise

    def post_search(self,
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
                    limit: int = 20,
                    offset: int = 0) -> List[Dict[str, Any]]:
        """
        Search posts with various filters

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
            limit: Maximum number of posts to return
            offset: Number of posts to skip
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

            query = self.client.table('carver_post').select(select_statement)

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
            query = query.order('updated_at', desc=True)
            query = query.range(offset, offset + limit - 1)

            result = query.execute()
            return result.data

        except Exception as e:
            logger.error(f"Error in post_search: {str(e)}")
            raise

    def post_get_by_identifiers(self, source_id: int, content_identifiers: List[str],
                              fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get posts by their source_id and content identifiers

        Args:
            source_id: The source ID
            content_identifiers: List of content identifiers to fetch
            fields: Optional list of specific fields to return
        """
        return self.post_search(
            source_id=source_id,
            content_identifier=content_identifiers,
            fields=fields,
            limit=len(content_identifiers)
        )

    def post_bulk_create(self, posts: List[Dict[str, Any]], chunk_size: int = 100) -> List[Dict[str, Any]]:
        """
        Bulk create posts with automatic chunking to avoid request size limits.
        Returns list of created posts.
        """
        created_posts = []

        # Process in chunks to avoid request size limits
        for chunk in chunks(posts, chunk_size):
            try:
                result = self.client.table('carver_post').insert(chunk).execute()
                if result.data:
                    created_posts.extend(result.data)
            except Exception as e:
                logger.error(f"Error in bulk create: {str(e)}")
                # Continue with next chunk even if one fails
                continue

        return created_posts

    def post_bulk_update(self, posts: List[Dict[str, Any]], chunk_size: int = 100) -> List[Dict[str, Any]]:
        """
        Bulk update posts with automatic chunking.
        Each item dict must contain 'id' field.
        Returns list of updated posts.
        """
        updated_posts = []

        # Validate all posts have IDs
        if not all('id' in item for item in posts):
            raise ValueError("All posts must have 'id' field for bulk update")

        # Process in chunks
        for chunk in chunks(posts, chunk_size):
            try:
                result = self.client.table('carver_post') \
                    .upsert(chunk) \
                    .execute()
                if result.data:
                    updated_posts.extend(result.data)
            except Exception as e:
                logger.error(f"Error in bulk update: {str(e)}")
                continue

        return updated_posts

    def post_bulk_update_flag(self, post_ids: List[int],
                              active: bool) -> List[Dict[str, Any]]:

        data = {'active': active }

        response = self.client.table('carver_post')\
                              .update(data)\
                              .in_('id', post_ids)\
                              .execute()

        return response.data

    def post_bulk_activate(self, source_id: int, content_identifiers: List[str]) -> List[Dict[str, Any]]:
        """Activate posts by their content identifiers"""
        try:
            # First get the posts to update
            posts = self.post_search(
                source_id=source_id,
                content_identifier=content_identifiers
            )
            post_ids = [p['id'] for p in posts]

            return self.post_bulk_update_flag(post_ids, active=True)
        except Exception as e:
            logger.error(f"Error in bulk activate: {str(e)}")
            raise

    def post_bulk_deactivate(self, source_id: int, content_identifiers: List[str]) -> List[Dict[str, Any]]:
        """Deactivate posts by their content identifiers"""
        try:
            # First get the posts to update
            posts = self.post_search(
                source_id=source_id,
                content_identifier=content_identifiers
            )

            posts = self.post_search(
                source_id=source_id,
                content_identifier=content_identifiers
            )
            post_ids = [p['id'] for p in posts]

            return self.post_bulk_update_flag(post_ids, active=False)
        except Exception as e:
            logger.error(f"Error in bulk deactivate: {str(e)}")
            raise

    def post_bulk_update_metadata(self, posts: List[Dict[str, Any]], chunk_size: int = 100) -> List[Dict[str, Any]]:
        """
        Bulk update posts' analysis metadata.
        Each item should have 'id' and 'analysis_metadata' fields.
        Preserves existing metadata.
        """
        updates = []
        for item in posts:
            if 'id' not in item or 'analysis_metadata' not in post:
                continue

            current = self.post_get(item['id'])
            if not current:
                continue

            existing_metadata = current.get('analysis_metadata', {}) or {}
            updated_metadata = {**existing_metadata, **item['analysis_metadata']}

            updates.append({
                'id': item['id'],
                'analysis_metadata': updated_metadata,
                'updated_at': datetime.utcnow().isoformat()
            })

        return self.post_bulk_update(updates, chunk_size)

    def post_bulk_set_processed(self, post_ids: List[int], processed: bool = True) -> List[Dict[str, Any]]:
        """Bulk update processed status for multiple posts"""
        try:
            updates = [{
                'id': post_id,
                'is_processed': processed,
                'updated_at': datetime.utcnow().isoformat()
            } for post_id in post_ids]

            return self.post_bulk_update(updates)
        except Exception as e:
            logger.error(f"Error in bulk set processed: {str(e)}")
            raise

    def post_search_with_artifacts(self,
                                   source_id: int,
                                   generator_name: Optional[str] = None,
                                   modified_after: Optional[datetime] = None,
                                   offset: int = 0,
                                   limit: int = 10) -> Dict[int, Dict]:
        """
        Find posts with their artifacts for a specific generator
        Returns a map of post_id -> {post: post_data, artifacts: [artifact_data]}
        """
        try:
            # Get active posts from source
            query = self.client.table('carver_post') \
                               .select('*') \
                               .eq('source_id', source_id) \
                               .eq('active', True)

            if modified_after:
                query = query.gte('updated_at', modified_after.isoformat())

            query  = query.range(offset, offset + limit - 1)
            result = query.execute()
            posts  = result.data

            if len(posts) == 0:
                return []

            post_ids = [i['id'] for i in posts]
            basequery = self.client.table('carver_artifact') \
                                   .select('*, spec:carver_artifact_specification!inner(id)') \
                                   .in_('post_id', post_ids) \
                                   .eq('active', True)\
                                   .eq('spec.active', True)

            if generator_name:
                basequery = query.eq('generator_name', generator_name)

            # Handle the case where there are more than 1000 artifacts
            # for a given source
            artifacts = []
            for offset in range(0, 10000, 1000):
                query = basequery.range(offset, offset+ 1000)
                inc_artifacts = query.execute()
                inc_artifacts = inc_artifacts.data
                artifacts.extend(inc_artifacts)
                if len(inc_artifacts) < 1000:
                    break

            for item in posts:
                post_artifacts = [a for a in artifacts if a['post_id'] == item['id']]
                item['artifacts'] = post_artifacts

            return posts

        except Exception as e:
            logger.error(f"Error in post_search_with_artifacts: {str(e)}")
            raise

    def post_search_without_artifacts(self,
                                      source_id: int,
                                      generator_name: Optional[str] = None,
                                      modified_after: Optional[datetime] = None,
                                      offset: int = 0,
                                      limit: int = 1000) -> List[Dict[str, Any]]:

        """Find posts without artifacts for a specific generator"""
        try:
            query = self.client.table('carver_artifact') \
                               .select('post_id, carver_post!inner(source_id)')\
                               .eq('active', True) \
                               .eq('carver_post.source_id', source_id)

            if generator_name:
                query = query.eq('generator_name', generator_name)

            posts_with_artifacts = query.execute()

            post_ids_with_artifacts = [item['post_id'] for item in posts_with_artifacts.data]

            query = self.client.table('carver_post') \
                               .select('*, source:carver_source!inner(id)') \
                               .eq('source_id', source_id) \
                               .eq('source.active', True)\
                               .eq('active', True)

            if post_ids_with_artifacts:
                query = query.not_.in_('id', post_ids_with_artifacts)

            if modified_after:
                query = query.gte('updated_at', modified_after.isoformat())

            query  = query.range(offset, offset + limit - 1)
            result = query.execute()
            return result.data

        except Exception as e:
            logger.error(f"Error in post_search_without_artifacts: {str(e)}")
            raise


    ##########################################################
    # Specification Methods
    ##########################################################
    def specification_get(self, spec_id: int) -> Optional[Dict[str, Any]]:
        """Get a single artifact specification by ID with related data"""
        try:
            result = self.client.table('carver_artifact_specification') \
                .select('*, source:carver_source!inner(*)') \
                .eq('id', spec_id) \
                .eq('source.active', True)\
                .execute()

            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Error getting specification {spec_id}: {str(e)}")
            raise

    def specification_search(self,
                          source_id: Optional[int] = None,
                          project_id: Optional[int] = None,
                          spec_id: Optional[int] = None,
                          name: Optional[str] = None,
                          active: Optional[bool] = None,
                          created_since: Optional[datetime] = None,
                          updated_since: Optional[datetime] = None,
                          limit: int = 100,
                          offset: int = 0,
                          fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Search artifact specifications with filters"""
        try:
            # Build select statement
            if fields:
                field_list = fields.copy()
                # If fields are specified but related tables aren't included, add minimal fields
                if 'carver_source' not in field_list:
                    field_list.append('carver_source(id, name, carver_project(id, name))')
                select_statement = ', '.join(field_list)
            else:
                select_statement = '*, carver_source!inner(*, carver_project!inner(*))'

            query = self.client.table('carver_artifact_specification').select(select_statement)

            if spec_id:
                query = query.eq('id', spec_id)
            if source_id:
                query = query.eq('source_id', source_id)
            if project_id:
                query = query.eq('carver_source.project_id', project_id)
            if name:
                query = query.or_(f'name.ilike.%{name}%,description.ilike.%{name}%')
            if active is not None:
                query = query.eq('active', active)
            if created_since:
                query = query.gte('created_at', created_since.isoformat())
            if updated_since:
                query = query.gte('updated_at', updated_since.isoformat())

            query = query.order('created_at', desc=True).range(offset, offset + limit - 1)

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
                .select('*, carver_artifact_specification!inner(*), carver_post!inner(*)') \
                .eq('id', artifact_id) \
                .eq('carver_artifact_specification.active', True)\
                .eq('carver_post.active', True)\
                .execute()

            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Error getting artifact {artifact_id}: {str(e)}")
            raise

    def artifact_search(self,
                        spec_id: Optional[int] = None,
                        post_id: Optional[int] = None,
                        artifact_type: Optional[str] = None,
                        status: Optional[str] = None,
                        active: Optional[bool] = None,
                        format: Optional[str] = None,
                        language: Optional[str] = None,
                        has_embedding: Optional[bool] = None,
                        modified_after: Optional[datetime] = None,
                        created_since: Optional[datetime] = None,
                        updated_since: Optional[datetime] = None,
                        published_after: Optional[datetime] = None,
                        artifact_ids: Optional[List[int]] = None,
                        limit: int = 100,
                        offset: int = 0,
                        fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Search artifacts with various filters

        Args:
            spec_id: Filter by specification ID
            post_id: Filter by item ID
            artifact_type: Filter by artifact type
            status: Filter by status
            active: Filter by active status
            format: Filter by format
            language: Filter by language
            modified_after: Filter by modification date
            published_after: Filter by published date
            created_since: Filter by creation date
            updated_since: Filter by update date
            artifact_ids: Filter by specific artifact IDs
            limit: Maximum number of results to return
            offset: Number of results to skip
            fields: Optional list of specific fields to return
        """
        try:
            # Build select statement
            if fields:
                field_list = fields.copy()
                # If fields are specified but related tables aren't included, add minimal fields
                if 'carver_artifact_specification' not in field_list:
                    field_list.append('carver_artifact_specification(id, name)')
                if 'carver_post' not in field_list:
                    field_list.append('carver_post(id, name, author, title, content_identifier)')
                select_statement = ', '.join(field_list)
            else:
                select_statement = '*, carver_artifact_specification!inner(*), carver_post!inner(name, title, author, description, content_type, content_identifier, url, published_at)'

            query = self.client.table('carver_artifact').select(select_statement)

            # Filter
            query = query.eq('carver_post.active', True)
            query = query.eq('carver_artifact_specification.active', True)

            if spec_id:
                query = query.eq('spec_id', spec_id)
            if post_id:
                query = query.eq('post_id', post_id)
            if artifact_type:
                query = query.eq('artifact_type', artifact_type)
            if status:
                query = query.eq('status', status)
            if active is not None:
                print("active")
                query = query.eq('active', active)
            if format:
                query = query.eq('format', format)
            if language:
                query = query.eq('language', language)
            if has_embedding is not None:
                if has_embedding:
                    query = query.not_.is_('content_embedding', None)
                else:
                    query = query.is_('content_embedding', None)
            if created_since:
                query = query.gte('created_at', created_since.isoformat())
            if updated_since:
                query = query.gte('updated_at', updated_since.isoformat())
            if modified_after:
                query = query.gte('updated_at', modified_after.isoformat())
            if published_after:
                query = query.gte('carver_post.published_at', published_after.isoformat())
            if artifact_ids:
                query = query.in_('id', artifact_ids)

            # Add sorting and pagination
            query = query.order('created_at', desc=True).range(offset, offset + limit - 1)

            result = query.execute()
            data = result.data
            return data

        except Exception as e:
            logger.error(f"Error in artifact search: {str(e)}")
            raise

    def artifact_bulk_create(self, artifacts: List[Dict[str, Any]],
                          chunk_size: int = 100) -> List[Dict[str, Any]]:
        """Bulk create artifacts with automatic chunking"""
        created = []

        # Ensure required fields
        required = {'spec_id', 'post_id', 'title', 'content',
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

    def artifact_bulk_update_flag(self, artifact_ids: List[int],
                                  active: bool) -> List[Dict[str, Any]]:

        data = {'active': active }

        response = self.client.table('carver_artifact')\
                              .update(data)\
                              .in_('id', artifact_ids)\
                              .execute()

        return response.data


    def artifact_bulk_update_chunked(self, artifacts: List[Dict[str, Any]],
                                     chunk_size: int = 100) -> List[Dict[str, Any]]:
        """
        Bulk update artifacts with automatic chunking.
        Only updates the specified fields for each artifact using raw SQL.

        Args:
            artifacts: List of dicts, each containing 'id' and fields to update
            chunk_size: Number of artifacts to update in each batch
        """
        updated = []

        # Validate all artifacts have IDs
        if not all('id' in a for a in artifacts):
            raise ValueError("All artifacts must have 'id' field for bulk update")

        for chunk in chunks(artifacts, chunk_size):
            try:
                result = self.artifact_bulk_update(self, chunk)
            except Exception as e:
                logger.error(f"Error in bulk update artifacts: {str(e)}")
                continue

        return updated

    def artifact_bulk_update(self, artifacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Bulk update artifacts using direct SQL connection

        Args:
            artifacts: List of dicts containing updates. Each dict must have 'id'
            update_columns: Optional list of columns to update
        """
        try:
            if not artifacts or len(artifacts) == 0:
                return []

            conn = None

            update_columns = [col for col in list(artifacts[0].keys()) if col not in ['id']]

            # Convert embeddings to list format
            updates = []
            for artifact in artifacts:
                update_dict = {'id': artifact['id']}
                for key, value in artifact.items():
                    if key == 'id':
                        continue
                    if isinstance(value, (list)):
                        update_dict[key] = [float(x) for x in value]
                    else:
                        update_dict[key] = value
                updates.append(update_dict)

            # Build the SQL query
            sql = f"""
            UPDATE carver_artifact t
            SET {', '.join(f"{col} = v.{col}" for col in update_columns)}
            FROM (
                SELECT
                    v.*
                FROM jsonb_to_recordset(%s)
                AS v(
                    id bigint,
                    {', '.join(
                        f"{col} vector(1536)" if col == 'content_embedding'
                        else f"{col} timestamp with time zone" if col == 'updated_at'
                        else f"{col} text"
                        for col in update_columns
                    )}
                )
            ) v
            WHERE t.id = v.id
            RETURNING t.*
            """

            # Execute using psycopg2
            if not hasattr(self, 'pool'):
                self.open_connection()

            conn = self.pool.getconn()
            with conn.cursor() as cur:
                cur.execute(sql, (json.dumps(updates),))
                results = cur.fetchall()

                # Get column names from cursor description
                columns = [desc[0] for desc in cur.description]

                # Convert results to dicts
                return_data = [dict(zip(columns, row)) for row in results]

            conn.commit()
            return return_data
        except:
            if conn:
                conn.rollback()
            logger.error(f"Error in bulk update artifacts: {str(e)}")
            raise

        finally:
            # Always return the connection to the pool
            if conn:
                self.pool.putconn(conn)

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

    def artifact_search_similar(self,
                              query_embedding: List[float],
                              match_threshold: float = 0.7,
                              match_count: int = 10,
                              spec_id: Optional[int] = None,
                              source_id: Optional[int] = None,
                              status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search for similar artifacts using vector similarity"""
        try:
            # Call the match_artifacts function with source_id
            result = self.client.rpc(
                'match_artifacts',
                {
                    'query_embedding': query_embedding,
                    'match_threshold': match_threshold,
                    'match_count': match_count,
                    'filter_spec_id': spec_id,
                    'filter_source_id': source_id,
                    'filter_status': status
                }
            ).execute()

            return result.data if result.data else []

        except Exception as e:
            logger.error(f"Error in similarity search: {str(e)}")
            raise
