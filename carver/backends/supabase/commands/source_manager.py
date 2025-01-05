import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from carver.generators import ArtifactGeneratorFactory

from carver.utils import parse_date_filter

logger = logging.getLogger(__name__)

class SourceManager:
    """Manages source operations"""

    def __init__(self, db_client):
        self.db = db_client

    def generate_knowledge_graph(self,
                               source_id: int,
                               batch_size: int = 50,
                               last_modified: Optional[datetime] = None,
                               spec_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Generate knowledge graph for a source using its transcripts.

        Args:
            source_id: Source ID to process
            batch_size: Number of posts to process per batch
            last_modified: Optional timestamp to filter posts
            spec_id: Optional specification ID (will search for active knowledge graph spec if not provided)

        Returns:
            Dict containing status and results
        """

        try:
            print("In generate knowledge graph")

            # Get or verify spec
            if spec_id:
                specs = self.db.specification_search(
                    source_id=source_id,
                    spec_id=spec_id,
                    active=True
                )
            else:
                # Find active knowledge graph spec for this source
                specs = self.db.specification_search(
                    source_id=source_id,
                    active=True
                )
                specs = [s for s in specs
                        if s['config'].get('generator') == 'knowledge_graph']

            if not specs:
                raise ValueError(f"No active knowledge graph specification found for source {source_id}")

            print(f"Specs found {len(specs)}")

            spec = specs[0]  # Use first matching spec

            # Get posts with artifacts
            posts = self.db.post_search_with_artifacts(
                source_id=source_id,
                modified_after=last_modified,
                limit=batch_size
            )

            print(f"Posts found {len(posts)}")
            if not posts:
                return {
                    'status': 'no_posts',
                    'message': 'No posts found requiring processing'
                }


            # Organize artifacts by post
            artifacts_by_post = {}
            for post in posts:
                artifacts_by_post[post['id']] = post.get('artifacts', [])

            # Get generator instance
            generator = ArtifactGeneratorFactory.get_generator('knowledge_graph')

            print("Generator", generator)

            results = generator.generate_bulk(
                posts=posts,
                config=spec['config'],
                existing_map=artifacts_by_post
            )

            if results:

                # Link the aggregrate map to the first post
                for r in results:
                    r['spec_id'] = spec['id']
                    r['post_id'] = posts[0]['id']

                print(json.dumps(results, indent=4))

                # Create artifacts
                artifacts = self.db.artifact_bulk_create(results)

                # Extract stats
                graph_data = json.loads(results[0]['content'])
                stats = {
                    'nodes': len(graph_data['nodes']),
                    'edges': len(graph_data['edges']),
                    'documents': len(graph_data['metadata']['document_references'])
                }

                return {
                    'status': 'success',
                    'stats': stats,
                    'artifacts': artifacts,
                    'spec_id': spec['id']
                }

            return {
                'status': 'no_results',
                'message': 'No graph artifacts generated'
            }

        except Exception as e:
            traceback.print_exc()
            logger.error(f"Error generating knowledge graph: {str(e)}")
            raise

    def generate_knowledge_graphs(self,
                                project_id: int,
                                batch_size: int = 50,
                                last_modified: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Generate knowledge graphs for all sources in a project.

        Args:
            project_id: Project ID
            batch_size: Posts to process per batch per source
            last_modified: Optional filter for post modification time

        Returns:
            List of results for each source
        """
        try:
            # Get all active sources for project
            sources = self.db.source_search(
                project_id=project_id,
                active=True
            )

            if not sources:
                logger.info(f"No active sources found for project {project_id}")
                return []

            results = []
            for source in sources:
                try:
                    result = self.generate_knowledge_graph(
                        source_id=source['id'],
                        batch_size=batch_size,
                        last_modified=last_modified
                    )
                    results.append({
                        'source_id': source['id'],
                        'source_name': source['name'],
                        **result
                    })
                except Exception as e:
                    logger.error(f"Error processing source {source['id']}: {str(e)}")
                    results.append({
                        'source_id': source['id'],
                        'source_name': source['name'],
                        'status': 'error',
                        'message': str(e)
                    })
                    continue

            return results

        except Exception as e:
            logger.error(f"Error generating knowledge graphs for project: {str(e)}")
            raise

    def bulk_activate(self, source_ids: List[int]) -> int:
        """
        Activate multiple sources by their IDs.

        Args:
            source_ids: List of source IDs to activate

        Returns:
            Number of sources updated
        """
        try:
            # Create update data for each source
            to_update = [
                {
                    'id': source_id,
                    'active': True,
                    'updated_at': datetime.utcnow().isoformat()
                }
                for source_id in source_ids
            ]

            # Perform bulk update
            updated = self.db.source_bulk_update(to_update)
            return len(updated)

        except Exception as e:
            logger.error(f"Error bulk activating sources: {str(e)}")
            raise

    def bulk_deactivate(self, source_ids: List[int]) -> int:
        """
        Deactivate multiple sources by their IDs.

        Args:
            source_ids: List of source IDs to deactivate

        Returns:
            Number of sources updated
        """
        try:
            # Create update data for each source
            to_update = [
                {
                    'id': source_id,
                    'active': False,
                    'updated_at': datetime.utcnow().isoformat()
                }
                for source_id in source_ids
            ]

            # Perform bulk update
            updated = self.db.source_bulk_update(to_update)
            return len(updated)

        except Exception as e:
            logger.error(f"Error bulk deactivating sources: {str(e)}")
            raise

