import os
import json
import logging
from typing import List, Dict, Any, Optional
from collections import defaultdict
from datetime import datetime

from llama_index.core import (
    Document,
    KnowledgeGraphIndex,
    Settings,
)
from llama_index.core.graph_stores import SimpleGraphStore
from llama_index.core.storage.storage_context import StorageContext
from llama_index.llms.openai import OpenAI

from .base import BaseArtifactGenerator
from carver.utils import get_config

logger = logging.getLogger(__name__)

class KnowledgeGraphGenerator(BaseArtifactGenerator):
    """Generates knowledge graph artifacts using LlamaIndex"""
    name = "knowledge_graph"
    description = "Extracts entities and relationships using LlamaIndex KG with source tracking"
    supported_platforms = ["*"]
    supported_source_types = ["*"]

    def validate_config(self, source: Dict[str, Any],
                        spec: Dict[str, Any]) -> bool:
        """Validate configuration parameters"""

        config = spec['config']
        required_fields = ['max_triplets_per_chunk', 'system_prompt']
        if not all(field in config for field in required_fields):
            raise ValueError(f"Missing required config fields: {required_fields}")

        if not isinstance(config['max_triplets_per_chunk'], int):
            raise ValueError("max_triplets_per_chunk must be an integer")

        return True

    def get_ids(self, config: Dict[str, Any]) -> List[str]:
        """Get list of generator IDs"""
        return ['en']

    def _get_transcript(self, post: Dict[str, Any], artifacts: List[Dict[str, Any]]) -> Optional[str]:
        """Extract transcript from post artifacts"""
        for artifact in artifacts:
            if ((artifact['generator_name'] == "transcription") and
                (artifact['generator_id'] in ['en', 'en-GB'])):
                return artifact['content']
        return None

    def _prepare_document(self, post: Dict[str, Any], transcript: str) -> Document:
        """Create document with metadata from post"""
        return Document(
            text=transcript,
            metadata={
                'post_id': post['id'],
                'title': post.get('title', ''),
                'url': post.get('url', ''),
                'published_at': post.get('published_at'),
                'content_type': post.get('content_type'),
                'source_id': post.get('source_id')
            }
        )

    def generate(self, post: Dict[str, Any],
                 spec: Dict[str, Any],
                 existing: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate artifact content from post data"""
        raise Exception("Not implemented")

    def generate_bulk(self, posts: List[Dict[str, Any]],
                     spec: Dict[str, Any],
                     existing_map: Dict[int, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Generate knowledge graph from multiple posts"""

        llmconfig = get_config()
        os.environ['OPENAI_API_KEY'] = llmconfig('OPENAI_API_KEY')

        config = spec['config']

        try:
            # Setup graph store and context
            graph_store = SimpleGraphStore()
            storage_context = StorageContext.from_defaults(graph_store=graph_store)

            # Configure LlamaIndex settings
            try:
                # Try new style Settings
                Settings.llm = OpenAI(model="gpt-4o-mini", system_prompt=config['system_prompt'])
                Settings.chunk_size = 512
                Settings.chunk_overlap = 20
            except Exception as e:
                logger.warning(f"Falling back to legacy settings: {e}")
                # Fall back to legacy style settings through ServiceContext
                from llama_index.core import ServiceContext
                service_context = ServiceContext.from_defaults(
                    llm=OpenAI(model="gpt-4o-mini", system_prompt=config['system_prompt']),
                    chunk_size=512,
                    chunk_overlap=20
                )

            # Prepare documents with metadata
            documents = []
            doc_map = {}  # Track document sources

            # Process each post
            for post in posts:
                post_artifacts = existing_map.get(post['id'], [])
                transcript = self._get_transcript(post, post_artifacts)

                if transcript:
                    doc = self._prepare_document(post, transcript)
                    documents.append(doc)
                    doc_map[doc.doc_id] = post['id']

            if not documents:
                logger.info("No documents to process")
                return []

            # Build knowledge graph
            logger.info(f"Building knowledge graph from {len(documents)} documents")
            kg_index = KnowledgeGraphIndex.from_documents(
                documents,
                storage_context=storage_context,
                max_triplets_per_chunk=5, #config.get('max_triplets_per_chunk', 10),
                include_embeddings=False, #config.get('include_embeddings', True)
            )

            # Initialize graph data structure
            graph_data = {
                "nodes": [],
                "edges": [],
                "metadata": {
                    "node_types": config.get('entity_types', []),
                    "edge_types": config.get('relationship_types', []),
                    "document_references": {}
                }
            }

            # Track nodes and their document references
            node_docs = defaultdict(set)
            node_map = {}

            # Extract relationships and track document references
            rel_map = graph_store.get_rel_map()
            print(f"Found {len(rel_map)} relationships")

            # Debug the first relationship if available
            if rel_map:
                first_key = next(iter(rel_map))
                print(f"Sample relationship - Key: {first_key}")
                print(f"Value type: {type(rel_map[first_key])}")
                print(f"Value: {rel_map[first_key]}")

            for rel_key, triplets in rel_map.items():
                for triplet in triplets:
                    # SimpleGraphStore returns basic triplets
                    subj_text, pred, obj_text = triplet  # Each triplet is just (subject, predicate, object)

                    # Create node for subject
                    if subj_text not in node_map:
                        node_map[subj_text] = {
                            "id": subj_text,
                            "label": subj_text,
                            "type": "entity",
                            "metadata": {
                                "doc_refs": [],  # We don't have doc refs in SimpleGraphStore
                                "mention_count": 1,
                                "embedding": None  # No embeddings with SimpleGraphStore
                            }
                        }
                    else:
                        node_map[subj_text]["metadata"]["mention_count"] += 1

                    # Create node for object
                    if obj_text not in node_map:
                        node_map[obj_text] = {
                            "id": obj_text,
                            "label": obj_text,
                            "type": "entity",
                            "metadata": {
                                "doc_refs": [],
                                "mention_count": 1,
                                "embedding": None
                            }
                        }
                    else:
                        node_map[obj_text]["metadata"]["mention_count"] += 1

                    # Add edge
                    graph_data["edges"].append({
                        "source": subj_text,
                        "target": obj_text,
                        "label": pred,
                        "metadata": {
                            "doc_ref": None,  # No doc refs in SimpleGraphStore
                            "confidence": None  # No confidence scores in SimpleGraphStore
                        }
                    })

            # Add nodes to graph data
            graph_data["nodes"] = list(node_map.values())

            # Add document metadata
            graph_data["metadata"]["document_references"] = {
                post['id']: {
                    'title': post.get('title', ''),
                    'url': post.get('url', ''),
                    'published_at': post.get('published_at'),
                    'content_type': post.get('content_type'),
                    'source_id': post.get('source_id')
                }
                for post in posts
                if any(doc.metadata.get('post_id') == post['id'] for doc in documents)
            }

            # Calculate statistics
            node_count = len(graph_data["nodes"])
            mention_counts = [n["metadata"]["mention_count"] for n in graph_data["nodes"]]
            avg_mentions = sum(mention_counts) / node_count if node_count > 0 else 0

            now = datetime.utcnow().isoformat()

            # Create artifact
            artifact = {
                'name': "Knowledge Graph",
                'generator_name': self.name,
                'generator_id': 'en',
                'title': f"Knowledge Graph: {len(posts)} Documents",
                'content': json.dumps(graph_data, indent=2),
                'format': 'json',
                'artifact_type': 'KNOWLEDGE_GRAPH',
                'active': True,
                "status": "draft",
                "version": 1,
                "language": "en",
                'analysis_metadata': {
                    'node_count': node_count,
                    'edge_count': len(graph_data["edges"]),
                    'document_count': len(documents),
                    'processing_stats': {
                        'avg_mentions_per_node': avg_mentions,
                        'max_mentions': max(mention_counts) if mention_counts else 0,
                        'total_mentions': sum(mention_counts),
                        'generated_at': datetime.utcnow().isoformat()
                    },
                },
                'created_at': now,
                'updated_at': now,
            }

            return [artifact]

        except Exception as e:
            logger.error(f"Failed to generate knowledge graph: {str(e)}")
            raise
