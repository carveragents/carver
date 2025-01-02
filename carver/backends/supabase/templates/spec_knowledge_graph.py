knowledge_graph_system_prompt = """
Extract key entities and relationships from the following transcript to construct a knowledge graph. Focus on:

1. Technical concepts and their relationships
2. Problem-solution patterns
3. Tools/technologies mentioned and their interactions
4. Architecture and system components
5. Implementation details and challenges

For each entity, identify:
- Type (Concept, Tool, Problem, Solution, Component)
- Key attributes
- Related documents where it appears

For each relationship:
- Type of connection (uses, solves, part_of, depends_on, implements)
- Direction of relationship
- Context where relationship appears

Return the structured data in the exact format shown in the knowledge graph visualization.
"""

def get_config(raw: bool = False, show: bool = False):

    return {
        "name": "Knowledge Graph Generator",
        "description": "Generate Knowledge Graphs from Content",
        "platforms": ["*"],  # Works with any platform
        "specifications": [
            {
                "id": 2001,
                "name": "Technical Knowledge Graph",
                "description": "Extract technical concepts and relationships",
                "config": {
                    "generator": "knowledge_graph",
                    "dependencies": ["Transcription"],
                    "system_prompt": knowledge_graph_system_prompt,
                    "max_triplets_per_chunk": 20,
                    "include_embeddings": True,
                    "min_confidence": 0.6,
                    "entity_types": [
                        "Concept",
                        "Tool",
                        "Problem",
                        "Solution",
                        "Component"
                    ],
                    "relationship_types": [
                        "uses",
                        "solves",
                        "part_of",
                        "depends_on",
                        "implements"
                    ]
                }
            }
        ]
    }
