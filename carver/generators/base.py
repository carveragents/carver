from typing import List, Dict, Any, Optional, Type
from abc import ABC, abstractmethod

class BaseArtifactGenerator(ABC):
    """Base class for artifact generators"""
    name: str = None
    description: str = None
    supported_platforms: List[str] = []
    supported_source_types: List[str] = []
    required_config: List[str] = []

    @classmethod
    def get_info(cls) -> Dict[str, Any]:
        """Get generator documentation and requirements"""
        return {
            'name': cls.name,
            'description': cls.description,
            'supported_platforms': cls.supported_platforms,
            'supported_source_types': cls.supported_source_types,
            'required_config': cls.required_config
        }

    @abstractmethod
    def generate(self, post: Dict[str, Any], config: Dict[str, Any], existing: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate artifact content from post data"""
        pass

    @abstractmethod
    def generate_bulk(self, posts: List[Dict[str, Any]],
                     config: Dict[str, Any],
                     existing_map: Dict[int, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Generate knowledge graph from multiple posts"""

    def validate_config(self, source: Dict[str, Any],
                        config: Dict[str, Any]) -> bool:

        if (("*" not in self.supported_platforms) and
            (source['platform'].upper() not in [x.upper() for x in self.supported_platforms])):
            raise ValueError(f"Unsupported platform: {source['platform']}")

        if (("*" not in self.supported_source_types) and
            (source['source_type'].upper() not in [x.upper() for x in self.supported_source_types])):
            raise ValueError(f"Unsupported source type: {source['source_type']}")

        return all(k in config for k in self.required_config)

    def get_ids(self, config: Dict[str, Any]):
        raise Exception("To be implemented by subclass")

