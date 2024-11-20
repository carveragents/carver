from typing import List, Dict, Any, Optional, Type
from abc import ABC, abstractmethod

class BaseArtifactGenerator(ABC):
    """Base class for artifact generators"""
    name = "base"

    @abstractmethod
    def generate(self, item: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Generate artifact content from item data"""
        pass

    @abstractmethod
    def validate_config(self, source: Dict[str, Any], config: Dict[str, Any]) -> bool:
        """Validate generator configuration"""
        pass
