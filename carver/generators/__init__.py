import os
import sys
import json
import traceback

from .base import BaseArtifactGenerator
from .thread import ThreadGenerator
from .summary import SummaryGenerator
from .transcription import TranscriptionGenerator
from .knowledgegraph import KnowledgeGraphGenerator
from .exa_content import ExaContentGenerator
class ArtifactGeneratorFactory:
    """Factory for creating artifact generators"""

    @classmethod
    def get_generator(cls, name: str) -> BaseArtifactGenerator:
        classes = BaseArtifactGenerator.__subclasses__()
        for cls in classes:
            # print(cls, cls.name)
            if cls.name == name:
                return cls()

        raise ValueError(f"No generator found for generator: {name}")

