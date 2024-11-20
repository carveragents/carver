import os
import sys
import json

from .base import BaseArtifactGenerator

from .thread import ThreadGenerator
from .summary import SummaryGenerator
from .transcription import TranscriptionGenerator

class ArtifactGeneratorFactory:
    """Factory for creating artifact generators"""

    @classmethod
    def get_generator(cls, name: str) -> BaseArtifactGenerator:
        classes = BaseArtifactGenerator.__subclasses__()
        for cls in classes:
            if cls.name == name:
                return cls()

        raise ValueError(f"No generator found for generator: {name}")
