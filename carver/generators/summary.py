import os
import sys
import json
import logging

from typing import List, Dict, Any, Optional, Union
from datetime import datetime

from .base import BaseArtifactGenerator

logger = logging.getLogger(__name__)

class SummaryGenerator(BaseArtifactGenerator):
    """Generates summary artifacts from content using multiple prompts"""
    name = "summary"
    description = "Summarizes content using configurable prompts and strategies"
    supported_platforms = ["*"]
    supported_source_types = ["*"]
    required_config = ['prompts']

    def validate_config(self, source: Dict[str, Any], config: Dict[str, Any]) -> bool:
        """
        Validate configuration parameters for summary generation.

        Args:
            source: Source configuration dictionary
            config: Summary configuration dictionary with structure:
                {
                    "prompts": [
                        {
                            "prompt": str,
                            "generator_id": str
                        },
                        ...
                    ]
                }

        Returns:
            bool: True if configuration is valid

        Raises:
            ValueError: If configuration is invalid
        """
        # Check basic config requirements
        if not all(k in config for k in self.required_config):
            raise ValueError(f"Missing required config fields: {self.required_config}")

        # Validate prompts list
        prompts = config.get('prompts', [])
        if not isinstance(prompts, list) or not prompts:
            raise ValueError("prompts must be a non-empty list")

        # Validate each prompt configuration
        for idx, prompt_config in enumerate(prompts):
            if not isinstance(prompt_config, dict):
                raise ValueError(f"Prompt config at index {idx} must be a dictionary")

            if 'prompt' not in prompt_config:
                raise ValueError(f"Missing 'prompt' in prompt config at index {idx}")

            if 'generator_id' not in prompt_config:
                raise ValueError(f"Missing 'generator_id' in prompt config at index {idx}")

            if not isinstance(prompt_config['prompt'], str):
                raise ValueError(f"Prompt at index {idx} must be a string")

            if not isinstance(prompt_config['generator_id'], str):
                raise ValueError(f"Artifact type at index {idx} must be a string")

        return True

    def generate(self, item: Dict[str, Any], config: Dict[str, Any],
                existing: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate summaries based on multiple prompts and configurations.

        Args:
            item: Dictionary containing content to summarize
            config: Dictionary containing summarization parameters including multiple prompts
            existing: List of existing artifacts

        Returns:
            List of generated summary artifacts
        """

        title = item['title']

        # => First get the transcript
        transcript = None
        for artifact in existing:
            if ((artifact['generator_name'] == "transcription") and
                (artifact['generator_id'] in ['en', 'en-GB'])):
                transcript = artifact['content']
        if transcript is None:
            logger.info(f"No transcript was found: ('transcript', 'en')")
            return []


        prefix = """\
You are an expert analyst of complex topics. The following is a set
of different outputs that you must generate based on the instructions

"""
        output = {}

        # => Now generate the configurations
        # Process each prompt configuration
        for prompt_config in config['prompts']:
            generator_id = prompt_config['generator_id']

            # Check if this type of summary already exists
            if self._artifact_exists(existing, generator_id):
                logger.info(f"Summary of type '{generator_id}' already exists")
                continue

            prompt = prompt_config['prompt']
            generator_id    = prompt_config['generator_id']
            prefix += f"{generator_id}\n-----\n{prompt}\n\n"
            output[generator_id] = "summary as instructed above"

        if len(output) == 0:
            logger.info(f"Nothing to do")
            return []

        complete_prompt = prefix
        complete_prompt += "Deliver the output in JSON format: \n" + json.dumps(output, indent=4)
        complete_prompt += "\n\nHere is the content to summarize:\n------\n"

        complete_prompt += transcript

        new_artifacts = []

        try:
            summarydict = self._generate_summary(
                prompt=complete_prompt,
                output=output
            )

            for generator_id, summary in summarydict.items():
                artifact_type = "SUMMARY"
                metadata = {
                    'source_length': len(transcript),
                    'summary_length': len(summary),
                    'compression_ratio': len(summary) / len(transcript),
                    'generated_at': datetime.utcnow().isoformat(),
                    'prompt_used': prompt_config['prompt']
                }

                artifact = {
                    'generator_name': self.name,
                    'generator_id': generator_id,
                    'title': f"{artifact_type}: {title}",
                    'content': summary,
                    'format': 'text',
                    'language': "",
                    'generator_name': self.name,
                    'artifact_type': artifact_type,
                    'analysis_metadata': metadata
                }
                new_artifacts.append(artifact)
                logger.info(f"Generated {artifact_type} for '{title}' with {metadata['compression_ratio']:.2f} compression ratio")

        except Exception as e:
            traceback.print_exc()
            logger.error(f"Failed to generate {artifact_type} for '{title}': {str(e)}")

        return new_artifacts

    def _artifact_exists(self, existing: List[Dict[str, Any]],
                         generator_id: str) -> bool:
        """Check if an artifact of given type and language already exists"""
        found = any(
            artifact.get('generator_name') == self.name and
            artifact.get('generator_id') == generator_id
            for artifact in existing
        )
        return found

    def _generate_summary(self, prompt: str, output: Dict[str, Any]) -> str:
        """
        Generate a summary using the specified prompt.

        Note: This is a placeholder implementation. In practice, this would integrate
        with an LLM API or other summarization service.
        """

        for generator_id in list(output.keys()):
            output[generator_id] = f"Summary generated using prompt: lakalakalaka"
        return output
