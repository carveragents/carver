import os
import sys
import json

from typing import List, Dict, Any, Optional, Type
from datetime import datetime
from abc import ABC, abstractmethod

from youtube_transcript_api import YouTubeTranscriptApi

from .base import BaseArtifactGenerator

class TranscriptionGenerator(BaseArtifactGenerator):
    """Generates transcription artifacts from audio/video"""
    name = "transcription"

    def validate_config(self, source: Dict[str, Any],
                        config: Dict[str, Any]) -> bool:

        if source['platform'] not in ['YOUTUBE', 'PODCAST']:
            raise ValueError(f"Unsupported platform for this generator: {source['platform']}")

        if source['source_type'] not in ['FEED', 'PLAYLIST', 'CHANNEL']:
            raise ValueError(f"Unsupported platform for this generator: {source['source_type']}")


        required = {'languages'}
        return all(k in config for k in required)

    def get_transcripts(self, youtube_id, languages=['en', 'en-GB']) -> Dict[str, Any]:
        """
        Generate or process transcription from content
        """
        print("getting transcripts for", youtube_id, languages)
        transcript_list = YouTubeTranscriptApi.list_transcripts(youtube_id)
        print("Available", [transcript.language_code for transcript in transcript_list])

        transcripts = []
        for transcript in transcript_list:
            lang = transcript.language_code
            if lang not in languages:
                print("Skipping", lang, "not in ", languages)
                continue
            textlist = transcript.fetch()
            duration = sum([line['duration'] for line in textlist])
            text = ' '.join([line['text'] for line in textlist])
            print("Found", youtube_id, lang, text[:10])
            transcripts.append({
                "generator_name": "transcription",
                "generator_id": lang,
                "format": "text",
                "language": lang,
                "content": text,
                "analysis_metadata": {
                    "duration": int(duration),
                    "is_generated": transcript.is_generated,
                    "is_translatable": transcript.is_translatable,
                    "word_count": len(text.split())
                }
            })

        return transcripts

    def generate(self, item: Dict[str, Any], config: Dict[str, Any], existing: List[Dict[str, Any]]) -> Dict[str, Any]:

        print("generate() - Generating for", item['content_identifier'])
        videoid = item['content_identifier']
        languages = config.get('languages', ['en', 'en-GB'])

        # Gather existing languages
        existing_languages = []
        for e in existing:
            lang = e.get('generator_id')
            if lang is not None:
                existing_languages.append(lang)

        missing_languages = [l for l in languages if l not in existing_languages]
        print("generate() existing languages", existing_languages)
        print("generate() missing languages", missing_languages)
        if len(missing_languages) == 0:
            # Nothing to do...
            return []

        artifacts = self.get_transcripts(videoid, languages=missing_languages)
        for artifact in artifacts:
            artifact.update({
                "artifact_type": "TRANSCRIPTION",
                'title': f"Transcription: {item.get('title', 'Untitled')}",
            })

        print("generate() Returning", len(artifacts), "artifacts")
        # print(json.dumps(artifacts, indent=4))

        return artifacts

