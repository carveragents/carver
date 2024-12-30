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
    description = "Extracts and processes transcriptions from audio/video content"
    supported_platforms = ['YOUTUBE', 'PODCAST']
    supported_source_types = ['FEED', 'PLAYLIST', 'CHANNEL', "SEARCH"]
    required_config = ['languages']

    def get_transcripts(self, youtube_id, languages=['en', 'en-GB']) -> Dict[str, Any]:
        """
        Generate or process transcription from content
        """
        print(f"[{self.name} {youtube_id} getting transcripts for", youtube_id, languages)
        transcript_list = YouTubeTranscriptApi.list_transcripts(youtube_id)
        available_languages = list(set([transcript.language_code for transcript in transcript_list]))
        print(f"[{self.name} {youtube_id}] Available", available_languages)

        transcripts = []
        seen = []
        for transcript in transcript_list:
            lang = transcript.language_code
            if lang in seen:
                continue
            if lang not in languages:
                print(f"[{self.name} {youtube_id}] Skipping", lang, "not in ", languages)
                continue
            seen.append(lang)
            textlist = transcript.fetch()
            duration = sum([line['duration'] for line in textlist])
            text = ' '.join([line['text'] for line in textlist])
            print(f"[{self.name} {youtube_id}] Found", youtube_id, lang, text[:10])
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

    def get_ids(self, config: Dict[str, Any]):
        languages = config.get('languages', ['en', 'en-GB'])
        return languages

    def generate(self, post: Dict[str, Any], config: Dict[str, Any], existing: List[Dict[str, Any]]) -> Dict[str, Any]:

        print(f"[{self.name}] Generating for", post['content_identifier'])
        videoid = post['content_identifier']
        languages = config.get('languages', ['en', 'en-GB'])

        # Gather existing languages
        existing_languages = []
        for e in existing:
            lang = e.get('generator_id')
            if ((e.get('generator_name', None) == self.name) and
                (lang is not None)):
                existing_languages.append(lang)

        missing_languages = [l for l in languages if l not in existing_languages]

        print(f"[{self.name} {videoid}] languages existing", existing_languages, " missing", missing_languages)
        if len(missing_languages) == 0:
            print(f"[{self.name}] Nothing to do")
            return []

        # print("Getting transcripts from youtube")
        artifacts = self.get_transcripts(videoid, languages=missing_languages)
        for artifact in artifacts:
            artifact.update({
                "artifact_type": "TRANSCRIPTION",
                'title': f"Transcription: {post.get('title', 'Untitled')}",
            })

        print(f"[{self.name}] Returning", len(artifacts), "artifacts")
        # print(json.dumps(artifacts, indent=4))

        return artifacts

