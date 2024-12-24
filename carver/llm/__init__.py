import os
import sys
import json

from typing import List

import openai

from carver.utils import get_config
__all__ = [
    'run_llm_summarize',
    'run_openai_summarize',
    'get_embedding'
]

def run_openai_summarize(system_prompt, user_prompt):

    config = get_config()
    api_key = config('OPENAI_API_KEY')

    client = openai.OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        response_format={"type": "json_object"}
    )

    summary = json.loads(response.choices[0].message.content)
    return summary

def run_llm_summarize(system_prompt, user_prompt):
    return run_openai_summarize(system_prompt, user_prompt)

def get_embedding(text: str, model: str = "text-embedding-3-small") -> List[float]:
    """Get embedding vector from OpenAI API"""
    config = get_config()
    api_key = config('OPENAI_API_KEY')

    client = openai.OpenAI(api_key=api_key)
    response = client.embeddings.create(
        model=model,
        input=text,
        encoding_format="float"
    )

    return response.data[0].embedding
