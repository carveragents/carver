import os
import sys
import json

import openai

from carver.utils import get_config
__all__ = [
    'run_llm_summarize',
    'run_openai_summarize'
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
