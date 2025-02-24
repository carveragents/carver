summary_prompt = """

Please analyze this technical post and return a structured JSON
response with the following schema:

{
  // Basic Paper Information
  'title': 'string',
  'authors': ['string'],
  'affiliations': ['string'],
  'year': 'number',
  'keywords': ['string'],

  'summary': ['string'],

  // Core Research Elements
  'key_questions': ['string'],
  'methodology': {
    'approach': ['string'],
    'datasets': ['string'],
    'technical_specs': ['string']
  },

  // Results and Impact
  'findings': {
    'main_results': ['string'],
    'quantitative_metrics': ['string']
  },
  'innovations': ['string'],
  'limitations': ['string'],
  'future_work': ['string'],
  'impact': ['string']

}

Note:
- The article content may be truncated. Thats fine. Stick the given content only
- Use null for fields where information is not available or not applicable
- All string arrays should be non-null but may be empty []
- Extract all available information that fits this schema
- for summary, innovations, impact, future_work see if you can extract upto 5 points
"""


def get_config(raw: bool = False, show: bool = False):
    return {
        "name": "Exa-Blogs",
        "description": "Article summaries",
        "platforms": ['Exa', "EXA"],
        "source_type": ["SEARCH"],
        "specifications": [
            {
                "id": 1001,
                "name": "Transcription",
                "description": "Transcripts",
                "config": {
                    "generator": "exa_content",
                    "languages": [
                        "en"
                    ],
                    "dependencies": []
                }
            },
            {
                "id": 1002,
                "name": "Article Summary (Business)",
                "description": "Summary to identify interesting new ideas",
                "config": {
                    "dependencies": [1001],
                    "prompts": [
                        {
                            "prompt": summary_prompt,
                            "generator_id": "en-article-summary"
                        }
                    ],
                    "generator": "summary"
                }
            }
        ]
    }

