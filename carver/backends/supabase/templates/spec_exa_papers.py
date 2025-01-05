summary_prompt = """

Please analyze this technical paper and return a
structured JSON response with the following schema:

{
  // Basic Paper Information
  'title': 'string',
  'authors': ['string'],
  'affiliations': ['string'],
  'year': 'number',
  'keywords': ['string'],
  'citation_count': 'number | null',
  'cited_by_papers': ['string'],

  'summary': ['string'],

  // Core Research Elements
  'research_questions': ['string'],
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
  'impact': ['string'],

  // Reinforcement Learning Specific Details
  'rl_specific': {
    'training_environment': 'string | null',
    'reward_structure': 'string | null',
    'action_space': 'string | null',
    'state_space': 'string | null',
    'baseline_comparisons': ['string'],
    'sample_efficiency': 'string | null'
  },

  // Alignment and Safety Information
  'alignment_specific': {
    'alignment_objective': 'string | null',
    'safety_considerations': ['string'],
    'human_feedback_incorporation': 'string | null',
    'reward_modeling': 'string | null',
    'value_learning': 'string | null',
    'robustness_measures': ['string'],
    'evaluation_metrics': ['string']
  }
}

Note:
- The paper content may be truncated. Thats fine. Stick the given content only
- Use null for fields where information is not available or not applicable
- All string arrays should be non-null but may be empty []
- Populate RL and alignment sections only if relevant to the paper
- Extract all available information that fits this schema
- for summary, innovations, impact, future_work see if you can extract upto 5 points
"""


def get_config(raw: bool = False, show: bool = False):
    return {
        "name": "Exa-Papers",
        "description": "ArXiv/other paper summaries",
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
                "name": "Paper Summary (Academic)",
                "description": "Summary to identify interesting new research content",
                "config": {
                    "dependencies": [1001],
                    "prompts": [
                        {
                            "prompt": summary_prompt,
                            "generator_id": "en-academic-summary"
                        }
                    ],
                    "generator": "summary"
                }
            }
        ]
    }

