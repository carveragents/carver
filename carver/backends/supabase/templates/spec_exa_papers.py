summary_prompt = """
Give me an exhaustive granular summary of:

(a) Pain points identified
(b) Key insights about the pain points
(c) Challenges encountered in developing a solution. What was difficult?
(d) Solution Summary. Identify architectural and technical choices, process.
(e) Outcomes delivered and impact. Be specific.
(f) Future directions and next steps
(g) Interesting projects and people

Give upto 5 points for each.

Return the output in JSON format given below

{{
    "Pain Points":: ["Point1", "Point2", ...],
    "Key Insights": ["Point1", "Point2", ...],
    "Challenges": ["Point1", "Point2", ...],
    "Solution Summary": ["Point1", "Point2", ...],
    "Outcomes": ["Point1", "Point2", ...],
    "Future Directions": ["Point1", "Point2", ...],
    "Interesting": ["Point1", "Point2", ...],
}}




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
                "name": "Product Intel",
                "description": "Product intelligence from content",
                "config": {
                    "dependencies": [1001],
                    "prompts": [
                        {
                            "prompt": summary_prompt,
                            "generator_id": "en-product"
                        }
                    ],
                    "generator": "summary"
                }
            }
        ]
    }

