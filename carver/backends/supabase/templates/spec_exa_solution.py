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

Return the output in format given below

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

cot_prompt = f"""
Given the following business case study, generate a chain of thought analysis that includes:
1. A specific business question or scenario
2. Step-by-step reasoning process
3. A clear conclusion or recommendation

Please format your response as JSON with the following structure:
{{
    "question": "What is the specific business question/scenario?",
    "reasoning": [
        "Step 1: Analysis of...",
        "Step 2: Consideration of...",
        "Step 3: Evaluation of...",
        "Step 4: Assessment of..."
    ],
    "conclusion": "Based on the analysis..."
}}

Generate atmost 3 different chain of thought examples focusing on
different aspects of the case study.  Each example should demonstrate
clear logical progression from the initial question through reasoning
to the final conclusion.

"""

def get_config(raw: bool = False, show: bool = False):
    return {
        "name": "Exa-Solutions",
        "description": "Exa Search for Technical Solutions",
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
            },
            {
                "id": 1003,
                "name": "ChainOfThought",
                "description": "CoT Dataset",
                "config": {
                    "dependencies": [1002],
                    "prompts": [
                        {
                            "prompt": cot_prompt,
                            "generator_id": "en-cot"
                        }
                    ],
                    "generator": "summary"
                }
            }
        ]
    }

