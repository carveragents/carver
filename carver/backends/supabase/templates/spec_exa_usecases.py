summary_prompt = """
Give me a detailed list of tasks, which could be thought of as usecases, identified in the document. Give upto 5 tasks. If there is no usecase, return empty list ([])

Return the output in JSON format given below

{{
    "name": "Name of the task",
    "description": "Description of the task",
    "details": "Details of the task such as coverage, frequency etc",
    "frequency": "Is the task one-time, weekly, monthly, or yearly",
    "risk": "Is there a cost mentioned for not completing the task",
    "persona": "What is the role within the organization that executes the task",
    "current_approach": "How is it being solved today",
    "proposed_solution": "Any proposed solution"
}}

"""
def get_config(raw: bool = False, show: bool = False):
    return {
        "name": "Exa-Usecases",
        "description": "Exa Search for Usecases",
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
                "name": "Usecase",
                "description": "Usecase identification",
                "config": {
                    "dependencies": [1001],
                    "prompts": [
                        {
                            "prompt": summary_prompt,
                            "generator_id": "en-usecase"
                        }
                    ],
                    "generator": "summary"
                }
            }
        ]
    }

