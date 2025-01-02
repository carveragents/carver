def get_config(raw: bool = False, show: bool = False):

    if raw:
        area = "large language models"
        topics = "Chain of thought, tree of thoughts, forest of thoughts, test time compute, test time inference,self reflection, zero shot vs few shot, let's think step by step, let's think in detail"
        num_results = 20
        _id = "LLM1"
        days = 3

        if show:
            print("ID:", _id)
            print("Area:", area)
            print("Topics:", topics)
            print("Num Results:", num_results)
            print("Days:", days)
    else:
        _id = input("Give an ID:")
        area = input("Area: ")
        topics = input("Topics separated by comma: ")
        num_results = input("Num results: ")
        num_results = int(num_results)
        days = input("Window (days): ")
        days = int(days)

    template = f"""I am looking for papers related to {area} discussing the following topics:"""

    topics = [t.strip() for t in topics.split(",")]
    query = template + ', '.join([f'"{topic}"' for topic in topics])

    return {
        "name": "Exa-Papers",
        "description": "Search Exa for papers",
        "platforms": ["Exa", "EXA"],
        "specifications": [
            {
                "id": 4001,
                "name": "Paper Search",
                "description": f"Searching for {area}",
                "platform": "EXA",
                "source_type": "search",
                "source_identifier": _id,
                "url": "https://exa.ai",
                "config": {
                    "query": query,
                    "type": "neural",
                    "num_results": num_results,
                    "category": "research paper",
                    "date_filter": f"{days}d"
                }
            }
        ]
    }

