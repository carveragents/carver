def get_config(raw: bool = False, show: bool = False):

    if raw:
        area = "large language models"
        topics = "domain-specific knowledgebase, domain-specific agents"
        num_results = 20
        _id = "LLM2"
        days = 3
        template_default = f"""I am looking for technical articles, papers, and posts related to {area} discussing the following topics:"""
        template = template_default

        if show:
            print("ID:", _id)
            print("Area:", area)
            print("Topics:", template, topics)
            print("Num Results:", num_results)
            print("Days:", days)
    else:
        _id = input("Give an ID:")
        area = input("Area: ")
        template_default = f"""I am looking for technical articles, papers, and posts related to {area} discussing the following topics:"""
        template = input(f"Template: {template_default}")
        if template.strip() == "":
            template = template_default
        topics = input("Topics separated by comma: ")
        num_results = input("Num results: ")
        num_results = int(num_results)
        days = input("Window (days): ")
        days = int(days)

    topics = [t.strip() for t in topics.split(",")]
    query = template + ', '.join([f'"{topic}"' for topic in topics])

    return {
        "name": "Exa-Domain-Agents",
        "description": "Search Exa for domain-specific agents",
        "platforms": ["Exa", "EXA"],
        "specifications": [
            {
                "id": 4001,
                "name": "Article Search",
                "description": f"Searching for {area}",
                "platform": "EXA",
                "source_type": "search",
                "source_identifier": _id,
                "url": "https://exa.ai",
                "config": {
                    "query": query,
                    "type": "neural",
                    "num_results": num_results,
                    "category": "blogs",
                    "date_filter": f"{days}d"
                }
            }
        ]
    }

