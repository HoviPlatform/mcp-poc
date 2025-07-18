import json
import os
import logging
import vertexai
from vertexai.language_models import ChatModel

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# initialize Vertex AI with your GCP project and region
def init_vertex(project_id: str, region: str = 'us-central1'):
    vertexai.init(project=project_id, location=region)
def get_llm(model_name: str = 'gemini-pro') -> ChatModel:
    return ChatModel.from_pretrained(model_name)

# build prompt for SQL generation
def build_sql_prompt(metadata: dict, params: list, examples: list = None) -> str:
    """
    Constructs a clear, structured prompt for the LLM, including:
    - Schema metadata (flattened keys + types)
    - Desired metrics with descriptions
    - Optional few-shot examples
    """
    schema_section = json.dumps(metadata, indent=2)
    metrics_section = '\n'.join([f"- {p['metric']}: {p['description']}" for p in params])

    prompt_lines = [
        "You are an expert SQL generation assistant.",
        "Given the following database schema (flattened keys with types):",
        schema_section,
        "And the following metrics to retrieve:",
        metrics_section,
    ]
    if examples:
        prompt_lines.extend([
            "Here are example inputs and expected SQL:",
            json.dumps(examples, indent=2)
        ])
    prompt_lines.extend([
        "Generate a single optimized SQL SELECT statement or series of statements that fetch these metrics using only available fields.",
        "If a metric cannot be directly retrieved, include a comment explaining why or suggest a fallback."
    ])
    return '\n\n'.join(prompt_lines)

# Send prompt to the LLM and return the response
def generate_sql(metadata: dict, params: list, examples: list = None) -> str:
    llm = get_llm()
    chat = llm.start_chat()
    prompt = build_sql_prompt(metadata, params, examples)
    logging.info("Prompt sent to LLM:\n%s", prompt)
    response = chat.send_message(prompt)
    logging.info("LLM response received")
    return response.text

# Example usage
if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    project = os.getenv('GCP_PROJECT')
    init_vertex(project)

    sample_metadata = {
        "data._airbyte_data.metrics.impressions": "integer",
        "data._airbyte_data.metrics.spend": "float",
        "data._airbyte_data.dimensions.stat_time_day": "string"
    }
    sample_params = [
        {"metric": "total_impressions", "description": "Total number of impressions per day"},
        {"metric": "total_spend",      "description": "Total spend per day"}
    ]
    sample_examples = [
        {
            "metadata": {"user_id": "integer", "clicks": "integer"},
            "params": [{"metric": "total_clicks", "description": "Sum of clicks"}],
            "sql": "SELECT SUM(clicks) AS total_clicks FROM data_table;"
        }
    ]
    result = generate_sql(sample_metadata, sample_params, sample_examples)
    print(result)
