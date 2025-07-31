import os
import json
import logging
import vertexai
from vertexai.language_models import ChatModel
import psycopg

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def init_vertex(project_id: str, region: str = 'us-central1'):
    """
    Initialize the Vertex AI SDK for your GCP project and region.
    """
    vertexai.init(project=project_id, location=region)
    logging.info(f"Vertex AI initialized for project {project_id} in {region}")

def get_llm(model_name: str = 'gemini-2.0-flash') -> ChatModel:
    """
    Retrieve a pretrained chat model from Vertex AI.
    """
    logging.info(f"Loading LLM model: {model_name}")
    return ChatModel.from_pretrained(model_name)

# def build_sql_prompt(metadata: dict, params: list, examples: list = None) -> str:
#     """
#     Construct a clear, structured prompt for the LLM, including:
#       • metrics schema (flattened keys + types)
#       • Desired metrics list with descriptions
#       • Optional few-shot examples
#     """
#     schema_section  = json.dumps(metadata, indent=2)
#     metrics_section = '\n'.join(f"- {p['metric']}: {p['description']}" for p in params)

#     prompt_lines = [
#         "You are an expert SQL generation assistant.",
#         "Given the following metrics schema (flattened keys + types):",
#         schema_section,
#         "And the following metrics to retrieve:",
#         metrics_section,
#     ]
#     if examples:
#         prompt_lines += [
#             "Here are example inputs and expected SQL:",
#             json.dumps(examples, indent=2)
#         ]
#     prompt_lines += [
#         "Generate a single optimized SQL SELECT statement or series of statements to fetch these metrics using only the available fields.",
#         "If a metric cannot be directly retrieved, include a comment explaining why or suggest a fallback."
#     ]

#     prompt = '\n\n'.join(prompt_lines)
#     logging.debug(f"Built prompt:\n{prompt}")
#     return prompt

def get_table_schema_text(table_name: str) -> str:
    """Fetch and format the actual table schema from PostgreSQL."""
    try:
        conn = psycopg.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", "5432")
        )
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s;
            """, (table_name,))
            rows = cursor.fetchall()
        conn.close()
    except Exception as e:
        return f"# Error fetching schema for {table_name}: {str(e)}"

    if not rows:
        return f"# No schema info found for table '{table_name}'."

    lines = [f"# Schema for table '{table_name}':"]
    for col, dtype, nullable, default in rows:
        null_flag = "NULLABLE" if nullable == "YES" else "REQUIRED"
        default_str = f"DEFAULT {default}" if default else ""
        lines.append(f"- {col}: {dtype.upper()} ({null_flag}) {default_str}".strip())
    return "\n".join(lines)

def build_sql_prompt(metadata: dict, params: list, examples: list = None) -> str:
    """
    Construct a clear, structured prompt for the LLM, including:
      • Flattened schema (from metadata)
      • Real table schema (from Postgres)
      • Desired metrics list with descriptions
      • Optional few-shot examples
    """
    schema_section  = json.dumps(metadata, indent=2)
    metrics_section = '\n'.join(f"- {p['metric']}: {p['description']}" for p in params)

    try:
        table_schema_text = get_table_schema_text("data_table")
    except Exception as e:
        table_schema_text = f"# (Schema unavailable due to error: {e})"

    prompt_lines = [
        "You are an expert SQL generation assistant.",
        "Given the following *flattened metrics schema* (keys and types):",
        schema_section,
        "\nThe actual table structure for context is:",
        table_schema_text,
        "\nAnd the following metrics to retrieve:",
        metrics_section,
    ]

    if examples:
        prompt_lines += [
            "\nHere are example inputs and expected SQL outputs:",
            json.dumps(examples, indent=2)
        ]

    prompt_lines += [
        "\nGenerate a single optimized SQL SELECT statement or series of statements to fetch these metrics using only the available fields.",
        "If a metric cannot be directly retrieved, include a comment explaining why or suggest a fallback."
    ]

    prompt = '\n\n'.join(prompt_lines)
    logging.debug(f"Built prompt:\n{prompt}")
    return prompt

def generate_sql(metadata: dict, params: list, examples: list = None) -> str:
    """
    Send the prompt to the LLM and return the generated SQL.
    """
    # llm   = get_llm()
    # chat  = llm.start_chat()
    prompt = build_sql_prompt(metadata, params, examples)

    logging.info("Sending prompt to LLM...")
    # response = chat.send_message(prompt)
    # sql = response.text.strip()
    logging.info("Received SQL from LLM.")
    return prompt
