import os
import json
import logging
import psycopg
from dotenv import load_dotenv

# Configure a dedicated logger for generation
logger = logging.getLogger("generate_service")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Load environment variables
load_dotenv()

# Database configuration (must match server.py)
DB_CONFIG = {
    'dbname':   os.getenv('DB_NAME'),
    'user':     os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host':     os.getenv('DB_HOST', 'localhost'),
    'port':     os.getenv('DB_PORT', '5432'),
}
META_TABLE = 'metadata_table'

def handle_generate_request(params: list) -> dict:
    """
    Fetch metadata, build & log the prompt, then call the LLM.
    Returns {'sql': ...} on success or {'error': ...}.
    """
    # 1) Validate params
    if not isinstance(params, list):
        return {'error': "params must be a JSON array of {metric,description} objects"}

    # 2) Fetch latest metadata
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT metadata FROM {META_TABLE} ORDER BY data_id DESC LIMIT 1"
                )
                row = cur.fetchone()
        if not row:
            return {'error': "No metadata found in metadata_table"}
        metadata = row[0]
        logger.info("Fetched metadata with %d keys", len(metadata))
    except Exception as e:
        logger.exception("DB error fetching metadata")
        return {'error': f"Database error: {e}"}

    # 3) Build & log the prompt
    try:
        from vertex_client import build_sql_prompt
        prompt = build_sql_prompt(metadata, params)
        logger.info("=== Prompt to LLM START ===\n%s\n=== Prompt to LLM END ===", prompt)
    except Exception as e:
        logger.exception("Error building prompt")
        return {'error': f"Prompt build error: {e}"}

    # 4) Initialize Vertex AI
    try:
        from vertex_client import init_vertex
        project = os.getenv('GCP_PROJECT')
        init_vertex(project)
    except Exception as e:
        logger.exception("Vertex AI initialization failed")
        return {'error': f"Vertex AI init error: {e}"}

    # 5) Call the LLM
    try:
        from vertex_client import generate_sql
        sql = generate_sql(metadata, params)
        logger.info("LLM generated SQL (%d chars)", len(sql))
        return {'sql': sql}
    except Exception as e:
        logger.exception("LLM call failed")
        return {'error': f"LLM error: {e}"}
