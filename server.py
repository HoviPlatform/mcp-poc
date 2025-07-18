from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import psycopg
import os
from dotenv import load_dotenv
import uuid
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
}
# Validate DB config
for key, val in DB_CONFIG.items():
    if not val:
        raise ValueError(f"Missing DB config: {key}")

# Tables
DATA_TABLE = 'data_table'
META_TABLE = 'metadata_table'

# SQL DDL
CREATE_DATA_TABLE = f"""
CREATE TABLE IF NOT EXISTS {DATA_TABLE} (
    id UUID PRIMARY KEY,
    data JSONB NOT NULL
);
"""
CREATE_META_TABLE = f"""
CREATE TABLE IF NOT EXISTS {META_TABLE} (
    id UUID PRIMARY KEY,
    data_id UUID REFERENCES {DATA_TABLE}(id),
    metadata JSONB NOT NULL
);
"""

# JSON flatten + metadata helpers
def flatten_json(obj, prefix=''):
    """Recursively flatten JSON with dot + index notation."""
    items = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else k
            items.update(flatten_json(v, key))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            key = f"{prefix}[{i}]"
            items.update(flatten_json(v, key))
    else:
        items[prefix] = obj
    return items

def infer_type(v):
    if isinstance(v, bool): return 'boolean'
    if isinstance(v, int): return 'integer'
    if isinstance(v, float): return 'float'
    if isinstance(v, list): return 'array'
    if isinstance(v, dict): return 'object'
    return 'string'

def extract_metadata(data):
    flat = flatten_json(data)
    return {k: infer_type(v) for k, v in flat.items()}

# Database operations
def init_db():
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_DATA_TABLE)
                cur.execute(CREATE_META_TABLE)
        logging.info("Database initialized.")
    except Exception as e:
        logging.critical("DB init error: %s", e)
        raise

def save_data_and_meta(payload):
    data_id = str(uuid.uuid4())
    meta_id = str(uuid.uuid4())
    metadata = extract_metadata(payload)
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Save raw JSON
                cur.execute(
                    f"INSERT INTO {DATA_TABLE} (id, data) VALUES (%s, %s)",
                    (data_id, json.dumps(payload))
                )
                # Save extracted metadata
                cur.execute(
                    f"INSERT INTO {META_TABLE} (id, data_id, metadata) VALUES (%s, %s, %s)",
                    (meta_id, data_id, json.dumps(metadata))
                )
        logging.info(f"Stored data {data_id} and metadata {meta_id}.")
    except Exception as e:
        logging.error("Insert failed: %s", e)
        raise

# HTTP handler
class Handler(BaseHTTPRequestHandler):
    def _set_headers(self, code=200):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw)
            save_data_and_meta(payload)
            self._set_headers(200)
            self.wfile.write(json.dumps({'status': 'success'}).encode())
        except json.JSONDecodeError:
            self._set_headers(400)
            self.wfile.write(json.dumps({'error': 'Invalid JSON'}).encode())
        except Exception as e:
            logging.exception("Request error")
            self._set_headers(500)
            self.wfile.write(json.dumps({'error': str(e)}).encode())

# Run server
def run(port=8000):
    init_db()
    server = HTTPServer(('', port), Handler)
    logging.info(f"Server listening on {port}")
    server.serve_forever()

if __name__ == '__main__':
    run()
