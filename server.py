from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import psycopg
import os
from dotenv import load_dotenv
import uuid
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
}

# Validate DB config
for key, value in DB_CONFIG.items():
    if not value:
        raise ValueError(f"Missing required environment variable for DB config: {key}")

# Db table names
DATA_TABLE = 'data_table'
META_TABLE = 'metadata_table'

# create tables if they don't exist
CREATE_DATA_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {DATA_TABLE} (
    id UUID PRIMARY KEY,
    data JSONB NOT NULL
);
"""

CREATE_METADATA_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {META_TABLE} (
    id UUID PRIMARY KEY,
    data_id UUID REFERENCES {DATA_TABLE}(id),
    metadata JSONB NOT NULL
);
"""

def init_db():
    """Create tables if they don’t already exist."""
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_DATA_TABLE_SQL)
                cur.execute(CREATE_METADATA_TABLE_SQL)
        logging.info("Database initialized successfully.")
    except Exception as e:
        logging.error("Database initialization failed: %s", e)
        raise

def insert_data_and_metadata(data: dict):
    """
    Inserts the raw JSON into data_table and
    extracts key→type metadata into metadata_table.
    """
    data_id = str(uuid.uuid4())
    metadata_id = str(uuid.uuid4())
    metadata = {k: type(v).__name__ for k, v in data.items()}

    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {DATA_TABLE} (id, data) VALUES (%s, %s)",
                    (data_id, json.dumps(data))
                )
                cur.execute(
                    f"INSERT INTO {META_TABLE} (id, data_id, metadata) VALUES (%s, %s, %s)",
                    (metadata_id, data_id, json.dumps(metadata))
                )
        logging.info(f"Inserted data and metadata with IDs: {data_id}, {metadata_id}")
    except Exception as e:
        logging.error("Data insertion failed: %s", e)
        raise

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def _set_headers(self, status=200):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)
        try:
            payload = json.loads(body.decode('utf-8'))
            insert_data_and_metadata(payload)
            self._set_headers(200)
            self.wfile.write(json.dumps({'status': 'success'}).encode('utf-8'))
        except json.JSONDecodeError:
            self._set_headers(400)
            self.wfile.write(json.dumps({'status': 'error', 'message': 'Invalid JSON'}).encode('utf-8'))
        except Exception as e:
            logging.exception("Unhandled error during request processing")
            self._set_headers(500)
            self.wfile.write(json.dumps({'status': 'error', 'message': str(e)}).encode('utf-8'))

def run(server_class=HTTPServer, handler_class=SimpleHTTPRequestHandler, port=8000):
    try:
        init_db()
        server_address = ('', port)
        httpd = server_class(server_address, handler_class)
        logging.info(f"Starting HTTP server on port {port}...")
        httpd.serve_forever()
    except Exception as e:
        logging.critical("Failed to start server: %s", e)

if __name__ == '__main__':
    run()
