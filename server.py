from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import psycopg
from psycopg.types.json import Json
import os
from dotenv import load_dotenv
import uuid
import logging
from urllib.parse import urlparse, parse_qs
from generate_service import handle_generate_request
import requests
import sqlparse
import regex as re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'dbname':   os.getenv('DB_NAME'),
    'user':     os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host':     os.getenv('DB_HOST', 'localhost'),
    'port':     os.getenv('DB_PORT', '5432'),
}
for k, v in DB_CONFIG.items():
    if not v:
        raise ValueError(f"Missing DB config: {k}")

DATA_TABLE = 'data_table'
META_TABLE = 'metadata_table'
ADVERTISERS_TABLE = 'advertisers'
CAMPAIGNS_TABLE = 'campaigns'
AD_GROUPS_TABLE = 'ad_groups'
ADS_TABLE = 'ads'
CLAUDE_PROMPT_LOGS = 'claude_prompt_logs'
# DDL for tables
CREATE_DATA_TABLE = f"""
CREATE TABLE IF NOT EXISTS {DATA_TABLE} (
    id UUID PRIMARY KEY,
    metrics JSONB NOT NULL,
    raw_data JSONB NOT NULL
);
"""
CREATE_META_TABLE = f"""
CREATE TABLE IF NOT EXISTS {META_TABLE} (
    id UUID PRIMARY KEY,
    data_id UUID REFERENCES {DATA_TABLE}(id),
    metadata JSONB NOT NULL
);
"""
CREATE_ADVERTISERS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {ADVERTISERS_TABLE} (
  language TEXT,
  license_no TEXT,
  industry TEXT,
  display_timezone TEXT,
  balance NUMERIC,
  contacter TEXT,
  timezone TEXT,
  name TEXT,
  role TEXT,
  currency TEXT,
  advertiser_id BIGINT PRIMARY KEY,
  email TEXT,
  telephone_number TEXT,
  advertiser_account_type TEXT,
  promotion_area TEXT,
  cellphone_number TEXT,
  create_time BIGINT,
  description TEXT,
  status TEXT,
  company TEXT,
  country TEXT
);
"""
CREATE_CAMPAIGNS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {CAMPAIGNS_TABLE} (
  campaign_id BIGINT PRIMARY KEY,
  modify_time TIMESTAMP,
  budget NUMERIC,
  is_search_campaign BOOLEAN,
  advertiser_id BIGINT NOT NULL,
  is_new_structure BOOLEAN,
  is_smart_performance_campaign BOOLEAN,
  budget_mode TEXT,
  app_promotion_type TEXT,
  create_time TIMESTAMP,
  campaign_name TEXT,
  campaign_type TEXT,
  roas_bid NUMERIC,
  objective TEXT,
  objective_type TEXT,
  operation_status TEXT,
  secondary_status TEXT,
  FOREIGN KEY (advertiser_id) REFERENCES {ADVERTISERS_TABLE}(advertiser_id)
);
"""
CREATE_AD_GROUP_TABLE = f"""
CREATE TABLE IF NOT EXISTS {AD_GROUPS_TABLE} (
    adgroup_id BIGINT PRIMARY KEY,
    advertiser_id BIGINT REFERENCES {ADVERTISERS_TABLE}(advertiser_id),
    campaign_id BIGINT REFERENCES {CAMPAIGNS_TABLE}(campaign_id),
    share_disabled BOOLEAN,
    creative_material_mode TEXT,
    dayparting TEXT,
    placements JSONB,
    is_hfss BOOLEAN,
    secondary_status TEXT,
    category_exclusion_ids JSONB,
    skip_learning_phase BOOLEAN,
    is_new_structure BOOLEAN,
    targeting_expansion JSONB,
    device_price_ranges JSONB,
    spending_power TEXT,
    billing_event TEXT,
    adgroup_name TEXT,
    pixel_id BIGINT,
    schedule_type TEXT,
    brand_safety_type TEXT,
    network_types JSONB,
    household_income JSONB,
    optimization_goal TEXT,
    bid_price INTEGER,
    languages JSONB,
    excluded_audience_ids JSONB,
    search_result_enabled BOOLEAN,
    age_groups JSONB,
    is_smart_performance_campaign BOOLEAN,
    deep_cpa_bid INTEGER,
    optimization_event TEXT,
    create_time TIMESTAMP,
    campaign_name TEXT,
    schedule_start_time TIMESTAMP,
    operating_systems JSONB,
    gender TEXT,
    category_id INTEGER,
    placement_type TEXT,
    promotion_type TEXT,
    inventory_filter_enabled BOOLEAN,
    interest_category_ids JSONB,
    bid_display_mode TEXT,
    ios14_quota_type TEXT,
    budget_mode TEXT,
    interest_keyword_ids JSONB,
    actions JSONB,
    operation_status TEXT,
    pacing TEXT,
    modify_time TIMESTAMP,
    budget INTEGER,
    audience_ids JSONB,
    comment_disabled BOOLEAN,
    conversion_bid_price INTEGER,
    included_custom_actions JSONB,
    location_ids JSONB,
    auto_targeting_enabled BOOLEAN,
    bid_type TEXT,
    isp_ids JSONB,
    device_model_ids JSONB,
    video_download_disabled BOOLEAN,
    contextual_tag_ids JSONB,
    schedule_end_time TIMESTAMP,
    excluded_custom_actions JSONB,
    scheduled_budget INTEGER
);
"""
CREATE_ADS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {ADS_TABLE} (
  ad_id BIGINT PRIMARY KEY,
  advertiser_id BIGINT REFERENCES {ADVERTISERS_TABLE}(advertiser_id),
  campaign_id BIGINT REFERENCES {CAMPAIGNS_TABLE}(campaign_id),
  adgroup_id BIGINT REFERENCES {AD_GROUPS_TABLE}(adgroup_id),
  ad_name TEXT,
  ad_text TEXT,
  ad_format TEXT,
  app_name TEXT,
  avatar_icon_web_uri TEXT,
  branded_content_disabled TEXT,
  call_to_action_id TEXT,
  campaign_name TEXT,
  create_time TIMESTAMP,
  dark_post_status TEXT,
  display_name TEXT,
  identity_id TEXT,
  identity_type TEXT,
  image_ids JSONB,
  landing_page_url TEXT,
  modify_time TIMESTAMP,
  operation_status TEXT,
  optimization_event TEXT,
  playable_url TEXT,
  profile_image_url TEXT,
  secondary_status TEXT,
  tiktok_item_id TEXT,
  tracking_pixel_id BIGINT,
  utm_params JSONB,
  vast_moat_enabled BOOLEAN,
  video_id TEXT,
  viewability_postbid_partner TEXT,
  brand_safety_postbid_partner TEXT,
  is_new_structure BOOLEAN,
  is_aco BOOLEAN,
  creative_authorized BOOLEAN
);
"""
CREATE_CLAUDE_PROMPT_LOGS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {CLAUDE_PROMPT_LOGS} (
    id SERIAL PRIMARY KEY,
    prompt TEXT NOT NULL,
    response TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
# Helpers for flattening and typing
def flatten_json(obj, prefix=''):
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
    if isinstance(v, bool):   return 'boolean'
    if isinstance(v, int):    return 'integer'
    if isinstance(v, float):  return 'float'
    if isinstance(v, list):   return 'array'
    if isinstance(v, dict):   return 'object'
    return 'string'

def extract_metadata(metrics):
    flat = flatten_json(metrics)
    return {k: infer_type(v) for k, v in flat.items()}

# Initialize database (create tables)
def init_db():
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_DATA_TABLE)
            cur.execute(CREATE_META_TABLE)
            cur.execute(CREATE_ADVERTISERS_TABLE)
            cur.execute(CREATE_CAMPAIGNS_TABLE)
            cur.execute(CREATE_AD_GROUP_TABLE)
            cur.execute(CREATE_ADS_TABLE)
            cur.execute(CREATE_CLAUDE_PROMPT_LOGS_TABLE)
    logging.info("Database initialized.")

# Ingest endpoint logic
def save_metrics_and_meta(payload):
    data_type = payload.get('elemtype')
    data_section = payload.get('data', {})
    ab_data      = data_section.get('_airbyte_data', {})
    if data_type == "metrics":
        metrics      = ab_data.get('metrics', {})
        if not isinstance(metrics, dict):
            raise ValueError("No metrics object found under data._airbyte_data.metrics")

        raw_attrs = {k: v for k, v in data_section.items() if k != '_airbyte_data'}

        data_id = str(uuid.uuid4())
        meta_id = str(uuid.uuid4())
        metadata = extract_metadata(metrics)

        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {DATA_TABLE} (id, metrics, raw_data) VALUES (%s, %s, %s)",
                    (data_id, json.dumps(metrics), json.dumps(raw_attrs))
                )
                cur.execute(
                    f"INSERT INTO {META_TABLE} (id, data_id, metadata) VALUES (%s, %s, %s)",
                    (meta_id, data_id, json.dumps(metadata))
                )
        logging.info(f"Stored metrics {data_id} and metadata {meta_id}.")
    elif data_type == "advertiser":
        insert_query = """
        INSERT INTO advertisers (
        language, license_no, industry, display_timezone, balance, contacter,
        timezone, name, role, currency, advertiser_id, email, telephone_number,
        advertiser_account_type, promotion_area, cellphone_number, create_time,
        description, status, company, country
        ) VALUES (
        %(language)s, %(license_no)s, %(industry)s, %(display_timezone)s, %(balance)s, %(contacter)s,
        %(timezone)s, %(name)s, %(role)s, %(currency)s, %(advertiser_id)s, %(email)s, %(telephone_number)s,
        %(advertiser_account_type)s, %(promotion_area)s, %(cellphone_number)s, %(create_time)s,
        %(description)s, %(status)s, %(company)s, %(country)s
        );
        """
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(insert_query, ab_data)
        logging.info("advertiser successfully added")
    elif data_type == "campaign": 
        insert_campaign_query = """
        INSERT INTO campaigns (
        modify_time,
        budget,
        is_search_campaign,
        advertiser_id,
        is_new_structure,
        is_smart_performance_campaign,
        budget_mode,
        app_promotion_type,
        create_time,
        campaign_name,
        campaign_type,
        roas_bid,
        objective,
        objective_type,
        campaign_id,
        operation_status,
        secondary_status
        )
        VALUES (
        %(modify_time)s,
        %(budget)s,
        %(is_search_campaign)s,
        %(advertiser_id)s,
        %(is_new_structure)s,
        %(is_smart_performance_campaign)s,
        %(budget_mode)s,
        %(app_promotion_type)s,
        %(create_time)s,
        %(campaign_name)s,
        %(campaign_type)s,
        %(roas_bid)s,
        %(objective)s,
        %(objective_type)s,
        %(campaign_id)s,
        %(operation_status)s,
        %(secondary_status)s
        )
        ON CONFLICT (campaign_id) DO NOTHING;
        """
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(insert_campaign_query, ab_data)
        logging.info("campaign successfully added")
    elif data_type == "ad_group": 
        insert_query = """
        INSERT INTO ad_groups (
        adgroup_id, advertiser_id, campaign_id, share_disabled, creative_material_mode, dayparting, placements,
        is_hfss, secondary_status, category_exclusion_ids, skip_learning_phase, is_new_structure, targeting_expansion,
        device_price_ranges, spending_power, billing_event, adgroup_name, pixel_id, schedule_type, brand_safety_type,
        network_types, household_income, optimization_goal, bid_price, languages, excluded_audience_ids,
        search_result_enabled, age_groups, is_smart_performance_campaign, deep_cpa_bid, optimization_event,
        create_time, campaign_name, schedule_start_time, operating_systems, gender, category_id, placement_type,
        promotion_type, inventory_filter_enabled, interest_category_ids, bid_display_mode, ios14_quota_type,
        budget_mode, interest_keyword_ids, actions, operation_status, pacing, modify_time, budget, audience_ids,
        comment_disabled, conversion_bid_price, included_custom_actions, location_ids, auto_targeting_enabled,
        bid_type, isp_ids, device_model_ids, video_download_disabled, contextual_tag_ids, schedule_end_time,
        excluded_custom_actions, scheduled_budget
        ) VALUES (
        %(adgroup_id)s, %(advertiser_id)s, %(campaign_id)s, %(share_disabled)s, %(creative_material_mode)s, %(dayparting)s, %(placements)s,
        %(is_hfss)s, %(secondary_status)s, %(category_exclusion_ids)s, %(skip_learning_phase)s, %(is_new_structure)s, %(targeting_expansion)s,
        %(device_price_ranges)s, %(spending_power)s, %(billing_event)s, %(adgroup_name)s, %(pixel_id)s, %(schedule_type)s, %(brand_safety_type)s,
        %(network_types)s, %(household_income)s, %(optimization_goal)s, %(bid_price)s, %(languages)s, %(excluded_audience_ids)s,
        %(search_result_enabled)s, %(age_groups)s, %(is_smart_performance_campaign)s, %(deep_cpa_bid)s, %(optimization_event)s,
        %(create_time)s, %(campaign_name)s, %(schedule_start_time)s, %(operating_systems)s, %(gender)s, %(category_id)s, %(placement_type)s,
        %(promotion_type)s, %(inventory_filter_enabled)s, %(interest_category_ids)s, %(bid_display_mode)s, %(ios14_quota_type)s,
        %(budget_mode)s, %(interest_keyword_ids)s, %(actions)s, %(operation_status)s, %(pacing)s, %(modify_time)s, %(budget)s, %(audience_ids)s,
        %(comment_disabled)s, %(conversion_bid_price)s, %(included_custom_actions)s, %(location_ids)s, %(auto_targeting_enabled)s,
        %(bid_type)s, %(isp_ids)s, %(device_model_ids)s, %(video_download_disabled)s, %(contextual_tag_ids)s, %(schedule_end_time)s,
        %(excluded_custom_actions)s, %(scheduled_budget)s
        );
        """

# Convert all required fields directly in data
        ab_data["share_disabled"] = bool(ab_data.get("share_disabled", False))
        ab_data["is_hfss"] = bool(ab_data.get("is_hfss", False))
        ab_data["skip_learning_phase"] = bool(ab_data.get("skip_learning_phase", False))
        ab_data["is_new_structure"] = bool(ab_data.get("is_new_structure", False))
        ab_data["expansion_enabled"] = bool(ab_data.get("targeting_expansion", {}).get("expansion_enabled", False))
        ab_data["search_result_enabled"] = bool(ab_data.get("search_result_enabled", False))
        ab_data["is_smart_performance_campaign"] = bool(ab_data.get("is_smart_performance_campaign", False))
        ab_data["inventory_filter_enabled"] = bool(ab_data.get("inventory_filter_enabled", False))
        ab_data["comment_disabled"] = bool(ab_data.get("comment_disabled", False))
        ab_data["auto_targeting_enabled"] = bool(ab_data.get("auto_targeting_enabled", False))
        ab_data["video_download_disabled"] = bool(ab_data.get("video_download_disabled", False))
        ab_data["placements"] = Json(ab_data.get("placements", []))
        ab_data["category_exclusion_ids"] = Json(ab_data.get("category_exclusion_ids", []))
        ab_data["targeting_expansion"] = Json(ab_data.get("targeting_expansion", {}))
        ab_data["device_price_ranges"] = Json(ab_data.get("device_price_ranges", []))
        ab_data["network_types"] = Json(ab_data.get("network_types", []))
        ab_data["household_income"] = Json(ab_data.get("household_income", []))
        ab_data["languages"] = Json(ab_data.get("languages", []))
        ab_data["excluded_audience_ids"] = Json(ab_data.get("excluded_audience_ids", []))
        ab_data["age_groups"] = Json(ab_data.get("age_groups", []))
        ab_data["operating_systems"] = Json(ab_data.get("operating_systems", []))
        ab_data["interest_category_ids"] = Json(ab_data.get("interest_category_ids", []))
        ab_data["interest_keyword_ids"] = Json(ab_data.get("interest_keyword_ids", []))
        ab_data["actions"] = Json(ab_data.get("actions", []))
        ab_data["audience_ids"] = Json(ab_data.get("audience_ids", []))
        ab_data["included_custom_actions"] = Json(ab_data.get("included_custom_actions", []))
        ab_data["location_ids"] = Json(ab_data.get("location_ids", []))
        ab_data["isp_ids"] = Json(ab_data.get("isp_ids", []))
        ab_data["device_model_ids"] = Json(ab_data.get("device_model_ids", []))
        ab_data["contextual_tag_ids"] = Json(ab_data.get("contextual_tag_ids", []))
        ab_data["excluded_custom_actions"] = Json(ab_data.get("excluded_custom_actions", []))
        logging.info("ab_data",ab_data)
        logging.info("insert_query",insert_query)
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(insert_query, ab_data)
        logging.info("ad group successfully added")
    elif data_type == "ads":
        insert_query = """
        INSERT INTO ads (
            ad_id, advertiser_id, campaign_id, adgroup_id, ad_name, ad_text, ad_format,
            app_name, avatar_icon_web_uri, branded_content_disabled, call_to_action_id,
            campaign_name, create_time, dark_post_status, display_name, identity_id,
            identity_type, image_ids, landing_page_url, modify_time, operation_status,
            optimization_event, playable_url, profile_image_url, secondary_status,
            tiktok_item_id, tracking_pixel_id, utm_params, vast_moat_enabled, video_id,
            viewability_postbid_partner, brand_safety_postbid_partner,
            is_new_structure, is_aco, creative_authorized
        ) VALUES (
            %(ad_id)s, %(advertiser_id)s, %(campaign_id)s, %(adgroup_id)s, %(ad_name)s, %(ad_text)s, %(ad_format)s,
            %(app_name)s, %(avatar_icon_web_uri)s, %(branded_content_disabled)s, %(call_to_action_id)s,
            %(campaign_name)s, %(create_time)s, %(dark_post_status)s, %(display_name)s, %(identity_id)s,
            %(identity_type)s, %(image_ids)s, %(landing_page_url)s, %(modify_time)s, %(operation_status)s,
            %(optimization_event)s, %(playable_url)s, %(profile_image_url)s, %(secondary_status)s,
            %(tiktok_item_id)s, %(tracking_pixel_id)s, %(utm_params)s, %(vast_moat_enabled)s, %(video_id)s,
            %(viewability_postbid_partner)s, %(brand_safety_postbid_partner)s,
            %(is_new_structure)s, %(is_aco)s, %(creative_authorized)s
        );
        """
        # Ensure the object is under the 'data' variable
        ab_data["vast_moat_enabled"] = bool(ab_data.get("vast_moat_enabled", False))
        ab_data["is_new_structure"] = bool(ab_data.get("is_new_structure", False))
        ab_data["is_aco"] = bool(ab_data.get("is_aco", False))
        ab_data["creative_authorized"] = bool(ab_data.get("creative_authorized", False))
        # branded_content_disabled is a string "False", so explicitly compare
        branded_content = ab_data.get("branded_content_disabled", "False")
        ab_data["branded_content_disabled"] = branded_content.lower() == "true"
        ab_data["image_ids"] = json.dumps(ab_data.get("image_ids", []))
        ab_data["utm_params"] = json.dumps(ab_data.get("utm_params", []))
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(insert_query, ab_data)
        logging.info("ad successfully added")

def handle_claude_sql_response(raw_response: str):
    try:
        print("raw resp")
        print(raw_response)
        # Step 1: Extract and parse JSON object
        # claude_response = json.loads(raw_response)
        # sql_query_v2 = claude_response["sql_query"]
        json_string = re.search(r'```json\s*\n(.*?)\n```', raw_response, re.DOTALL).group(1)
        print(" json_string")
        print(json_string)
        parsed = json.loads(json_string)
        print(" parsed")
        print(parsed)
        # Step 2: Validate structure
        if "sql_query" not in parsed or not parsed["sql_query"].strip():
            raise ValueError("Missing or empty 'sql_query' in Claude response")

        sql_query = parsed["sql_query"].strip()
        print("sql_query")
        print(sql_query)
        # Step 3: (Optional) Basic SQL syntax check
        parsed_sql = sqlparse.parse(sql_query)
        if not parsed_sql or len(parsed_sql) == 0:
            raise ValueError("Invalid SQL syntax")

        # Step 4: Execute the SQL query
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_query)
                if cur.description:  # SELECT-like
                    columns = [desc.name for desc in cur.description]
                    rows = cur.fetchall()
                    result = [dict(zip(columns, row)) for row in rows]
                else:  # INSERT/UPDATE/DELETE
                    result = {"rowcount": cur.rowcount}

        # return {
        #     "status": "success",
        #     "sql_query": sql_query,
        #     "result": result
        # }
        return {"result": result}

    except (json.JSONDecodeError, ValueError) as e:
        return {"status": "error", "message": str(e)}
    except Exception as e:
        logging.exception("Error processing Claude SQL response")
        return {"status": "error", "message": "Internal server error"}

# HTTP request handler
class Handler(BaseHTTPRequestHandler):
    def _set_headers(self, code=200):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
    def do_POST(self):
        parsed = urlparse(self.path)
        logging.info("Incoming POST %s", parsed.path)

        # ── Claude Prompt Endpoint ───────────────────────────────
        if parsed.path == '/claude_prompt':
            ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
            CLAUDE_API_URL    = "https://api.anthropic.com/v1/messages"

            length = int(self.headers.get('Content-Length', 0))
            logging.info("Reading %d bytes for Claude prompt", length)
            raw = self.rfile.read(length)
            prompt = raw.decode('utf-8', errors='replace')
            logging.debug("Prompt received: %s", prompt[:200] + ('…' if len(prompt)>200 else ''))

            if not prompt.strip():
                logging.warning("No prompt provided in body")
                self._set_headers(400)
                self.wfile.write(json.dumps({'error': "Missing 'prompt' in request body"}).encode())
                return

            try:
                headers = {
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                }
                body = {
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 4096,
                    "temperature": 0.7,
                    "messages": [{"role": "user", "content": prompt}]
                }
                logging.info("Sending prompt to Claude API")
                resp = requests.post(CLAUDE_API_URL, headers=headers, json=body)
                resp.raise_for_status()
                reply = resp.json()["content"][0]["text"]
                logging.info("Received response from Claude (length %d)", len(reply))

                logging.info("Saving prompt/response to DB")
                with psycopg.connect(**DB_CONFIG) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO claude_prompt_logs (prompt, response) VALUES (%s, %s)",
                            (prompt, reply)
                        )
                logging.info("Saved to claude_prompt_logs")
                sqlres = handle_claude_sql_response(reply)
                print("sqlres: " )
                print(sqlres)
                self._set_headers(200)
                self.wfile.write(json.dumps({'prompt': prompt, 'response': reply, 'sqlres': str(sqlres)}).encode())

            except Exception as e:
                logging.exception("Error handling /claude_prompt: " ,e)
                self._set_headers(500)
                self.wfile.write(json.dumps({'error': 'Internal server error'}).encode())
            return

        # ── Ingest Endpoint ────────────────────────────────────────
        length = int(self.headers.get('Content-Length', 0))
        logging.info("Reading %d bytes for ingest", length)
        raw = self.rfile.read(length)
        logging.debug("Ingest raw body: %s", raw.decode('utf-8', errors='replace'))

        try:
            payload = json.loads(raw)
            logging.info("Parsed ingest JSON keys: %s", list(payload.keys()))
            save_metrics_and_meta(payload)
            logging.info("save_metrics_and_meta() succeeded")

            self._set_headers(200)
            resp = {'status': 'success'}
            logging.info("Responding with: %s", resp)
            self.wfile.write(json.dumps(resp).encode())

        except Exception as e:
            logging.exception("Ingest error")
            self._set_headers(500)
            resp = {'error': str(e)}
            logging.info("Responding with: %s", resp)
            self.wfile.write(json.dumps(resp).encode())

    def do_GET(self):
        """Generate SQL: GET /generate?params=[...]"""
        parsed = urlparse(self.path)
        if parsed.path != '/generate':
            self._set_headers(404)
            self.wfile.write(json.dumps({'error': 'Not found'}).encode())
            return

        raw = parse_qs(parsed.query).get('params', ['[]'])[0]
        try:
            params = json.loads(raw)
        except json.JSONDecodeError:
            logging.error("Invalid params JSON: %s", raw)
            self._set_headers(400)
            self.wfile.write(json.dumps({'error': 'Invalid JSON in params'}).encode())
            return

        result = handle_generate_request(params)
        status = 200 if 'sql' in result else 400
        self._set_headers(status)
        self.wfile.write(json.dumps(result).encode())

# Run the HTTP server
def run(port=8000):
    init_db()
    server = HTTPServer(('', port), Handler)
    logging.info(f"Server listening on port {port}")
    server.serve_forever()

if __name__ == '__main__':
    run()
