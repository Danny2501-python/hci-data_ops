"""
Agent Ingestion: CareSoft API → Iceberg Bronze Layer

Simple API flow - gọi trực tiếp GET /agents và lưu vào Bronze
Không cần scan ID, chỉ lấy page 1 (tất cả agents)

Flow:
    CareSoft API (/agents) → Bronze (caresoft.cs_agents_api)
"""

import os
import sys
import json
import time
import requests
import gc
import pyarrow as pa
from datetime import date, datetime, timedelta
from loguru import logger
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, TimestampType, DateType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.io.pyarrow import schema_to_pyarrow

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from common.brand_config import get_active_brands
from common.utils import get_current_timestamp_naive
from common.caresoft_api import get_api_url, get_api_headers

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')

# S3 configuration
S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

CARESOFT_PAGE_SIZE = int(os.getenv('CARESOFT_PAGE_SIZE', '500'))
AGENT_BRONZE_TABLE = 'caresoft.cs_agents_api'


class AgentIngestion:
    """Pull Agent data from CareSoft API and store in Bronze Iceberg table"""
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None
        
        logger.info(f"Initializing AgentIngestion for snapshot_date={self.snapshot_date}")
        
        # Connect to Bronze catalog
        self.bronze_catalog = load_catalog(
            CATALOG_NAME_BRONZE,
            **{
                "uri": ICEBERG_REST_URI_BRONZE,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY,
                "s3.secret-access-key": AWS_SECRET_KEY,
                "s3.region": AWS_REGION,
            }
        )
        
        self.ensure_table_exists()

    def ensure_table_exists(self):
        """Create agent bronze table if not exists"""
        try:
            table = self.bronze_catalog.load_table(AGENT_BRONZE_TABLE)
            logger.info(f"Table {AGENT_BRONZE_TABLE} already exists")
        except Exception:
            logger.info(f"Creating table {AGENT_BRONZE_TABLE}")
            
            # Schema definition matching cs_agents_api_v1 DDL + API response
            schema = Schema(
                NestedField(1, "id", IntegerType(), required=False),
                NestedField(2, "username", StringType(), required=False),
                NestedField(3, "email", StringType(), required=False),
                NestedField(4, "phone_no", StringType(), required=False),
                NestedField(5, "agent_id", StringType(), required=False),
                NestedField(6, "created_at", StringType(), required=False),
                NestedField(7, "updated_at", StringType(), required=False),
                NestedField(8, "group_id", StringType(), required=False),
                NestedField(9, "group_name", StringType(), required=False),
                NestedField(10, "role_id", StringType(), required=False),
                NestedField(11, "login_status", StringType(), required=False),
                NestedField(12, "call_status", StringType(), required=False),
                NestedField(13, "ticket_status", StringType(), required=False),  # Added
                NestedField(14, "chat_status", StringType(), required=False),    # Added
                NestedField(15, "raw_data", StringType(), required=False),
                NestedField(16, "brand_id", IntegerType(), required=False),
                
                # Metadata fields
                NestedField(17, "snapshot_date", DateType(), required=True),
                NestedField(18, "ingestion_timestamp", TimestampType(), required=True),
            )
            
            # Partition by snapshot_date
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=17,  # Updated to match new field numbering
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            table = self.bronze_catalog.create_table(
                identifier=AGENT_BRONZE_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"Created table {AGENT_BRONZE_TABLE}")

    def fetch_agents(self, brand: dict, days_ago: int = 3) -> list:
        """
        Fetch agents from API
        Logic matches cs_extract.py: 
        - start_time_since = (snapshot_date - 3 days) 00:00:00
        - start_time_to = snapshot_date 00:00:00
        - Only fetch page 1 (all agents fit in one page)
        """
        anchor_date = datetime.combine(self.snapshot_date, datetime.min.time())
        
        start_time_to = anchor_date
        start_time_since = anchor_date - timedelta(days=days_ago)
        
        start_time_since_str = start_time_since.strftime('%Y-%m-%dT%H:%M:%SZ')
        start_time_to_str = start_time_to.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        logger.info(f"Fetching agents for brand {brand['name']} from {start_time_since_str} to {start_time_to_str}")
        
        page = 1
        agent_data = []
        
        # Only fetch page 1 for agents (matching cs_extract.py: while page == 1)
        while page == 1:
            params = {
                'start_time_since': start_time_since_str,
                'start_time_to': start_time_to_str,
                'page': page,
                'count': CARESOFT_PAGE_SIZE
            }
            
            try:
                url = get_api_url(brand, "agents")
                headers = get_api_headers(brand)
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    new_agents = data.get('agents', [])
                    
                    agent_data.extend(new_agents)
                    logger.info(f"Fetched {len(new_agents)} agents from page {page}")
                    
                    page += 1
                    time.sleep(2)  # Rate limiting (matching cs_extract.py)
                else:
                    logger.error(f"API request failed: {response.status_code} - {response.text[:200]}")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching agents page {page}: {e}")
                break
                
        return agent_data

    def run(self, brand: dict, days_ago: int = 3):
        brand_erp_id = brand["erp_id"]
        brand_name = brand["name"]
        
        logger.info(f"Processing agent ingestion for {brand_name} (id={brand_erp_id})")
        
        agents = self.fetch_agents(brand, days_ago)
        
        if not agents:
            logger.warning(f"No agents found for brand {brand_name}")
            return

        # Transform
        ingestion_timestamp = get_current_timestamp_naive()
        records = []
        
        def safe_str(val):
            """Convert to string, handling None and empty string"""
            if val is None or val == "":
                return None
            return str(val)
        
        for agent in agents:
            record = {
                "id": agent.get("id"),
                "username": safe_str(agent.get("username")),
                "email": safe_str(agent.get("email")),
                "phone_no": safe_str(agent.get("phone_no")),
                "agent_id": safe_str(agent.get("agent_id")),
                "created_at": safe_str(agent.get("created_at")),
                "updated_at": safe_str(agent.get("updated_at")),
                "group_id": safe_str(agent.get("group_id")),
                "group_name": safe_str(agent.get("group_name")),
                "role_id": safe_str(agent.get("role_id")),
                "login_status": safe_str(agent.get("login_status")),
                "call_status": safe_str(agent.get("call_status")),
                "ticket_status": safe_str(agent.get("ticket_status")),  # Added
                "chat_status": safe_str(agent.get("chat_status")),      # Added
                "raw_data": json.dumps(agent, ensure_ascii=False),
                "brand_id": brand_erp_id,
                "snapshot_date": self.snapshot_date,
                "ingestion_timestamp": ingestion_timestamp
            }
            records.append(record)
            
        # Write to Iceberg
        table = self.bronze_catalog.load_table(AGENT_BRONZE_TABLE)
        
        # Overwrite partition logic
        try:
             logger.info(f"Deleting old data for {brand_name} at {self.snapshot_date}")
             table.delete(
                 And(
                     EqualTo("snapshot_date", self.snapshot_date),
                     EqualTo("brand_id", brand_erp_id)
                 )
             )
        except Exception:
            pass
            
        # Append
        try:
            pa_schema = schema_to_pyarrow(table.schema())
            df = pa.Table.from_pylist(records, schema=pa_schema)
            table.append(df)
            logger.success(f"Successfully wrote {len(records)} agents for {brand_name}")
        except Exception as e:
            logger.error(f"Failed to write agents: {e}")
            
        gc.collect()

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else date.today()
    
    logger.info(f"Starting Agent Ingestion for {snapshot_date}")
    
    brands = get_active_brands()
    for brand in brands:
        try:
            job = AgentIngestion(snapshot_date)
            job.run(brand, days_ago=3)
        except Exception as e:
            logger.error(f"Failed brand {brand['name']}: {e}")

if __name__ == "__main__":
    main()
