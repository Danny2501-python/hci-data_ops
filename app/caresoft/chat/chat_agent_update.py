"""
Flow 10: Agent Update from Chat (Transform)
Scan cs_chat_api for records with missing agent_email, re-fetch from API, and update (append) to Iceberg.
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
from pyiceberg.expressions import And, EqualTo, IsNull, NotNull
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

CARESOFT_PAGE_SIZE = int(os.getenv('CARESOFT_PAGE_SIZE'))
CHAT_BRONZE_TABLE = 'caresoft.cs_chat_api'

class ChatAgentUpdate:
    """Scan and update missing agent emails in Chat data"""
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None
        
        logger.info(f"Initializing ChatAgentUpdate for snapshot_date={self.snapshot_date}")
        logger.info(f"ICEBERG_REST_URI_BRONZE: {ICEBERG_REST_URI_BRONZE}")
        logger.info(f"CATALOG_NAME_BRONZE: {CATALOG_NAME_BRONZE}")
        
        if not ICEBERG_REST_URI_BRONZE:
            raise ValueError("ICEBERG_REST_URI_BRONZE environment variable is not set")
            
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
        
    def get_tickets_to_update(self, brand_erp_id: int):
        """
        Find ALL ticket_ids to update in current snapshot.
        Criteria: agent_email is null AND ticket_no is not null.
        Order by: start_time DESC.
        Uses Parquet spill for memory safety.
        """
        try:
            table = self.bronze_catalog.load_table(CHAT_BRONZE_TABLE)
        except Exception:
            logger.warning(f"Table {CHAT_BRONZE_TABLE} not found")
            return []

        # PyIceberg scan
        # Note: PyIceberg filtering on NULLs can be tricky.
        # We scan essentially everything for the brand and filter in DuckDB/Pandas for complex logic 
        # because PyIceberg limited pushdown.
        
        # Filter by current snapshot_date to align with API query logic
        scan = table.scan(
            row_filter=And(
                EqualTo("brand_id", brand_erp_id), 
                EqualTo("snapshot_date", self.snapshot_date)
            ),
            selected_fields=("ticket_id", "conversation_id", "start_time", "agent_email", "ticket_no")
        )
        
        # Temp dir setup
        temp_dir = os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb')
        os.makedirs(temp_dir, exist_ok=True)
        temp_parquet = os.path.join(temp_dir, f"agent_update_{brand_erp_id}_{os.getpid()}.parquet")
        
        try:
            # Write scan result to Parquet temp file
            import pyarrow.parquet as pq
            
            reader = scan.to_arrow_batch_reader()
            
            # Check if empty first to avoid writing empty file handling issues if strict
            try:
                # Read all to table for writing. 
                # Note: If massive, we should stream write, but for "snapshot_date" slice it should be fit in mem-to-disk
                arrow_table = reader.read_all()
            except Exception:
                return []
                
            if len(arrow_table) == 0:
                return []
                
            pq.write_table(arrow_table, temp_parquet)
            del arrow_table
            
            # Query with DuckDB
            import duckdb
            
            con = duckdb.connect(config={
                'memory_limit': '2GB',
                'preserve_insertion_order': 'false',
                'temp_directory': temp_dir,
                'threads': '1'
            })
            
            query = f"""
                SELECT ticket_id, MAX(start_time) as start_time
                FROM read_parquet('{temp_parquet}')
                WHERE (agent_email IS NULL OR agent_email = '')
                AND ticket_no IS NOT NULL
                GROUP BY ticket_id
                ORDER BY start_time DESC
            """
            
            result = con.execute(query).fetchall()
            con.close()
            
            tickets = []
            for row in result:
                tickets.append({
                    "ticket_id": row[0],
                    # Conversation_id not strictly needed for the fetch, just ticket_id
                    "start_time": row[1]
                })
            return tickets
            
        finally:
            # Cleanup
            if os.path.exists(temp_parquet):
                try:
                    os.remove(temp_parquet)
                except:
                    pass

    def fetch_chats_by_ticket(self, brand: dict, ticket_id: int, start_time: datetime) -> list:
        """Fetch chats for a specific ticket_id using its start_time for the time window"""
        
        # Logic matches legacy: use the chat's start_time to define the 00:00 -> 23:59 window
        if not start_time:
            # Should not happen given the query filters, but fallback to snapshot_date if needed
            anchor_date = self.snapshot_date
        else:
             # If it's a timestamp (microsecond integer) or similar, convert
            if isinstance(start_time, (int, float)):
                 start_time = datetime.fromtimestamp(start_time / 1000000.0)
            
            # Check if start_time is date (without time) or datetime
            if isinstance(start_time, datetime):
                anchor_date = start_time.date()
            elif isinstance(start_time, date):
                anchor_date = start_time
            else:
                 # Fallback
                 anchor_date = self.snapshot_date

        start_time_since = anchor_date + timedelta(days=1)
        start_time_to = anchor_date + timedelta(days=1)
        
        start_time_since_str = start_time_since.strftime('%Y-%m-%dT00:00:00Z')
        start_time_to_str = start_time_to.strftime('%Y-%m-%dT23:59:59Z')
        
        # logger.info(f"Refetching chats for Ticket {ticket_id} (Window: {start_time_since_str})")
        
        page = 1
        chat_data = []
        
        while True:
            params = {
                'start_time_since': start_time_since_str,
                'start_time_to': start_time_to_str,
                'conversation_type': '1',
                'page': page,
                'count': CARESOFT_PAGE_SIZE,
                'ticket_id': ticket_id
            }
            
            try:
                url = get_api_url(brand, "chats")
                headers = get_api_headers(brand)
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    new_chats = data.get('chats', [])
                    
                    if not new_chats:
                        break
                        
                    chat_data.extend(new_chats)
                    page += 1
                    time.sleep(0.5) # Reduced sleep for batch processing
                else:
                    logger.error(f"API request failed: {response.status_code}")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching chats: {e}")
                break
                
        return chat_data

    def run(self, brand: dict):
        brand_erp_id = brand["erp_id"]
        brand_name = brand["name"]
        
        logger.info(f"Checking for Agent updates for {brand_name}...")
        
        tickets = self.get_tickets_to_update(brand_erp_id)
        
        if not tickets:
            logger.info("No tickets found needing agent update.")
            return

        logger.info(f"Found {len(tickets)} tickets to update for {brand_name}")
        
        all_chats_to_update = []
        
        # Batch Fetch
        for i, target in enumerate(tickets):
            ticket_id = target['ticket_id']
            if (i+1) % 10 == 0:
                logger.info(f"Processing {i+1}/{len(tickets)} tickets...")
                
            chats = self.fetch_chats_by_ticket(brand, ticket_id, target['start_time'])
            if chats:
                all_chats_to_update.extend(chats)
            
            # Small delay between tickets
            time.sleep(0.1)

        if not all_chats_to_update:
            logger.warning(f"No chats returned from API for any tickets")
            return

        # Transform & Write in one go (or batches if massive, but list extension is fine for hundreds)
        ingestion_timestamp = get_current_timestamp_naive()
        records = []
        
        def safe_ts(val_ms):
            if val_ms:
                if isinstance(val_ms, str):
                    try:
                         return datetime.fromisoformat(val_ms.replace('Z', '+00:00'))
                    except: pass
                if isinstance(val_ms, (int, float)):
                     return datetime.fromtimestamp(val_ms / 1000.0)
            return None

        for chat in all_chats_to_update:
            record = {
                "ticket_id": chat.get("ticket_id"),
                "ticket_no": chat.get("ticket_no"),
                "customer_id": chat.get("customer_id"),
                "group_name": chat.get("group_name"),
                "conversation_id": str(chat.get("conversation_id")),
                "cus_email": chat.get("cus_email"),
                "cus_name": chat.get("cus_name"),
                "cus_phone": chat.get("cus_phone"),
                "start_time": safe_ts(chat.get("start_time")),
                "end_time": safe_ts(chat.get("end_time")),
                "chat_duration": chat.get("chat_duration"),
                "agent_email": chat.get("agent_email"), # Hopefully filled now
                "ring_time": str(chat.get("ring_time")) if chat.get("ring_time") is not None else None,
                "meet_time": str(chat.get("meet_time")) if chat.get("meet_time") is not None else None,
                "waittime": str(chat.get("waitTime")) if chat.get("waitTime") is not None else None,
                "answer_time": str(chat.get("answer_time")) if chat.get("answer_time") is not None else None,
                "chat_status": str(chat.get("chat_status")),
                "landing_page": chat.get("landing_page"),
                "referrer": chat.get("referrer"),
                "is_trigger": str(chat.get("is_trigger")) if chat.get("is_trigger") is not None else None,
                "facebook_page_id": str(chat.get("facebook_page_id")) if chat.get("facebook_page_id") is not None else None,
                "service_id": chat.get("service_id"),
                "brand_id": brand_erp_id,
                "raw_data": json.dumps(chat, ensure_ascii=False),
                "agent_name": chat.get("agent_name"),
                "snapshot_date": self.snapshot_date,
                "ingestion_timestamp": ingestion_timestamp
            }
            records.append(record)
            
        # Append to Iceberg
        try:
            table = self.bronze_catalog.load_table(CHAT_BRONZE_TABLE)
            pa_schema = schema_to_pyarrow(table.schema())
            df = pa.Table.from_pylist(records, schema=pa_schema)
            table.append(df)
            logger.success(f"Successfully updated {len(records)} chats across {len(tickets)} tickets")
        except Exception as e:
            logger.error(f"Failed to write updates: {e}")
            
        gc.collect()

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Chat Agent Update for {snapshot_date}")
    
    brands = get_active_brands()
    for brand in brands:
        try:
            job = ChatAgentUpdate(snapshot_date)
            job.run(brand)
        except Exception as e:
            logger.error(f"Failed brand {brand['name']}: {e}")

if __name__ == "__main__":
    main()
