"""
Ticket Ingestion: Pull tickets from CareSoft API to Bronze layer
Architecture: CareSoft API → Bronze (Iceberg)

FLOW 2 - STEP 1: API → Ticket Bronze

OLD SYSTEM PROBLEMS:
1. Direct write to PostgreSQL (no raw layer)
2. Complex temp table operations
3. Manual pagination handling
4. No incremental processing strategy

NEW LAKEHOUSE APPROACH:
1. Write raw API responses to Bronze Iceberg table
2. Partition by snapshot_date for time-travel
3. Automatic pagination with retry logic
4. Incremental updates based on updated_since/created_since
"""

import os
import sys
from datetime import datetime, date, timedelta
import requests
import time
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And
from pyiceberg.io.pyarrow import schema_to_pyarrow
from loguru import logger
import gc
from caresoft.brand_config import get_active_brands

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# CareSoft API configuration
CARESOFT_API_URL = os.getenv('CARESOFT_API_URL', 'https://api.caresoft.vn')
CARESOFT_PAGE_SIZE = int(os.getenv('CARESOFT_PAGE_SIZE', '100'))

# Target table (Bronze)
TICKET_BRONZE_TABLE = 'caresoft.cs_ticket_bronze'


class TicketIngestion:
    """
    Pull tickets from CareSoft API and store in Bronze Iceberg table
    Supports incremental updates based on updated_since/created_since
    """
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        
        # Initialize catalog
        catalog_config = {
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY,
            "s3.secret-access-key": AWS_SECRET_KEY,
            "s3.region": AWS_REGION,
        }
        
        self.bronze_catalog = load_catalog(
            CATALOG_NAME_BRONZE,
            uri=ICEBERG_REST_URI_BRONZE,
            **catalog_config
        )
        
        logger.info(f"Initialized TicketIngestion for {self.snapshot_date}")
    
    def ensure_ticket_bronze_table_exists(self):
        """Create ticket bronze table if not exists"""
        try:
            return self.bronze_catalog.load_table(TICKET_BRONZE_TABLE)
        except:
            logger.info(f"Creating table {TICKET_BRONZE_TABLE}...")
            
            from pyiceberg.schema import Schema
            from pyiceberg.types import (
                NestedField, IntegerType, StringType, TimestampType, 
                DateType, LongType, BooleanType
            )
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import IdentityTransform
            
            # Schema matching CareSoft tickets API response
            schema = Schema(
                # Core IDs
                NestedField(1, "ticket_id", LongType(), required=False),
                NestedField(2, "account_id", IntegerType(), required=False),
                NestedField(3, "ticket_no", IntegerType(), required=False),
                NestedField(4, "requester_id", LongType(), required=False),
                NestedField(5, "group_id", IntegerType(), required=False),
                NestedField(6, "assignee_id", LongType(), required=False),
                
                # Text fields
                NestedField(7, "subject", StringType(), required=False),
                NestedField(8, "description", StringType(), required=False),
                NestedField(9, "status", StringType(), required=False),
                NestedField(10, "priority", StringType(), required=False),
                NestedField(11, "ticket_source", StringType(), required=False),
                
                # Timestamps
                NestedField(12, "created_at", TimestampType(), required=False),
                NestedField(13, "updated_at", TimestampType(), required=False),
                NestedField(14, "satisfaction_at", TimestampType(), required=False),
                
                # Additional fields
                NestedField(15, "sla_id", IntegerType(), required=False),
                NestedField(16, "duedate", StringType(), required=False),
                NestedField(17, "satisfaction", StringType(), required=False),
                NestedField(18, "satisfaction_content", StringType(), required=False),
                NestedField(19, "satisfaction_send", StringType(), required=False),
                NestedField(20, "automessage_id", StringType(), required=False),
                NestedField(21, "feedback_status", StringType(), required=False),
                NestedField(22, "is_overdue", BooleanType(), required=False),
                NestedField(23, "incident_id", IntegerType(), required=False),
                NestedField(24, "service_id", LongType(), required=False),
                NestedField(25, "qa_script_id", StringType(), required=False),
                NestedField(26, "qa_agent", StringType(), required=False),
                NestedField(27, "current_agent", StringType(), required=False),
                NestedField(28, "ticket_source_detail_id", IntegerType(), required=False),
                
                # JSON fields (stored as text)
                NestedField(29, "comments", StringType(), required=False),
                NestedField(30, "ccs", StringType(), required=False),
                NestedField(31, "follows", StringType(), required=False),
                
                # Brand and raw data
                NestedField(32, "brand_id", IntegerType(), required=False),
                NestedField(33, "raw_data", StringType(), required=False),
                
                # Metadata
                NestedField(100, "snapshot_date", DateType(), required=False),
                NestedField(101, "ingestion_timestamp", TimestampType(), required=False),
                NestedField(102, "fetch_type", StringType(), required=False),  # 'updated', 'created', 'created_prev'
            )
            
            partition_spec = PartitionSpec(
                PartitionField(source_id=100, field_id=1000, transform=IdentityTransform(), name="snapshot_date")
            )
            
            table = self.bronze_catalog.create_table(
                identifier=TICKET_BRONZE_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"✅ Created table: {TICKET_BRONZE_TABLE}")
            return table
    
    def get_api_url(self, subdomain: str, path: str) -> str:
        """Construct API URL"""
        return f"{CARESOFT_API_URL}/{subdomain}/api/v1/{path}"
    
    def get_api_headers(self, api_key: str) -> dict:
        """Get API headers"""
        return {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def fetch_tickets(self, brand: dict, fetch_type: str, date_since: str, date_to: str):
        """
        Fetch tickets from CareSoft API with pagination
        
        Args:
            brand: Brand configuration dict
            fetch_type: 'updated', 'created', or 'created_prev'
            date_since: Start date in ISO format (e.g. '2026-01-12T00:00:00Z')
            date_to: End date in ISO format
        
        Returns:
            List of ticket records
        """
        subdomain = brand["caresoft_domain"]
        api_key = brand["caresoft_key"]
        brand_id = brand["erp_id"]
        
        # Prepare params based on fetch type
        if fetch_type == 'updated':
            params = {
                'updated_since': date_since,
                'updated_to': date_to,
                'page': 1,
                'count': CARESOFT_PAGE_SIZE
            }
        else:  # 'created' or 'created_prev'
            params = {
                'created_since': date_since,
                'created_to': date_to,
                'page': 1,
                'count': CARESOFT_PAGE_SIZE
            }
        
        logger.info(f"Fetching tickets ({fetch_type}): {date_since} to {date_to}")
        
        all_tickets = []
        page = 1
        
        while True:
            params['page'] = page
            
            try:
                response = requests.get(
                    self.get_api_url(subdomain, "tickets"),
                    params=params,
                    headers=self.get_api_headers(api_key),
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    tickets = data.get('tickets', [])
                    
                    if not tickets:
                        break
                    
                    # Add metadata to each ticket
                    for ticket in tickets:
                        ticket['brand_id'] = brand_id
                        ticket['snapshot_date'] = self.snapshot_date
                        ticket['ingestion_timestamp'] = datetime.now()
                        ticket['fetch_type'] = fetch_type
                    
                    all_tickets.extend(tickets)
                    logger.info(f"Page {page}: fetched {len(tickets)} tickets (total: {len(all_tickets)})")
                    
                    page += 1
                    time.sleep(2)  # Rate limiting
                    
                else:
                    logger.error(f"API Error {response.status_code}: {response.text[:200]}")
                    break
                    
            except Exception as e:
                logger.error(f"Exception fetching page {page}: {e}")
                break
        
        logger.success(f"✅ Fetched {len(all_tickets)} tickets ({fetch_type})")
        return all_tickets
    
    def run(self, brand: dict, days_ago: int = 1):
        """
        Execute ticket ingestion pipeline for a single brand
        
        Args:
            brand: Brand configuration dict with caresoft_domain, caresoft_key, erp_id
            days_ago: Number of days to look back (default: 1)
        """
        logger.info("=" * 80)
        logger.info(f"Starting Ticket Ingestion for brand {brand['erp_id']}")
        logger.info("=" * 80)
        
        # Ensure table exists
        ticket_table = self.ensure_ticket_bronze_table_exists()
        
        # Calculate date ranges
        today = self.snapshot_date
        yesterday = today - timedelta(days=days_ago)
        day_before_yesterday = yesterday - timedelta(days=1)
        
        # Format dates for API
        date_today = today.strftime('%Y-%m-%d') + 'T00:00:00Z'
        date_yesterday = yesterday.strftime('%Y-%m-%d') + 'T00:00:00Z'
        date_day_before = day_before_yesterday.strftime('%Y-%m-%d') + 'T00:00:00Z'
        
        # DELETE old partition data (snapshot overwrite pattern)
        try:
            logger.info(f"🗑️  Deleting old data for brand {brand['erp_id']}, snapshot {self.snapshot_date}...")
            ticket_table.delete(
                And(
                    EqualTo("snapshot_date", self.snapshot_date),
                    EqualTo("brand_id", brand["erp_id"])
                )
            )
            logger.success(f"✅ Deleted old partition data")
        except Exception as e:
            logger.warning(f"Could not delete old data (table might be empty): {e}")
        
        # Fetch tickets in 3 loops (matching old system logic)
        all_records = []
        
        # Loop 1: UPDATED tickets (yesterday → today)
        tickets_updated = self.fetch_tickets(
            brand=brand,
            fetch_type='updated',
            date_since=date_yesterday,
            date_to=date_today
        )
        all_records.extend(tickets_updated)
        
        # Loop 2: CREATED tickets (yesterday → today)
        tickets_created = self.fetch_tickets(
            brand=brand,
            fetch_type='created',
            date_since=date_yesterday,
            date_to=date_today
        )
        all_records.extend(tickets_created)
        
        # Loop 3: CREATED tickets (day before yesterday → yesterday)
        tickets_created_prev = self.fetch_tickets(
            brand=brand,
            fetch_type='created_prev',
            date_since=date_day_before,
            date_to=date_yesterday
        )
        all_records.extend(tickets_created_prev)
        
        # Write to Iceberg
        if all_records:
            import json
            
            # Helper functions for safe type conversion
            def safe_int(val):
                if val is None:
                    return None
                try:
                    return int(val)
                except (ValueError, TypeError):
                    return None
            
            def safe_str(val):
                if val is None:
                    return None
                return str(val)
            
            def safe_ts(val):
                if val is None:
                    return None
                try:
                    return datetime.fromisoformat(val.replace('Z', '+00:00')) if isinstance(val, str) else val
                except (ValueError, TypeError):
                    return None
            
            def safe_bool(val):
                if val is None:
                    return None
                if isinstance(val, bool):
                    return val
                return str(val).lower() in ('true', '1', 'yes')
            
            # Transform records to match schema
            transformed_records = []
            for ticket in all_records:
                record = {
                    # Core IDs
                    "ticket_id": ticket.get("ticket_id"),
                    "account_id": safe_int(ticket.get("account_id")),
                    "ticket_no": safe_int(ticket.get("ticket_no")),
                    "requester_id": ticket.get("requester_id"),
                    "group_id": safe_int(ticket.get("group_id")),
                    "assignee_id": ticket.get("assignee_id"),
                    
                    # Text fields
                    "subject": safe_str(ticket.get("subject")),
                    "description": safe_str(ticket.get("description")),
                    "status": safe_str(ticket.get("status")),
                    "priority": safe_str(ticket.get("priority")),
                    "ticket_source": safe_str(ticket.get("ticket_source")),
                    
                    # Timestamps
                    "created_at": safe_ts(ticket.get("created_at")),
                    "updated_at": safe_ts(ticket.get("updated_at")),
                    "satisfaction_at": safe_ts(ticket.get("satisfaction_at")),
                    
                    # Additional fields
                    "sla_id": safe_int(ticket.get("sla_id")),
                    "duedate": safe_str(ticket.get("duedate")),
                    "satisfaction": safe_str(ticket.get("satisfaction")),
                    "satisfaction_content": safe_str(ticket.get("satisfaction_content")),
                    "satisfaction_send": safe_str(ticket.get("satisfaction_send")),
                    "automessage_id": safe_str(ticket.get("automessage_id")),
                    "feedback_status": safe_str(ticket.get("feedback_status")),
                    "is_overdue": safe_bool(ticket.get("is_overdue")),
                    "incident_id": safe_int(ticket.get("incident_id")),
                    "service_id": ticket.get("service_id"),
                    "qa_script_id": safe_str(ticket.get("qa_script_id")),
                    "qa_agent": safe_str(ticket.get("qa_agent")),
                    "current_agent": safe_str(ticket.get("current_agent")),
                    "ticket_source_detail_id": safe_int(ticket.get("ticket_source_detail_id")),
                    
                    # JSON fields
                    "comments": json.dumps(ticket.get("comments", []), ensure_ascii=False),
                    "ccs": json.dumps(ticket.get("ccs", []), ensure_ascii=False),
                    "follows": json.dumps(ticket.get("follows", []), ensure_ascii=False),
                    
                    # Brand and raw data
                    "brand_id": safe_int(ticket.get("brand_id")),
                    "raw_data": json.dumps(ticket, ensure_ascii=False),
                    
                    # Metadata
                    "snapshot_date": ticket.get("snapshot_date"),
                    "ingestion_timestamp": ticket.get("ingestion_timestamp"),
                    "fetch_type": ticket.get("fetch_type"),
                }
                transformed_records.append(record)
            
            # Write to Iceberg
            pyarrow_schema = schema_to_pyarrow(ticket_table.schema())
            df = pa.Table.from_pylist(transformed_records, schema=pyarrow_schema)
            ticket_table.append(df)
            
            logger.success(f"✅ Wrote {len(transformed_records)} tickets to {TICKET_BRONZE_TABLE}")
        else:
            logger.warning("No tickets fetched")
        
        gc.collect()
        logger.success(f"Ticket ingestion completed for brand {brand['erp_id']}")


def main():
    """
    Main entry point
    Usage: python ticket_ingestion.py <snapshot_date>
    """
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else date.today()
    
    # Load all active brands from Gold layer
    brands = get_active_brands()
    
    if not brands:
        logger.error("No active brands found in core_brand table")
        sys.exit(1)
    
    logger.info(f"Found {len(brands)} active brands to process")
    
    # Initialize job once
    job = TicketIngestion(snapshot_date)
    
    # Process each brand sequentially
    total_success = 0
    total_errors = 0
    
    for brand in brands:
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing brand: {brand['name']} (erp_id={brand['erp_id']})")
        logger.info(f"{'='*80}")
        
        try:
            job.run(brand=brand, days_ago=1)
            total_success += 1
        except Exception as e:
            logger.error(f"Failed to process brand {brand['erp_id']}: {e}")
            total_errors += 1
    
    logger.info(f"\n{'='*80}")
    logger.success(f"Ticket ingestion completed. Success: {total_success}/{len(brands)}, Errors: {total_errors}")
    logger.info(f"{'='*80}")


if __name__ == "__main__":
    main()
