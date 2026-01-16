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
from common.brand_config import get_active_brands

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# CareSoft API configuration
CARESOFT_PAGE_SIZE = int(os.getenv('CARESOFT_PAGE_SIZE'))

# Target table (Bronze) - matching old system naming
TICKET_BRONZE_TABLE = 'caresoft.cs_ticket_api'


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
            
            # Schema matching legacy cs_ticket_api table (PostgreSQL)
            # Field order and numbering matches PostgreSQL DDL exactly
            schema = Schema(
                # 1-27: Fields from old PostgreSQL table (in DDL order)
                NestedField(1, "incident_id", LongType(), required=False),
                NestedField(2, "ticket_no", IntegerType(), required=False),
                NestedField(3, "ticket_status", StringType(), required=False),
                NestedField(4, "ticket_subject", StringType(), required=False),
                NestedField(5, "created_at", TimestampType(), required=False),
                NestedField(6, "updated_at", TimestampType(), required=False),
                NestedField(7, "ticket_source_end_status", StringType(), required=False),
                NestedField(8, "ticket_source", StringType(), required=False),
                NestedField(9, "ticket_priority", StringType(), required=False),
                NestedField(10, "service_id", LongType(), required=False),
                NestedField(11, "ticket_id", LongType(), required=False),
                NestedField(12, "requester_id", LongType(), required=False),
                NestedField(13, "assignee", StringType(), required=False),  # JSON object as string
                NestedField(14, "assignee_id", LongType(), required=False),
                NestedField(15, "campaign_id", StringType(), required=False),
                NestedField(16, "campaign", StringType(), required=False),
                NestedField(17, "duedate", StringType(), required=False),
                NestedField(18, "custom_fields", StringType(), required=False),  # JSON array as string
                NestedField(19, "requester", StringType(), required=False),  # JSON object as string
                NestedField(20, "tags", StringType(), required=False),  # JSON array as string
                NestedField(21, "brand_id", IntegerType(), required=False),
                NestedField(22, "raw_data", StringType(), required=False),
                NestedField(23, "ghi_chu", StringType(), required=False),
                NestedField(24, "id_value_phan_loai_ket_noi", StringType(), required=False),
                NestedField(25, "id_value_muc_do_hai_long", StringType(), required=False),
                NestedField(26, "ad_id_value", StringType(), required=False),
                NestedField(27, "id_value_dich_vu_chi_tiet", StringType(), required=False),
                
                # 28-30: NEW metadata fields for Lakehouse (not in old system)
                NestedField(28, "snapshot_date", DateType(), required=False),
                NestedField(29, "ingestion_timestamp", TimestampType(), required=False),
                NestedField(30, "fetch_type", StringType(), required=False),  # 'updated', 'created', 'created_prev'
            )
            
            partition_spec = PartitionSpec(
                PartitionField(source_id=28, field_id=1000, transform=IdentityTransform(), name="snapshot_date")
            )
            
            table = self.bronze_catalog.create_table(
                identifier=TICKET_BRONZE_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"✅ Created table: {TICKET_BRONZE_TABLE}")
            return table
    
    def get_api_url(self, subdomain: str, path: str) -> str:
        """Construct API URL using brand's caresoft_domain"""
        # API URL pattern: https://api.caresoft.vn/{subdomain}/api/v1/{path}
        return f"https://api.caresoft.vn/{subdomain}/api/v1/{path}"
    
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
        # Store as list of tuples: (ticket, fetch_type)
        all_records = []
        
        # Loop 1: UPDATED tickets (yesterday → today)
        tickets_updated = self.fetch_tickets(
            brand=brand,
            fetch_type='updated',
            date_since=date_yesterday,
            date_to=date_today
        )
        all_records.extend([(ticket, 'updated') for ticket in tickets_updated])
        
        # Loop 2: CREATED tickets (yesterday → today)
        tickets_created = self.fetch_tickets(
            brand=brand,
            fetch_type='created',
            date_since=date_yesterday,
            date_to=date_today
        )
        all_records.extend([(ticket, 'created') for ticket in tickets_created])
        
        # Loop 3: CREATED tickets (day before yesterday → yesterday)
        tickets_created_prev = self.fetch_tickets(
            brand=brand,
            fetch_type='created_prev',
            date_since=date_day_before,
            date_to=date_yesterday
        )
        all_records.extend([(ticket, 'created_prev') for ticket in tickets_created_prev])
        
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
            
            def extract_ad_id(tags):
                """Extract ad_id value from tags array
                
                Tags format: [{"name": "ad_id.123456"}, {"name": "other_tag"}]
                Returns: "123456" or empty string
                """
                if not tags:
                    return '[]'  # Empty list as string (matching old system)
                
                try:
                    # Parse JSON if string
                    if isinstance(tags, str):
                        import json
                        tags = json.loads(tags)
                    
                    # Join all 'name' values
                    all_names = ' '.join(item.get('name', '') for item in tags if isinstance(item, dict))
                    
                    # Extract ad_id using regex
                    import re
                    matches = re.findall(r"ad_id\.(\w+)", all_names)
                    
                    # Return string representation of list (matching old system: .astype(str))
                    return str(matches)  # "['123456']" or "[]"
                except Exception:
                    return '[]'  # Empty list as string on error
            
            
            def extract_custom_fields(custom_fields, brand_config):
                """Extract specific custom field values using brand config
                
                Args:
                    custom_fields: List of custom field objects or JSON string
                    brand_config: Dict with custom_field_mappings JSON
                
                Returns:
                    Dict with extracted values: {
                        'ghi_chu': '...',
                        'id_value_phan_loai_ket_noi': '...',
                        'id_value_muc_do_hai_long': '...',
                        'id_value_dich_vu_chi_tiet': '...'
                    }
                """
                result = {
                    'ghi_chu': '',  # Default to empty string (matching old system)
                    'id_value_phan_loai_ket_noi': '',
                    'id_value_muc_do_hai_long': '',
                    'id_value_dich_vu_chi_tiet': '',
                }
                
                if not custom_fields:
                    return result
                
                try:
                    # Parse custom_fields if string
                    if isinstance(custom_fields, str):
                        custom_fields = json.loads(custom_fields)
                    
                    # Parse brand config mappings
                    mappings = {}
                    if brand_config.get('custom_field_mappings'):
                        try:
                            mappings = json.loads(brand_config['custom_field_mappings'])
                        except:
                            pass
                    
                    # If no mappings, return empty
                    if not mappings:
                        return result
                    
                    # Extract values by matching IDs
                    for item in custom_fields:
                        if not isinstance(item, dict):
                            continue
                        
                        item_id = item.get('id')
                        item_value = item.get('value')
                        
                        # Skip if no ID
                        if item_id is None:
                            continue
                        
                        # Convert None to empty string (matching old system behavior)
                        if item_value is None:
                            item_value = ''
                        
                        # Convert both to string for comparison
                        # This handles: int vs int, int vs str, str vs str, any number including 0
                        item_id_str = str(item_id)
                        
                        # Match against configured IDs (convert mapping values to str too)
                        if mappings.get('ghi_chu') is not None and item_id_str == str(mappings.get('ghi_chu')):
                            result['ghi_chu'] = item_value
                        elif mappings.get('phan_loai_ket_noi') is not None and item_id_str == str(mappings.get('phan_loai_ket_noi')):
                            result['id_value_phan_loai_ket_noi'] = item_value
                        elif mappings.get('muc_do_hai_long') is not None and item_id_str == str(mappings.get('muc_do_hai_long')):
                            result['id_value_muc_do_hai_long'] = item_value
                        elif mappings.get('dich_vu_chi_tiet') is not None and item_id_str == str(mappings.get('dich_vu_chi_tiet')):
                            result['id_value_dich_vu_chi_tiet'] = item_value
                    
                    return result
                    
                except Exception as e:
                    logger.warning(f"Error extracting custom fields: {e}")
                    return result
            
            def serialize_for_json(obj):
                """Convert date/datetime objects to ISO format strings for JSON serialization"""
                if isinstance(obj, datetime):
                    return obj.isoformat()
                elif isinstance(obj, date):
                    return obj.isoformat()
                return obj
            
            # Transform records to match schema (legacy cs_ticket_api fields)
            transformed_records = []
            ingestion_timestamp = datetime.now()  # Single timestamp for entire batch
            
            for ticket, fetch_type in all_records:
                # Extract custom field values using brand config
                extracted_custom_fields = extract_custom_fields(ticket.get("custom_fields", []), brand)
                
                record = {
                    # Core IDs (matching old table)
                    "ticket_id": ticket.get("ticket_id"),
                    "ticket_no": safe_int(ticket.get("ticket_no")),
                    "requester_id": ticket.get("requester_id"),
                    "assignee_id": ticket.get("assignee_id"),
                    "incident_id": safe_int(ticket.get("incident_id")),
                    "service_id": ticket.get("service_id"),
                    "campaign_id": safe_str(ticket.get("campaign_id")),
                    
                    # Status and classification (matching old table)
                    "ticket_status": safe_str(ticket.get("ticket_status")),
                    "ticket_priority": safe_str(ticket.get("ticket_priority")),
                    "ticket_source": safe_str(ticket.get("ticket_source")),
                    "ticket_source_end_status": safe_str(ticket.get("ticket_source_end_status")),
                    
                    # Text fields (matching old table)
                    "ticket_subject": safe_str(ticket.get("ticket_subject")),
                    "campaign": safe_str(ticket.get("campaign")),
                    "duedate": safe_str(ticket.get("duedate")),
                    
                    # Timestamps (matching old table)
                    "created_at": safe_ts(ticket.get("created_at")),
                    "updated_at": safe_ts(ticket.get("updated_at")),
                    
                    # JSON fields (matching old table - stored as JSON strings)
                    "custom_fields": json.dumps(ticket.get("custom_fields", []), ensure_ascii=False, default=serialize_for_json) if ticket.get("custom_fields") else None,
                    "assignee": json.dumps(ticket.get("assignee", {}), ensure_ascii=False, default=serialize_for_json) if ticket.get("assignee") else None,
                    "requester": json.dumps(ticket.get("requester", {}), ensure_ascii=False, default=serialize_for_json) if ticket.get("requester") else None,
                    "tags": json.dumps(ticket.get("tags", []), ensure_ascii=False, default=serialize_for_json) if ticket.get("tags") else None,
                    
                    # Brand and metadata (matching old table)
                    "brand_id": safe_int(brand["erp_id"]),  # From brand config, not ticket
                    "raw_data": json.dumps(ticket, ensure_ascii=False, default=serialize_for_json),
                    "ghi_chu": extracted_custom_fields['ghi_chu'],  # Extracted from custom_fields
                    
                    # Custom extracted fields (matching old table)
                    "id_value_phan_loai_ket_noi": extracted_custom_fields['id_value_phan_loai_ket_noi'],  # Extracted from custom_fields
                    "id_value_muc_do_hai_long": extracted_custom_fields['id_value_muc_do_hai_long'],  # Extracted from custom_fields
                    "ad_id_value": extract_ad_id(ticket.get("tags", [])),  # Extract from tags
                    "id_value_dich_vu_chi_tiet": extracted_custom_fields['id_value_dich_vu_chi_tiet'],  # Extracted from custom_fields
                    
                    # NEW: Metadata for Lakehouse (added here, not in original ticket)
                    "snapshot_date": self.snapshot_date,  # date object
                    "ingestion_timestamp": ingestion_timestamp,  # datetime object
                    "fetch_type": fetch_type,  # From tuple unpacking
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
