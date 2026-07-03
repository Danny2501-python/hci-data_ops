"""
Webhook Transformation THV: Silver transformation class specialized for THV (erp_id=8)
"""

import os
import sys
import json
import ast
from datetime import date, datetime, timedelta
import pyarrow as pa
from pyiceberg.expressions import EqualTo, And, Or, StartsWith
from loguru import logger
import gc
import duckdb

# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

import webhook_transformation_silver

# Override globals for THV
webhook_transformation_silver.SOURCE_TABLE = 'caresoft.wehook_api_caresoft_thv'
webhook_transformation_silver.TARGET_TABLE = 'caresoft.cs_deal_wehook_thv'
webhook_transformation_silver.TARGET_TABLE_ERROR = 'caresoft.cs_deal_wehook_error_thv'


class WebhookTransformationThv(webhook_transformation_silver.WebhookTransformation):
    """
    Webhook transformation specialized for THV (erp_id=8)
    """
    
    def parse_and_transform_batch(self, bronze_table):
        """
        Generator function that yields batches of (success_rows, error_ids)
        Uses batch streaming and DuckDB for efficient SQL-based transformation
        Overridden to support both JSON format and string literal dictionary formats in the input field
        """
        BATCH_SIZE = 20000
        
        logger.info(f"Scanning Bronze table caresoft.wehook_api_caresoft_thv for {self.snapshot_date}...")
        logger.info(f"Processing batch size: {BATCH_SIZE:,} records")
        
        # Support both Python representation and standard JSON representation
        scan = bronze_table.scan(
            row_filter=And(
                EqualTo("snapshot_date", self.snapshot_date),
                Or(
                    StartsWith("input", "{'deal'"),
                    StartsWith("input", '{"deal"')
                )
            ),
            selected_fields=("pk_id", "id", "input", "brand_etl")
        )
        
        def safe_eval(s):
            try:
                if not s or not s.strip():
                    return None
                
                # Check for standard JSON first
                try:
                    return json.loads(s)
                except Exception:
                    pass
                
                # Fallback to ast.literal_eval
                try:
                    data = ast.literal_eval(s)
                    if isinstance(data, dict) and 'deal' in data:
                        return data
                except Exception:
                    pass
                
                return None
            except:
                return None

        total_processed = 0
        
        # Configure DuckDB for Production (In-Memory, 2GB Limit)
        temp_dir = os.getenv('DUCKDB_TEMP_DIR', '/tmp/duckdb')
        
        duckdb_config = {
            'temp_directory': temp_dir,
            'memory_limit': '2GB',          # Safe limit for Pod 3GB
            'preserve_insertion_order': 'false', # Save RAM
            'threads': '1'                  # Single thread for stability on small Pods
        }
        
        con = duckdb.connect(config=duckdb_config)
        logger.info(f"🦆 DuckDB Configured: {duckdb_config}")
        
        accumulated_raw_data = []
        accumulated_errors = []
        
        EMPTY_JSON = json.dumps({}, ensure_ascii=False)
            
        for arrow_batch in scan.to_arrow_batch_reader():
            batch_size = len(arrow_batch)
            
            for i in range(batch_size):
                pk = arrow_batch["pk_id"][i].as_py()
                row_id = arrow_batch["id"][i].as_py()
                input_str = arrow_batch["input"][i].as_py()
                brand_etl = arrow_batch["brand_etl"][i].as_py()
                
                data_dict = safe_eval(input_str)
                if not data_dict:
                     accumulated_errors.append((pk, row_id, brand_etl))
                     continue
                
                try:
                    deal_info = data_dict.get('deal', {})
                    
                    def safe_str(val):
                        """Convert value to string, handling None and booleans"""
                        if val is None:
                            return None
                        if isinstance(val, bool):
                            return str(int(val))
                        return str(val) if val != '' else None
                    
                    def safe_json(val):
                        """Safely serialize value - handles dict, list, and primitives"""
                        if not val:
                            return EMPTY_JSON
                        if isinstance(val, (dict, list)):
                            return json.dumps(val, ensure_ascii=False)
                        return str(val)
                    
                    row = {
                        "pk_id": pk,
                        "id": row_id,
                        "deal_id": safe_str(deal_info.get("deal_id")),
                        "value": safe_str(deal_info.get("value")),
                        "probability": safe_str(deal_info.get("probability")),
                        "pipeline_id": safe_str(deal_info.get("pipeline_id")),
                        "pipeline_stage_id": safe_str(deal_info.get("pipeline_stage_id")),
                        "closed_type": safe_str(deal_info.get("closed_type")),
                        "source_detail_id": safe_str(deal_info.get("source_detail_id")),
                        "service_id": safe_str(deal_info.get("service_id")),
                        "requester_id": safe_str(deal_info.get("requester_id")),
                        "assignee_id": safe_str(deal_info.get("assignee_id")),
                        "ticket_source_end_status": safe_str(deal_info.get("ticket_source_end_status")),
                        
                        "created_at": safe_str(deal_info.get("created_at")),
                        "updated_at": safe_str(deal_info.get("updated_at")),
                        "last_change_status_at": safe_str(deal_info.get("last_change_status_at")),
                        "last_change_stage_at": safe_str(deal_info.get("last_change_stage_at")),
                        
                        "name": safe_str(deal_info.get("name")),
                        "source": safe_str(deal_info.get("source")),
                        "facebook_page_id": safe_str(deal_info.get("facebook_page_id")),
                        "activity": safe_str(deal_info.get("activity")),
                        "addition_fields": safe_str(deal_info.get("addition_fields")),
                        "gclid": safe_str(deal_info.get("gclid")),
                        "addition_details": safe_str(deal_info.get("addition_details")),
                        "comment": safe_str(deal_info.get("comment")),
                        "convert_type": safe_str(deal_info.get("convert_type")),
                        "convert_at": safe_str(deal_info.get("convert_at")),
                        "closed_by": safe_str(deal_info.get("closed_by")),
                        "closed_at": safe_str(deal_info.get("closed_at")),
                        "unqualified_reasons": safe_str(deal_info.get("unqualified_reasons")),
                        "lost_reasons": safe_str(deal_info.get("lost_reasons")),
                        "estimated_closed_date": safe_str(deal_info.get("estimated_closed_date")),
                        "utm_campaign": safe_str(deal_info.get("utm_campaign")),
                        
                        "call_info": safe_json(data_dict.get("call_info")),
                        "agent_submit": safe_json(data_dict.get("agent_submit")),
                        "updated_properties": safe_json(data_dict.get("updated_properties")),
                        "account_id": safe_json(data_dict.get("account_id")),
                        "activity_source": safe_json(data_dict.get("activity_source")),
                        "action": safe_json(data_dict.get("action")),
                        "convert_from_object": safe_json(data_dict.get("convert_from_object")),
                        "object": safe_json(data_dict.get("object")),
                        
                        "fb_psid": safe_str(deal_info.get("fb_psid")),
                        "csa_acc": safe_str(deal_info.get("csa_acc")),
                        "commentator_id": safe_str(deal_info.get("commentator_id")),
                        "zalo_page_id": safe_str(deal_info.get("zalo_page_id")),
                        "convert_by": safe_str(deal_info.get("convert_by")),
                        
                        "brand_etl": brand_etl,
                        "snapshot_date": self.snapshot_date
                    }
                    accumulated_raw_data.append(row)
                    
                    # Check and flush immediately after each append
                    if len(accumulated_raw_data) >= BATCH_SIZE:
                        logger.info(f"Processing accumulated batch of {len(accumulated_raw_data):,} records (total scanned so far: ~{total_processed + i + 1:,})")
                        
                        batch_success = None
                        if accumulated_raw_data:
                            try:
                                batch_success = self._transform_with_duckdb(con, accumulated_raw_data)
                            except Exception as e:
                                logger.error(f"DuckDB Transformation Error: {e}")
                                for row in accumulated_raw_data:
                                    accumulated_errors.append((row["pk_id"], row["id"], row["brand_etl"]))
 
                        yield batch_success, accumulated_errors
                        
                        accumulated_raw_data = []
                        accumulated_errors = []
                        gc.collect()
                        
                except Exception:
                    accumulated_errors.append((pk, row_id, brand_etl))
 
            total_processed += batch_size
            
            del arrow_batch
            gc.collect()
    
        if accumulated_raw_data or accumulated_errors:
            logger.info(
                f"Processing final batch: {len(accumulated_raw_data):,} records, "
                f"{len(accumulated_errors):,} accumulated errors"
            )
            
            batch_success = None
            if accumulated_raw_data:
                try:
                    batch_success = self._transform_with_duckdb(con, accumulated_raw_data)
                except Exception as e:
                    logger.error(f"DuckDB Transformation Error: {e}")
                    for row in accumulated_raw_data:
                         accumulated_errors.append((row["pk_id"], row["id"], row["brand_etl"]))
            
            yield batch_success, accumulated_errors
            
        con.close()
        logger.info(f"Completed processing {float(total_processed):,} total records")


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    start = datetime.now()
    job = WebhookTransformationThv(snapshot_date)
    job.run()
    logger.info(f"Total Duration: {(datetime.now() - start).total_seconds()}s")


if __name__ == "__main__":
    main()
