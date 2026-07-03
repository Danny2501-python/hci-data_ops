"""
Deal Comments Transformation
Parse comments JSON from cs_deal_detail_api and write to cs_deal_api_trans_comments

Flow:
1. Read from gold.caresoft.cs_deal_detail_api (deals with comments JSON)
2. Parse comments field (JSON array)
3. Flatten to individual comment records
4. Write to gold.caresoft.cs_deal_api_trans_comments
"""

import os
import sys
from datetime import datetime, date, timedelta
from typing import List, Dict
import json
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual, EqualTo, And
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, LongType, IntegerType, StringType, TimestampType, DateType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from loguru import logger
from common.utils import get_current_timestamp_naive
import gc

# Iceberg configuration
ICEBERG_REST_URI_GOLD = os.getenv('ICEBERG_REST_URI_GOLD')
CATALOG_NAME_GOLD = os.getenv('CATALOG_NAME_GOLD')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

SOURCE_TABLE = 'caresoft.cs_deal_detail_api'
TARGET_TABLE = 'caresoft.cs_deal_api_trans_comments'


class DealCommentsTransformation:
    """
    Transform deal comments from JSON to flat table
    """
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or (date.today() - timedelta(days=1))
        
        # Initialize Gold catalog
        catalog_config = {
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY,
            "s3.secret-access-key": AWS_SECRET_KEY,
            "s3.region": AWS_REGION,
        }
        
        self.gold_catalog = load_catalog(
            CATALOG_NAME_GOLD,
            uri=ICEBERG_REST_URI_GOLD,
            **catalog_config
        )
        
        logger.info(f"Initialized DealCommentsTransformation for {self.snapshot_date}")
    
    def ensure_comments_table_exists(self):
        """
        Create comments table if not exists
        Schema matches legacy cs_deal_api_trans_comments
        """
        try:
            table = self.gold_catalog.load_table(TARGET_TABLE)
            logger.info(f"Table {TARGET_TABLE} exists")
            return table
        except:
            logger.info(f"Creating table {TARGET_TABLE}...")
            
            # Schema matching legacy table + brand_id
            schema = Schema(
                NestedField(1, "ticket_comments_id", LongType(), required=False),
                NestedField(2, "ticket_id", LongType(), required=False),
                NestedField(3, "comment", StringType(), required=False),
                NestedField(4, "commentator_id", LongType(), required=False),
                NestedField(5, "username", StringType(), required=False),
                NestedField(6, "facebook", StringType(), required=False),
                NestedField(7, "facebook_name", StringType(), required=False),
                NestedField(8, "comment_source", StringType(), required=False),
                NestedField(9, "created_at", TimestampType(), required=False),
                NestedField(10, "attack_file_id", StringType(), required=False),
                NestedField(11, "file_name", StringType(), required=False),
                NestedField(12, "file_id", StringType(), required=False),
                NestedField(13, "download_connection_string", StringType(), required=False),
                NestedField(14, "call_id", StringType(), required=False),
                NestedField(15, "email_receive_id", StringType(), required=False),
                NestedField(16, "addition_details", StringType(), required=False),
                NestedField(17, "is_public", IntegerType(), required=False),
                NestedField(18, "can_hide", StringType(), required=False),
                NestedField(19, "can_remove", StringType(), required=False),
                NestedField(20, "can_reply_privately", StringType(), required=False),
                NestedField(21, "facebook_comment_state", IntegerType(), required=False),
                NestedField(22, "role_id", IntegerType(), required=False),
                NestedField(23, "avatar", StringType(), required=False),
                NestedField(24, "path", StringType(), required=False),
                NestedField(25, "file_path", StringType(), required=False),
                NestedField(26, "brand_id", IntegerType(), required=False),  # Brand identifier
                
                # Metadata
                NestedField(100, "snapshot_date", DateType(), required=False),
                NestedField(101, "ingestion_timestamp", TimestampType(), required=False),
            )
            
            # Partition by snapshot_date
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=100,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            table = self.gold_catalog.create_table(
                identifier=TARGET_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            
            logger.success(f"✅ Created table: {TARGET_TABLE}")
            return table
    
    def get_deals_with_comments(self) -> pd.DataFrame:
        """
        Read deals with comments for current snapshot_date
        Process only deals from the same snapshot as deal_detail_enrichment
        """
        source_table = self.gold_catalog.load_table(SOURCE_TABLE)
        
        logger.info(f"Reading deals for snapshot_date = {self.snapshot_date}...")
        
        # Scan for deals in current snapshot with comments
        scan = source_table.scan(
            row_filter=EqualTo("snapshot_date", self.snapshot_date),
            selected_fields=("id", "brand_id", "comments", "snapshot_date")  # Include id and brand_id
        )
        
        # Convert to pandas
        df = scan.to_pandas()
        
        # Filter out deals without comments
        df = df[df['comments'].notna()]
        df = df[df['comments'] != '']
        df = df[df['comments'] != '[]']
        
        logger.info(f"Found {len(df)} deals with comments in snapshot {self.snapshot_date}")
        return df
    
    def parse_comments(self, deals_df: pd.DataFrame) -> List[Dict]:
        """
        Parse comments JSON from each deal
        Add brand_id and ticket_id to each comment
        """
        all_comments = []
        errors = 0
        
        for idx, row in deals_df.iterrows():
            comments_json = row['comments']
            deal_id = row.get('id')  # ticket_id from deal
            brand_id = row.get('brand_id')  # brand_id from deal
            
            try:
                # Parse JSON (handle both string and already-parsed)
                if isinstance(comments_json, str):
                    # Try ast.literal_eval first (for Python literal strings)
                    try:
                        import ast
                        comments = ast.literal_eval(comments_json)
                    except:
                        # Fall back to json.loads
                        comments = json.loads(comments_json)
                else:
                    comments = comments_json
                
                # Ensure it's a list
                if not isinstance(comments, list):
                    logger.warning(f"Row {idx}: comments is not a list")
                    continue
                
                # Add each comment
                for comment in comments:
                    if isinstance(comment, dict):
                        # Add deal context
                        if 'ticket_id' not in comment or not comment['ticket_id']:
                            comment['ticket_id'] = deal_id
                        comment['brand_id'] = brand_id
                        
                        # Add metadata
                        comment['snapshot_date'] = self.snapshot_date
                        comment['ingestion_timestamp'] = get_current_timestamp_naive()
                        all_comments.append(comment)
                
            except Exception as e:
                errors += 1
                if errors <= 5:  # Log first 5 errors
                    logger.warning(f"Failed to parse comments for row {idx}: {e}")
        
        logger.info(f"Parsed {len(all_comments)} comments ({errors} errors)")
        return all_comments
    
    def write_comments(self, comments: List[Dict]):
        """
        Write comments to Iceberg table
        """
        if not comments:
            logger.warning("No comments to write")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(comments)
        
        # Ensure all expected columns exist
        expected_columns = [
            'ticket_comments_id', 'ticket_id', 'comment', 'commentator_id',
            'username', 'facebook', 'facebook_name', 'comment_source',
            'created_at', 'attack_file_id', 'file_name', 'file_id',
            'download_connection_string', 'call_id', 'email_receive_id',
            'addition_details', 'is_public', 'can_hide', 'can_remove',
            'can_reply_privately', 'facebook_comment_state', 'role_id',
            'avatar', 'path', 'file_path', 'brand_id',
            'snapshot_date', 'ingestion_timestamp'
        ]
        
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None
        
        # Select only expected columns
        df = df[expected_columns]
        
        # Type conversions
        # Convert numeric columns (Long)
        for col in ['ticket_comments_id', 'ticket_id', 'commentator_id']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        
        # Convert numeric columns (Integer)
        for col in ['is_public', 'facebook_comment_state', 'role_id', 'brand_id']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int32')
        
        # Convert string columns (explicitly convert to string to avoid PyArrow conversion errors)
        string_columns = [
            'comment', 'username', 'facebook', 'facebook_name', 'comment_source',
            'attack_file_id', 'file_name', 'file_id', 'download_connection_string',
            'call_id', 'email_receive_id', 'addition_details', 'can_hide', 'can_remove',
            'can_reply_privately', 'avatar', 'path', 'file_path'
        ]
        for col in string_columns:
            if col in df.columns:
                # Convert to string, but keep None/NaN as None (not string 'nan')
                df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else None)
        
        # Convert timestamp
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        
        # Ensure table exists
        table = self.ensure_comments_table_exists()
        
        # Delete old partition
        try:
            logger.info(f"Deleting partition for {self.snapshot_date}...")
            table.delete(
                delete_filter=EqualTo("snapshot_date", self.snapshot_date)
            )
        except Exception as e:
            logger.warning(f"Could not delete partition: {e}")
        
        # Write to Iceberg
        try:
            pyarrow_schema = schema_to_pyarrow(table.schema())
            arrow_table = pa.Table.from_pandas(df, schema=pyarrow_schema)
            
            table.append(arrow_table)
            logger.success(f"✅ Wrote {len(df)} comments to {TARGET_TABLE}")
            
        except Exception as e:
            logger.error(f"Failed to write comments: {e}")
            raise
    
    def run(self):
        """
        Execute comments transformation for current snapshot_date
        Process only deals from the same snapshot as deal_detail_enrichment
        """
        logger.info("="*80)
        logger.info(f"Starting Deal Comments Transformation for {self.snapshot_date}")
        logger.info("="*80)
        
        try:
            # Always ensure target table exists first
            self.ensure_comments_table_exists()
            
            # 1. Get deals with comments (same snapshot_date)
            deals_df = self.get_deals_with_comments()
            
            if deals_df.empty:
                logger.info(f"No deals with comments found for snapshot {self.snapshot_date}")
                return
            
            # 2. Parse comments
            comments = self.parse_comments(deals_df)
            
            # 3. Write to table
            self.write_comments(comments)
            
            logger.success("✅ Deal comments transformation completed")
            
        except Exception as e:
            logger.error(f"❌ Deal comments transformation failed: {e}")
            raise
        finally:
            gc.collect()


def main():
    """Main execution function"""
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Deal Comments Transformation for {snapshot_date}")
    
    try:
        job = DealCommentsTransformation(snapshot_date)
        job.run()
        
        logger.success("✅ Deal comments transformation completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Deal comments transformation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
