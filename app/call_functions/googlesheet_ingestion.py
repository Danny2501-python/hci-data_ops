"""
Google Sheet Ingestion to Bronze Iceberg Layer
Reads data from Google Sheets and ingests into Iceberg Bronze tables
Uses dedicated declare_api_googlesheet configuration table
"""

import os
import sys
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, FloatType, 
    DoubleType, BooleanType, DateType, TimestampType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.expressions import EqualTo, AlwaysTrue
from loguru import logger

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from common.googlesheet_config import get_active_googlesheet_configs
from common.utils import get_current_timestamp_naive

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# Google Sheets credentials
GOOGLE_CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH')


class GoogleSheetIngestion:
    """
    Ingest data from Google Sheets to Iceberg Bronze Layer
    """
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or (date.today() - timedelta(days=1))
        
        # Initialize Bronze catalog
        catalog_config = {
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY,
            "s3.secret-access-key": AWS_SECRET_KEY,
            "s3.region": AWS_REGION,
        }
        
        self.catalog = load_catalog(
            CATALOG_NAME_BRONZE,
            uri=ICEBERG_REST_URI_BRONZE,
            **catalog_config
        )
        
        # Initialize Google Sheets client
        self.gsheet_client = None
        
    def connect_google_sheets(self):
        """Connect to Google Sheets API"""
        try:
            scope = ['https://www.googleapis.com/auth/spreadsheets']
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                GOOGLE_CREDENTIALS_PATH, 
                scope
            )
            self.gsheet_client = gspread.authorize(credentials)
            logger.info("Connected to Google Sheets API")
        except Exception as e:
            logger.error(f"Failed to connect to Google Sheets: {e}")
            raise
    
    def read_google_sheet(self, spreadsheet_id: str, sheet_name: str, 
                         remove_column: Optional[str] = None,
                         rename_column: Optional[str] = None) -> pd.DataFrame:
        """
        Read data from Google Sheet (matching old logic exactly)
        
        Args:
            spreadsheet_id: Google Sheet ID
            sheet_name: Sheet name
            remove_column: Comma-separated list of columns to remove
            rename_column: JSON string mapping old column names to new names
        """
        if not self.gsheet_client:
            self.connect_google_sheets()
        
        try:
            logger.info(f"Reading Google Sheet: {spreadsheet_id}/{sheet_name}")
            
            # Open spreadsheet and worksheet
            spreadsheet = self.gsheet_client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
            
            # Get all values (matching old logic)
            data_matrix = worksheet.get_all_values()
            
            if not data_matrix or len(data_matrix) < 2:
                logger.warning(f"No data found in sheet {sheet_name}")
                return pd.DataFrame()
            
            # Lấy tiêu đề từ hàng đầu tiên
            expected_headers = data_matrix[0]
            
            # Lấy dữ liệu từ hàng thứ hai trở đi
            data_1f = data_matrix[1:]
            
            # Tạo DataFrame từ dữ liệu và tiêu đề
            df = pd.DataFrame(data_1f, columns=expected_headers)
            
            # Chuyển đổi các cột số về định dạng số ban đầu (matching old logic)
            for col in df.columns:
                try:
                    df[col] = df[col].astype(float)
                except ValueError:
                    continue
            
            # Remove columns if specified (CSV format)
            if remove_column and remove_column.strip():
                columns_list_key = remove_column.split(',')
                for column in columns_list_key:
                    if column in df.columns:
                        df = df.drop(columns=column)
                logger.info(f"Removed columns: {columns_list_key}")
            
            # Rename columns if specified (JSON format)
            if rename_column and rename_column.strip():
                try:
                    import json
                    column_mapping = json.loads(rename_column)
                    df = df.rename(columns=column_mapping)
                    logger.info(f"Renamed columns: {column_mapping}")
                except Exception as err:
                    logger.warning(f"Failed to parse rename_column JSON: {err}")
            
            # Xử lý data -> chuyển column thành lower (matching old logic)
            df.columns = df.columns.str.lower()
            
            # Remove empty columns
            df = df.loc[:, df.columns != '']
            
            logger.info(f"Read {len(df)} rows with columns: {df.columns.tolist()}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read Google Sheet: {e}")
            raise
    
    def infer_iceberg_schema(self, df: pd.DataFrame, table_name: str) -> Schema:
        """
        Infer Iceberg schema from pandas DataFrame
        Note: DataFrame should NOT have metadata columns yet
        """
        def get_iceberg_type(dtype):
            """Map pandas dtype to Iceberg type with proper precedence"""
            # Check bool BEFORE int (bool is subtype of int in pandas)
            if pd.api.types.is_bool_dtype(dtype):
                return BooleanType()
            # Check datetime before numeric (some timestamps can be numeric)
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                return TimestampType()
            # Check integer
            elif pd.api.types.is_integer_dtype(dtype):
                return IntegerType()
            # Check float
            elif pd.api.types.is_float_dtype(dtype):
                return DoubleType()
            # Default to string
            else:
                return StringType()
        
        # Infer fields from DataFrame columns (data only, no metadata)
        fields = []
        for i, (col_name, dtype) in enumerate(df.dtypes.items()):
            fields.append(NestedField(
                field_id=i + 1,
                name=col_name,
                field_type=get_iceberg_type(dtype),
                required=False  # All fields optional (PyArrow default)
            ))
        
        # Add metadata columns at the end
        next_id = len(fields) + 1
        fields.append(NestedField(
            field_id=next_id,
            name="snapshot_date",
            field_type=DateType(),
            required=False
        ))
        fields.append(NestedField(
            field_id=next_id + 1,
            name="ingestion_timestamp",
            field_type=TimestampType(),
            required=False
        ))
        
        return Schema(*fields)
    
    def ensure_table_exists(self, table_name: str, schema: Schema):
        """
        Create Iceberg table if it doesn't exist
        """
        full_table_name = f'call_functions.{table_name}'
        
        try:
            table = self.catalog.load_table(full_table_name)
            logger.info(f"Table {full_table_name} already exists")
            return table
        except Exception:
            logger.info(f"Creating table {full_table_name}")
            
            # Create partition spec (partition by snapshot_date)
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=schema.find_field("snapshot_date").field_id,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            table = self.catalog.create_table(
                identifier=full_table_name,
                schema=schema,
                partition_spec=partition_spec
            )
            
            logger.info(f"Created table {full_table_name}")
            return table
    
    def write_to_iceberg(self, table_name: str, df: pd.DataFrame, 
                        method_data: bool = False):
        """
        Write DataFrame to Iceberg table
        
        Args:
            table_name: Target table name
            df: DataFrame to write (WITHOUT metadata columns)
            method_data: If True, truncate (delete all) before writing
        """
        if df.empty:
            logger.warning(f"No data to write for {table_name}")
            return
        
        # Infer schema from data columns BEFORE adding metadata
        schema = self.infer_iceberg_schema(df, table_name)
        table = self.ensure_table_exists(table_name, schema)
        
        # Add metadata columns AFTER schema inference
        df['snapshot_date'] = self.snapshot_date
        df['ingestion_timestamp'] = get_current_timestamp_naive()
        
        # Convert to PyArrow Table
        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        
        try:
            if method_data:
                # method_data=True means TRUNCATE (delete all data)
                logger.warning(f"Truncating table {table_name} (method_data=True)")
                table.delete(delete_filter=AlwaysTrue())
            else:
                # Delete existing data for this snapshot_date only
                logger.info(f"Deleting existing data for {self.snapshot_date}")
                table.delete(delete_filter=EqualTo("snapshot_date", self.snapshot_date))
            
            # Update schema if new columns exist (schema evolution)
            try:
                from pyiceberg.table import UpdateSchema
                with table.update_schema() as update:
                    for field in schema.fields:
                        if field.name not in [f.name for f in table.schema().fields]:
                            update.add_column(field.name, field.field_type, required=field.required)
                            logger.info(f"Added new column to schema: {field.name}")
            except Exception as schema_err:
                logger.debug(f"Schema update skipped: {schema_err}")
            
            # Append new data
            logger.info(f"Writing {len(df)} rows to {table_name}")
            table.append(arrow_table)
            
            logger.success(f"✅ Successfully wrote {len(df)} rows to {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to write to Iceberg: {e}")
            raise
    
    def process_sheet(self, config: Dict):
        """
        Process a single Google Sheet configuration
        """
        sheet_id = config['id']
        name = config['name']
        spreadsheet_id = config.get('spreadsheet_id')
        sheet_name = config.get('sheet')
        table_data = config.get('table_data')  # Target table name
        
        if not spreadsheet_id or not sheet_name or not table_data:
            logger.warning(f"Missing required fields for sheet {name} (ID: {sheet_id})")
            return
        
        # Generate Google Sheet link for easy access
        sheet_link = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        logger.info(f"📊 Processing Google Sheet: {name} -> {table_data}")
        logger.info(f"🔗 Sheet Link: {sheet_link}")
        logger.info(f"📄 Sheet Name: {sheet_name}")
        
        # Parse configurations (matching old schema)
        remove_column = config.get('remove_column')
        rename_column = config.get('rename_column')
        method_data = config.get('method_data', False)
        
        # Log transformations
        if remove_column:
            logger.info(f"🗑️  Remove columns: {remove_column}")
        if rename_column:
            logger.info(f"✏️  Rename columns: {rename_column}")
        if method_data:
            logger.warning(f"⚠️  Truncate mode: Will delete all existing data")
        
        try:
            # Read from Google Sheet
            df = self.read_google_sheet(
                spreadsheet_id=spreadsheet_id,
                sheet_name=sheet_name,
                remove_column=remove_column,
                rename_column=rename_column
            )
            
            if df.empty:
                logger.warning(f"No data to process for {table_data}")
                return
            
            # Write to Iceberg
            self.write_to_iceberg(
                table_name=table_data,
                df=df,
                method_data=method_data
            )
            
        except Exception as e:
            logger.error(f"Failed to process sheet {name}: {e}")
            raise
    
    def get_sheet_configs(self, sheet_id: Optional[int] = None) -> List[Dict]:
        """
        Read Google Sheet configurations (only IN direction)
        """
        configs = get_active_googlesheet_configs(sheet_id=sheet_id, loai='IN')
        logger.info(f"Loaded {len(configs)} Google Sheet configurations")
        return configs
    
    def run(self, sheet_id: Optional[int] = None):
        """Run Google Sheet ingestion"""
        logger.info(f"Starting Google Sheet Ingestion for {self.snapshot_date}")
        
        configs = self.get_sheet_configs(sheet_id=sheet_id)
        
        if not configs:
            logger.warning("No active Google Sheet configurations found")
            return
        
        for config in configs:
            try:
                self.process_sheet(config)
            except Exception as e:
                logger.error(f"Failed to process {config['name']}: {e}")


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    sheet_id = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] else None
    
    try:
        job = GoogleSheetIngestion(snapshot_date)
        job.run(sheet_id)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
