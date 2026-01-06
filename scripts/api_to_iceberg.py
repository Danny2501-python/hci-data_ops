#!/usr/bin/env python3
"""
Example: API to Iceberg using pyiceberg
Fetch data from API and write to Iceberg table on MinIO
"""
import os
import sys
from datetime import datetime
import pandas as pd
import pyarrow as pa
import requests
from pyiceberg.catalog import load_catalog
from loguru import logger

# Configure logger
logger.remove()
logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")

# Configuration from environment variables
API_URL = os.getenv('API_URL', 'https://jsonplaceholder.typicode.com/posts')
ICEBERG_REST_URI = os.getenv('ICEBERG_REST_URI', 'http://iceberg-rest-bronze-zone:8181')
S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://10.152.183.65:9000')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'admin')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'password')
CATALOG_NAME = os.getenv('CATALOG_NAME', 'bronze')
TABLE_NAME = os.getenv('TABLE_NAME', 'api_data.posts')


def extract_api_data(url: str) -> pd.DataFrame:
    """Extract data from API"""
    logger.info(f"Fetching data from: {url}")
    
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    df = pd.DataFrame(data)
    
    logger.info(f"✅ Fetched {len(df)} records")
    return df


def transform_data(df: pd.DataFrame, execution_date: str) -> pd.DataFrame:
    """Transform data"""
    logger.info("Transforming data...")
    
    # Add metadata columns
    df['extracted_at'] = pd.Timestamp.now()
    df['execution_date'] = execution_date
    
    # Data cleaning (example)
    df = df[df['userId'] > 0]  # Filter invalid records
    
    logger.info(f"✅ Transformed {len(df)} records")
    return df


def create_table_if_not_exists(catalog, table_name: str):
    """Create Iceberg table if it doesn't exist"""
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType, IntegerType, TimestampType
    
    try:
        # Try to load existing table
        table = catalog.load_table(table_name)
        logger.info(f"✅ Table already exists: {table_name}")
        return table
    except Exception:
        # Table doesn't exist, create it
        logger.info(f"Table not found, creating: {table_name}")
        
        # Parse namespace and table name
        parts = table_name.split('.')
        if len(parts) != 2:
            raise ValueError(f"Invalid table name format: {table_name}. Expected: namespace.table")
        
        namespace, table = parts[0], parts[1]
        
        # Create namespace if not exists
        try:
            catalog.create_namespace(namespace)
            logger.info(f"✅ Created namespace: {namespace}")
        except Exception as e:
            logger.debug(f"Namespace already exists: {e}")
        
        # Define schema for API data
        schema = Schema(
            NestedField(1, "userId", IntegerType(), required=False),
            NestedField(2, "id", IntegerType(), required=False),
            NestedField(3, "title", StringType(), required=False),
            NestedField(4, "body", StringType(), required=False),
            NestedField(5, "extracted_at", TimestampType(), required=False),
            NestedField(6, "execution_date", StringType(), required=False),
        )
        
        # Create table
        location = f"s3://bronze-zone/{namespace}/{table}"
        created_table = catalog.create_table(
            table_name,
            schema=schema,
            location=location
        )
        
        logger.success(f"✅ Created table: {table_name}")
        logger.info(f"   Location: {created_table.location()}")
        
        return created_table


def load_to_iceberg(df: pd.DataFrame, catalog_name: str, table_name: str):
    """Load data to Iceberg table on MinIO"""
    logger.info(f"Loading to Iceberg table: {catalog_name}.{table_name}")
    
    # Load catalog
    catalog = load_catalog(
        catalog_name,
        **{
            "uri": ICEBERG_REST_URI,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY,
            "s3.secret-access-key": AWS_SECRET_KEY,
        }
    )
    
    # Create table if not exists
    table = create_table_if_not_exists(catalog, table_name)
    
    # Convert pandas DataFrame to PyArrow Table with proper schema
    logger.info("Converting pandas DataFrame to PyArrow Table...")
    
    # Cast int columns to int32 to match Iceberg schema (IntegerType = int32)
    df['userId'] = df['userId'].astype('int32')
    df['id'] = df['id'].astype('int32')
    
    # Define PyArrow schema matching Iceberg table
    arrow_schema = pa.schema([
        pa.field('userId', pa.int32()),
        pa.field('id', pa.int32()),
        pa.field('title', pa.string()),
        pa.field('body', pa.string()),
        pa.field('extracted_at', pa.timestamp('us')),
        pa.field('execution_date', pa.string()),
    ])
    
    # Convert to PyArrow table with explicit schema
    arrow_table = pa.Table.from_pandas(df, schema=arrow_schema)
    
    # Append data
    logger.info(f"Appending {len(df)} rows...")
    table.append(arrow_table)
    
    logger.info(f"✅ Loaded {len(df)} rows to {table_name}")
    logger.info(f"Table location: {table.location()}")


def main():
    """Main ETL pipeline"""
    # Get execution date from command line or use today
    execution_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    
    logger.info("=" * 60)
    logger.info("Starting API to Iceberg ETL")
    logger.info("=" * 60)
    logger.info(f"Execution date: {execution_date}")
    logger.info(f"API URL: {API_URL}")
    logger.info(f"Iceberg Catalog: {CATALOG_NAME}")
    logger.info(f"Target Table: {TABLE_NAME}")
    
    try:
        # Extract
        df = extract_api_data(API_URL)
        
        # Transform
        df = transform_data(df, execution_date)
        
        # Load
        load_to_iceberg(df, CATALOG_NAME, TABLE_NAME)
        
        logger.info("=" * 60)
        logger.success("✅ ETL completed successfully!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"❌ ETL failed: {e}")
        logger.error("=" * 60)
        raise


if __name__ == "__main__":
    main()
