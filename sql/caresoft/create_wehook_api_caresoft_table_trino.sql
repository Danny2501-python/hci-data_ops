-- Create Iceberg table via Trino: caresoft.wehook_api_caresoft
-- This table matches the PostgreSQL wehook_api_caresoft schema
-- Run this in Trino CLI or DBeaver connected to Trino

CREATE TABLE IF NOT EXISTS bronze.caresoft.wehook_api_caresoft (
    -- Auto-generated primary key (MD5 hash)
    pk_id BIGINT COMMENT 'Auto-generated unique ID (MD5 hash of id+brand_etl+dates)',
    
    -- Original fields from ERP api_log table
    id INTEGER COMMENT 'Original ID from ERP api_log',
    name VARCHAR COMMENT 'Webhook name',
    create_uid INTEGER COMMENT 'User ID who created the record',
    create_date TIMESTAMP(6) COMMENT 'Creation timestamp',
    write_uid INTEGER COMMENT 'User ID who last modified the record',
    write_date TIMESTAMP(6) COMMENT 'Last modification timestamp',
    type_log VARCHAR COMMENT 'Type of log entry',
    model_id INTEGER COMMENT 'Model ID reference',
    id_record INTEGER COMMENT 'Record ID reference',
    input VARCHAR COMMENT 'Input data (JSON)',
    response VARCHAR COMMENT 'Response data (JSON)',
    status_code INTEGER COMMENT 'HTTP status code',
    url VARCHAR COMMENT 'API endpoint URL',
    header VARCHAR COMMENT 'HTTP headers',
    
    -- Brand identifier (to distinguish data from different ERPs)
    brand_etl INTEGER COMMENT 'Brand ETL ID: 7=SCI, 1=KN, 3=PR, 4=HH',
    
    -- Lakehouse metadata
    snapshot_date DATE COMMENT 'Snapshot date for partitioning',
    ingestion_timestamp TIMESTAMP(6) COMMENT 'Timestamp when data was ingested into lakehouse'
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['snapshot_date', 'brand_etl'],
    location = 's3://bronze-zone/caresoft/wehook_api_caresoft'
);

-- Verify table created
SHOW CREATE TABLE bronze.caresoft.wehook_api_caresoft;

-- Check table properties
SELECT * FROM bronze."caresoft$properties" WHERE table_name = 'wehook_api_caresoft';
