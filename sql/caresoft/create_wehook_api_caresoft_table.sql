-- Create Iceberg table: caresoft.wehook_api_caresoft
-- This table matches the PostgreSQL wehook_api_caresoft schema
-- with additional columns for lakehouse metadata

CREATE TABLE IF NOT EXISTS bronze.caresoft.wehook_api_caresoft (
    -- Auto-generated primary key (MD5 hash)
    pk_id BIGINT COMMENT 'Auto-generated unique ID (MD5 hash of id+brand_etl+dates)',
    
    -- Original fields from ERP api_log table
    id INT COMMENT 'Original ID from ERP api_log',
    name STRING COMMENT 'Webhook name',
    create_uid INT COMMENT 'User ID who created the record',
    create_date TIMESTAMP COMMENT 'Creation timestamp',
    write_uid INT COMMENT 'User ID who last modified the record',
    write_date TIMESTAMP COMMENT 'Last modification timestamp',
    type_log STRING COMMENT 'Type of log entry',
    model_id INT COMMENT 'Model ID reference',
    id_record INT COMMENT 'Record ID reference',
    input STRING COMMENT 'Input data (JSON)',
    response STRING COMMENT 'Response data (JSON)',
    status_code INT COMMENT 'HTTP status code',
    url STRING COMMENT 'API endpoint URL',
    header STRING COMMENT 'HTTP headers',
    
    -- Brand identifier (to distinguish data from different ERPs)
    brand_etl INT COMMENT 'Brand ETL ID: 7=SCI, 1=KN, 3=PR, 4=HH',
    
    -- Lakehouse metadata
    snapshot_date DATE COMMENT 'Snapshot date for partitioning',
    ingestion_timestamp TIMESTAMP COMMENT 'Timestamp when data was ingested into lakehouse'
)
USING iceberg
PARTITIONED BY (snapshot_date, brand_etl)
COMMENT 'Bronze layer: Raw webhook data from 4 ERP systems (SCI, KN, PR, HH)'
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy'
);

-- Create indexes for common queries
-- Note: Iceberg uses metadata for filtering, no explicit indexes needed
