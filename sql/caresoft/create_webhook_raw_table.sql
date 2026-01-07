-- Bronze Layer: Raw ERP webhook data
-- Table: caresoft.webhook_raw (in bronze catalog)
-- Purpose: Store all raw webhook data from PostgreSQL wehook_api_caresoft table

CREATE TABLE IF NOT EXISTS caresoft.webhook_raw (
    pk_id BIGINT COMMENT 'Auto-generated unique primary key (MD5 hash)',
    id INT COMMENT 'Original webhook ID from PostgreSQL',
    name STRING COMMENT 'Webhook name',
    create_uid INT COMMENT 'User ID who created the record',
    create_date TIMESTAMP COMMENT 'Record creation timestamp',
    write_uid INT COMMENT 'User ID who last modified the record',
    write_date TIMESTAMP COMMENT 'Last modification timestamp',
    type_log STRING COMMENT 'Log type',
    model_id INT COMMENT 'Model ID',
    id_record INT COMMENT 'Record ID',
    input STRING COMMENT 'Raw JSON input payload',
    response STRING COMMENT 'Response data',
    status_code INT COMMENT 'HTTP status code',
    url STRING COMMENT 'Webhook URL',
    header STRING COMMENT 'HTTP headers',
    brand_etl INT COMMENT 'Brand/tenant identifier',
    snapshot_date DATE COMMENT 'Date of ETL snapshot (partition key)',
    ingestion_timestamp TIMESTAMP COMMENT 'Timestamp when data was ingested into lakehouse'
)
USING iceberg
PARTITIONED BY (snapshot_date, brand_etl)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
);

COMMENT ON TABLE caresoft.webhook_raw IS 
'Bronze layer: Raw ERP webhook data from PostgreSQL wehook_api_caresoft table. 
This is the landing zone for all webhook data before processing.
PK_ID is auto-generated using MD5 hash for uniqueness.';
