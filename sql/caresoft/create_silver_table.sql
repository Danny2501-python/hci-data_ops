-- Silver Layer: Parsed and cleaned CareSoft deal data
-- Table: caresoft.deals (in bronze catalog - using same catalog for simplicity)
-- Purpose: Store parsed JSON data with proper schema

CREATE TABLE IF NOT EXISTS caresoft.deals (
    pk_id BIGINT,
    webhook_id BIGINT,
    deal_id BIGINT,
    account_id STRING,
    activity_source STRING,
    action STRING,
    object STRING,
    convert_from_object STRING,
    stage STRING,
    status STRING,
    deal_name STRING,
    deal_value DECIMAL(18,2),
    currency STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    last_change_status_at TIMESTAMP,
    last_change_stage_at TIMESTAMP,
    activity STRING,
    addition_fields STRING,
    call_info STRING,
    agent_submit STRING,
    updated_properties STRING,
    snapshot_date DATE,
    ingestion_timestamp TIMESTAMP,
    processing_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (snapshot_date, account_id)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
);

COMMENT ON TABLE caresoft.deals IS 
'Silver layer: Parsed and cleaned CareSoft deal data from Bronze webhook snapshots.';
