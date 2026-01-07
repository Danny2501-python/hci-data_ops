-- Bronze Layer: Raw webhook snapshots from PostgreSQL
-- Table: caresoft.webhook_snapshots (in bronze catalog)
-- Purpose: Store daily snapshots of wehook_api_caresoft table

-- Note: This SQL is for reference. Tables should be created via PyIceberg or Spark
-- For PyIceberg table creation, see Python scripts

CREATE TABLE IF NOT EXISTS caresoft.webhook_snapshots (
    pk_id BIGINT,
    id BIGINT,
    input STRING,
    created_at TIMESTAMP,
    snapshot_date DATE,
    ingestion_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
);

-- Table comment
COMMENT ON TABLE caresoft.webhook_snapshots IS 
'Bronze layer: Daily snapshots of CareSoft webhook data from PostgreSQL wehook_api_caresoft table.';
