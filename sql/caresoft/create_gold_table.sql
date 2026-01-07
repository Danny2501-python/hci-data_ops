-- Gold Layer: Aggregated daily deal metrics
-- Tables: caresoft.daily_deal_metrics, caresoft.account_daily_summary
-- Purpose: Business-ready aggregated metrics for analytics

CREATE TABLE IF NOT EXISTS caresoft.daily_deal_metrics (
    snapshot_date DATE,
    account_id STRING,
    stage STRING,
    status STRING,
    activity_source STRING,
    total_deals BIGINT,
    total_deal_value DECIMAL(18,2),
    avg_deal_value DECIMAL(18,2),
    min_deal_value DECIMAL(18,2),
    max_deal_value DECIMAL(18,2),
    new_deals BIGINT,
    updated_deals BIGINT,
    processing_timestamp TIMESTAMP,
    record_count BIGINT
)
USING iceberg
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
);

COMMENT ON TABLE caresoft.daily_deal_metrics IS 
'Gold layer: Daily aggregated metrics for CareSoft deals.';


CREATE TABLE IF NOT EXISTS caresoft.account_daily_summary (
    snapshot_date DATE,
    account_id STRING,
    total_deals BIGINT,
    total_value DECIMAL(18,2),
    avg_value DECIMAL(18,2),
    processing_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
);

COMMENT ON TABLE caresoft.account_daily_summary IS 
'Gold layer: Daily summary metrics aggregated by account.';
