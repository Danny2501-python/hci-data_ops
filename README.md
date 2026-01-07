# CareSoft Lakehouse ETL Pipeline

## Overview
Complete ETL pipeline for CareSoft data using **Medallion Architecture** with **2 separate flows**:

**Flow 1**: ERP Systems → PostgreSQL → Iceberg Bronze (Raw Webhooks)  
**Flow 2**: Iceberg Bronze → Silver → Gold (Deal Processing & Analytics)

**Tech Stack:**
- **Orchestration**: Apache Airflow with KubernetesPodOperator
- **Storage**: Apache Iceberg on MinIO
- **Processing**: Python + PyIceberg + Pandas
- **Container**: `111050/python-etl-base:1.0.0`
- **Code Deployment**: NFS mount

## Architecture

### Complete Data Flow
```
ERP Systems (SCI, KN, HH, PR)
    ↓ [Dev_huonghh_ETL_ERP_Thuonghieu.py - Existing Airflow DAG]
PostgreSQL (wehook_api_caresoft)
    ↓ [Flow 1: erp_webhook_ingestion.py]
Bronze: caresoft.webhook_raw (All raw webhooks with PK_ID)
    ↓ [Flow 2: bronze_ingestion.py - Filter for deals]
Bronze: caresoft.webhook_snapshots (Daily deal snapshots)
    ↓ [silver_transformation.py]
Silver: caresoft.deals (Parsed & cleaned deals)
    ↓ [gold_aggregation.py]
Gold: caresoft.daily_deal_metrics (Business metrics)
```

### Flow 1: ERP Webhook Raw Ingestion
**Purpose**: Ingest ALL webhook data from PostgreSQL to Iceberg  
**Frequency**: Daily (incremental)  
**Script**: `erp_webhook_ingestion.py`

- **Source**: PostgreSQL `wehook_api_caresoft` table
- **Target**: Iceberg `caresoft.webhook_raw`
- **Features**:
  - Auto-generates `pk_id` using MD5 hash (ensures uniqueness)
  - Incremental loading (only new/updated records)
  - Preserves all fields from PostgreSQL
  - Partitioned by `snapshot_date` and `brand_etl`

### Flow 2: Deal Processing Pipeline
**Purpose**: Process deal-related webhooks into analytics-ready data  
**Frequency**: Daily  
**Scripts**: `bronze_ingestion.py` → `silver_transformation.py` → `gold_aggregation.py`

#### Bronze Snapshot Layer
- **Source**: Iceberg `caresoft.webhook_raw`
- **Target**: Iceberg `caresoft.webhook_snapshots`
- **Processing**: Filter for deal-related webhooks only
- **Partitioning**: `snapshot_date`

#### Silver Layer
- **Source**: Bronze `caresoft.webhook_snapshots`
- **Target**: Iceberg `caresoft.deals`
- **Processing**: JSON parsing, data quality checks, schema normalization
- **Partitioning**: `snapshot_date` + `account_id`

#### Gold Layer
- **Source**: Silver `caresoft.deals`
- **Targets**: 
  - `caresoft.daily_deal_metrics` (detailed metrics)
  - `caresoft.account_daily_summary` (account summaries)
- **Processing**: Aggregations, business metrics calculation
- **Partitioning**: `snapshot_date`

## Directory Structure
```
data-ops/
├── app/
│   └── caresoft/
│       ├── __init__.py
│       ├── erp_webhook_ingestion.py    # Flow 1: PostgreSQL → Bronze Raw
│       ├── bronze_ingestion.py         # Flow 2: Bronze Raw → Snapshots
│       ├── silver_transformation.py    # Flow 2: Snapshots → Silver
│       └── gold_aggregation.py         # Flow 2: Silver → Gold
├── dags/
│   └── caresoft_lakehouse_dag.py       # Airflow orchestration (2 flows)
├── sql/
│   └── caresoft/
│       ├── create_webhook_raw_table.sql      # Bronze raw table
│       ├── create_bronze_table.sql           # Bronze snapshot table
│       ├── create_silver_table.sql           # Silver deals table
│       └── create_gold_table.sql             # Gold metrics tables
└── README.md
```

## Configuration

### Environment Variables (Set in Airflow DAG)
```bash
# PostgreSQL Source
POSTGRES_HOST=postgresql.default.svc.cluster.local
POSTGRES_PORT=5432
POSTGRES_DB=custom_db_dev
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password

# Iceberg Catalog
ICEBERG_REST_URI=http://iceberg-rest:8181
CATALOG_NAME=bronze

# MinIO/S3
S3_ENDPOINT=http://minio:9000
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=password

# NFS Mount
NFS_SERVER=10.10.10.245
NFS_PATH=/srv/nfs_share/app
```

## DAG Execution Flow

### Task Dependencies
```
start
  ├─> erp_webhook_ingestion ────────────────> end
  └─> bronze_snapshot ─> silver ─> gold ─> end
```

**Parallel Execution**:
- Flow 1 (ERP ingestion) and Flow 2 (Deal processing) run in parallel
- Both flows converge at the end task

### Schedule
- **Frequency**: Daily at 2:00 AM (UTC+7)
- **Catchup**: False (only current snapshot)
- **Retries**: 3
- **Retry Delay**: 5 minutes

## Deployment

### 1. Deploy Scripts to NFS
```bash
# Copy all CareSoft scripts to NFS
cp -r app/caresoft /srv/nfs_share/app/

# Verify
ls -la /srv/nfs_share/app/caresoft/
```

### 2. Create Iceberg Tables
You need to create 4 Iceberg tables:
1. `caresoft.webhook_raw` - Raw webhooks from PostgreSQL
2. `caresoft.webhook_snapshots` - Daily deal snapshots
3. `caresoft.deals` - Parsed deal data
4. `caresoft.daily_deal_metrics` & `caresoft.account_daily_summary` - Aggregated metrics

See SQL files in `sql/caresoft/` for schemas.

### 3. Trigger DAG
```bash
# Manual trigger
airflow dags trigger caresoft_lakehouse_etl

# Monitor
airflow dags state caresoft_lakehouse_etl <execution_date>
```

## Query Examples

### Using Trino
```sql
-- Query Raw Webhooks (all brands, all types)
SELECT brand_etl, type_log, COUNT(*) as total
FROM bronze.caresoft.webhook_raw 
WHERE snapshot_date = DATE '2026-01-07'
GROUP BY brand_etl, type_log;

-- Query Deal Snapshots (filtered for deals only)
SELECT * FROM bronze.caresoft.webhook_snapshots 
WHERE snapshot_date = DATE '2026-01-07'
LIMIT 10;

-- Query Parsed Deals
SELECT deal_id, account_id, stage, status, deal_value, created_at
FROM bronze.caresoft.deals
WHERE snapshot_date = DATE '2026-01-07'
ORDER BY created_at DESC;

-- Query Daily Metrics
SELECT 
    account_id,
    stage,
    status,
    total_deals,
    total_deal_value,
    avg_deal_value
FROM bronze.caresoft.daily_deal_metrics
WHERE snapshot_date = DATE '2026-01-07'
ORDER BY total_deal_value DESC;
```

## Key Features

✅ **Separation of Concerns**
- Flow 1: Raw data ingestion (all webhooks)
- Flow 2: Deal-specific processing

✅ **Auto-Generated PK_ID**
- Uses MD5 hash of key fields
- Ensures uniqueness across brands and records

✅ **Incremental Loading**
- Flow 1 supports incremental mode
- Only loads new/updated records since last run

✅ **Multi-Brand Support**
- `brand_etl` field identifies data source
- Partitioned by brand for efficient querying

✅ **Flexible Architecture**
- Easy to add new processing flows
- Can process other webhook types (not just deals)

## Monitoring

### Airflow UI
- **URL**: http://airflow.lakehouse.com
- **DAG**: `caresoft_lakehouse_etl`

### Check Task Logs
```bash
# Flow 1: ERP Webhook Ingestion
airflow tasks logs caresoft_lakehouse_etl erp_webhook_ingestion <date>

# Flow 2: Deal Processing
airflow tasks logs caresoft_lakehouse_etl bronze_snapshot_processing <date>
airflow tasks logs caresoft_lakehouse_etl silver_transformation <date>
airflow tasks logs caresoft_lakehouse_etl gold_aggregation <date>
```

### Kubernetes Pods
```bash
# List pods
kubectl get pods -n default | grep caresoft

# View logs
kubectl logs -f <pod-name> -n default
```

## Data Retention
- **webhook_raw**: 90 days (raw landing zone)
- **webhook_snapshots**: 90 days (deal snapshots)
- **deals (Silver)**: 365 days (parsed data)
- **metrics (Gold)**: Unlimited (aggregated metrics)

## Troubleshooting

### Issue: No data in webhook_raw
**Cause**: ERP → PostgreSQL pipeline not running  
**Solution**: Check `Dev_huonghh_ETL_ERP_Thuonghieu` DAG status

### Issue: Duplicate PK_IDs
**Cause**: Hash collision (very rare)  
**Solution**: Modify hash algorithm in `erp_webhook_ingestion.py`

### Issue: Missing deals in snapshots
**Cause**: Filter criteria too strict  
**Solution**: Check `input ILIKE '%deal%'` filter in `bronze_ingestion.py`

## Next Steps
- [ ] Create all Iceberg tables
- [ ] Deploy scripts to NFS
- [ ] Test Flow 1 (ERP ingestion)
- [ ] Test Flow 2 (Deal processing)
- [ ] Setup monitoring and alerts
- [ ] Implement data quality checks
- [ ] Add support for other webhook types

## Support
For issues or questions, contact the Data Engineering team.
