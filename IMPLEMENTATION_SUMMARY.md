# CareSoft Lakehouse ETL - Updated Implementation Summary

## ✅ Architecture Overview

### 2-Flow Design

**Flow 1: ERP Webhook Raw Ingestion**
```
ERP Systems → PostgreSQL (wehook_api_caresoft) → Iceberg Bronze (webhook_raw)
```
- **Purpose**: Land ALL webhook data with auto-generated PK_ID
- **Frequency**: Daily incremental
- **Script**: `erp_webhook_ingestion.py`

**Flow 2: Deal Processing Pipeline**
```
Iceberg Bronze (webhook_raw) → Snapshots → Silver → Gold
```
- **Purpose**: Process deal-specific data for analytics
- **Frequency**: Daily snapshot
- **Scripts**: `bronze_ingestion.py` → `silver_transformation.py` → `gold_aggregation.py`

## 📁 Complete Project Structure

```
data-ops/
├── app/caresoft/
│   ├── __init__.py
│   ├── erp_webhook_ingestion.py      # NEW: PostgreSQL → webhook_raw
│   ├── bronze_ingestion.py           # UPDATED: webhook_raw → snapshots
│   ├── silver_transformation.py      # Deal parsing
│   └── gold_aggregation.py           # Metrics aggregation
├── dags/
│   └── caresoft_lakehouse_dag.py     # UPDATED: 2 parallel flows
├── sql/caresoft/
│   ├── create_webhook_raw_table.sql  # NEW: Raw webhooks table
│   ├── create_bronze_table.sql       # Deal snapshots table
│   ├── create_silver_table.sql       # Parsed deals table
│   └── create_gold_table.sql         # Metrics tables
├── README.md                          # UPDATED: 2-flow architecture
└── IMPLEMENTATION_SUMMARY.md          # This file
```

## 🔑 Key Changes from Original Design

### 1. **Separated Data Ingestion**
**Before**: Direct PostgreSQL → Bronze snapshots  
**After**: PostgreSQL → Bronze raw → Bronze snapshots

**Benefits**:
- Preserves ALL webhook data (not just deals)
- Enables processing other webhook types in future
- Auto-generated PK_ID ensures uniqueness

### 2. **Auto-Generated PK_ID**
**Implementation**: MD5 hash of `id + brand_etl + create_date + write_date`

**Why**:
- PostgreSQL table has no unique PK
- Supports multi-brand data
- Prevents duplicates

### 3. **Incremental Loading**
**Flow 1** supports incremental mode:
- Tracks last `write_date` from Bronze
- Only loads new/updated records
- Reduces processing time and costs

### 4. **Parallel Execution**
DAG runs 2 flows in parallel:
- Flow 1: ERP ingestion (independent)
- Flow 2: Deal processing (depends on Flow 1 data)

## 📊 Data Tables

### Bronze Layer

#### 1. `caresoft.webhook_raw` (NEW)
**Purpose**: Landing zone for ALL webhooks  
**Partitioning**: `snapshot_date`, `brand_etl`  
**Schema**:
```
pk_id (BIGINT) - Auto-generated MD5 hash
id, name, create_uid, create_date, write_uid, write_date
type_log, model_id, id_record
input (STRING) - Raw JSON payload
response, status_code, url, header
brand_etl (INT) - Brand identifier
snapshot_date (DATE) - Partition key
ingestion_timestamp (TIMESTAMP)
```

#### 2. `caresoft.webhook_snapshots`
**Purpose**: Daily snapshots of deal webhooks  
**Partitioning**: `snapshot_date`  
**Schema**:
```
pk_id, id, input, created_at
snapshot_date, ingestion_timestamp
```

### Silver Layer

#### 3. `caresoft.deals`
**Purpose**: Parsed and cleaned deal data  
**Partitioning**: `snapshot_date`, `account_id`  
**Schema**: See `create_silver_table.sql`

### Gold Layer

#### 4. `caresoft.daily_deal_metrics`
**Purpose**: Daily aggregated metrics  
**Partitioning**: `snapshot_date`

#### 5. `caresoft.account_daily_summary`
**Purpose**: Account-level summaries  
**Partitioning**: `snapshot_date`

## 🚀 Deployment Steps

### Step 1: Create Iceberg Tables

**Option A: Using PyIceberg (Recommended)**
```python
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import *
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

catalog = load_catalog("bronze", uri="http://iceberg-rest:8181")

# Create webhook_raw table
schema = Schema(
    NestedField(1, "pk_id", LongType(), required=True),
    NestedField(2, "id", IntegerType(), required=False),
    NestedField(3, "name", StringType(), required=False),
    NestedField(4, "create_uid", IntegerType(), required=False),
    NestedField(5, "create_date", TimestampType(), required=False),
    NestedField(6, "write_uid", IntegerType(), required=False),
    NestedField(7, "write_date", TimestampType(), required=False),
    NestedField(8, "type_log", StringType(), required=False),
    NestedField(9, "model_id", IntegerType(), required=False),
    NestedField(10, "id_record", IntegerType(), required=False),
    NestedField(11, "input", StringType(), required=False),
    NestedField(12, "response", StringType(), required=False),
    NestedField(13, "status_code", IntegerType(), required=False),
    NestedField(14, "url", StringType(), required=False),
    NestedField(15, "header", StringType(), required=False),
    NestedField(16, "brand_etl", IntegerType(), required=False),
    NestedField(17, "snapshot_date", DateType(), required=True),
    NestedField(18, "ingestion_timestamp", TimestampType(), required=True),
)

partition_spec = PartitionSpec(
    PartitionField(source_id=17, field_id=1000, transform=IdentityTransform(), name="snapshot_date"),
    PartitionField(source_id=16, field_id=1001, transform=IdentityTransform(), name="brand_etl")
)

catalog.create_table(
    "caresoft.webhook_raw",
    schema=schema,
    partition_spec=partition_spec
)
```

Repeat for other tables (webhook_snapshots, deals, metrics).

### Step 2: Deploy Scripts to NFS
```bash
# From data-ops directory
cp -r app/caresoft /srv/nfs_share/app/

# Verify
ls -la /srv/nfs_share/app/caresoft/
# Should see:
# - __init__.py
# - erp_webhook_ingestion.py
# - bronze_ingestion.py
# - silver_transformation.py
# - gold_aggregation.py
```

### Step 3: Test Flow 1 (ERP Ingestion)
```bash
# Trigger just the ERP ingestion task
airflow tasks test caresoft_lakehouse_etl erp_webhook_ingestion 2026-01-07

# Check data in webhook_raw
# Via Trino:
SELECT COUNT(*) FROM bronze.caresoft.webhook_raw WHERE snapshot_date = DATE '2026-01-07';
```

### Step 4: Test Flow 2 (Deal Processing)
```bash
# Trigger bronze snapshot
airflow tasks test caresoft_lakehouse_etl bronze_snapshot_processing 2026-01-07

# Trigger silver
airflow tasks test caresoft_lakehouse_etl silver_transformation 2026-01-07

# Trigger gold
airflow tasks test caresoft_lakehouse_etl gold_aggregation 2026-01-07
```

### Step 5: Run Full DAG
```bash
# Trigger complete pipeline
airflow dags trigger caresoft_lakehouse_etl

# Monitor
airflow dags state caresoft_lakehouse_etl <execution_date>
```

## 🔍 Validation Queries

### Check Data Flow
```sql
-- 1. Check raw webhooks ingested
SELECT 
    brand_etl,
    COUNT(*) as total_webhooks,
    COUNT(DISTINCT pk_id) as unique_webhooks
FROM bronze.caresoft.webhook_raw
WHERE snapshot_date = DATE '2026-01-07'
GROUP BY brand_etl;

-- 2. Check deal snapshots created
SELECT COUNT(*) as total_deals
FROM bronze.caresoft.webhook_snapshots
WHERE snapshot_date = DATE '2026-01-07';

-- 3. Check parsed deals
SELECT 
    account_id,
    COUNT(*) as total_deals,
    SUM(deal_value) as total_value
FROM bronze.caresoft.deals
WHERE snapshot_date = DATE '2026-01-07'
GROUP BY account_id;

-- 4. Check aggregated metrics
SELECT * 
FROM bronze.caresoft.daily_deal_metrics
WHERE snapshot_date = DATE '2026-01-07'
ORDER BY total_deal_value DESC
LIMIT 10;
```

## ⚠️ Important Notes

### 1. Existing ERP Pipeline
The existing `Dev_huonghh_ETL_ERP_Thuonghieu` DAG continues to run:
- Pulls data from ERP systems (SCI, KN, HH, PR)
- Writes to PostgreSQL `wehook_api_caresoft`
- **No changes needed to this pipeline**

### 2. Data Consistency
- Flow 1 runs incrementally (only new data)
- Flow 2 processes ALL deal webhooks in `webhook_raw`
- Ensure Flow 1 completes before Flow 2 for latest data

### 3. PK_ID Generation
- MD5 hash ensures uniqueness
- Hash collision probability is extremely low
- If collision occurs, modify hash algorithm

### 4. Performance Considerations
- `webhook_raw` will grow large over time
- Implement retention policy (90 days recommended)
- Use partition pruning in queries

## 📈 Monitoring Metrics

Track these metrics for each run:

**Flow 1: ERP Ingestion**
- Records read from PostgreSQL
- Records written to webhook_raw
- Execution time
- Incremental vs full load

**Flow 2: Deal Processing**
- Bronze: Deal webhooks filtered
- Silver: Records parsed successfully
- Silver: Records filtered by quality checks
- Gold: Metric records generated

## 🎯 Success Criteria

- [ ] All 5 Iceberg tables created
- [ ] Scripts deployed to NFS
- [ ] Flow 1 runs successfully (ERP → Bronze raw)
- [ ] Flow 2 runs successfully (Bronze → Silver → Gold)
- [ ] Data visible in all layers
- [ ] Queries return expected results
- [ ] No errors in Airflow logs
- [ ] Incremental loading works correctly

## 📝 Next Enhancements

1. **Add Data Quality Checks**
   - Validate JSON structure
   - Check for required fields
   - Alert on anomalies

2. **Implement Retention Policies**
   - Auto-delete old partitions
   - Archive to cold storage

3. **Support Other Webhook Types**
   - Extend Flow 2 for non-deal webhooks
   - Create separate processing pipelines

4. **Optimize Performance**
   - Add indexes on frequently queried fields
   - Implement caching for lookups

5. **Enhanced Monitoring**
   - Data quality dashboards
   - SLA tracking
   - Alert on failures

---
**Created**: 2026-01-07  
**Author**: Data Engineering Team  
**Version**: 2.0.0 (2-Flow Architecture)
