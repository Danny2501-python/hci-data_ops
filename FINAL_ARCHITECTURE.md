# CareSoft Lakehouse ETL - Final Architecture Summary

## 🎯 Complete Architecture Overview

### Data Flow
```
ERP Systems (SCI, KN, PR, HH)
  ├─ api_log tables (source data)
  │
  ↓ [Airflow: caresoft_lakehouse_etl DAG]
  │
  ├─ PostgresHook loads ERP connections
  ├─ Passes to KubernetesPodOperator as env vars
  │
  ↓ [erp_webhook_ingestion.py in Pod]
  │
Bronze Zone Catalog (iceberg-rest-bronze-zone:8181)
  ├─ caresoft.webhook_raw (all webhooks, auto PK_ID)
  │
  ↓ [bronze_ingestion.py]
  │
  ├─ caresoft.webhook_snapshots (deal snapshots)
  │
  ↓ [silver_transformation.py]
  │
Silver Zone Catalog (iceberg-rest-silver-zone:8181)
  ├─ caresoft.deals (parsed & cleaned)
  │
  ↓ [gold_aggregation.py]
  │
Gold Zone Catalog (iceberg-rest-gold-zone:8181)
  ├─ caresoft.daily_deal_metrics
  └─ caresoft.account_daily_summary
```

## 🔧 Key Components

### 1. Airflow Connections (PostgresHook)
**Connection IDs**:
- `sci_erp_sci` → SCI ERP (brand_etl = 7)
- `sci_erp_kn` → KN ERP (brand_etl = 8)
- `sci_erp_pr` → PR ERP (brand_etl = 9)
- `sci_erp_hh` → HH ERP (brand_etl = 10)

**How it works**:
```python
# At DAG parse time
hook = PostgresHook(postgres_conn_id='sci_erp_sci')
conn = hook.get_connection('sci_erp_sci')

# Extract and pass to pod
env_vars['ERP_SCI_HOST'] = conn.host
env_vars['ERP_SCI_USER'] = conn.login
# ... etc
```

### 2. Multi-Catalog Iceberg Setup

| Zone   | Service                  | URI                                  | Warehouse         | Purpose                    |
|--------|--------------------------|--------------------------------------|-------------------|----------------------------|
| Bronze | iceberg-rest-bronze-zone | http://iceberg-rest-bronze-zone:8181 | s3://bronze-zone/ | Raw & snapshot data        |
| Silver | iceberg-rest-silver-zone | http://iceberg-rest-silver-zone:8181 | s3://silver-zone/ | Parsed & cleaned data      |
| Gold   | iceberg-rest-gold-zone   | http://iceberg-rest-gold-zone:8181   | s3://gold-zone/   | Aggregated business metrics|

### 3. Python Scripts

#### `erp_webhook_ingestion.py`
- **Reads from**: 4 ERP systems (api_log tables)
- **Writes to**: Bronze zone `caresoft.webhook_raw`
- **Features**:
  - Connects to all 4 ERPs in parallel
  - Auto-generates PK_ID (MD5 hash)
  - Incremental loading per brand
  - Fault-tolerant (1 ERP fail doesn't stop others)

#### `bronze_ingestion.py`
- **Reads from**: Bronze zone `caresoft.webhook_raw`
- **Writes to**: Bronze zone `caresoft.webhook_snapshots`
- **Features**:
  - Filters for deal-related webhooks
  - Creates daily snapshots

#### `silver_transformation.py`
- **Reads from**: Bronze zone `caresoft.webhook_snapshots`
- **Writes to**: Silver zone `caresoft.deals`
- **Features**:
  - Parses JSON payloads
  - Data quality checks
  - Schema normalization

#### `gold_aggregation.py`
- **Reads from**: Silver zone `caresoft.deals`
- **Writes to**: Gold zone `caresoft.daily_deal_metrics` & `account_daily_summary`
- **Features**:
  - Business metrics calculation
  - Multi-dimensional aggregations

## 📋 Deployment Checklist

### Prerequisites
- [ ] Airflow installed with KubernetesPodOperator support
- [ ] 3 Iceberg REST catalog services running
- [ ] MinIO/S3 storage accessible
- [ ] NFS share mounted at `/srv/nfs_share/app`
- [ ] Docker image `111050/python-etl-base:1.0.0` available

### Step 1: Configure Airflow Connections
```bash
# Add 4 ERP connections via Airflow UI or CLI
airflow connections add 'sci_erp_sci' --conn-type 'postgres' ...
airflow connections add 'sci_erp_kn' --conn-type 'postgres' ...
airflow connections add 'sci_erp_pr' --conn-type 'postgres' ...
airflow connections add 'sci_erp_hh' --conn-type 'postgres' ...
```

### Step 2: Create Iceberg Tables
Create tables in each catalog zone:

**Bronze Zone**:
- `caresoft.webhook_raw`
- `caresoft.webhook_snapshots`

**Silver Zone**:
- `caresoft.deals`

**Gold Zone**:
- `caresoft.daily_deal_metrics`
- `caresoft.account_daily_summary`

### Step 3: Deploy Scripts to NFS
```bash
cd data-ops
cp -r app/caresoft /srv/nfs_share/app/
```

### Step 4: Deploy DAG to Airflow
```bash
cp dags/caresoft_lakehouse_dag.py /path/to/airflow/dags/
```

### Step 5: Test Pipeline
```bash
# Test individual tasks
airflow tasks test caresoft_lakehouse_etl erp_direct_ingestion 2026-01-07
airflow tasks test caresoft_lakehouse_etl bronze_snapshot_processing 2026-01-07
airflow tasks test caresoft_lakehouse_etl silver_transformation 2026-01-07
airflow tasks test caresoft_lakehouse_etl gold_aggregation 2026-01-07

# Trigger full DAG
airflow dags trigger caresoft_lakehouse_etl
```

## 🔍 Validation Queries

### Check Data in Each Zone

**Bronze Zone**:
```sql
-- Check raw webhooks from all ERPs
SELECT brand_etl, COUNT(*) as total
FROM bronze.caresoft.webhook_raw
WHERE snapshot_date = DATE '2026-01-07'
GROUP BY brand_etl;

-- Check deal snapshots
SELECT COUNT(*) FROM bronze.caresoft.webhook_snapshots
WHERE snapshot_date = DATE '2026-01-07';
```

**Silver Zone**:
```sql
-- Check parsed deals
SELECT account_id, stage, status, COUNT(*) as total
FROM silver.caresoft.deals
WHERE snapshot_date = DATE '2026-01-07'
GROUP BY account_id, stage, status;
```

**Gold Zone**:
```sql
-- Check daily metrics
SELECT * FROM gold.caresoft.daily_deal_metrics
WHERE snapshot_date = DATE '2026-01-07'
ORDER BY total_deal_value DESC
LIMIT 10;

-- Check account summaries
SELECT * FROM gold.caresoft.account_daily_summary
WHERE snapshot_date = DATE '2026-01-07';
```

## 🎯 Success Criteria

- [ ] All 4 ERP connections configured in Airflow
- [ ] All Iceberg tables created in correct catalogs
- [ ] Scripts deployed to NFS
- [ ] DAG visible in Airflow UI
- [ ] DAG runs without errors
- [ ] Data flows through all layers (Bronze → Silver → Gold)
- [ ] Queries return expected results
- [ ] Incremental loading works correctly

## 📊 Monitoring

### Key Metrics to Track
1. **ERP Ingestion**:
   - Records per ERP
   - Failed ERPs
   - Execution time

2. **Bronze Snapshot**:
   - Deal webhooks filtered
   - Snapshot records created

3. **Silver Transformation**:
   - Records parsed successfully
   - Records failed quality checks
   - Parsing errors

4. **Gold Aggregation**:
   - Metric records generated
   - Aggregation time

### Logs Location
```bash
# Airflow task logs
airflow tasks logs caresoft_lakehouse_etl <task_id> <execution_date>

# Kubernetes pod logs
kubectl logs -f <pod-name> -n default
```

## 🚨 Troubleshooting

### Issue: "Connection not found"
**Cause**: Airflow connection not configured  
**Solution**: Add connection via Airflow UI or CLI

### Issue: "Table not found"
**Cause**: Iceberg table not created  
**Solution**: Create table using PyIceberg or Spark SQL

### Issue: "No data from ERP"
**Cause**: Connection details incorrect or ERP unreachable  
**Solution**: Verify connection details and network connectivity

### Issue: "Catalog not reachable"
**Cause**: Iceberg REST service not running  
**Solution**: Check Kubernetes services: `kubectl get svc | grep iceberg-rest`

## 📝 Files Summary

### Created/Updated Files
```
data-ops/
├── app/caresoft/
│   ├── erp_webhook_ingestion.py      ✅ Direct ERP ingestion
│   ├── bronze_ingestion.py           ✅ Bronze zone snapshots
│   ├── silver_transformation.py      ✅ Bronze → Silver
│   └── gold_aggregation.py           ✅ Silver → Gold
├── dags/
│   └── caresoft_lakehouse_dag.py     ✅ Multi-catalog orchestration
├── sql/caresoft/
│   ├── create_webhook_raw_table.sql  ✅ Bronze raw table
│   ├── create_bronze_table.sql       ✅ Bronze snapshot table
│   ├── create_silver_table.sql       ✅ Silver deals table
│   └── create_gold_table.sql         ✅ Gold metrics tables
├── AIRFLOW_CONNECTIONS_SETUP.md      ✅ Connection setup guide
├── README.md                          ✅ User documentation
└── IMPLEMENTATION_SUMMARY.md          ✅ Implementation guide
```

## 🎉 Benefits of This Architecture

1. **Direct ERP Access**: No intermediate PostgreSQL staging
2. **Multi-Catalog Isolation**: Each zone has dedicated catalog
3. **Fault Tolerance**: ERP failures don't stop entire pipeline
4. **Incremental Loading**: Only new data processed
5. **Auto PK Generation**: Ensures data uniqueness
6. **Multi-Brand Support**: Handles 4 ERPs with different brands
7. **Scalable**: Easy to add new ERPs or data types
8. **Maintainable**: Clear separation of concerns

---
**Version**: 3.0.0 (Multi-Catalog + Direct ERP)  
**Last Updated**: 2026-01-07  
**Maintained By**: Data Engineering Team
