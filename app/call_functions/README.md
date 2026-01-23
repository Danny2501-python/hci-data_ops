# ETL Staging SCI Migration Guide

## 📋 Overview

Migration of `etl_staging_sci` from Django-based ETL to **Airflow + Kubernetes + Iceberg** architecture.

### **Old System:**
- Django ORM + PostgreSQL metadata
- Cron jobs
- Direct PostgreSQL writes
- Temp files for COPY operations

### **New System:**
- Airflow orchestration
- Kubernetes execution
- Iceberg data lake (Bronze layer)
- Metadata in Iceberg Gold zone

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Source Databases                                            │
│  (ERP, GiaoViec, Seeding, HongHa, etc.)                     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Bronze Layer (Iceberg)                                      │
│  - Raw data from sources                                     │
│  - Partitioned by snapshot_date                              │
│  - Full schema preservation                                  │
│  - Tables: call_functions.{table_name}_bronze                   │
└─────────────────────────────────────────────────────────────┘
```

---

## 📦 Components

### **1. Metadata Table**
**File**: `app/call_functions/create_metadata_table.py`

**Purpose**: Create and manage ETL configuration in Iceberg

**Table**: `etl_metadata.table_config` (Gold zone)

**Schema**:
```python
- id: IntegerType
- table_name: StringType
- table_name_source: StringType
- source: StringType (ERP, GiaoViec, etc.)
- server: StringType (IP address)
- active: BooleanType
- keyword: StringType (date column for incremental)
- primary_keyword: StringType (primary keys)
- rename_column: StringType (JSON mapping)
- consider: StringType (additional WHERE clause)
- truncate: BooleanType
- ... (other fields)
```

### **2. Bronze Ingestion Script**
**File**: `app/call_functions/bronze_ingestion.py`

**Purpose**: Pull data from source DBs → Iceberg Bronze

**Features**:
- ✅ Read metadata from Iceberg
- ✅ Connect to multiple source DBs
- ✅ Incremental and full load support
- ✅ Column renaming
- ✅ Type conversions
- ✅ Partition by snapshot_date

### **3. Airflow DAG**
**File**: `dags/etl_staging_sci_dag.py`

**Schedule**: Daily at 01:00 AM

**Parameters**:
- `snapshot_date`: Override date (for backfill)
- `table_id`: Process specific table only

---

## 🚀 Setup Instructions

### **Step 1: Create Metadata Table**

```bash
# Set environment variables
export ICEBERG_REST_URI_GOLD=http://iceberg-rest-gold-zone:8181
export CATALOG_NAME_GOLD=gold
export S3_ENDPOINT=http://10.152.183.65:9000
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export AWS_REGION=us-east-1

# Create table
python app/call_functions/create_metadata_table.py
```

### **Step 2: Migrate Metadata from PostgreSQL**

```bash
# Set PostgreSQL connection
export POSTGRES_STAGING_URI=postgresql://user:pass@10.10.10.214:5432/staging_sci

# Uncomment migrate_data_from_postgres() in create_metadata_table.py
# Then run:
python app/call_functions/create_metadata_table.py
```

### **Step 3: Configure Source DB Connections**

Update `dags/etl_staging_sci_dag.py` with correct connection strings:

```python
common_env_vars = {
    'ERP_SCI_URI': 'postgresql://user:pass@10.10.10.154:5432/erp',
    'GIAOVIEC_URI': 'postgresql://user:pass@10.10.10.153:5432/giaoviec',
    # ... add all source DBs
}
```

**OR** use Airflow Connections (recommended):

```bash
# Add connections via Airflow UI or CLI
airflow connections add 'erp_sci' \
    --conn-type 'postgres' \
    --conn-host '10.10.10.154' \
    --conn-schema 'erp' \
    --conn-login 'user' \
    --conn-password 'pass'
```

### **Step 4: Deploy Scripts to NFS**

```bash
# Copy scripts to NFS share
cp -r app/etl_staging_sci /srv/nfs_share/app/

# Verify
ls -la /srv/nfs_share/app/call_functions/
```

### **Step 5: Deploy DAG to Airflow**

```bash
# Copy DAG file
cp dags/etl_staging_sci_dag.py /path/to/airflow/dags/

# Or if using Git sync
git add dags/etl_staging_sci_dag.py
git commit -m "Add ETL Staging SCI DAG"
git push
```

### **Step 6: Test Run**

```bash
# Trigger manual run via Airflow UI
# Or via CLI:
airflow dags trigger etl_staging_sci \
    --conf '{"snapshot_date": "2026-01-21", "table_id": 1}'
```

---

## 🧪 Testing

### **Test Single Table**

```bash
# Run locally (outside Airflow)
export ICEBERG_REST_URI_BRONZE=http://iceberg-rest-bronze-zone:8181
export ICEBERG_REST_URI_GOLD=http://iceberg-rest-gold-zone:8181
export CATALOG_NAME_BRONZE=bronze
export CATALOG_NAME_GOLD=gold
# ... (set all env vars)

# Run for specific table
python app/call_functions/bronze_ingestion.py 2026-01-21 1

# Run for all tables
python app/call_functions/bronze_ingestion.py 2026-01-21
```

### **Verify Data in Iceberg**

```sql
-- Trino/DBeaver
SELECT * FROM bronze.call_functions.table_name_bronze
WHERE snapshot_date = DATE '2026-01-21'
LIMIT 10;

-- Check partitions
SELECT * FROM bronze.call_functions."table_name_bronze$partitions";

-- Count records
SELECT snapshot_date, COUNT(*) 
FROM bronze.call_functions.table_name_bronze
GROUP BY snapshot_date
ORDER BY snapshot_date DESC;
```

---

## 📊 Monitoring

### **Check DAG Status**

```bash
# Airflow UI
http://airflow.example.com/dags/etl_staging_sci

# CLI
airflow dags list-runs -d etl_staging_sci
```

### **View Logs**

```bash
# Airflow UI → DAG → Task → Logs

# Or via kubectl
kubectl logs -n default -l dag_id=etl_staging_sci
```

### **Check Iceberg Snapshots**

```sql
-- View snapshots
SELECT * FROM bronze.call_functions."table_name_bronze$snapshots"
ORDER BY committed_at DESC
LIMIT 5;

-- View history
SELECT * FROM bronze.call_functions."table_name_bronze$history"
ORDER BY made_current_at DESC;
```

---

## 🔧 Troubleshooting

### **Issue: Connection timeout to source DB**

**Solution**: Check network connectivity, firewall rules

```bash
# Test connection from K8s pod
kubectl run -it --rm debug --image=postgres:13 --restart=Never -- \
    psql -h 10.10.10.154 -U user -d erp
```

### **Issue: Metadata table not found**

**Solution**: Run create_metadata_table.py first

```bash
python app/call_functions/create_metadata_table.py
```

### **Issue: Schema mismatch**

**Solution**: Drop and recreate Bronze table, or use schema evolution

```sql
-- Drop table
DROP TABLE bronze.call_functions.table_name_bronze;

-- Re-run ingestion to auto-create with new schema
```

---

## 📝 Migration Checklist

- [ ] Create metadata table in Iceberg Gold
- [ ] Migrate metadata from PostgreSQL
- [ ] Configure source DB connections
- [ ] Deploy scripts to NFS
- [ ] Deploy DAG to Airflow
- [ ] Test with 1-2 pilot tables
- [ ] Validate data accuracy
- [ ] Run parallel (old + new) for 1 week
- [ ] Cutover to new system
- [ ] Disable old Django cron jobs

---

## 🎯 Next Steps

1. **Silver Layer**: Add transformation logic
2. **Gold Layer**: Create serving tables
3. **Data Quality**: Add validation checks
4. **Monitoring**: Set up alerts
5. **Documentation**: Update runbooks

---

## 📞 Support

For issues or questions, contact:
- Data Engineering Team
- Slack: #data-engineering
- Email: data-eng@example.com
