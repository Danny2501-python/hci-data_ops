# Data-Ops Architecture Review

## Overview

Hệ thống Data-Ops hiện tại hỗ trợ **3 nguồn dữ liệu chính**:

1. **ERP** - Database ERP system
2. **Standard** - Các nguồn database khác (Staging SCI, Caresoft, etc.)
3. **Google Sheets** - Google Sheets API

## Architecture Pattern

### Bronze → Gold → Serving

```
┌─────────────────┐
│  Data Sources   │
│  - ERP DB       │
│  - Staging DB   │
│  - Google Sheet │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Bronze Layer   │  ← Raw data, partitioned by snapshot_date
│  (Iceberg)      │  ← Includes: ingestion_timestamp
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Gold Layer     │  ← Metadata & Configuration
│  (Iceberg)      │  ← declare_table_etl, declare_api_googlesheet
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Serving Layer   │  ← Production tables
│ (PostgreSQL)    │  ← Optimized for queries
└─────────────────┘
```

## Metadata Columns (Bronze Layer)

Tất cả bảng Bronze đều có các metadata columns sau:

| Column | Type | Description | Populated By |
|--------|------|-------------|--------------|
| `snapshot_date` | date | Ngày snapshot dữ liệu | All ingestion scripts |
| `ingestion_timestamp` | timestamp | Thời điểm ingest vào Iceberg | All ingestion scripts |
| `source_server` | string | Server nguồn (ERP only) | bronze_ingestion.py |
| `brand_etl` | int | Brand ID (multi-brand only) | bronze_ingestion.py |

### Purpose of Each Column

#### `snapshot_date`
- **Partition key** cho tất cả Bronze tables
- Cho phép query hiệu quả theo ngày
- Hỗ trợ incremental load và backfill
- Format: `YYYY-MM-DD`

#### `ingestion_timestamp`
- Track **khi nào** data được ingest vào lakehouse
- Hỗ trợ audit và debugging
- Cho phép tìm data freshness
- Format: `YYYY-MM-DD HH:MM:SS.microseconds`

#### `source_server` (ERP only)
- Phân biệt data từ server nào (multi-server setup)
- Ví dụ: `erp_prod`, `erp_staging`

#### `brand_etl` (Multi-brand only)
- Phân biệt data của brand nào
- Ví dụ: `1` (Brand A), `2` (Brand B)

## Configuration Tables (Gold Layer)

### 1. `declare_table_etl` (General ETL)

Dùng cho **ERP** và **Standard** sources:

```sql
-- Key fields
id, table_name, table_name_source, source, server,
keyword, rename_column, primary_keyword, 
is_transfrom, consider, truncate, active
```

**Sources supported:**
- `ERP`
- `Staging sci`
- `CS` (Caresoft)
- `GV` (Giao việc)
- `SE` (Seeding)
- `HongHa`
- `facebook`
- `Sanbox`
- `portal_guide`
- `Other`

### 2. `declare_api_googlesheet` (Google Sheets)

Dùng **riêng** cho Google Sheets:

```sql
-- Key fields
id, name, spreadsheet_id, sheet, table_data,
method_data, remove_column, rename_column,
key, loai, active, schedule, active_schedule
```

**Why separate?**
- Giữ nguyên logic cũ 100%
- Hỗ trợ bidirectional (IN/OUT)
- Schema khác biệt (có `loai`, `query`, etc.)

## Ingestion Scripts

### 1. `bronze_ingestion.py`

**Purpose:** Ingest từ PostgreSQL databases → Iceberg Bronze

**Features:**
- Streaming ingestion (memory efficient)
- Incremental load based on `keyword` field
- Full load support
- Column renaming
- Multi-brand support
- Adds: `snapshot_date`, `ingestion_timestamp`, `source_server`, `brand_etl`

**Usage:**
```bash
python bronze_ingestion.py "2026-02-03" "" "incremental" "ERP"
```

**Config source:** `declare_table_etl` (Gold)

### 2. `googlesheet_ingestion.py`

**Purpose:** Ingest từ Google Sheets → Iceberg Bronze

**Features:**
- Google Sheets API integration
- Column removal (`remove_column`)
- Column renaming (`rename_column`)
- Truncate mode (`method_data=True`)
- Numeric type conversion
- Adds: `snapshot_date`, `ingestion_timestamp`

**Usage:**
```bash
python googlesheet_ingestion.py "2026-02-03" "123"
```

**Config source:** `declare_api_googlesheet` (Gold)

### 3. `serving_ingestion.py`

**Purpose:** Sync từ Iceberg Bronze → PostgreSQL Serving

**Features:**
- Efficient COPY protocol
- UPSERT based on primary key
- Truncate mode support
- Dependency management (views/materialized views)
- Fallback strategy for failed upserts
- Schema auto-creation

**Usage:**
```bash
python serving_ingestion.py "2026-02-03" "" "ERP" "false"
```

**Config source:** `declare_table_etl` OR `declare_api_googlesheet`

## Data Flow Examples

### Example 1: ERP Table

```
1. Bronze Ingestion
   erp_prod.sales_orders (PostgreSQL)
   → bronze.call_functions.sales_orders (Iceberg)
   
   Metadata added:
   - snapshot_date = 2026-02-03
   - ingestion_timestamp = 2026-02-03 10:30:15.123456
   - source_server = 'erp_prod'

2. Serving Ingestion
   bronze.call_functions.sales_orders (Iceberg)
   → dwh_product.sales_orders_v1 (PostgreSQL)
   
   UPSERT based on primary_keyword = 'id'
```

### Example 2: Google Sheet

```
1. Bronze Ingestion
   Google Sheet "Budget 2024" (spreadsheet_id: 1ABC...XYZ)
   → bronze.call_functions.budget_2024 (Iceberg)
   
   Transformations:
   - Remove columns: "notes,comments"
   - Rename: {"Month": "month", "Amount": "amount"}
   - Lowercase all columns
   
   Metadata added:
   - snapshot_date = 2026-02-03
   - ingestion_timestamp = 2026-02-03 10:35:22.789012

2. Serving Ingestion
   bronze.call_functions.budget_2024 (Iceberg)
   → dwh_product.budget_2024 (PostgreSQL)
   
   UPSERT based on key = 'month,department'
```

## Airflow DAGs

### 1. `call_function_erp_dag.py`

**Schedule:** Daily 1 AM  
**Tasks:**
- `erp_bronze_layer` → Bronze ingestion
- `erp_serving_layer` → Serving ingestion

**Filters:** `source='ERP'`

### 2. `call_functions_standard_dag.py`

**Schedule:** Daily 2 AM  
**Tasks:**
- `standard_bronze_layer` → Bronze ingestion
- `standard_serving_layer` → Serving ingestion

**Filters:** `is_transfrom=True` (no source filter)

### 3. `googlesheet_lakehouse_dag.py`

**Schedule:** Daily 1 AM  
**Tasks:**
- `googlesheet_bronze_layer` → Bronze ingestion

**Filters:** `loai='IN'`, `active=True`

## Best Practices

### 1. Partitioning
- **Always partition by `snapshot_date`** in Bronze layer
- Enables efficient time-based queries
- Supports incremental processing

### 2. Metadata Columns
- **Always add `ingestion_timestamp`** for audit trail
- Use `snapshot_date` for business logic
- Use `ingestion_timestamp` for debugging

### 3. Configuration
- Keep Google Sheets config separate (`declare_api_googlesheet`)
- Use `declare_table_etl` for database sources
- Document `source` values in `source_types.py`

### 4. Error Handling
- Log all errors with context
- Use retry logic in Airflow (2 retries, 5 min delay)
- Monitor failed tasks via Airflow UI

### 5. Performance
- Use streaming for large datasets (Bronze ingestion)
- Use COPY protocol for PostgreSQL writes (Serving ingestion)
- Batch size: 20,000 rows (configurable)

## Monitoring & Observability

### Key Metrics

1. **Data Freshness**
   ```sql
   SELECT MAX(ingestion_timestamp) 
   FROM bronze.call_functions.sales_orders
   WHERE snapshot_date = CURRENT_DATE - 1;
   ```

2. **Row Counts**
   ```sql
   SELECT snapshot_date, COUNT(*) 
   FROM bronze.call_functions.sales_orders
   GROUP BY snapshot_date
   ORDER BY snapshot_date DESC;
   ```

3. **Ingestion Lag**
   ```sql
   SELECT 
     snapshot_date,
     MAX(ingestion_timestamp) - MIN(ingestion_timestamp) as lag
   FROM bronze.call_functions.sales_orders
   GROUP BY snapshot_date;
   ```

### Logs

- **Airflow logs:** Kubernetes pod logs
- **Application logs:** Loguru (console output)
- **Iceberg metadata:** Snapshot history

## Migration Checklist

- [x] ERP ingestion (bronze + serving)
- [x] Standard ingestion (bronze + serving)
- [x] Google Sheets ingestion (bronze)
- [x] Metadata columns (snapshot_date, ingestion_timestamp)
- [x] Configuration tables (Gold layer)
- [x] Airflow DAGs
- [ ] Google Sheets serving layer sync
- [ ] Data quality checks
- [ ] Monitoring dashboards
- [ ] Documentation

## Future Enhancements

1. **Google Sheets OUT direction** (DB → Google Sheet)
2. **Data quality framework** (Great Expectations)
3. **Schema evolution** (automated DDL sync)
4. **Incremental Google Sheets** (track changes)
5. **Multi-region support** (S3 replication)
6. **Real-time streaming** (Kafka → Iceberg)
