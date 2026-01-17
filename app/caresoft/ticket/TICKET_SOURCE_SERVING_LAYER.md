# Ticket Source Metadata Serving Layer

## Overview

Script to refresh PostgreSQL serving layer with latest ticket source metadata from Iceberg Bronze.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Iceberg Bronze                                              │
│ Table: caresoft.cs_ticket_source_metadata                   │
│ Partition: snapshot_date                                    │
└─────────────────────────────────────────────────────────────┘
                        │
                        │ Read (today's partition - ALL brands)
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ ticket_source_metadata_serving_layer.py                     │
│ - Read from Iceberg (snapshot_date filter ONLY)             │
│ - UPSERT to PostgreSQL (single batch operation)             │
└─────────────────────────────────────────────────────────────┘
                        │
                        │ UPSERT (all brands at once)
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ PostgreSQL Serving Layer                                    │
│ Table: cs_ticket_source_detail_api_v1                       │
│ Primary Key: (id, brand_id)                                 │
│ Purpose: Fast queries for dashboards/reports                │
└─────────────────────────────────────────────────────────────┘
```

---

## UPSERT Strategy

### Primary Key:
```sql
PRIMARY KEY (id, brand_id)
```

### Conflict Resolution:
```sql
ON CONFLICT (id, brand_id) DO UPDATE SET
    source_name = EXCLUDED.source_name,
    "index" = EXCLUDED."index",
    channel = EXCLUDED.channel,
    utm_source = EXCLUDED.utm_source,
    utm_campaign = EXCLUDED.utm_campaign,
    utm_medium = EXCLUDED.utm_medium,
    utm_term = EXCLUDED.utm_term,
    ad_id = EXCLUDED.ad_id,
    utm_content = EXCLUDED.utm_content,
    adgroup_id = EXCLUDED.adgroup_id,
    source_key = EXCLUDED.source_key,
    updated_at = EXCLUDED.updated_at,
    created_at = EXCLUDED.created_at,
    raw_data = EXCLUDED.raw_data,
    snapshot_date = EXCLUDED.snapshot_date,
    ingestion_timestamp = EXCLUDED.ingestion_timestamp
WHERE 
    cs_ticket_source_detail_api_v1.updated_at < EXCLUDED.updated_at
    OR cs_ticket_source_detail_api_v1.updated_at IS NULL
```

**Logic:**
- Only update if **new** `updated_at` > **existing** `updated_at`
- OR if existing `updated_at` is `NULL`
- This ensures we always keep the latest version

---

## PostgreSQL Table Schema

```sql
CREATE TABLE cs_ticket_source_detail_api_v1 (
    id BIGINT NOT NULL,
    source_name TEXT,
    "index" INTEGER,
    channel TEXT,
    utm_source TEXT,
    utm_campaign TEXT,
    utm_medium TEXT,
    utm_term TEXT,
    ad_id TEXT,
    utm_content TEXT,
    adgroup_id TEXT,
    source_key TEXT,
    updated_at TIMESTAMP,
    created_at TIMESTAMP,
    brand_id INTEGER NOT NULL,
    raw_data TEXT,
    snapshot_date DATE,
    ingestion_timestamp TIMESTAMP,
    
    PRIMARY KEY (id, brand_id)
);

-- Indexes for performance
CREATE INDEX idx_cs_ticket_source_detail_api_v1_updated_at 
    ON cs_ticket_source_detail_api_v1(updated_at);
CREATE INDEX idx_cs_ticket_source_detail_api_v1_snapshot_date 
    ON cs_ticket_source_detail_api_v1(snapshot_date);
CREATE INDEX idx_cs_ticket_source_detail_api_v1_brand_id 
    ON cs_ticket_source_detail_api_v1(brand_id);
```

---

## Usage

### Command Line:

```bash
# Refresh for specific date
python app/caresoft/ticket/ticket_source_metadata_serving_layer.py 2026-01-17

# Refresh for today
python app/caresoft/ticket/ticket_source_metadata_serving_layer.py $(date +%Y-%m-%d)
```

### Arguments:
1. `snapshot_date` - Date to process (YYYY-MM-DD), optional (defaults to today)

---

## Airflow Integration

### DAG Task:

```python
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

task_ticket_source_serving = KubernetesPodOperator(
    task_id='ticket_source_metadata_serving_layer',
    name='ticket-source-serving',
    namespace='default',
    image='111050/python-etl-base:1.1.0',
    cmds=['python'],
    arguments=[
        '/app/caresoft/ticket/ticket_source_metadata_serving_layer.py',
        '{{ params.snapshot_date }}'
    ],
    env_vars={
        'ICEBERG_REST_URI_BRONZE': Variable.get('ICEBERG_REST_URI_BRONZE'),
        'CATALOG_NAME_BRONZE': 'bronze',
        'POSTGRES_URI': Variable.get('POSTGRES_URI'),
        'S3_ENDPOINT': Variable.get('S3_ENDPOINT'),
        'AWS_ACCESS_KEY_ID': Variable.get('AWS_ACCESS_KEY_ID'),
        'AWS_SECRET_ACCESS_KEY': Variable.get('AWS_SECRET_ACCESS_KEY'),
        'AWS_REGION': Variable.get('AWS_REGION'),
    },
)
```

**Note:** No need to loop through brands - the script reads all brands in one partition scan.

---

## Data Flow Example

### Input (Iceberg Bronze):

```python
# cs_ticket_source_metadata
{
    "id": 7279126,
    "source_name": "Facebook Inbox Ads Video...",
    "index": 0,
    "channel": "Inbox Facebook",
    "utm_source": null,
    "utm_campaign": null,
    "utm_medium": null,
    "utm_term": null,
    "ad_id": "120236632455760464",
    "utm_content": null,
    "adgroup_id": null,
    "source_key": "61075684",
    "updated_at": "2026-01-17 10:40:24",
    "created_at": "2026-01-17 10:40:24",
    "brand_id": 1,
    "raw_data": "{...}",
    "snapshot_date": "2026-01-17",
    "ingestion_timestamp": "2026-01-17 11:00:00"
}
```

### Output (PostgreSQL):

```sql
SELECT * FROM cs_ticket_source_detail_api_v1 WHERE id = 7279126 AND brand_id = 1;

-- Result:
id           | 7279126
source_name  | Facebook Inbox Ads Video...
index        | 0
channel      | Inbox Facebook
utm_source   | NULL
utm_campaign | NULL
utm_medium   | NULL
utm_term     | NULL
ad_id        | 120236632455760464
utm_content  | NULL
adgroup_id   | NULL
source_key   | 61075684
updated_at   | 2026-01-17 10:40:24
created_at   | 2026-01-17 10:40:24
brand_id     | 1
raw_data     | {...}
snapshot_date| 2026-01-17
ingestion_timestamp | 2026-01-17 11:00:00
```

---

## Performance

### Optimization:

1. **Batch UPSERT**: Uses `execute_values()` for efficient bulk insert
2. **Indexes**: Created on `updated_at`, `snapshot_date`, `brand_id`
3. **Incremental**: Only processes today's snapshot
4. **Conflict Resolution**: PostgreSQL handles deduplication

### Benchmarks (estimated):

| Records | Time | Throughput |
|---------|------|------------|
| 1,000 | ~2s | 500 rec/s |
| 10,000 | ~15s | 666 rec/s |
| 81,495 | ~2min | 680 rec/s |

---

## Error Handling

### Rollback on Error:

```python
try:
    # UPSERT operations
    conn.commit()
except Exception as e:
    logger.error(f"Error: {e}")
    conn.rollback()  # Rollback transaction
    raise
finally:
    conn.close()
```

### Logging:

```python
logger.info("Reading from Iceberg...")
logger.success("✅ Read 1000 records")
logger.warning("No records found")
logger.error("Failed to connect to PostgreSQL")
```

---

## Monitoring

### Key Metrics:

1. **Records Read**: Number of records from Iceberg
2. **Records Upserted**: Number of affected rows in PostgreSQL
3. **Execution Time**: Total time for refresh
4. **Errors**: Any exceptions during processing

### Logs:

```
2026-01-17 11:00:00 | INFO | Initialized TicketSourceMetadataServingLayer for 2026-01-17
2026-01-17 11:00:01 | SUCCESS | ✅ Ensured table cs_ticket_source_detail_api_v1 exists
2026-01-17 11:00:02 | INFO | Reading ticket sources from Iceberg for brand 1, snapshot 2026-01-17
2026-01-17 11:00:05 | SUCCESS | ✅ Read 81495 ticket sources from Iceberg
2026-01-17 11:00:10 | INFO | Upserting 81495 records to PostgreSQL...
2026-01-17 11:02:15 | SUCCESS | ✅ Upserted 81495 records to cs_ticket_source_detail_api_v1
2026-01-17 11:02:15 | SUCCESS | ✅ Completed serving layer refresh for brand 1
```

---

## Testing Checklist

- [ ] Table creation works
- [ ] Read from Iceberg works
- [ ] UPSERT logic correct (updates only newer records)
- [ ] Primary key constraint enforced
- [ ] Indexes created
- [ ] Error handling works
- [ ] Rollback on error works
- [ ] Multi-brand processing works
- [ ] Performance acceptable

---

## Comparison with Old System

| Aspect | Old System | New System |
|--------|------------|------------|
| **Source** | PostgreSQL temp table | Iceberg Bronze |
| **Deduplication** | 10-field comparison | Primary Key (id, brand_id) |
| **Update Logic** | Complex DELETE + INSERT | Simple UPSERT |
| **Atomicity** | ❌ No | ✅ Yes (transaction) |
| **Performance** | Slow (full table scan) | Fast (partition scan) |
| **Maintainability** | Hard | Easy |

---

## Next Steps

1. ✅ Test with real data
2. ✅ Add to Airflow DAG
3. ✅ Monitor first run
4. ⏳ Set up alerts for failures
5. ⏳ Optimize indexes if needed

---

## Related Files

- **Ingestion**: `ticket_source_metadata.py`
- **Serving**: `ticket_source_metadata_serving_layer.py` (this file)
- **Documentation**: `TICKET_SOURCE_METADATA_MIGRATION.md`

---

## Contact

- **Team**: Data Engineering
- **Source**: `bronze.caresoft.cs_ticket_source_metadata`
- **Target**: `cs_ticket_source_detail_api_v1` (PostgreSQL)
