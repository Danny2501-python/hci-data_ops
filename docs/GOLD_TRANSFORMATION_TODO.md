# Gold Layer Transformation - TODO

## Objective
Transform Silver snapshot-based tables to Gold incremental tables:
- `silver.cs_ticket_api` → `gold.cs_ticket_api_final`
- `silver.cs_ticket_source_api` → `gold.cs_ticket_source_api_final`

## Current State (2026-01-16)
- ✅ Bronze: Snapshot-based, daily overwrite
- ✅ Silver: Snapshot-based, enriched from last 10 days
- ⏳ Gold: Not implemented yet

## Gold Requirements

### Table 1: cs_ticket_api_final
**Purpose:** Incremental ticket master data

**Schema:**
- All fields from Bronze cs_ticket_api
- Additional metadata:
  - `first_seen_date`: First time ticket appeared
  - `last_updated_date`: Last time ticket was updated
  - `is_active`: Boolean flag

**Partition:** By `brand_id` or `month(created_at)`

**Update Logic:**
```sql
MERGE INTO gold.cs_ticket_api_final AS target
USING (
  SELECT * FROM silver.cs_ticket_api 
  WHERE snapshot_date = CURRENT_DATE
) AS source
ON target.ticket_id = source.ticket_id
WHEN MATCHED THEN UPDATE SET
  updated_at = source.updated_at,
  last_updated_date = CURRENT_DATE,
  ... (other fields)
WHEN NOT MATCHED THEN INSERT
  VALUES (source.*, CURRENT_DATE, CURRENT_DATE, TRUE)
```

### Table 2: cs_ticket_source_api_final
**Purpose:** Incremental ticket detail/enrichment data

**Schema:**
- All fields from Silver cs_ticket_source_api
- Additional metadata:
  - `first_enriched_date`: First enrichment date
  - `last_enriched_date`: Last enrichment date
  - `enrichment_count`: Number of times enriched

**Partition:** By `brand_id`

**Update Logic:** Similar MERGE as above

## Implementation Steps

### Step 1: Create Gold Tables
```python
# File: app/caresoft/create_gold_tables.py
# Create Iceberg tables in Gold catalog
```

### Step 2: Create Transformation Script
```python
# File: app/caresoft/ticket_gold_transformation.py
class TicketGoldTransformation:
    def transform_ticket_api(self):
        # Scan Silver snapshot
        # MERGE into Gold
        pass
    
    def transform_ticket_source(self):
        # Scan Silver snapshot
        # MERGE into Gold
        pass
```

### Step 3: Add to DAG
```python
# File: dags/caresoft_ticket_pipeline_dag.py
task_gold_transformation = KubernetesPodOperator(
    task_id='gold_transformation',
    ...
)

# Dependency
task_ticket_enrichment >> task_gold_transformation
```

## Performance Considerations

1. **MERGE Performance:**
   - Use partition pruning (filter by brand_id)
   - Batch MERGE operations
   - Monitor merge duration

2. **Data Volume:**
   - Estimate: ~1M tickets total
   - Daily new/updated: ~10K tickets
   - MERGE should be fast (<5 min)

3. **Compaction:**
   - Run Iceberg compaction weekly
   - Remove old snapshots (retention: 30 days)

## Testing Plan

1. **Unit Test:** Test MERGE logic with sample data
2. **Integration Test:** Run full pipeline Bronze→Silver→Gold
3. **Data Quality:** Validate counts, duplicates, nulls
4. **Performance:** Monitor execution time, memory usage

## Timeline

- **Phase 1 (Current):** Bronze + Silver stable ✅
- **Phase 2 (Next):** Implement Gold transformation
- **Phase 3 (Future):** Optimize and productionize

## Notes

- Silver tables are snapshot-based (keep 10 days for deduplication)
- Gold tables are incremental (keep all history)
- Use PyIceberg MERGE API (available in PyIceberg 0.6+)
- Consider using DuckDB for complex transformations if needed
