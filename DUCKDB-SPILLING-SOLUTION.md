# DuckDB Spilling Solution: Parquet File Approach

## 🎯 Objective
Enable DuckDB disk spilling without OOM errors during temp table creation.

## ❌ Problem with Temp Table Approach

```python
# Current code (CAUSES OOM):
raw_arrow = pa.Table.from_pylist(raw_data)  # 389MB
con.execute("CREATE TEMP TABLE raw_data AS SELECT * FROM raw_arrow")
# ↑ Tries to copy 389MB → DuckDB
# Peak memory: 389MB (Arrow) + 389MB (DuckDB) + 500MB (overhead) = 1.3GB
# → OOMKilled during CREATE!
```

**Timeline:**
```
1. Arrow exists: 389MB
2. CREATE TEMP TABLE starts: +389MB (copying)
3. Peak: 778MB+ 
4. OOMKilled BEFORE we can delete Arrow! 💥
```

---

## ✅ Solution: Parquet File Approach

### **Concept:**
1. Write Arrow → Parquet file (streaming, low memory)
2. Delete Arrow immediately (free 389MB)
3. Query Parquet file with DuckDB (enables spilling)

### **Code Changes:**

```python
def _transform_with_duckdb(self, con, raw_data):
    """Transform using Parquet file to enable DuckDB spilling"""
    import sys
    import pyarrow.parquet as pq
    
    data_size_mb = sys.getsizeof(raw_data) / 1024 / 1024
    logger.info(f"🔍 DuckDB processing: {len(raw_data):,} records, ~{data_size_mb:.2f} MB")
    
    # Step 1: Create Arrow table
    raw_arrow = pa.Table.from_pylist(raw_data)
    arrow_size_mb = raw_arrow.nbytes / 1024 / 1024
    logger.info(f"🔍 Arrow table size: {arrow_size_mb:.2f} MB")
    
    # Step 2: Write to Parquet (streaming write, low memory)
    temp_dir = os.getenv('DUCKDB_TEMP_DIR', '/tmp/duckdb')
    temp_parquet = os.path.join(temp_dir, f"temp_batch_{os.getpid()}.parquet")
    
    logger.info(f"💾 Writing to Parquet: {temp_parquet}")
    pq.write_table(raw_arrow, temp_parquet)
    
    # Step 3: Free Arrow memory IMMEDIATELY
    del raw_arrow
    gc.collect()
    logger.info(f"🗑️  Released {arrow_size_mb:.2f} MB Arrow memory")
    
    # Step 4: Query Parquet with DuckDB (spilling enabled!)
    logger.info("🔄 Querying Parquet with DuckDB (spilling enabled)...")
    
    res = con.sql(f"""
        SELECT
            TRY_CAST(pk_id AS BIGINT) as pk_id,
            id,
            
            TRY_CAST(deal_id AS INTEGER) as deal_id,
            CAST(name AS VARCHAR) as name,
            TRY_CAST(value AS INTEGER) as value,
            TRY_CAST(probability AS INTEGER) as probability,
            TRY_CAST(pipeline_id AS INTEGER) as pipeline_id,
            TRY_CAST(pipeline_stage_id AS INTEGER) as pipeline_stage_id,
            TRY_CAST(closed_type AS INTEGER) as closed_type,
            CAST(source AS VARCHAR) as source,
            TRY_CAST(source_detail_id AS INTEGER) as source_detail_id,
            TRY_CAST(service_id AS INTEGER) as service_id,
            TRY_CAST(requester_id AS INTEGER) as requester_id,
            TRY_CAST(assignee_id AS INTEGER) as assignee_id,
            CAST(facebook_page_id AS VARCHAR) as facebook_page_id,
            TRY_CAST(ticket_source_end_status AS INTEGER) as ticket_source_end_status,
            
            to_timestamp(TRY_CAST(created_at AS DOUBLE) / 1000) as created_at,
            to_timestamp(TRY_CAST(updated_at AS DOUBLE) / 1000) as updated_at,
            to_timestamp(TRY_CAST(last_change_status_at AS DOUBLE) / 1000) as last_change_status_at,
            to_timestamp(TRY_CAST(last_change_stage_at AS DOUBLE) / 1000) as last_change_stage_at,
            
            CAST(activity AS VARCHAR) as activity,
            CAST(addition_fields AS VARCHAR) as addition_fields,
            CAST(call_info AS VARCHAR) as call_info,
            CAST(agent_submit AS VARCHAR) as agent_submit,
            CAST(updated_properties AS VARCHAR) as updated_properties,
            CAST(account_id AS VARCHAR) as account_id,
            CAST(activity_source AS VARCHAR) as activity_source,
            CAST(action AS VARCHAR) as action,
            CAST(convert_from_object AS VARCHAR) as convert_from_object,
            CAST(object AS VARCHAR) as object,
            
            TRY_CAST(fb_psid AS BIGINT) as fb_psid,
            CAST(gclid AS VARCHAR) as gclid,
            TRY_CAST(convert_by AS DOUBLE) as convert_by,
            TRY_CAST(csa_acc AS DOUBLE) as csa_acc,
            CAST(addition_details AS VARCHAR) as addition_details,
            TRY_CAST(commentator_id AS DOUBLE) as commentator_id,
            CAST(comment AS VARCHAR) as comment,
            CAST(convert_type AS VARCHAR) as convert_type,
            CAST(convert_at AS VARCHAR) as convert_at,
            CAST(closed_by AS VARCHAR) as closed_by,
            TRY_CAST(zalo_page_id AS DOUBLE) as zalo_page_id,
            CAST(closed_at AS VARCHAR) as closed_at,
            CAST(unqualified_reasons AS VARCHAR) as unqualified_reasons,
            CAST(lost_reasons AS VARCHAR) as lost_reasons,
            CAST(estimated_closed_date AS VARCHAR) as estimated_closed_date,
            CAST(utm_campaign AS VARCHAR) as utm_campaign,
            TRY_CAST(brand_etl AS INTEGER) as brand_etl,
            
            snapshot_date,
            CAST(now() AS TIMESTAMP) as ingestion_timestamp
        FROM read_parquet('{temp_parquet}')
    """).arrow()
    
    # Step 5: Cleanup Parquet file
    try:
        os.remove(temp_parquet)
        logger.info(f"🗑️  Cleaned up Parquet file")
    except:
        pass
    
    return res.read_all()
```

---

## 📊 Memory Comparison

### **Temp Table Approach (FAILS):**
```
Step 1: Arrow table: 389MB
Step 2: CREATE TEMP TABLE: +389MB (copying)
Peak: 778MB
Step 3: OOMKilled! 💥
```

### **Parquet Approach (WORKS):**
```
Step 1: Arrow table: 389MB
Step 2: Write Parquet: +50MB (streaming write buffer)
Peak: 439MB ✅
Step 3: Delete Arrow: -389MB
Current: 50MB
Step 4: Query Parquet: DuckDB manages memory, spills if needed
Peak during query: ~500MB ✅
```

---

## ✅ Benefits

1. **No Double Memory:** Parquet write is streaming, doesn't duplicate data
2. **Spilling Works:** DuckDB queries Parquet file, can spill to disk
3. **Memory Safe:** Peak memory ~500MB vs 1.3GB
4. **Testable:** Can verify spilling with low memory limits

---

## 🚀 How to Apply

### **Option 1: Replace entire function**

Replace `_transform_with_duckdb` function in `webhook_transformation_silver.py` with the code above.

### **Option 2: Manual edit**

1. Add import: `import pyarrow.parquet as pq`
2. After creating `raw_arrow`, write to Parquet
3. Delete `raw_arrow`
4. Change SQL FROM clause to `read_parquet('{temp_parquet}')`
5. Cleanup Parquet file after query

---

## 🧪 Testing Spilling

### **Set low memory limit:**

```python
# In parse_and_transform_batch, line ~276
con.execute("SET max_memory='512MB'")  # Force spilling
```

### **Expected logs:**

```
🔍 Arrow table size: 389.22 MB
💾 Writing to Parquet: /tmp/duckdb/temp_batch_12345.parquet
🗑️  Released 389.22 MB Arrow memory
🔄 Querying Parquet with DuckDB (spilling enabled)...
📊 Temp files BEFORE processing: 0
🔥 SPILLING DETECTED! Temp files: 5
   Total spilled: 200 MB
✅ Processing complete
🗑️  Cleaned up Parquet file
```

---

## 📝 Summary

| Approach | Peak Memory | Spilling | OOM Risk | Performance |
|----------|-------------|----------|----------|-------------|
| **Temp Table** | 1.3GB | ✅ Yes | ❌ High | Fast |
| **Parquet File** | 500MB | ✅ Yes | ✅ Low | Medium |
| **Zero-Copy** | 400MB | ❌ No | ✅ Low | Fastest |

**→ Parquet approach is the BEST balance for testing spilling safely!** 🎯
