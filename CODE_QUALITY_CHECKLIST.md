# Code Quality Checklist - CareSoft Data Pipeline

## ✅ Unicode/Vietnamese Characters Handling

### Rule: ALWAYS use `ensure_ascii=False` with `json.dumps()`

**WHY:**
- Default `json.dumps()` escapes Unicode → `\u1ec5` instead of `ễ`
- Vietnamese characters will be unreadable in database
- Affects all JSON fields: assignee, requester, comments, etc.

**CORRECT:**
```python
json.dumps(data, ensure_ascii=False)
```

**WRONG:**
```python
json.dumps(data)  # ❌ Will escape Vietnamese characters
```

### Files to check:
- [x] `webhook_transformation_silver.py` - Line 267, 299
- [x] `deal_detail_enrichment.py` - Lines 635-645
- [ ] Any new files using `json.dumps()`

---

## ✅ DuckDB Configuration

### Rule: ALWAYS configure temp_directory and max_memory

**Required settings:**
```python
con = duckdb.connect()
con.execute(f"SET temp_directory='{temp_dir}'")
con.execute("SET max_memory='3GB'")
```

**DO NOT:**
```python
con.execute("SET enable_external_access=true")  # ❌ Causes error
```

### Files to check:
- [x] `webhook_transformation_silver.py`
- [x] `deal_detail_enrichment.py`
- [x] `deal_detail_serving_layer.py`

---

## ✅ Batch Processing

### Rule: Use appropriate batch sizes

**Guidelines:**
- Bronze → Silver: 100,000 rows
- Silver → Gold: 10,000 rows (API calls)
- Gold → Serving: 10,000 rows (PostgreSQL insert)

**Always implement:**
- Progress logging every N batches
- Final flush for remaining records
- Separate batch tracking for errors

---

## ✅ Error Handling

### Rule: Batch error logging, not immediate writes

**CORRECT:**
```python
batch_error_records = []
# ... collect errors
if len(batch_error_records) >= BATCH_SIZE:
    error_table.append(...)
```

**WRONG:**
```python
# Immediate write per error
error_table.append(single_error)  # ❌ Too slow
```

---

## ✅ Database Schema

### Rule: Use TRUNCATE instead of DROP/CREATE for serving tables

**CORRECT:**
```sql
CREATE TABLE IF NOT EXISTS table_name (...);
TRUNCATE TABLE table_name;
```

**WRONG:**
```sql
DROP TABLE IF EXISTS table_name;  # ❌ Loses metadata
CREATE TABLE table_name (...);
```

---

## ✅ Kubernetes Resources

### Rule: Always mount disk-backed emptyDir for DuckDB

**Required:**
```yaml
volumes:
  - name: duckdb-temp
    emptyDir:
      medium: ''  # Disk-backed
      sizeLimit: 10Gi
```

**Environment:**
```python
'DUCKDB_TEMP_DIR': '/tmp/duckdb'
```

---

## 🔍 Pre-commit Checklist

Before committing code, verify:

1. [ ] All `json.dumps()` have `ensure_ascii=False`
2. [ ] DuckDB has `temp_directory` and `max_memory` configured
3. [ ] Batch processing implemented (not row-by-row)
4. [ ] Error handling uses batch writes
5. [ ] Appropriate logging at batch intervals
6. [ ] Memory limits set in DAG (4Gi limit, 2Gi request)
7. [ ] Volume mounts include duckdb-temp for tasks using DuckDB

---

## 📝 Common Mistakes to Avoid

1. ❌ `json.dumps(data)` without `ensure_ascii=False`
2. ❌ `con.execute("SET enable_external_access=true")` after connect
3. ❌ Writing errors immediately instead of batching
4. ❌ Using `DROP TABLE` instead of `TRUNCATE` for serving layer
5. ❌ Not setting `max_memory` for DuckDB (causes OOM)
6. ❌ Using tmpfs (RAM) instead of disk-backed emptyDir

---

**Last Updated:** 2026-01-12
**Maintained by:** Data Engineering Team
