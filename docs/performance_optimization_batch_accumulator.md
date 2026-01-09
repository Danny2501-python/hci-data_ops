# Performance Optimization: Batch Accumulator Pattern

## Problem Statement

**Before optimization:**
```python
for arrow_batch in scan.to_arrow_batch_reader():
    # Process each Arrow batch immediately
    # Arrow batch size: UNCONTROLLED (could be 100 or 100,000 rows)
    raw_data = []
    for row in arrow_batch:
        raw_data.append(parse_row(row))
    
    # DuckDB query for EACH Arrow batch
    result = duckdb.sql("SELECT ... FROM raw_data")
    yield result
```

**Issues:**
1. **Uncontrolled batch sizes**: PyIceberg returns batches of varying sizes
2. **DuckDB overhead**: Creating SQL query for each small batch is inefficient
3. **Python loop bottleneck**: Processing rows one-by-one in Python is slow

---

## Solution: Accumulator Pattern

**After optimization:**
```python
accumulated_data = []
PROCESSING_BATCH_SIZE = 50,000  # Configurable

for arrow_batch in scan.to_arrow_batch_reader():
    for row in arrow_batch:
        accumulated_data.append(parse_row(row))
    
    # Only process when accumulated enough data
    if len(accumulated_data) >= PROCESSING_BATCH_SIZE:
        result = duckdb.sql("SELECT ... FROM accumulated_data")
        yield result
        accumulated_data = []  # Reset

# Process remaining data
if accumulated_data:
    result = duckdb.sql("SELECT ... FROM accumulated_data")
    yield result
```

---

## Key Improvements

### 1. **Controlled Batch Size**
```python
def parse_and_transform_batch(self, bronze_table, processing_batch_size=50000):
```
- Default: 50,000 records per DuckDB query
- Configurable via parameter
- Optimal for DuckDB vectorization

### 2. **Accumulator Pattern**
```python
accumulated_raw_data = []
accumulated_errors = []

for arrow_batch in scan.to_arrow_batch_reader():
    # Parse rows from Arrow batch
    for row in arrow_batch:
        accumulated_raw_data.append(parsed_row)
    
    # Process when threshold reached
    if len(accumulated_raw_data) >= processing_batch_size:
        yield process_with_duckdb(accumulated_raw_data)
        accumulated_raw_data = []
```

### 3. **Immediate Arrow Batch Cleanup**
```python
for arrow_batch in scan.to_arrow_batch_reader():
    # Extract data
    pk_ids = arrow_batch["pk_id"].to_pylist()
    ...
    
    # Cleanup IMMEDIATELY after extraction
    del ids, inputs, pk_ids, arrow_batch
    gc.collect()
```
- Reduces memory pressure
- Allows Python GC to reclaim memory faster

### 4. **Final Batch Processing**
```python
# After loop completes
if accumulated_raw_data or accumulated_errors:
    logger.info(f"Processing final batch of {len(accumulated_raw_data):,} records")
    yield process_with_duckdb(accumulated_raw_data)
```
- Ensures no data is lost
- Handles partial batches at the end

---

## Performance Comparison

| Metric | Before (Per Arrow Batch) | After (Accumulator) | Improvement |
|--------|--------------------------|---------------------|-------------|
| **DuckDB Queries** | ~1000 queries (if 1000 Arrow batches) | ~20 queries (1M records / 50K) | **50x fewer queries** |
| **DuckDB Overhead** | High (many small queries) | Low (few large queries) | **Significant reduction** |
| **Memory Usage** | Spiky (depends on Arrow batch size) | Controlled (max 50K records) | **Predictable** |
| **Throughput** | Variable | Consistent | **More stable** |

---

## Batch Size Tuning Guide

### **How to choose `processing_batch_size`?**

| Batch Size | Memory Usage | DuckDB Efficiency | Use Case |
|------------|--------------|-------------------|----------|
| 10,000 | Low (~50MB) | Medium | Memory-constrained environments |
| **50,000** | Medium (~250MB) | **High** | **Default (recommended)** |
| 100,000 | High (~500MB) | Very High | High-memory environments |
| 200,000+ | Very High (1GB+) | Diminishing returns | Not recommended |

**Formula:**
```
Memory per batch ≈ batch_size × avg_row_size × 2 (Python + Arrow overhead)
```

For webhook data (~5KB per row):
- 50,000 rows × 5KB × 2 = **~500MB** (safe for most environments)

---

## Code Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ Iceberg Scan (Streaming)                                    │
│ ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐        │
│ │ Batch 1 │  │ Batch 2 │  │ Batch 3 │  │ Batch N │  ...   │
│ │ 5K rows │  │ 8K rows │  │ 12K rows│  │ 3K rows │        │
│ └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘        │
└──────┼───────────┼────────────┼────────────┼──────────────┘
       │           │            │            │
       ▼           ▼            ▼            ▼
┌──────────────────────────────────────────────────────────────┐
│ Accumulator (Python List)                                    │
│ ┌──────────────────────────────────────────────────────────┐ │
│ │ accumulated_raw_data = []                                │ │
│ │ [row1, row2, ..., row49999, row50000]  ← 50K threshold   │ │
│ └──────────────────────────────────────────────────────────┘ │
└──────────────────────┬───────────────────────────────────────┘
                       │ When len >= 50,000
                       ▼
┌──────────────────────────────────────────────────────────────┐
│ DuckDB Processing                                            │
│ ┌──────────────────────────────────────────────────────────┐ │
│ │ raw_arrow = pa.Table.from_pylist(accumulated_raw_data)   │ │
│ │ result = con.sql("SELECT TRY_CAST(...) FROM raw_arrow")  │ │
│ └──────────────────────────────────────────────────────────┘ │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│ Yield to Iceberg Writer                                      │
│ ┌──────────────────────────────────────────────────────────┐ │
│ │ yield batch_success, accumulated_errors                  │ │
│ │ accumulated_raw_data = []  # Reset                       │ │
│ └──────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

---

## Example Execution Log

```
INFO: Scanning Bronze table caresoft.wehook_api_caresoft for 2026-01-09...
INFO: Processing batch size: 50,000 records

# Arrow batches accumulate...
INFO: Processing accumulated batch of 50,123 records (total scanned: 75,000)
INFO: Processing accumulated batch of 50,089 records (total scanned: 150,000)
INFO: Processing accumulated batch of 50,234 records (total scanned: 225,000)
INFO: Processing final batch of 23,456 records
INFO: Completed processing 248,456 total records

SUCCESS: Silver Transformation Completed. Success: 247,890, Errors: 566
```

---

## Configuration via Environment Variable

You can also make batch size configurable via env var:

```python
# In webhook_transformation_silver.py
PROCESSING_BATCH_SIZE = int(os.getenv('SILVER_PROCESSING_BATCH_SIZE', '50000'))

def parse_and_transform_batch(self, bronze_table, processing_batch_size=None):
    batch_size = processing_batch_size or PROCESSING_BATCH_SIZE
    logger.info(f"Processing batch size: {batch_size:,} records")
    ...
```

**Usage:**
```bash
# Airflow DAG env vars
export SILVER_PROCESSING_BATCH_SIZE=100000  # For high-memory pods
python webhook_transformation_silver.py 2026-01-09
```

---

## Monitoring & Tuning

### **Metrics to watch:**
1. **Memory usage**: `kubectl top pod <pod-name>`
2. **Processing time per batch**: Check log timestamps
3. **DuckDB query time**: Add timing logs if needed

### **Signs batch size is too large:**
- OOMKilled errors
- Swap usage increasing
- GC pauses

### **Signs batch size is too small:**
- Many DuckDB queries (check logs)
- Low CPU utilization
- Slow overall throughput

---

## Conclusion

**Accumulator Pattern Benefits:**
- ✅ **50x fewer DuckDB queries** (1000 → 20)
- ✅ **Predictable memory usage** (controlled batch size)
- ✅ **Better DuckDB vectorization** (larger batches)
- ✅ **Configurable performance tuning** (batch size parameter)
- ✅ **Immediate memory cleanup** (del + gc.collect())

**Recommended Settings:**
- **Default**: 50,000 records (balanced)
- **Low memory**: 10,000 records
- **High memory**: 100,000 records
