# RAM Optimization Analysis & Recommendations

## Current RAM Usage Breakdown

### **For 50,000 records batch:**

| Component | Memory Usage | Calculation |
|-----------|--------------|-------------|
| **Arrow Batch Lists** | ~150MB | 3 lists × 50K rows × 1KB avg |
| **Python Dicts** | ~300MB | 50K dicts × 6KB each (46 fields + overhead) |
| **JSON Strings** | ~100MB | 8 JSON fields × 50K rows × 250 bytes |
| **DuckDB Processing** | ~200MB | Temporary Arrow table + SQL execution |
| **Total Peak** | **~750MB** | Per processing batch |

---

## Optimization Strategies

### **1. Arrow Native Iteration (Highest Impact)** ⭐

**Current (dòng 190-192):**
```python
pk_ids = arrow_batch["pk_id"].to_pylist()    # Creates Python list (copy)
ids = arrow_batch["id"].to_pylist()          # Creates Python list (copy)
inputs = arrow_batch["input"].to_pylist()    # Creates Python list (copy)

for i, input_str in enumerate(inputs):
    pk = pk_ids[i]
    row_id = ids[i]
```

**Optimized:**
```python
# Zero-copy iteration using Arrow native API
batch_size = len(arrow_batch)
for i in range(batch_size):
    pk = arrow_batch["pk_id"][i].as_py()      # Direct access, no intermediate list
    row_id = arrow_batch["id"][i].as_py()
    input_str = arrow_batch["input"][i].as_py()
```

**Savings:** ~150MB per batch (50K rows × 3KB per row)

---

### **2. Lazy JSON Serialization**

**Current (dòng 251-258):**
```python
"call_info": json.dumps(data_dict.get("call_info", {})),
"agent_submit": json.dumps(data_dict.get("agent_submit", {})),
# ... 8 fields total
```
→ Tạo 8 JSON strings cho MỖI row, kể cả khi empty

**Optimized:**
```python
# Pre-compile empty JSON constant
EMPTY_JSON = json.dumps({})  # Reuse same string object

# Conditional serialization
"call_info": (json.dumps(data_dict["call_info"]) 
              if "call_info" in data_dict and data_dict["call_info"] 
              else EMPTY_JSON),
```

**Savings:** ~50MB per batch (avoid creating duplicate "{}" strings)

---

### **3. Reduce Python Dict Overhead**

**Current:**
```python
row = {
    "pk_id": pk,
    "id": row_id,
    # ... 46 fields
}
accumulated_raw_data.append(row)
```
→ Python dict overhead: ~240 bytes per dict

**Option A: Use NamedTuple (Read-only)**
```python
from collections import namedtuple
DealRow = namedtuple('DealRow', ['pk_id', 'id', 'deal_id', ...])

row = DealRow(pk_id=pk, id=row_id, deal_id=..., ...)
```
**Savings:** ~100 bytes per row → ~5MB per 50K batch

**Option B: Use __slots__ Class**
```python
class DealRow:
    __slots__ = ('pk_id', 'id', 'deal_id', ...)
    def __init__(self, pk_id, id, deal_id, ...):
        self.pk_id = pk_id
        self.id = id
        ...
```
**Savings:** ~120 bytes per row → ~6MB per 50K batch

**⚠️ Trade-off:** DuckDB `from_pylist()` expects dicts, so cần convert lại

---

### **4. Batch Size Tuning**

**Current:** `processing_batch_size=50000` (default)

**Low RAM Environment:**
```python
# Reduce batch size to 10K
job.parse_and_transform_batch(tbl_source, processing_batch_size=10000)
```
**Memory:** ~150MB peak (vs 750MB)

**High RAM Environment:**
```python
# Increase to 100K for better throughput
job.parse_and_transform_batch(tbl_source, processing_batch_size=100000)
```
**Memory:** ~1.5GB peak (but faster processing)

---

### **5. Explicit Memory Management**

**Add to code (dòng 277):**
```python
# After processing each Arrow batch
del ids, inputs, pk_ids, arrow_batch
gc.collect()  # ✅ Already implemented

# Additionally: Force GC after DuckDB processing
if len(accumulated_raw_data) >= processing_batch_size:
    # ... DuckDB processing ...
    
    # Explicit cleanup
    del raw_arrow, res, batch_success
    accumulated_raw_data.clear()  # Instead of = []
    accumulated_errors.clear()
    gc.collect()
```

---

## Recommended Implementation Priority

### **Priority 1: Arrow Native Iteration** (Easiest, Highest Impact)
```python
# Replace lines 189-196
batch_size = len(arrow_batch)
for i in range(batch_size):
    pk = arrow_batch["pk_id"][i].as_py()
    row_id = arrow_batch["id"][i].as_py()
    input_str = arrow_batch["input"][i].as_py()
```
**Effort:** Low  
**Savings:** ~150MB per batch  
**Risk:** Very Low (Arrow API is stable)

### **Priority 2: Lazy JSON Serialization**
```python
# Add before loop (line 183)
EMPTY_JSON = json.dumps({})

# Replace lines 251-258
"call_info": (json.dumps(data_dict["call_info"]) 
              if "call_info" in data_dict and data_dict["call_info"] 
              else EMPTY_JSON),
```
**Effort:** Low  
**Savings:** ~50MB per batch  
**Risk:** Low

### **Priority 3: Batch Size Tuning**
```python
# Make configurable via env var
PROCESSING_BATCH_SIZE = int(os.getenv('SILVER_PROCESSING_BATCH_SIZE', '50000'))
```
**Effort:** Very Low  
**Savings:** Variable (depends on environment)  
**Risk:** None

---

## Memory Profiling Commands

### **Monitor during execution:**
```bash
# Watch memory usage in real-time
kubectl top pod <silver-transformation-pod> --containers

# Or inside container
watch -n 1 'ps aux | grep python'
```

### **Python memory profiling:**
```python
# Add to code
import tracemalloc

tracemalloc.start()
# ... processing ...
snapshot = tracemalloc.take_snapshot()
top_stats = snapshot.statistics('lineno')
for stat in top_stats[:10]:
    print(stat)
```

---

## Expected Results After Optimization

| Metric | Before | After (P1+P2) | Improvement |
|--------|--------|---------------|-------------|
| **Peak RAM** | ~750MB | ~550MB | **27% reduction** |
| **Arrow Lists** | 150MB | 0MB | **100% eliminated** |
| **JSON Strings** | 100MB | 50MB | **50% reduction** |
| **Python Dicts** | 300MB | 300MB | No change (acceptable) |
| **DuckDB** | 200MB | 200MB | No change |

---

## Code Changes Summary

### **Minimal Changes (Recommended):**

1. **Replace lines 189-196** with Arrow native iteration
2. **Add line 183**: `EMPTY_JSON = json.dumps({})`
3. **Replace lines 251-258** with conditional JSON dumps

**Total changes:** ~15 lines  
**Risk:** Very low  
**Benefit:** 27% RAM reduction

### **Full Optimization (Advanced):**

Add all Priority 1-3 changes above.

**Total changes:** ~30 lines  
**Risk:** Low  
**Benefit:** 30-40% RAM reduction (depending on batch size tuning)

---

## Testing Recommendations

### **1. Baseline Test**
```bash
# Current code
python webhook_transformation_silver.py 2026-01-09
# Monitor: kubectl top pod
```

### **2. After Optimization**
```bash
# With optimizations
python webhook_transformation_silver.py 2026-01-09
# Compare memory usage
```

### **3. Stress Test**
```bash
# Process large partition (e.g., 1M records)
# Ensure no OOMKilled errors
```

---

## Conclusion

**Current code is already well-optimized** with:
- ✅ Batch accumulator pattern
- ✅ Immediate Arrow batch cleanup
- ✅ Configurable batch size
- ✅ Explicit gc.collect()

**Additional optimizations available:**
- 🔧 Arrow native iteration (27% RAM reduction)
- 🔧 Lazy JSON serialization (7% RAM reduction)
- 🔧 Batch size tuning (variable, environment-dependent)

**Recommendation:**
- Implement Priority 1 (Arrow native iteration) if RAM is constrained
- Keep current code if RAM is sufficient (simpler, more readable)
- Monitor production usage before optimizing further
