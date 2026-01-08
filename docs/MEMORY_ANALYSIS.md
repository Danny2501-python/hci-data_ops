# 🔍 Memory Analysis: Why OOM with 8GB?

## 📊 **Estimated Memory Usage**

### **1. Initial Scan (Verification)**
```
Scan 341,438 rows × 18 columns × ~100 bytes/column ≈ 615 MB
```

### **2. Read from ERP KN**
```
Read 283,851 rows × 18 columns × ~100 bytes/column ≈ 511 MB
```

### **3. Transform & Add Metadata**
```
Original DataFrame:     511 MB
+ Generate PK_IDs:      511 MB (copy)
+ Add metadata columns: 511 MB (copy)
Total:                  ~1.5 GB
```

### **4. Convert to PyArrow**
```
Pandas DataFrame:  1.5 GB
PyArrow Table:     1.5 GB (separate memory)
Total:             ~3 GB
```

### **5. Write Batches (50k each)**
```
Batch 1 (50k rows): ~150 MB
Batch 2 (50k rows): ~150 MB
Batch 3 (50k rows): ~150 MB
```

### **Total Peak Memory:**
```
615 MB (scan before) +
511 MB (read ERP) +
1.5 GB (transform) +
1.5 GB (PyArrow) +
450 MB (3 batches) +
Python overhead (~500 MB)
= ~5 GB

With garbage collection delays: ~8+ GB → OOM!
```

---

## 🔴 **Root Causes**

### **1. Scan verification holds memory**
```python
df_before = table.scan(...).to_pandas()  # 615 MB
# ... delete ...
df_after = table.scan(...).to_pandas()   # Another scan

# df_before NOT freed until garbage collection!
```

### **2. Large text columns**
Columns `input` and `response` can be **VERY LARGE**:
- JSON payloads
- API responses
- Can be 10KB - 100KB per record!

**If average is 50KB per record:**
```
283,851 records × 50 KB = 14 GB just for these 2 columns!
```

### **3. Python memory fragmentation**
- Pandas creates copies during operations
- PyArrow conversion doesn't free Pandas memory immediately
- Garbage collector runs infrequently

---

## ✅ **Fixes Applied**

### **Fix 1: Explicit memory cleanup**
```python
df_before = table.scan(...).to_pandas()
rows_before = len(df_before)

# FREE IMMEDIATELY
del df_before
gc.collect()
```

### **Fix 2: Reduced batch size**
```python
# Before: 50,000 rows/batch
# After:  20,000 rows/batch
BATCH_SIZE = 20000
```

### **Fix 3: Delete DataFrames after each ERP**
```python
# Already in code
del df_erp
del df_bronze
```

---

## 📈 **Expected Memory Usage After Fixes**

### **With explicit gc.collect():**
```
Scan before:    615 MB → freed → 0 MB
Read ERP:       511 MB
Transform:      1.5 GB
PyArrow:        1.5 GB (but Pandas freed after conversion)
Batch write:    ~100 MB (20k rows instead of 50k)

Peak: ~3 GB (well within 8GB limit) ✅
```

---

## 💡 **Additional Optimizations (If Still OOM)**

### **Option 1: Disable verification scan**
```python
# Skip the "before delete" scan if not critical
# Just delete and verify after
```

### **Option 2: Reduce batch size further**
```python
BATCH_SIZE = 10000  # Even smaller
```

### **Option 3: Increase memory limit**
```yaml
resources:
  limits:
    memory: 16Gi  # Double the limit
```

### **Option 4: Stream processing**
```python
# Don't load entire ERP table
# Read in chunks from ERP
for chunk in pd.read_sql(query, engine, chunksize=50000):
    process_and_write(chunk)
```

---

## 🎯 **Recommendation**

**Try current fixes first:**
1. ✅ Explicit `gc.collect()` after scans
2. ✅ Reduced `BATCH_SIZE` to 20,000

**If still OOM:**
1. Check actual size of `input` and `response` columns
2. Consider streaming from ERP (chunked read)
3. Or increase memory limit to 16GB

---

**Date:** 2026-01-08  
**Status:** Fixes applied, ready to test
