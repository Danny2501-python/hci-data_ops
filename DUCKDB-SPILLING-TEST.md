# DuckDB Disk Spilling Test Guide

## 🎯 Objective
Verify that DuckDB automatically spills to disk when memory limit is exceeded.

## 📋 Setup

### 1. Current Configuration (TESTING MODE)
```python
# webhook_transformation_silver.py (line 264)
con.execute("SET max_memory='256MB'")  # ← Very low to force spilling
```

**Why 256MB?**
- Normal dataset will exceed 256MB easily
- Forces DuckDB to spill to disk
- Proves spilling mechanism works

### 2. Temp Directory
```python
# DAG configuration
duckdb_temp_volume = k8s.V1Volume(
    name='duckdb-temp',
    empty_dir=k8s.V1EmptyDirVolumeSource(
        medium='',  # Disk-backed (not RAM)
        size_limit='10Gi'
    )
)

# Mounted at:
mount_path='/tmp/duckdb'
```

---

## 🚀 How to Test

### Method 1: Run DAG and Monitor Logs

```bash
# 1. Trigger DAG in Airflow UI
# Task: webhook_transformation_silver

# 2. Watch pod logs
kubectl logs -f -n default -l task_id=webhook_transformation_silver

# Look for:
# ⚠️ DuckDB memory set to 256MB for SPILLING TEST
# 📁 DuckDB temp directory: /tmp/duckdb
```

### Method 2: Monitor Temp Files (Real-time)

```bash
# Run monitoring script
bash monitor-duckdb-spilling.sh

# Output will show:
# ✅ Spilling detected! Files: 5, Size: 1.2G
# -rw-r--r-- 1 root root 256M Jan 14 10:25 duckdb_temp_storage_abc123.tmp
```

### Method 3: Manual Check Inside Pod

```bash
# Get pod name
POD=$(kubectl get pods -n default | grep transformation-silver | grep Running | awk '{print $1}')

# Check temp directory
kubectl exec -n default $POD -- ls -lh /tmp/duckdb/

# Check disk usage
kubectl exec -n default $POD -- df -h /tmp

# Watch in real-time
kubectl exec -n default $POD -- watch -n 2 'ls -lh /tmp/duckdb/ && echo && du -sh /tmp/duckdb'
```

---

## ✅ Expected Results

### When Spilling Occurs:

```
📁 /tmp/duckdb/ contents:
-rw-r--r-- 1 root root 256M Jan 14 10:25 duckdb_temp_storage_1.tmp
-rw-r--r-- 1 root root 512M Jan 14 10:26 duckdb_temp_storage_2.tmp
-rw-r--r-- 1 root root 128M Jan 14 10:26 duckdb_temp_storage_3.tmp

Total: 896M (spilled to disk)
```

### Logs Will Show:

```
[INFO] Processing accumulated batch of 100,000 records
[WARNING] ⚠️ DuckDB memory set to 256MB for SPILLING TEST
[INFO] 📁 DuckDB temp directory: /tmp/duckdb
[INFO] Processing batch...
(DuckDB internally spilling to disk - no explicit log)
[INFO] Batch completed
```

### Performance Impact:

```
With 256MB limit (TESTING):
- Small batch (<10K rows): ~2-5 seconds (in-memory)
- Medium batch (100K rows): ~10-30 seconds (spilling)
- Large batch (1M rows): ~60-180 seconds (heavy spilling)

With 2GB limit (PRODUCTION):
- Small batch: ~1-2 seconds
- Medium batch: ~3-8 seconds
- Large batch: ~20-60 seconds (minimal spilling)
```

---

## 🔍 Troubleshooting

### Issue 1: No Temp Files Created

**Possible causes:**
1. Dataset too small (< 256MB)
   - Solution: Process larger dataset
   
2. Temp directory not writable
   ```bash
   kubectl exec $POD -- ls -ld /tmp/duckdb
   # Should show: drwxrwxrwx
   ```

3. DuckDB not actually using temp dir
   ```bash
   # Check DuckDB config
   kubectl logs $POD | grep "temp directory"
   ```

### Issue 2: Pod OOMKilled Even with 256MB

**Cause:** Python overhead + 256MB DuckDB > Pod limit

**Solution:**
```python
# Increase pod limit temporarily
limits={'memory': '2Gi'}  # In DAG

# Or reduce DuckDB further
con.execute("SET max_memory='128MB'")
```

### Issue 3: Spilling Too Slow

**Expected:** With 256MB limit, spilling will be SLOW (this is normal for testing)

**For production:** Change back to 2GB
```python
con.execute("SET max_memory='2GB'")
```

---

## 🎯 After Testing

### 1. Verify Spilling Works ✅

If you see temp files in `/tmp/duckdb/`:
```
✅ Spilling mechanism confirmed working!
✅ DuckDB can handle datasets larger than memory
✅ No OOMKilled errors
```

### 2. Restore Production Settings

```python
# Change back from 256MB to 2GB
con.execute("SET max_memory='2GB'")  # Remove test config
```

```bash
# Commit changes
git add app/caresoft/webhook_transformation_silver.py
git commit -m "Restore DuckDB memory to 2GB after spilling test"
git push
```

### 3. Re-run with Production Config

```bash
# Trigger DAG again
# Should be much faster with 2GB limit
```

---

## 📊 Comparison

| Config | Memory Limit | Spilling Frequency | Performance | Use Case |
|--------|--------------|-------------------|-------------|----------|
| **Test** | 256MB | Very frequent | Slow | Verify spilling works |
| **Production** | 2GB | Rare (only huge datasets) | Fast | Normal operations |
| **Conservative** | 1GB | Occasional | Medium | Memory-constrained cluster |

---

## 📝 Summary

### Testing Steps:
1. ✅ Set DuckDB memory to 256MB (done)
2. ✅ Add logging (done)
3. ✅ Run DAG
4. ✅ Monitor temp files
5. ✅ Verify spilling occurs
6. ✅ Restore to 2GB

### Success Criteria:
- ✅ Temp files appear in `/tmp/duckdb/`
- ✅ Pod doesn't OOMKill
- ✅ Processing completes (even if slow)
- ✅ Temp files cleaned up after completion

**→ If all criteria met, spilling mechanism is working correctly!** 🎉
