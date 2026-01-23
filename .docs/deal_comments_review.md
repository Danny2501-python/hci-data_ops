# Deal Comments Pipeline Review

**Date:** 2026-01-22  
**Reviewer:** Data Engineering Team  
**Status:** ✅ Fixed Critical Issues

---

## 📊 OVERVIEW

Luồng deal comments xử lý việc parse và sync comments từ deal details API sang PostgreSQL serving layer.

### Architecture Flow

```
ERP → Bronze → Silver → Gold (Deal Detail API)
                           ↓ (contains comments JSON)
                    Gold (Comments Transformation)
                           ↓ (flatten JSON → records)
                    PostgreSQL Serving Layer
```

### Components

1. **deal_detail_enrichment.py** - Fetch deal details từ CareSoft API (chứa comments JSON)
2. **deal_comments_transformation.py** - Parse JSON comments → flatten thành individual records
3. **deal_comments_serving_layer.py** - Sync từ Iceberg Gold → PostgreSQL

---

## ✅ ĐIỂM MẠNH

### 1. deal_comments_transformation.py
- ✅ **Clear separation of concerns**: Tách biệt rõ ràng giữa read, parse, write
- ✅ **Proper filtering**: Chỉ xử lý deals của snapshot_date hiện tại
- ✅ **Iceberg best practices**: Delete-then-append pattern đúng chuẩn
- ✅ **Error handling**: Log 5 lỗi đầu tiên, không crash toàn bộ pipeline
- ✅ **Type safety**: Convert đầy đủ Int64, Int32, Timestamp
- ✅ **Memory management**: Explicit gc.collect()
- ✅ **Flexible JSON parsing**: Hỗ trợ cả `ast.literal_eval` và `json.loads`

### 2. Overall Architecture
- ✅ **Medallion architecture**: Bronze → Silver → Gold → Serving
- ✅ **Partitioning**: Partition by snapshot_date cho performance
- ✅ **Idempotency**: Delete partition trước khi append → có thể re-run
- ✅ **Scalability**: Xử lý theo batch, không load toàn bộ vào memory

---

## 🚨 CRITICAL ISSUES (ĐÃ FIX)

### Issue #1: PyIceberg Filter Type Mismatch ❌ → ✅

**Problem:**
```python
# ❌ BEFORE (deal_comments_serving_layer.py:97)
scan = table.scan(
    row_filter=EqualTo("snapshot_date", self.snapshot_date.isoformat())  # String "2026-01-22"
)
```

**Root Cause:**
- Schema định nghĩa `snapshot_date` là `DateType`
- Filter dùng string → PyIceberg không match được records
- **Result**: Luôn trả về 0 records!

**Fix:**
```python
# ✅ AFTER
scan = table.scan(
    row_filter=EqualTo("snapshot_date", self.snapshot_date)  # date object
)
```

**Impact:** 🔴 Critical - Pipeline không sync được data

---

### Issue #2: Non-Atomic Transaction ❌ → ✅

**Problem:**
```python
# ❌ BEFORE
def delete_existing_comments(ticket_ids):
    cursor.execute("DELETE FROM ...")
    self.pg_conn.commit()  # Commit #1

def insert_comments(rows):
    execute_values(cursor, "INSERT INTO ...")
    self.pg_conn.commit()  # Commit #2

# In run():
self.delete_existing_comments(ticket_ids)  # ← Nếu crash ở đây
self.insert_comments(rows)                  # ← Data bị mất!
```

**Root Cause:**
- DELETE và INSERT là 2 transactions riêng biệt
- Nếu INSERT fail → data đã DELETE không recover được

**Fix:**
```python
# ✅ AFTER
def sync_comments(columns, rows):
    """Delete + Insert in single transaction"""
    cursor = self.pg_conn.cursor()
    try:
        # 1. Delete
        cursor.execute("DELETE FROM ...")
        
        # 2. Insert
        execute_values(cursor, "INSERT INTO ...")
        
        # 3. Commit once (atomic)
        self.pg_conn.commit()
    except:
        self.pg_conn.rollback()
        raise
```

**Impact:** 🔴 Critical - Risk of data loss

---

### Issue #3: Missing Index on ticket_id ❌ → ✅

**Problem:**
```python
# ❌ BEFORE
CREATE TABLE cs_deal_api_trans_comments_v1 (
    ticket_id TEXT,  -- No index!
    ...
)

# Query performance:
DELETE FROM table WHERE ticket_id IN (...)  -- Full table scan!
```

**Root Cause:**
- Không có index trên `ticket_id`
- DELETE operation phải scan toàn bộ table
- Với 1M+ records → rất chậm

**Fix:**
```python
# ✅ AFTER
CREATE INDEX IF NOT EXISTS idx_cs_deal_api_trans_comments_v1_ticket_id 
ON cs_deal_api_trans_comments_v1 (ticket_id)
```

**Impact:** 🟡 High - Performance degradation khi scale

---

## 📈 OPTIMIZATION RECOMMENDATIONS

### 1. Incremental Processing (Future Enhancement)

**Current:**
```python
# Xử lý toàn bộ comments của snapshot_date
scan = table.scan(
    row_filter=EqualTo("snapshot_date", self.snapshot_date)
)
```

**Proposed:**
```python
# Chỉ xử lý deals có comments mới/thay đổi
scan = table.scan(
    row_filter=And(
        EqualTo("snapshot_date", self.snapshot_date),
        GreaterThan("updated_at", last_processed_timestamp)
    )
)
```

**Benefits:**
- Giảm data processing volume
- Faster execution time
- Giảm load lên PostgreSQL

**Trade-off:**
- Cần track `last_processed_timestamp`
- Phức tạp hơn về logic

---

### 2. Batch Size Tuning

**Current:**
```python
execute_values(cursor, sql, rows, page_size=1000)
```

**Recommendation:**
- Monitor PostgreSQL performance
- Nếu có nhiều comments (>100k/day) → tăng `page_size=5000`
- Nếu gặp timeout → giảm `page_size=500`

---

### 3. Add Monitoring Metrics

**Proposed additions:**
```python
def run(self):
    start_time = time.time()
    
    columns, rows = self.get_comments_for_date()
    
    # Log metrics
    logger.info(f"Processing {len(rows)} comments")
    logger.info(f"Unique tickets: {len(set(row[ticket_id_idx] for row in rows))}")
    
    self.sync_comments(columns, rows)
    
    duration = time.time() - start_time
    logger.info(f"Completed in {duration:.2f}s")
    logger.info(f"Throughput: {len(rows)/duration:.0f} comments/sec")
```

**Benefits:**
- Track performance over time
- Detect anomalies early
- Capacity planning

---

### 4. Add Data Quality Checks

**Proposed:**
```python
def validate_comments(self, rows):
    """Validate comment data quality"""
    issues = []
    
    for i, row in enumerate(rows):
        # Check required fields
        if not row[ticket_id_idx]:
            issues.append(f"Row {i}: missing ticket_id")
        
        if not row[comment_idx] or row[comment_idx].strip() == '':
            issues.append(f"Row {i}: empty comment")
    
    if issues:
        logger.warning(f"Data quality issues: {len(issues)}")
        for issue in issues[:10]:  # Log first 10
            logger.warning(issue)
    
    return len(issues) == 0
```

---

## 🎯 PERFORMANCE ANALYSIS

### Current Performance (Estimated)

| Metric | Value | Notes |
|--------|-------|-------|
| **Daily comments** | ~10k-50k | Depends on deal volume |
| **Processing time** | ~30-60s | With current batch size |
| **PostgreSQL write** | ~1000 rows/sec | With page_size=1000 |
| **Memory usage** | ~500MB | Arrow table + pandas |

### Bottlenecks

1. **PyIceberg scan** - Depends on partition size
2. **PostgreSQL DELETE** - Now optimized with index ✅
3. **Network I/O** - Between Kubernetes pod and PostgreSQL

### Scalability Limits

- **Max daily comments**: ~500k (with current setup)
- **Max processing time**: ~5 minutes (acceptable for daily batch)
- **Memory limit**: 3Gi (DAG config) → can handle ~1M comments

---

## ✅ VERIFICATION CHECKLIST

- [x] **Fix #1**: PyIceberg filter uses date object
- [x] **Fix #2**: Single atomic transaction for DELETE+INSERT
- [x] **Fix #3**: Index on ticket_id column
- [ ] **Test**: Run with sample data
- [ ] **Test**: Verify transaction rollback on error
- [ ] **Test**: Check index creation in PostgreSQL
- [ ] **Monitor**: Check logs for performance metrics
- [ ] **Validate**: Compare row counts (Iceberg vs PostgreSQL)

---

## 🚀 DEPLOYMENT PLAN

### Phase 1: Critical Fixes (DONE ✅)
- [x] Fix PyIceberg filter type mismatch
- [x] Implement atomic transaction
- [x] Add index on ticket_id

### Phase 2: Testing (TODO)
1. Test với snapshot_date có data
2. Verify index được tạo trong PostgreSQL
3. Test error handling (simulate INSERT failure)
4. Check performance với large dataset

### Phase 3: Monitoring (TODO)
1. Add metrics logging
2. Set up alerts for failures
3. Monitor execution time trends

### Phase 4: Optimization (Future)
1. Implement incremental processing
2. Tune batch sizes based on metrics
3. Add data quality checks

---

## 📝 CONCLUSION

### Summary

Luồng deal comments có **architecture tốt** nhưng có **3 critical bugs** đã được fix:

1. ✅ PyIceberg filter type mismatch → Fixed
2. ✅ Non-atomic transaction → Fixed  
3. ✅ Missing index → Fixed

### Next Steps

1. **Deploy fixes** to production
2. **Monitor** first run carefully
3. **Validate** data consistency
4. **Consider** optimization recommendations for future

### Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Data loss from transaction failure | Low (fixed) | High | Atomic transaction ✅ |
| No data synced (filter bug) | Low (fixed) | High | Correct filter ✅ |
| Slow DELETE performance | Low (fixed) | Medium | Index added ✅ |
| Memory overflow | Low | Medium | Monitor metrics |

**Overall Status:** 🟢 Ready for production after testing
