# Deal Full Snapshot Implementation - Summary

## ✅ Đã implement (Gold Enrichment)

### **Changes made to `deal_detail_enrichment.py`:**

1. **Added `get_gold_snapshot_yesterday()` method**
   - Đọc Gold snapshot hôm qua
   - Trả về dict: `{deal_id: record}`

2. **Updated `run()` method**
   - STEP 1: Đọc Gold snapshot hôm qua
   - STEP 2: Tìm deals cần enriched (NEW/UPDATED)
   - STEP 3: Gọi API và collect vào `all_enriched_records`
   - STEP 4: MERGE yesterday + enriched → full snapshot
   - STEP 5: DELETE partition + GHI full snapshot

3. **Added `all_enriched_records` list**
   - Track tất cả enriched deals
   - Dùng cho merge cuối cùng

### **Logic Flow:**

```python
# STEP 1: Read yesterday's Gold
gold_yesterday = {
    100: {id: 100, subject: "Deal A", updated_at: "2026-01-15", ...},
    101: {id: 101, subject: "Deal B", updated_at: "2026-01-14", ...},
    ...  # 10,000 deals
}

# STEP 2: Find deals to enrich
deals_to_enrich = [100, 105]  # Deal 100 updated, Deal 105 new

# STEP 3: Enrich deals
all_enriched_records = [
    {id: 100, subject: "Deal A Updated", updated_at: "2026-01-16", ...},
    {id: 105, subject: "Deal E New", updated_at: "2026-01-16", ...},
]

# STEP 4: Merge
enriched_dict = {
    100: {...},  # Updated
    105: {...},  # New
}

full_snapshot = dict(gold_yesterday)  # Copy yesterday
full_snapshot.update(enriched_dict)   # Update with enriched

# Result:
full_snapshot = {
    100: {...},  # Updated version
    101: {...},  # From yesterday (unchanged)
    102: {...},  # From yesterday (unchanged)
    ...
    105: {...},  # New
}
# Total: 10,001 deals

# STEP 5: Write full snapshot
DELETE partition (snapshot_date=2026-01-16, brand_id=X)
INSERT 10,001 deals
```

---

## ⏳ TODO: Serving Layer

### **Need to create: `deal_detail_serving_layer.py` (Incremental)**

```python
class DealDetailServingLayer:
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
    
    def get_today_deals(self):
        """Read ONLY today's Gold snapshot (full state)"""
        scan = table.scan(
            row_filter=EqualTo("snapshot_date", self.snapshot_date)
        )
        # No dedup needed - Gold is full snapshot
        
    def upsert_serving_table(self, columns, rows):
        """UPSERT instead of TRUNCATE + INSERT"""
        INSERT INTO cs_deal_detail_api_v1 (...)
        VALUES %s
        ON CONFLICT (id, brand_id)
        DO UPDATE SET ...
        WHERE updated_at < EXCLUDED.updated_at
```

---

## 📊 Expected Results

### **Gold Storage:**

**Before (Incremental only):**
```
Day 1: 100 deals enriched
Day 2: 50 deals enriched
Day 3: 75 deals enriched
Total: 225 deals (3 days)
```

**After (Full snapshot):**
```
Day 1: 10,000 deals (full)
Day 2: 10,050 deals (full)
Day 3: 10,100 deals (full)
Total: 30,150 deals (3 days)
```

**Storage increase: ~135x** ❌ (Trade-off for serving speed)

### **Serving Performance:**

**Before (Full refresh):**
- Scan: ALL snapshots (30 days)
- Time: ~30 seconds
- Memory: High

**After (Incremental):**
- Scan: TODAY only
- Time: ~10 seconds
- Memory: Low

**Speed improvement: 3x** ✅

---

## 🎯 Next Steps

1. ✅ Test Gold enrichment với full snapshot logic
2. ⏳ Implement serving layer incremental
3. ⏳ Update DAG với snapshot_date parameter
4. ⏳ Test end-to-end pipeline

---

## ⚠️ Known Issues

### **Issue 1: Batch write during loop**

**Current code** vẫn ghi batch vào Gold trong loop:
```python
if len(batch_records) >= BATCH_SIZE:
    detail_table.append(df)  # ← Ghi vào Gold
    batch_records = []
```

**Problem:** Sau đó lại DELETE và ghi lại full snapshot → Inefficient!

**Solution:** Chỉ ghi 1 lần cuối (full snapshot), không ghi batch trong loop.

**Note:** Cần cẩn thận với memory nếu có nhiều deals (>10K). Có thể cần batch write nhưng không DELETE cho đến cuối.

---

## 💡 Recommendations

1. **Monitor storage growth** - Gold sẽ lớn hơn nhiều
2. **Set retention policy** - Chỉ giữ 30-60 ngày Gold snapshots
3. **Consider compression** - Enable Parquet compression
4. **Test with production data** - Verify memory usage

---

## 📝 Status

- [x] Gold enrichment - Full snapshot logic
- [ ] Serving layer - Incremental UPSERT
- [ ] DAG update - snapshot_date parameter
- [ ] Testing - End-to-end
- [ ] Documentation - Update README
