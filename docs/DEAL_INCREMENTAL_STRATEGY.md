# So sánh Ticket vs Deal Pipeline - Khả năng áp dụng Incremental

## 📊 So sánh Architecture

### **TICKET PIPELINE (Incremental)**

```
┌─────────────────────────────────────────────────────────────┐
│ BRONZE: cs_ticket_api (Full snapshot mỗi ngày)             │
├─────────────────────────────────────────────────────────────┤
│ Cơ chế:                                                     │
│ 1. DELETE partition cũ (snapshot_date + brand_id)          │
│ 2. Gọi API 3 lần (updated, created, created_prev)          │
│ 3. INSERT tất cả tickets từ 3 lần gọi                      │
│                                                             │
│ Kết quả snapshot_date=2026-01-16:                          │
│ → Chứa TẤT CẢ tickets hiện có (có thể có duplicate)       │
│ → Mỗi snapshot = FULL STATE của tickets                    │
└─────────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────────┐
│ SILVER: cs_ticket_source_api (Enriched)                    │
├─────────────────────────────────────────────────────────────┤
│ Cơ chế:                                                     │
│ 1. Đọc Bronze snapshot HÔM NAY                             │
│ 2. Dedup by ticket_id (MAX updated_at)                     │
│ 3. Gọi API cho MỖI ticket_id unique                        │
│ 4. GHI Silver snapshot HÔM NAY                             │
│                                                             │
│ Kết quả snapshot_date=2026-01-16:                          │
│ → Chứa TẤT CẢ tickets enriched (từ Bronze hôm nay)        │
│ → Mỗi snapshot = FULL STATE của enriched tickets           │
└─────────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────────┐
│ SERVING: cs_ticket_source_api_v1 (PostgreSQL)              │
├─────────────────────────────────────────────────────────────┤
│ Cơ chế: INCREMENTAL                                        │
│ 1. Đọc Silver snapshot HÔM NAY                             │
│ 2. KHÔNG cần dedup (Silver đã unique)                      │
│ 3. UPSERT vào PostgreSQL                                   │
│                                                             │
│ → Chỉ xử lý data hôm nay!                                  │
└─────────────────────────────────────────────────────────────┘
```

---

### **DEAL PIPELINE (Hiện tại)**

```
┌─────────────────────────────────────────────────────────────┐
│ SILVER: cs_deal_wehook (Full snapshot mỗi ngày)            │
├─────────────────────────────────────────────────────────────┤
│ Cơ chế:                                                     │
│ - Webhook events được transform                            │
│ - Mỗi snapshot chứa TẤT CẢ webhook events hiện có         │
│                                                             │
│ Kết quả snapshot_date=2026-01-16:                          │
│ → Chứa TẤT CẢ deals (10,050 deals)                        │
│ → Mỗi snapshot = FULL STATE của webhooks                   │
└─────────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────────┐
│ GOLD: cs_deal_detail_api (Enriched)                        │
├─────────────────────────────────────────────────────────────┤
│ Cơ chế: INCREMENTAL (Vấn đề!)                              │
│ 1. So sánh Silver today vs yesterday                       │
│ 2. Tìm deals MỚI hoặc UPDATED (50 deals)                  │
│ 3. Gọi API CHỈ cho 50 deals thay đổi                      │
│ 4. DELETE partition cũ + INSERT 50 deals                   │
│                                                             │
│ Kết quả snapshot_date=2026-01-16:                          │
│ → CHỈ chứa 50 deals (deals enriched hôm nay)              │
│ → KHÔNG phải FULL STATE! ❌                                │
└─────────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────────┐
│ SERVING: cs_deal_detail_api_v1 (PostgreSQL)                │
├─────────────────────────────────────────────────────────────┤
│ Cơ chế: FULL REFRESH (Bắt buộc!)                           │
│ 1. Quét TẤT CẢ Gold snapshots (30 ngày)                   │
│ 2. Tìm latest snapshot per deal_id                         │
│ 3. TRUNCATE + INSERT                                        │
│                                                             │
│ → Phải quét toàn bộ lịch sử! ❌                            │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔍 Phân tích Khác biệt Chính

| Aspect | Ticket | Deal | Impact |
|--------|--------|------|--------|
| **Bronze/Silver source** | Full snapshot | Full snapshot | ✅ Giống nhau |
| **Enrichment input** | Bronze TODAY | Silver TODAY vs YESTERDAY | ⚠️ Khác |
| **Enrichment output** | FULL STATE | INCREMENTAL ONLY | ❌ Khác biệt lớn |
| **Serving input** | Silver TODAY | Gold ALL HISTORY | ❌ Khác biệt lớn |

**Root cause:**
- **Ticket**: Mỗi layer đều chứa FULL STATE
- **Deal**: Gold chỉ chứa INCREMENTAL changes

---

## ✅ Giải pháp: Áp dụng cơ chế Ticket cho Deal

### **Option A: Thay đổi Gold thành Full Snapshot (Recommended)**

**Ý tưởng:** Gold mỗi snapshot = FULL STATE (giống Silver của Ticket)

**Thay đổi `deal_detail_enrichment.py`:**

```python
class DealDetailEnrichment:
    def run(self):
        # --- STEP 1: Đọc TẤT CẢ deals từ Silver HÔM NAY ---
        all_deals_today = self.get_all_deals_from_silver(self.snapshot_date)
        # → 10,050 deals
        
        # --- STEP 2: Đọc Gold snapshot HÔM QUA ---
        gold_yesterday = self.get_gold_snapshot(self.snapshot_date - timedelta(days=1))
        # → 10,000 deals (enriched trước đó)
        
        # --- STEP 3: Tìm deals cần RE-ENRICH ---
        deals_to_enrich = self.compare_and_find_changed(
            all_deals_today, 
            gold_yesterday
        )
        # → 50 deals (NEW hoặc UPDATED)
        
        # --- STEP 4: Gọi API cho deals thay đổi ---
        enriched_new = self.enrich_deals(deals_to_enrich)
        # → 50 deals enriched
        
        # --- STEP 5: MERGE với Gold hôm qua ---
        # Giữ lại deals cũ + update deals mới
        gold_today_full = self.merge_snapshots(
            gold_yesterday,  # 10,000 deals cũ
            enriched_new     # 50 deals mới/updated
        )
        # → 10,050 deals (FULL STATE)
        
        # --- STEP 6: DELETE + INSERT Gold HÔM NAY ---
        gold_table.delete(
            And(
                EqualTo("snapshot_date", self.snapshot_date),
                EqualTo("brand_id", brand["erp_id"])
            )
        )
        gold_table.append(gold_today_full)
        # → Gold snapshot HÔM NAY chứa TẤT CẢ deals!
```

**Kết quả:**
```
Gold snapshot_date=2026-01-15: 10,000 deals (FULL STATE)
Gold snapshot_date=2026-01-16: 10,050 deals (FULL STATE)
Gold snapshot_date=2026-01-17: 10,100 deals (FULL STATE)

→ Mỗi snapshot = FULL STATE!
→ Serving chỉ cần đọc snapshot HÔM NAY!
```

**Serving layer mới:**
```python
def get_today_deals(self):
    # Chỉ scan HÔM NAY
    scan = table.scan(
        row_filter=EqualTo("snapshot_date", self.snapshot_date)
    )
    # → 10,050 deals (FULL STATE)
    
def upsert_serving_table(self, columns, rows):
    # UPSERT thay vì TRUNCATE
    INSERT ... ON CONFLICT (id, brand_id)
    DO UPDATE SET ...
    WHERE updated_at < EXCLUDED.updated_at
```

---

### **Ưu điểm Option A:**

1. ✅ **Serving incremental** - Chỉ scan snapshot hôm nay
2. ✅ **Consistent với Ticket** - Cùng pattern
3. ✅ **Dễ hiểu** - Mỗi snapshot = full state
4. ✅ **Scalable** - Serving O(1) thay vì O(n)

### **Nhược điểm Option A:**

1. ❌ **Gold storage tăng** - Duplicate data mỗi ngày
2. ❌ **Phải đọc Gold hôm qua** - Thêm I/O
3. ❌ **Logic phức tạp hơn** - Merge snapshots

---

### **Option B: Giữ Gold incremental, thêm Gold Snapshot table**

**Ý tưởng:** Tạo thêm 1 table Gold snapshot (full state) từ Gold incremental

```
Silver → Gold Incremental (cs_deal_detail_api)
              ↓
         Gold Snapshot (cs_deal_detail_snapshot) ← Mới!
              ↓
         Serving (incremental)
```

**Logic:**
```python
# Chạy sau Gold enrichment
def create_gold_snapshot(self):
    # 1. Đọc Gold hôm qua (snapshot)
    snapshot_yesterday = read_gold_snapshot(yesterday)
    
    # 2. Đọc Gold hôm nay (incremental)
    incremental_today = read_gold_incremental(today)
    
    # 3. Merge
    snapshot_today = merge(snapshot_yesterday, incremental_today)
    
    # 4. Ghi Gold snapshot
    write_gold_snapshot(snapshot_today)
```

**Ưu điểm:**
- ✅ Giữ nguyên Gold incremental (không breaking change)
- ✅ Serving vẫn incremental
- ✅ Tách biệt concerns

**Nhược điểm:**
- ❌ Thêm 1 table (complexity)
- ❌ Thêm 1 job (maintenance)

---

## 📊 So sánh Storage

### **Hiện tại (Gold Incremental):**

```
Day 1: 100 deals enriched → 100 records
Day 2: 50 deals enriched  → 50 records
Day 3: 75 deals enriched  → 75 records
...
Day 30: 60 deals enriched → 60 records

Total: ~2,000 records (30 ngày)
```

### **Option A (Gold Full Snapshot):**

```
Day 1: 10,000 deals (full) → 10,000 records
Day 2: 10,050 deals (full) → 10,050 records
Day 3: 10,100 deals (full) → 10,100 records
...
Day 30: 11,500 deals (full) → 11,500 records

Total: ~315,000 records (30 ngày)
```

**Storage tăng: ~157x** ❌

---

## 💡 Recommendation

### **Giải pháp Hybrid (Best of both worlds):**

**1. Giữ Gold Incremental (hiện tại)**
- ✅ Tiết kiệm storage
- ✅ Không breaking change
- ✅ Logic đã stable

**2. Tối ưu Serving với Window Scan**
- Giới hạn scan 30 ngày gần nhất
- Thêm cache/materialized view nếu cần

```python
# deal_detail_serving_layer.py
def get_latest_deals(self):
    from datetime import timedelta
    
    # Chỉ scan 30 ngày gần nhất
    cutoff_date = date.today() - timedelta(days=30)
    
    scan = table.scan(
        row_filter=GreaterThanOrEqual("snapshot_date", cutoff_date)
    )
    
    # DuckDB dedup (vẫn nhanh với 30 ngày)
    WITH latest_snapshots AS (
        SELECT id, MAX(snapshot_date) as max_date
        FROM last_30_days
        GROUP BY id
    )
    ...
```

**3. Nếu cần tối ưu hơn → Option A**
- Chỉ khi serving quá chậm (>5 phút)
- Chấp nhận trade-off storage

---

## 🎯 Kết luận

**CÓ THỂ áp dụng cơ chế Ticket cho Deal, NHƯNG:**

| Criteria | Ticket | Deal | Recommendation |
|----------|--------|------|----------------|
| **Data volume** | Nhỏ (~1K/day) | Lớn (~10K total) | ⚠️ Cân nhắc |
| **Change rate** | Cao (~50%) | Thấp (~0.5%) | ✅ Incremental tốt |
| **Storage cost** | Chấp nhận được | Tăng 157x | ❌ Đắt |
| **Serving speed** | Nhanh (10s) | Chậm hơn (30s) | ⚠️ Chấp nhận được |

**Đề xuất cuối:**
1. **Ngắn hạn**: Giữ nguyên, tối ưu window scan (30 ngày)
2. **Dài hạn**: Nếu data tăng nhiều → Implement Option A

**Anh muốn implement Option nào?** 🤔
