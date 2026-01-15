# Thời Gian Chạy Các Bảng CareSoft
## Dependency Analysis for Custom Function

---

## 🎯 Câu Hỏi

**Bạn muốn chạy 1 function sau khi các bảng này được update xong:**
- `cs_ticket_api`
- `cs_contact_api`  
- `cs_custom_fields`
- `cs_chat_api`

**→ Nên chạy lúc nào?**

---

## 📊 Lịch Chạy Các Bảng (Hệ Thống Cũ)

### 1. `cs_ticket_api`

| Time | Cron | Function | Status |
|------|------|----------|--------|
| **01:30** | `30 1 * * *` | `update_caresoft_tickets_data_day_by_day_ago_from_settings` | ✅ Run 1 |
| **08:35** | `35 08 * * *` | `update_caresoft_tickets_data_day_by_day_ago_from_settings` | ✅ Run 2 |

**Kết thúc:** ~**01:45** (run 1) và ~**08:50** (run 2)

---

### 2. `cs_contact_api`

| Time | Cron | Function | Status |
|------|------|----------|--------|
| **Every 15 min** | `*/15 * * * *` | `update_caresoft_contact_field_data_day_by_day_ago_from_settings` | ✅ Continuous |
| **01:45** | `45 1 * * *` | `update_caresoft_contacts_ttc_field_data_day_by_day_ago_from_settings` | ✅ Daily |

**Kết thúc:** Continuous (mỗi 15 phút), Daily run kết thúc ~**02:00**

---

### 3. `cs_custom_fields`

| Time | Cron | Function | Status |
|------|------|----------|--------|
| **05:30** | `30 5 * * *` | `update_caresoft_custom_field_data_day_by_day_ago_from_settings` | ✅ Run 1 |
| **05:45** | `45 5 * * *` | `update_caresoft_custom_field_khach_hang_data_day_by_day_ago_from_settings` | ✅ Run 2 |

**Kết thúc:** ~**06:00**

---

### 4. `cs_chat_api`

| Time | Cron | Function | Status |
|------|------|----------|--------|
| **02:30** | `30 2 * * *` | `update_caresoft_chats_field_data_day_by_day_ago_from_settings` | ✅ Daily |
| **Every hour :45** | `45 * * * *` | `update_caresoft_agent_from_cs_chat_data_day_by_day_ago_from_settings` | ✅ Hourly |

**Kết thúc:** Daily run ~**02:45**, Hourly run continuous

---

## ⏰ Timeline Visualization

```
00:00 ────────────────────────────────────────────────────────────────── 24:00
  │
  01:30 ── cs_ticket_api (run 1) ────────────────┐
  01:45 ── cs_contact_api (ttc) ─────────────────┤
  02:00 ── [cs_contact_api ttc DONE] ────────────┤
  02:30 ── cs_chat_api ──────────────────────────┤
  02:45 ── [cs_chat_api DONE] ───────────────────┤
  03:00 ─────────────────────────────────────────┤
  05:30 ── cs_custom_fields (run 1) ─────────────┤
  05:45 ── cs_custom_fields (run 2) ─────────────┤
  06:00 ── [cs_custom_fields DONE] ──────────────┘ ← ALL 4 TABLES DONE
  08:35 ── cs_ticket_api (run 2) ────────────────┐
  08:50 ── [cs_ticket_api run 2 DONE] ───────────┘
  
  + Every 15 min: cs_contact_api (continuous)
  + Every hour :45: cs_chat_api agent update (continuous)
```

---

## 🎯 Trả Lời: Nên Chạy Function Lúc Nào?

### ✅ Option 1: Sau Khi TẤT CẢ 4 Bảng Update Xong (Daily)

**Thời gian:** **06:15** (hoặc muộn hơn)

**Lý do:**
- `cs_ticket_api` (run 1): Done ~01:45
- `cs_contact_api` (ttc): Done ~02:00
- `cs_chat_api`: Done ~02:45
- `cs_custom_fields`: Done ~**06:00** ← **MUỘN NHẤT**

**Crontab:**
```python
('15 6 * * *', 'your_module.your_function'),  # 06:15 daily
```

**Ưu điểm:**
- ✅ Đảm bảo TẤT CẢ 4 bảng đã update xong
- ✅ Chạy 1 lần/ngày, dữ liệu đầy đủ nhất

**Nhược điểm:**
- ❌ Phải đợi đến 06:00 (muộn)
- ❌ Không catch được `cs_ticket_api` run 2 (08:35)

---

### ✅ Option 2: Sau Mỗi Lần Update (Multiple Times)

**Thời gian:** Sau mỗi bảng update

**Crontab:**
```python
# Sau cs_ticket_api run 1
('00 2 * * *', 'your_module.your_function'),  # 02:00 daily

# Sau cs_chat_api
('00 3 * * *', 'your_module.your_function'),  # 03:00 daily

# Sau cs_custom_fields
('15 6 * * *', 'your_module.your_function'),  # 06:15 daily

# Sau cs_ticket_api run 2
('00 9 * * *', 'your_module.your_function'),  # 09:00 daily
```

**Ưu điểm:**
- ✅ Catch được data sớm hơn
- ✅ Có thể xử lý incremental

**Nhược điểm:**
- ❌ Chạy nhiều lần (tốn resource)
- ❌ Logic phức tạp hơn

---

### ✅ Option 3: Chạy Continuous (Mỗi 15-30 phút)

**Thời gian:** Continuous

**Crontab:**
```python
('*/30 * * * *', 'your_module.your_function'),  # Every 30 minutes
```

**Ưu điểm:**
- ✅ Luôn có data mới nhất
- ✅ Không cần quan tâm schedule của các bảng khác

**Nhược điểm:**
- ❌ Chạy rất nhiều lần (tốn resource)
- ❌ Có thể xử lý duplicate data

---

## 📋 Dependency Matrix

| Bảng | Chạy Lúc | Kết Thúc | Frequency |
|------|----------|-----------|-----------|
| `cs_ticket_api` | 01:30, 08:35 | ~01:45, ~08:50 | 2x/day |
| `cs_contact_api` (ttc) | 01:45 | ~02:00 | 1x/day |
| `cs_contact_api` | Every 15 min | Continuous | 96x/day |
| `cs_chat_api` | 02:30 | ~02:45 | 1x/day |
| `cs_chat_api` (agent) | Every hour :45 | Continuous | 24x/day |
| `cs_custom_fields` | 05:30, 05:45 | ~06:00 | 2x/day |

**Latest Completion Time (Daily):** **~06:00** (`cs_custom_fields`)

---

## 🎯 Recommendation

### Nếu Bạn Cần Data Đầy Đủ Nhất (1x/day):

```python
# File: app/settings.py
CRONJOBS = [
    ...
    # Chạy SAU KHI TẤT CẢ 4 bảng update xong
    ('15 6 * * *', 'your_module.your_function_after_all_tables_updated'),
    ...
]
```

**Thời gian:** **06:15 daily**

**Lý do:**
- ✅ `cs_custom_fields` kết thúc lúc ~06:00 (muộn nhất)
- ✅ Tất cả 4 bảng đã có data đầy đủ
- ✅ Chạy 1 lần/ngày, tiết kiệm resource

---

### Nếu Bạn Cần Data Realtime (Continuous):

```python
# File: app/settings.py
CRONJOBS = [
    ...
    # Chạy mỗi 30 phút
    ('*/30 * * * *', 'your_module.your_function_continuous'),
    ...
]
```

**Thời gian:** **Every 30 minutes**

**Lý do:**
- ✅ Luôn có data mới nhất
- ✅ Không phụ thuộc vào schedule của các bảng khác
- ❌ Tốn resource hơn

---

## 📝 Example Implementation

### Option 1: Daily After All Tables (Recommended)

```python
# File: your_module/cron_jobs.py

def your_function_after_all_tables_updated():
    """
    Chạy sau khi TẤT CẢ 4 bảng đã update xong:
    - cs_ticket_api (done ~01:45)
    - cs_contact_api (done ~02:00)
    - cs_chat_api (done ~02:45)
    - cs_custom_fields (done ~06:00) ← MUỘN NHẤT
    
    Schedule: 06:15 daily
    """
    from core import log
    from django.utils.translation import gettext_lazy as _
    
    try:
        log.etl_logging(
            name=_("[Your Function] Start processing after all tables updated"),
            source="caresoft",
            status="INFO",
            function_name="your_function_after_all_tables_updated",
            note="Processing data from cs_ticket_api, cs_contact_api, cs_custom_fields, cs_chat_api"
        )
        
        # Your logic here
        # Example: Join data from all 4 tables
        # result = process_all_tables()
        
        log.etl_logging(
            name=_("[Your Function] Success"),
            source="caresoft",
            status="SUCCESS",
            function_name="your_function_after_all_tables_updated",
            note="Processed successfully"
        )
        
    except Exception as e:
        log.logging_exception(None, "your_function_after_all_tables_updated", e.args)
        log.etl_logging(
            name=_("[Your Function] Error"),
            source="caresoft",
            status="ERROR",
            function_name="your_function_after_all_tables_updated",
            note=str(e.args)
        )
```

### Add to settings.py

```python
# File: app/settings.py

CRONJOBS = [
    # ... existing jobs ...
    
    # Your custom function - chạy sau khi tất cả 4 bảng update xong
    ('15 6 * * *', 'your_module.cron_jobs.your_function_after_all_tables_updated'),
]
```

---

## ✅ Summary

| Scenario | Recommended Time | Cron | Frequency |
|----------|------------------|------|-----------|
| **Daily (Full Data)** | **06:15** | `15 6 * * *` | 1x/day |
| **After Morning Updates** | 03:00 | `0 3 * * *` | 1x/day |
| **Continuous (Realtime)** | Every 30 min | `*/30 * * * *` | 48x/day |

**Best Practice:** **06:15 daily** - Sau khi tất cả 4 bảng đã update xong!
