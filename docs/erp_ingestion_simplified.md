# ERP Direct Ingestion - Simplified Version

## 📋 Tổng quan

Script đơn giản hóa để đọc dữ liệu từ 4 hệ thống ERP và ghi vào Iceberg Bronze layer.

**Chiến lược:** Mỗi lần chạy sẽ **DROP partition cũ** (nếu tồn tại) → **APPEND dữ liệu mới**

---

## 🎯 Những gì đã thay đổi

### ❌ **ĐÃ XÓA:**
- ❌ Incremental mode logic
- ❌ `get_last_write_date()` method
- ❌ Conditional query building (incremental vs full)
- ❌ `first_write` flag logic
- ❌ `mode` parameter trong `write_to_bronze()`
- ❌ Tham số `incremental` từ command line

### ✅ **GIỮ LẠI:**
- ✅ Full load từ ERP (đọc toàn bộ bảng `api_log`)
- ✅ Sequential processing (xử lý từng ERP một)
- ✅ Batch writing (50k records/batch)
- ✅ Partition by `snapshot_date`
- ✅ Auto-create table nếu chưa tồn tại

---

## 🔄 Luồng hoạt động

```
1. Khởi tạo
   ├─ Load Iceberg catalog
   └─ Set snapshot_date (từ argument hoặc today)

2. Đảm bảo table tồn tại
   ├─ Nếu chưa có → Tạo table với partition spec
   └─ Nếu đã có → Sử dụng table hiện tại

3. Xử lý từng ERP (SCI → KN → PR → HH)
   │
   ├─ 3.1. Kết nối ERP
   │   └─ Create SQLAlchemy engine
   │
   ├─ 3.2. Đọc dữ liệu (FULL LOAD)
   │   └─ SELECT * FROM api_log ORDER BY write_date
   │
   ├─ 3.3. Thêm metadata
   │   ├─ Generate pk_id (MD5 hash)
   │   ├─ Add snapshot_date
   │   └─ Add ingestion_timestamp
   │
   ├─ 3.4. Ghi vào Bronze
   │   ├─ DROP partition snapshot_date=YYYY-MM-DD (nếu tồn tại)
   │   ├─ APPEND batch 1 (50k records)
   │   ├─ APPEND batch 2 (50k records)
   │   └─ APPEND batch N (remaining records)
   │
   └─ 3.5. Giải phóng RAM
       ├─ Dispose engine
       └─ Delete DataFrames

4. Kết thúc
   └─ Return stats (success/failed per ERP)
```

---

## 📊 Ví dụ cụ thể

### **Scenario: Chạy lại trong cùng ngày**

**Trước khi chạy:**
```
s3://lakehouse/bronze/caresoft.wehook_api_caresoft/
├── snapshot_date=2026-01-07/  → 100,000 records
├── snapshot_date=2026-01-08/  → 80,000 records (dữ liệu cũ)
```

**Khi chạy script với snapshot_date=2026-01-08:**

```
Step 1: Ensure table exists ✅

Step 2: Process SCI
  - Read 30,000 records from SCI
  - DROP partition snapshot_date=2026-01-08  ← Xóa 80k records cũ
  - APPEND 30,000 records mới

Step 3: Process KN
  - Read 25,000 records from KN
  - APPEND 25,000 records (vào partition 2026-01-08)

Step 4: Process PR
  - Read 20,000 records from PR
  - APPEND 20,000 records

Step 5: Process HH
  - Read 15,000 records from HH
  - APPEND 15,000 records
```

**Sau khi chạy:**
```
s3://lakehouse/bronze/caresoft.wehook_api_caresoft/
├── snapshot_date=2026-01-07/  → 100,000 records (giữ nguyên)
├── snapshot_date=2026-01-08/  → 90,000 records (30k+25k+20k+15k) ✅
```

---

## 🔑 Key Methods

### **1. `__init__(snapshot_date)`**
```python
ingestion = ERPDirectIngestion(date(2026, 1, 8))
```
- Khởi tạo với ngày snapshot
- Load Iceberg catalog

### **2. `create_erp_connection(erp_name)`**
```python
engine = self.create_erp_connection('sci')
```
- Tạo SQLAlchemy engine
- Fix URI cho SQLAlchemy 2.x

### **3. `read_from_erp(erp_name, engine)`**
```python
df = self.read_from_erp('sci', engine)
```
- **LUÔN LUÔN** đọc toàn bộ dữ liệu
- Query: `SELECT * FROM api_log ORDER BY write_date`

### **4. `add_metadata_and_pk(df)`**
```python
df_bronze = self.add_metadata_and_pk(df)
```
- Generate `pk_id` (MD5 hash)
- Add `snapshot_date` và `ingestion_timestamp`

### **5. `ensure_table_exists()`**
```python
table = self.ensure_table_exists()
```
- Load table nếu tồn tại
- Tạo mới nếu chưa có (với partition spec)

### **6. `drop_partition_if_exists(table)`**
```python
self.drop_partition_if_exists(table)
```
- Xóa partition của `snapshot_date` hiện tại
- Idempotent: không lỗi nếu partition không tồn tại

### **7. `write_to_bronze(df)`**
```python
self.write_to_bronze(df_bronze)
```
- Drop partition (nếu tồn tại)
- Append dữ liệu mới theo batch (50k records/batch)

### **8. `run()`**
```python
stats = ingestion.run()
```
- Xử lý tuần tự 4 ERP
- Return stats dictionary

---

## 🚀 Cách sử dụng

### **Từ Command Line:**
```bash
python erp_webhook_ingestion.py "2026-01-08"
```

### **Từ Airflow DAG:**
```python
arguments=['/app/caresoft/erp_webhook_ingestion.py', '{{ ds }}']
```

---

## 💡 Ưu điểm của phiên bản mới

| Aspect | Trước | Sau |
|--------|-------|-----|
| **Lines of code** | ~600 dòng | ~400 dòng |
| **Complexity** | High (2 modes) | Low (1 mode) |
| **Logic** | Conditional | Straightforward |
| **Idempotency** | ✅ | ✅ |
| **Memory usage** | Same | Same |
| **Performance** | Same | Same |
| **Maintainability** | Medium | **High** ✅ |
| **Debugging** | Complex | **Simple** ✅ |

---

## ⚠️ Lưu ý quan trọng

### **1. Luôn drop partition trước khi append**
```python
# Đảm bảo không bao giờ bị duplicate
self.drop_partition_if_exists(table)  # Xóa cũ
self.write_to_bronze(df)              # Ghi mới
```

### **2. Xử lý tuần tự để tiết kiệm RAM**
```python
# Không load 4 ERP cùng lúc
for erp in ['sci', 'kn', 'pr', 'hh']:
    process(erp)
    free_memory()  # Giải phóng trước khi xử lý ERP tiếp theo
```

### **3. Batch writing**
```python
# Chia nhỏ để tránh OOM
BATCH_SIZE = 50000
for batch in batches:
    table.append(batch)
```

---

## 🔧 Troubleshooting

### **Q: Dữ liệu bị duplicate?**
**A:** Không thể xảy ra vì luôn drop partition trước khi append

### **Q: Mất dữ liệu các ngày khác?**
**A:** Không thể xảy ra vì chỉ drop partition của `snapshot_date` hiện tại

### **Q: Chạy lại nhiều lần trong ngày?**
**A:** Hoàn toàn an toàn - partition sẽ được refresh mỗi lần chạy

### **Q: OOM error?**
**A:** Tăng memory limit hoặc giảm `BATCH_SIZE`

---

## 📈 Performance Metrics

**Ước tính với 1 triệu records:**

| Metric | Value |
|--------|-------|
| Read time | ~2-3 minutes |
| Transform time | ~1 minute |
| Write time | ~3-4 minutes |
| **Total** | **~6-8 minutes** |
| Memory peak | ~4-5 GB |

---

## ✅ Checklist Deploy

- [ ] Copy script vào NFS: `/srv/nfs_share/app/caresoft/erp_webhook_ingestion.py`
- [ ] Verify DAG arguments: `['{{ ds }}']` (không có incremental param)
- [ ] Test với 1 ngày cụ thể
- [ ] Verify partition được drop và recreate
- [ ] Check logs không có error
- [ ] Enable DAG schedule

---

## 📝 Example Output

```
================================================================================
Starting ERP Direct Ingestion for 2026-01-08
================================================================================

============================================================
Processing ERP: SCI
============================================================
Created connection to ERP SCI
Reading ALL records from ERP SCI api_log table (brand_etl=7)
Successfully read 30000 records from ERP SCI
Adding PK_ID and metadata columns
Generated PK_IDs for 30000 records
Writing to Bronze table: caresoft.wehook_api_caresoft
Table caresoft.wehook_api_caresoft exists
Attempting to drop partition snapshot_date=2026-01-08
✅ Dropped partition snapshot_date=2026-01-08
Writing 30000 records to partition snapshot_date=2026-01-08
Processing 1 batches of up to 50000 records each
Batch 1/1: Processing 30000 records (0 to 30000)
✅ Batch 1/1: Wrote 30000 records
✅ Successfully wrote all 30000 records to Bronze layer in 1 batches
✅ ERP SCI: 30000 records written to Bronze

[... KN, PR, HH ...]

================================================================================
ERP Direct Ingestion completed
Total records ingested: 90000
Duration: 245.32 seconds
Stats by ERP:
  SCI: {'status': 'success', 'records': 30000}
  KN: {'status': 'success', 'records': 25000}
  PR: {'status': 'success', 'records': 20000}
  HH: {'status': 'success', 'records': 15000}
================================================================================
```
