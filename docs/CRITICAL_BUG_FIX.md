# 🔴 CRITICAL BUG FIX: Drop Partition Logic

## ❌ **LỖI NGHIÊM TRỌNG ĐÃ PHÁT HIỆN**

### **Vấn đề:**
Hàm `drop_partition_if_exists()` được gọi **BÊN TRONG** `write_to_bronze()`, mà `write_to_bronze()` được gọi **4 LẦN** (mỗi ERP 1 lần).

### **Code SAI (Version 1):**

```python
def write_to_bronze(self, df):
    table = self.ensure_table_exists()
    self.drop_partition_if_exists(table)  # ← DROP MỖI LẦN GỌI!
    # Append data...

def run(self):
    for erp_name in ['sci', 'kn', 'pr', 'hh']:
        df = process_erp(erp_name)
        self.write_to_bronze(df)  # ← Gọi 4 lần = DROP 4 lần!
```

### **Kết quả thảm họa:**

```
Partition ban đầu: EMPTY

1. Process SCI:
   - DROP partition (empty, không làm gì)
   - APPEND 30,000 records
   → Partition có: 30,000 records ✅

2. Process KN:
   - DROP partition ← XÓA 30,000 RECORDS CỦA SCI! ❌
   - APPEND 25,000 records
   → Partition có: 25,000 records (MẤT 30k của SCI!)

3. Process PR:
   - DROP partition ← XÓA 25,000 RECORDS CỦA KN! ❌
   - APPEND 20,000 records
   → Partition có: 20,000 records (MẤT 25k của KN!)

4. Process HH:
   - DROP partition ← XÓA 20,000 RECORDS CỦA PR! ❌
   - APPEND 15,000 records
   → Partition có: 15,000 records (MẤT 20k của PR!)

KẾT QUẢ CUỐI CÙNG: CHỈ CÒN 15,000 RECORDS (CHỈ CÓ HH)
MẤT TOÀN BỘ DỮ LIỆU SCI, KN, PR! ❌❌❌
```

---

## ✅ **GIẢI PHÁP: DROP 1 LẦN DUY NHẤT**

### **Code ĐÚNG (Version 2):**

```python
def write_to_bronze(self, df, table):
    # Không drop partition ở đây nữa!
    # Chỉ append data
    # ...

def run(self):
    # DROP PARTITION 1 LẦN DUY NHẤT TRƯỚC KHI XỬ LÝ TẤT CẢ ERP
    table = self.ensure_table_exists()
    
    try:
        logger.info(f"Dropping partition snapshot_date={self.snapshot_date}")
        table.delete(f"snapshot_date = date('{self.snapshot_date}')")
        logger.info("✅ Dropped partition")
    except Exception as e:
        logger.info(f"Partition does not exist: {e}")
    
    # Sau đó xử lý từng ERP và APPEND vào partition đã drop
    for erp_name in ['sci', 'kn', 'pr', 'hh']:
        df = process_erp(erp_name)
        self.write_to_bronze(df, table)  # Chỉ APPEND, không DROP
```

### **Kết quả ĐÚNG:**

```
Partition ban đầu: Có 100,000 records cũ (duplicate)

0. Prepare table:
   - DROP partition ← XÓA 100,000 RECORDS CŨ
   → Partition: EMPTY ✅

1. Process SCI:
   - APPEND 30,000 records (không drop)
   → Partition có: 30,000 records ✅

2. Process KN:
   - APPEND 25,000 records (không drop)
   → Partition có: 55,000 records (30k + 25k) ✅

3. Process PR:
   - APPEND 20,000 records (không drop)
   → Partition có: 75,000 records (55k + 20k) ✅

4. Process HH:
   - APPEND 15,000 records (không drop)
   → Partition có: 90,000 records (75k + 15k) ✅

KẾT QUẢ CUỐI CÙNG: 90,000 RECORDS (TẤT CẢ 4 ERP)
ĐÚNG VÀ ĐẦY ĐỦ! ✅✅✅
```

---

## 🔧 **THAY ĐỔI CỤ THỂ**

### **1. Xóa `drop_partition_if_exists()` method**
```python
# ❌ DELETED
def drop_partition_if_exists(self, table):
    try:
        table.delete(f"snapshot_date = date('{self.snapshot_date}')")
    except Exception as e:
        logger.info(f"Partition does not exist: {e}")
```

### **2. Sửa `write_to_bronze()` signature**
```python
# Trước
def write_to_bronze(self, df: pd.DataFrame) -> None:
    table = self.ensure_table_exists()
    self.drop_partition_if_exists(table)  # ← XÓA DÒNG NÀY
    # ...

# Sau
def write_to_bronze(self, df: pd.DataFrame, table) -> None:
    # table được truyền vào từ run()
    # Không drop partition ở đây
    # ...
```

### **3. Sửa `run()` method**
```python
def run(self):
    # ...
    
    # ✅ THÊM: Drop partition 1 lần trước khi xử lý ERP
    logger.info("Preparing Bronze table")
    table = self.ensure_table_exists()
    
    try:
        logger.info(f"Dropping partition snapshot_date={self.snapshot_date}")
        table.delete(f"snapshot_date = date('{self.snapshot_date}')")
        logger.info("✅ Dropped partition")
    except Exception as e:
        logger.info(f"Partition does not exist: {e}")
    
    # Process ERPs
    for erp_name in ['sci', 'kn', 'pr', 'hh']:
        # ...
        self.write_to_bronze(df_bronze, table)  # ✅ Truyền table vào
```

---

## 📊 **SO SÁNH KẾT QUẢ**

### **Version 1 (SAI):**
| ERP | Records Read | After Write | Cumulative |
|-----|--------------|-------------|------------|
| SCI | 30,000 | 30,000 | 30,000 |
| KN  | 25,000 | 25,000 | 25,000 ❌ (mất SCI) |
| PR  | 20,000 | 20,000 | 20,000 ❌ (mất KN) |
| HH  | 15,000 | 15,000 | **15,000** ❌ (mất PR) |

**Tổng trong partition: 15,000 (chỉ HH)** ❌

### **Version 2 (ĐÚNG):**
| ERP | Records Read | After Write | Cumulative |
|-----|--------------|-------------|------------|
| DROP | - | 0 | 0 ✅ |
| SCI | 30,000 | 30,000 | 30,000 ✅ |
| KN  | 25,000 | 25,000 | 55,000 ✅ |
| PR  | 20,000 | 20,000 | 75,000 ✅ |
| HH  | 15,000 | 15,000 | **90,000** ✅ |

**Tổng trong partition: 90,000 (tất cả ERP)** ✅

---

## 🎯 **TẠI SAO LẠI SAI?**

### **Nguyên nhân:**
Tôi đã nghĩ rằng:
- "Mỗi ERP ghi vào partition riêng" ❌ SAI!
- Thực tế: **Tất cả ERP ghi vào CÙNG 1 partition** (snapshot_date=YYYY-MM-DD)

### **Bài học:**
- Partition key là `snapshot_date`, KHÔNG PHẢI `brand_etl`
- Tất cả records có cùng `snapshot_date` nằm trong **CÙNG 1 partition**
- DROP partition = xóa **TẤT CẢ** records trong partition đó (bất kể brand_etl)

---

## ✅ **VERIFICATION**

### **Cách kiểm tra:**

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog('bronze', ...)
table = catalog.load_table('caresoft.wehook_api_caresoft')

# Query partition
df = table.scan(row_filter="snapshot_date = date('2026-01-08')").to_pandas()

# Check counts per brand
print(df.groupby('brand_etl').size())

# Expected output (Version 2):
# brand_etl
# 1    25000  (KN)
# 3    20000  (PR)
# 4    15000  (HH)
# 7    30000  (SCI)
# Total: 90000 ✅

# Wrong output (Version 1):
# brand_etl
# 4    15000  (HH only)
# Total: 15000 ❌
```

---

## 🚀 **DEPLOYMENT**

### **Bước 1: Copy code mới**
```bash
cp erp_webhook_ingestion.py /srv/nfs_share/app/caresoft/
```

### **Bước 2: Chạy lại DAG**
```bash
# Trigger trong Airflow UI
# Hoặc test local:
python erp_webhook_ingestion.py "2026-01-08"
```

### **Bước 3: Verify**
```bash
# Check logs xem có message:
# "Dropping partition snapshot_date=2026-01-08"
# "✅ Dropped partition"
# "✅ ERP SCI: 30000 records written"
# "✅ ERP KN: 25000 records written"
# ...
# "Total records ingested: 90000"
```

---

## 📝 **SUMMARY**

| Aspect | Version 1 (SAI) | Version 2 (ĐÚNG) |
|--------|-----------------|------------------|
| **Drop location** | Inside `write_to_bronze()` | Inside `run()` before loop |
| **Drop frequency** | 4 times (per ERP) | **1 time** (before all ERPs) |
| **Result** | Only last ERP data | **All ERP data** ✅ |
| **Data loss** | 75% data lost | **No data loss** ✅ |
| **Correctness** | ❌ WRONG | ✅ CORRECT |

---

**Date:** 2026-01-08  
**Severity:** 🔴 CRITICAL  
**Status:** ✅ FIXED  
**Impact:** Prevents 75% data loss
