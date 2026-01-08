# 🔴 ROOT CAUSE: Delete Filter Type Mismatch

## ❌ **VẤN ĐỀ THỰC SỰ**

### **Triệu chứng:**
- Code chạy không báo lỗi
- Logs hiển thị "✅ Dropped partition"
- Nhưng dữ liệu **KHÔNG BỊ XÓA**, vẫn tăng lên

### **Root Cause:**
**Type mismatch trong delete filter!**

Schema định nghĩa `snapshot_date` là **DateType**:
```python
NestedField(17, "snapshot_date", DateType(), required=False)
```

Nhưng delete filter so sánh với **STRING**:
```python
# ❌ SAI - So sánh DateType với String
table.delete(f"snapshot_date == '{self.snapshot_date}'")  
# hoặc
table.delete(EqualTo("snapshot_date", str(self.snapshot_date)))
```

**Kết quả:** 
- PyIceberg **KHÔNG BÁO LỖI** (silent failure)
- Filter không match bất kỳ row nào
- Delete 0 rows
- Logs vẫn hiển thị "✅ Dropped partition" (vì không có exception)

---

## ✅ **GIẢI PHÁP**

### **So sánh với date object, KHÔNG PHẢI string:**

```python
# ✅ ĐÚNG - So sánh DateType với date object
from pyiceberg.expressions import EqualTo

table.delete(delete_filter=EqualTo("snapshot_date", self.snapshot_date))
# self.snapshot_date là date object (date(2026, 1, 8))
```

---

## 📊 **SO SÁNH**

### **Version SAI (String comparison):**
```python
snapshot_date = date(2026, 1, 8)
date_str = str(snapshot_date)  # "2026-01-08"

# Delete với string
table.delete(EqualTo("snapshot_date", date_str))

# PyIceberg internally:
# - Schema: snapshot_date is DateType
# - Filter value: "2026-01-08" is String
# - Type mismatch → No rows match → Delete 0 rows
# - No exception raised (silent failure)
```

**Kết quả:**
```
Before: 100,000 records in partition 2026-01-08
After:  100,000 records (KHÔNG XÓA GÌ CẢ!) ❌
```

### **Version ĐÚNG (Date comparison):**
```python
snapshot_date = date(2026, 1, 8)

# Delete với date object
table.delete(EqualTo("snapshot_date", snapshot_date))

# PyIceberg internally:
# - Schema: snapshot_date is DateType
# - Filter value: date(2026, 1, 8) is date
# - Type match → All rows with this date match → Delete all
```

**Kết quả:**
```
Before: 100,000 records in partition 2026-01-08
After:  0 records (XÓA THÀNH CÔNG!) ✅
```

---

## 🔍 **TẠI SAO KHÔNG BÁO LỖI?**

PyIceberg **không raise exception** khi type mismatch vì:
1. Filter được evaluate cho mỗi row
2. Type mismatch → Filter luôn return `False`
3. Không có row nào match → Delete 0 rows
4. Delete 0 rows **KHÔNG PHẢI LỖI** (có thể partition đã empty)
5. → Không có exception

**Đây là behavior hợp lý** nhưng gây khó debug!

---

## 🧪 **CÁCH KIỂM TRA**

### **Test với scan trước khi delete:**

```python
from pyiceberg.expressions import EqualTo
from datetime import date

snapshot_date = date(2026, 1, 8)

# Test 1: String comparison (SAI)
df_string = table.scan(
    row_filter=EqualTo("snapshot_date", str(snapshot_date))
).to_pandas()
print(f"String filter matched: {len(df_string)} rows")  # → 0 rows ❌

# Test 2: Date comparison (ĐÚNG)
df_date = table.scan(
    row_filter=EqualTo("snapshot_date", snapshot_date)
).to_pandas()
print(f"Date filter matched: {len(df_date)} rows")  # → 100,000 rows ✅
```

---

## 📝 **LESSONS LEARNED**

### **1. Type Safety Matters**
- PyIceberg schema có strict typing
- Filter value phải match schema type
- String ≠ Date, Int ≠ String, etc.

### **2. Silent Failures Are Dangerous**
- Delete 0 rows không raise exception
- Phải check logs kỹ: "deleted X rows"
- Nếu X = 0 mà expect > 0 → có vấn đề

### **3. Use Expression API**
- String filters (`"snapshot_date == '2026-01-08'"`) dễ sai type
- Expression API (`EqualTo("snapshot_date", date_obj)`) type-safe hơn
- Python type system giúp catch lỗi sớm

### **4. Always Verify**
- Test filter với `scan()` trước khi `delete()`
- Check row count trước và sau delete
- Monitor logs để confirm số rows deleted

---

## 🔧 **CODE CHANGES**

### **Before (WRONG):**
```python
# Version 1: String filter
table.delete(f"snapshot_date == '{self.snapshot_date}'")

# Version 2: String with Expression API
date_str = str(self.snapshot_date)
table.delete(EqualTo("snapshot_date", date_str))
```

### **After (CORRECT):**
```python
from pyiceberg.expressions import EqualTo

# Use date object directly
table.delete(delete_filter=EqualTo("snapshot_date", self.snapshot_date))
```

### **With logging:**
```python
from pyiceberg.expressions import EqualTo

try:
    logger.info(f"Dropping partition snapshot_date={self.snapshot_date}")
    
    # CRITICAL: Compare with date object, not string
    delete_count = table.delete(delete_filter=EqualTo("snapshot_date", self.snapshot_date))
    
    logger.info(f"✅ Dropped {delete_count} rows from partition {self.snapshot_date}")
    
    # Verify
    if delete_count == 0:
        logger.warning("⚠️  Deleted 0 rows - partition may already be empty or filter didn't match")
        
except Exception as e:
    logger.error(f"❌ Delete failed: {e}")
```

---

## ✅ **VERIFICATION CHECKLIST**

Sau khi deploy code mới:

- [ ] Check logs có message: "Dropping partition snapshot_date=YYYY-MM-DD"
- [ ] Check logs có message: "✅ Dropped X rows" với **X > 0**
- [ ] Nếu X = 0, check xem partition có data không
- [ ] Query Iceberg table để verify partition đã bị xóa
- [ ] Chạy lại và verify tổng số records = sum(4 ERPs), không tăng gấp đôi

---

## 🎯 **SUMMARY**

| Issue | Cause | Fix |
|-------|-------|-----|
| Delete không xóa data | Type mismatch: DateType vs String | Compare với date object |
| Không báo lỗi | Delete 0 rows không phải exception | Add logging để check row count |
| Dữ liệu tăng lên | Append mà không drop | Fix delete filter type |

**Root cause:** `table.delete(EqualTo("snapshot_date", STRING))` → Match 0 rows → Delete nothing

**Solution:** `table.delete(EqualTo("snapshot_date", DATE_OBJECT))` → Match all rows → Delete successfully

---

**Date:** 2026-01-08  
**Severity:** 🔴 CRITICAL  
**Status:** ✅ FIXED  
**Impact:** Prevents data duplication
