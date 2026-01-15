# Hardcode Brand 1 và 3 - Giải Thích Chi Tiết
## Step-by-Step Explanation

---

## 🎯 Tổng Quan

Hệ thống cũ **hardcode** các custom field IDs khác nhau cho Brand 1 và Brand 3 vì:
- Mỗi brand có **custom fields khác nhau** trong CareSoft
- Cùng một field (ví dụ "Ghi chú") nhưng có **ID khác nhau** ở mỗi brand

---

## 📊 Step 1: API Response (Giống Nhau Về Cấu Trúc)

### Brand 1 (erp_id = 1) - API Response

```json
GET https://api.caresoft.vn/brand1/api/v1/tickets

{
    "ticket_id": 123456,
    "ticket_status": "open",
    "custom_fields": [
        {
            "id": 6014,  // ← ID khác với Brand 3!
            "label": "Ghi chú",
            "type": "Text Area",
            "value": "Khách hàng muốn tư vấn"
        },
        {
            "id": 7937,  // ← ID khác với Brand 3!
            "label": "Phân loại kết nối",
            "type": "Single drop-down list",
            "value": "Tư vấn"
        },
        {
            "id": 5972,  // ← ID khác với Brand 3!
            "label": "Mức độ hài lòng",
            "type": "Single drop-down list",
            "value": "Hài lòng"
        },
        {
            "id": 5833,  // ← ID khác với Brand 3!
            "label": "Dịch vụ chi tiết",
            "type": "Single drop-down list",
            "value": "Tư vấn da"
        }
    ]
}
```

### Brand 3 (erp_id = 3) - API Response

```json
GET https://api.caresoft.vn/brand3/api/v1/tickets

{
    "ticket_id": 789012,
    "ticket_status": "open",
    "custom_fields": [
        {
            "id": 6016,  // ← ID KHÁC với Brand 1! (6014 vs 6016)
            "label": "Ghi chú",  // ← Cùng tên nhưng ID khác!
            "type": "Text Area",
            "value": "Khách hàng hỏi về giá"
        },
        {
            "id": 5864,  // ← ID KHÁC với Brand 1! (7937 vs 5864)
            "label": "Phân loại kết nối",  // ← Cùng tên nhưng ID khác!
            "type": "Single drop-down list",
            "value": "Hỏi giá"
        },
        {
            "id": 5979,  // ← ID KHÁC với Brand 1! (5972 vs 5979)
            "label": "Mức độ hài lòng",  // ← Cùng tên nhưng ID khác!
            "type": "Single drop-down list",
            "value": "Bình thường"
        },
        {
            "id": 5873,  // ← ID KHÁC với Brand 1! (5833 vs 5873)
            "label": "Dịch vụ chi tiết",  // ← Cùng tên nhưng ID khác!
            "type": "Single drop-down list",
            "value": "Tư vấn răng"
        }
    ]
}
```

---

## 🔧 Step 2: Hardcode Logic (cs_extract.py lines 310-326)

```python
# Hardcode IDs cho từng brand
if self.obj_brand.erp_id == 1:
    ghi_chu = 6014                      # Brand 1: "Ghi chú" có ID = 6014
    id_value_phan_loai_ket_noi = 7937  # Brand 1: "Phân loại kết nối" có ID = 7937
    id_value_muc_do_hai_long = 5972    # Brand 1: "Mức độ hài lòng" có ID = 5972
    id_value_dich_vu_chi_tiet = 5833   # Brand 1: "Dịch vụ chi tiết" có ID = 5833

elif self.obj_brand.erp_id == 3:
    ghi_chu = 6016                      # Brand 3: "Ghi chú" có ID = 6016 (KHÁC!)
    id_value_phan_loai_ket_noi = 5864  # Brand 3: "Phân loại kết nối" có ID = 5864 (KHÁC!)
    id_value_muc_do_hai_long = 5979    # Brand 3: "Mức độ hài lòng" có ID = 5979 (KHÁC!)
    id_value_dich_vu_chi_tiet = 5873   # Brand 3: "Dịch vụ chi tiết" có ID = 5873 (KHÁC!)

else:
    # Brand khác (không phải 1 hoặc 3) → Không extract được!
    ghi_chu = 0
    id_value_phan_loai_ket_noi = 0
    id_value_muc_do_hai_long = 0
    id_value_dich_vu_chi_tiet = 0
```

---

## 📋 Step 3: Mapping Table

### Brand 1 vs Brand 3 - Custom Field IDs

| Field Name (Label) | Brand 1 ID | Brand 3 ID | Chênh Lệch |
|-------------------|------------|------------|------------|
| **Ghi chú** | 6014 | 6016 | +2 |
| **Phân loại kết nối** | 7937 | 5864 | -2073 |
| **Mức độ hài lòng** | 5972 | 5979 | +7 |
| **Dịch vụ chi tiết** | 5833 | 5873 | +40 |

**Kết luận:** Mỗi brand có **custom field IDs hoàn toàn khác nhau**!

---

## 🔄 Step 4: Extraction Process

### Ví Dụ Cụ Thể: Brand 1

```python
# 1. API trả về custom_fields
custom_fields = [
    {"id": 6014, "label": "Ghi chú", "value": "Khách hàng muốn tư vấn"},
    {"id": 7937, "label": "Phân loại kết nối", "value": "Tư vấn"},
    {"id": 5972, "label": "Mức độ hài lòng", "value": "Hài lòng"},
    {"id": 5833, "label": "Dịch vụ chi tiết", "value": "Tư vấn da"}
]

# 2. Hardcode IDs cho Brand 1
ghi_chu = 6014
id_value_phan_loai_ket_noi = 7937
id_value_muc_do_hai_long = 5972
id_value_dich_vu_chi_tiet = 5833

# 3. Loop qua custom_fields và extract
for item in custom_fields:
    # Tìm "Ghi chú" (ID = 6014)
    if item["id"] == ghi_chu:  # 6014 == 6014 ✅
        df.at[i, "ghi_chu"] = item["value"]  # "Khách hàng muốn tư vấn"
    
    # Tìm "Phân loại kết nối" (ID = 7937)
    if item["id"] == id_value_phan_loai_ket_noi:  # 7937 == 7937 ✅
        df.at[i, "id_value_phan_loai_ket_noi"] = item["value"]  # "Tư vấn"
    
    # Tìm "Mức độ hài lòng" (ID = 5972)
    if item["id"] == id_value_muc_do_hai_long:  # 5972 == 5972 ✅
        df.at[i, "id_value_muc_do_hai_long"] = item["value"]  # "Hài lòng"
    
    # Tìm "Dịch vụ chi tiết" (ID = 5833)
    if item["id"] == id_value_dich_vu_chi_tiet:  # 5833 == 5833 ✅
        df.at[i, "id_value_dich_vu_chi_tiet"] = item["value"]  # "Tư vấn da"

# 4. Kết quả: DataFrame có thêm 4 columns mới
# ghi_chu = "Khách hàng muốn tư vấn"
# id_value_phan_loai_ket_noi = "Tư vấn"
# id_value_muc_do_hai_long = "Hài lòng"
# id_value_dich_vu_chi_tiet = "Tư vấn da"
```

### Ví Dụ Cụ Thể: Brand 3

```python
# 1. API trả về custom_fields
custom_fields = [
    {"id": 6016, "label": "Ghi chú", "value": "Khách hàng hỏi về giá"},
    {"id": 5864, "label": "Phân loại kết nối", "value": "Hỏi giá"},
    {"id": 5979, "label": "Mức độ hài lòng", "value": "Bình thường"},
    {"id": 5873, "label": "Dịch vụ chi tiết", "value": "Tư vấn răng"}
]

# 2. Hardcode IDs cho Brand 3 (KHÁC với Brand 1!)
ghi_chu = 6016  # ← KHÁC! (Brand 1 là 6014)
id_value_phan_loai_ket_noi = 5864  # ← KHÁC! (Brand 1 là 7937)
id_value_muc_do_hai_long = 5979  # ← KHÁC! (Brand 1 là 5972)
id_value_dich_vu_chi_tiet = 5873  # ← KHÁC! (Brand 1 là 5833)

# 3. Loop qua custom_fields và extract
for item in custom_fields:
    # Tìm "Ghi chú" (ID = 6016)
    if item["id"] == ghi_chu:  # 6016 == 6016 ✅
        df.at[i, "ghi_chu"] = item["value"]  # "Khách hàng hỏi về giá"
    
    # Tìm "Phân loại kết nối" (ID = 5864)
    if item["id"] == id_value_phan_loai_ket_noi:  # 5864 == 5864 ✅
        df.at[i, "id_value_phan_loai_ket_noi"] = item["value"]  # "Hỏi giá"
    
    # Tìm "Mức độ hài lòng" (ID = 5979)
    if item["id"] == id_value_muc_do_hai_long:  # 5979 == 5979 ✅
        df.at[i, "id_value_muc_do_hai_long"] = item["value"]  # "Bình thường"
    
    # Tìm "Dịch vụ chi tiết" (ID = 5873)
    if item["id"] == id_value_dich_vu_chi_tiet:  # 5873 == 5873 ✅
        df.at[i, "id_value_dich_vu_chi_tiet"] = item["value"]  # "Tư vấn răng"

# 4. Kết quả: DataFrame có thêm 4 columns mới
# ghi_chu = "Khách hàng hỏi về giá"
# id_value_phan_loai_ket_noi = "Hỏi giá"
# id_value_muc_do_hai_long = "Bình thường"
# id_value_dich_vu_chi_tiet = "Tư vấn răng"
```

---

## ❌ Step 5: Vấn Đề Khi Có Brand Mới (Brand 5)

### Brand 5 - API Response

```json
{
    "ticket_id": 999999,
    "custom_fields": [
        {
            "id": 8888,  // ← ID MỚI, không có trong hardcode!
            "label": "Ghi chú",
            "value": "Khách hàng VIP"
        },
        {
            "id": 9999,  // ← ID MỚI, không có trong hardcode!
            "label": "Phân loại kết nối",
            "value": "VIP"
        }
    ]
}
```

### Code Xử Lý Brand 5

```python
# Hardcode logic
if self.obj_brand.erp_id == 1:
    ghi_chu = 6014
elif self.obj_brand.erp_id == 3:
    ghi_chu = 6016
else:
    # Brand 5 rơi vào đây!
    ghi_chu = 0  # ❌ Không extract được!

# Loop qua custom_fields
for item in custom_fields:
    if item["id"] == ghi_chu:  # 8888 == 0 ❌ KHÔNG MATCH!
        df.at[i, "ghi_chu"] = item["value"]  # ← KHÔNG CHẠY!

# Kết quả: Brand 5 KHÔNG extract được custom fields!
# ghi_chu = NULL hoặc 0
# id_value_phan_loai_ket_noi = NULL hoặc 0
```

---

## 📊 Comparison Table

### Extraction Results

| Brand | erp_id | Ghi Chú ID | Extracted? | Value |
|-------|--------|------------|------------|-------|
| Brand 1 | 1 | 6014 | ✅ Yes | "Khách hàng muốn tư vấn" |
| Brand 3 | 3 | 6016 | ✅ Yes | "Khách hàng hỏi về giá" |
| Brand 5 | 5 | 8888 | ❌ No | NULL (hardcode = 0) |
| Brand 7 | 7 | 7777 | ❌ No | NULL (hardcode = 0) |

---

## 🔧 Tại Sao Phải Hardcode?

### Lý Do

1. **Mỗi brand tự setup custom fields** trong CareSoft
2. **CareSoft tự động tạo ID** cho mỗi custom field
3. **IDs không đồng nhất** giữa các brands
4. **Không có API** để query custom field definitions

### Ví Dụ Setup

**Brand 1 setup custom fields:**
```
Admin CareSoft Brand 1 → Settings → Custom Fields → Create
- Tạo field "Ghi chú" → CareSoft tự động assign ID = 6014
- Tạo field "Phân loại kết nối" → CareSoft tự động assign ID = 7937
```

**Brand 3 setup custom fields:**
```
Admin CareSoft Brand 3 → Settings → Custom Fields → Create
- Tạo field "Ghi chú" → CareSoft tự động assign ID = 6016 (KHÁC!)
- Tạo field "Phân loại kết nối" → CareSoft tự động assign ID = 5864 (KHÁC!)
```

---

## 🎯 Giải Pháp Tốt Hơn

### Option 1: Database Configuration

```sql
CREATE TABLE caresoft_custom_field_mapping (
    brand_id INT,
    field_name VARCHAR(100),
    custom_field_id INT,
    PRIMARY KEY (brand_id, field_name)
);

-- Brand 1
INSERT INTO caresoft_custom_field_mapping VALUES
(1, 'ghi_chu', 6014),
(1, 'phan_loai_ket_noi', 7937),
(1, 'muc_do_hai_long', 5972),
(1, 'dich_vu_chi_tiet', 5833);

-- Brand 3
INSERT INTO caresoft_custom_field_mapping VALUES
(3, 'ghi_chu', 6016),
(3, 'phan_loai_ket_noi', 5864),
(3, 'muc_do_hai_long', 5979),
(3, 'dich_vu_chi_tiet', 5873);

-- Brand 5 (NEW!)
INSERT INTO caresoft_custom_field_mapping VALUES
(5, 'ghi_chu', 8888),
(5, 'phan_loai_ket_noi', 9999);
```

**Code:**
```python
# Load from database
mappings = get_custom_field_mappings(brand_id)
# mappings = {'ghi_chu': 6014, 'phan_loai_ket_noi': 7937, ...}

# Extract
for item in custom_fields:
    if item["id"] == mappings.get("ghi_chu"):
        df.at[i, "ghi_chu"] = item["value"]
```

### Option 2: Store Full JSON (Recommended)

```python
# Không extract, lưu full JSON
record = {
    "custom_fields": json.dumps(custom_fields, ensure_ascii=False)
}

# Extract sau khi cần, ở downstream processing
```

---

## ✅ Summary

### Hardcode Như Thế Nào?

1. **Mỗi brand có custom field IDs khác nhau**
2. **Code hardcode IDs cho từng brand**:
   - Brand 1: `ghi_chu = 6014`
   - Brand 3: `ghi_chu = 6016`
3. **Loop qua custom_fields và match by ID**
4. **Extract value vào column mới**

### Vấn Đề

- ❌ Chỉ work cho 2 brands (1 và 3)
- ❌ Brand mới phải sửa code
- ❌ Không scalable
- ❌ Không maintainable

### Giải Pháp

- ✅ Lưu full JSON
- ✅ Extract ở downstream
- ✅ Config-driven (database)
- ✅ Works cho unlimited brands
