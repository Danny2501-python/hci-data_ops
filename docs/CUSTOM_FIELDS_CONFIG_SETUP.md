# Custom Fields Extraction - Config Setup Guide
## How to Configure Brand-Specific Custom Field Mappings

---

## ✅ Đã Implement!

Hệ thống mới **ĐÃ EXTRACT** các custom fields như hệ thống cũ:
- `ghi_chu`
- `id_value_phan_loai_ket_noi`
- `id_value_muc_do_hai_long`
- `id_value_dich_vu_chi_tiet`

**Nhưng thay vì hardcode**, hệ thống mới sử dụng **config từ database**!

---

## 🎯 Cách Hoạt Động

### Old System (Hardcode)

```python
# ❌ Hardcode trong code
if brand_id == 1:
    ghi_chu = 6014
    phan_loai_ket_noi = 7937
elif brand_id == 3:
    ghi_chu = 6016
    phan_loai_ket_noi = 5864
```

### New System (Config-Driven)

```python
# ✅ Load config từ database
brand_config = get_brand(brand_id)
mappings = json.loads(brand_config['custom_field_mappings'])

# Extract using config
for item in custom_fields:
    if item['id'] == mappings['ghi_chu']:
        value = item['value']
```

---

## 📋 Setup Guide

### Step 1: Update `core_brand` Table

Add `custom_field_mappings` column (JSON string):

```sql
-- Example for Brand 1 (erp_id = 1)
UPDATE caresoft.core_brand
SET custom_field_mappings = '{
    "ghi_chu": 6014,
    "phan_loai_ket_noi": 7937,
    "muc_do_hai_long": 5972,
    "dich_vu_chi_tiet": 5833
}'
WHERE erp_id = 1;

-- Example for Brand 3 (erp_id = 3)
UPDATE caresoft.core_brand
SET custom_field_mappings = '{
    "ghi_chu": 6016,
    "phan_loai_ket_noi": 5864,
    "muc_do_hai_long": 5979,
    "dich_vu_chi_tiet": 5873
}'
WHERE erp_id = 3;
```

---

### Step 2: Find Custom Field IDs for Your Brand

#### Method 1: Check CareSoft Admin

1. Login to CareSoft admin for your brand
2. Go to **Settings** → **Custom Fields**
3. Note down the ID for each field you want to extract

#### Method 2: Query API Response

```python
# Call tickets API
response = requests.get(
    f"https://api.caresoft.vn/{domain}/api/v1/tickets",
    params={"page": 1, "count": 1}
)

# Check custom_fields in response
ticket = response.json()['tickets'][0]
for field in ticket['custom_fields']:
    print(f"ID: {field['id']}, Label: {field['label']}")

# Output example:
# ID: 6014, Label: Ghi chú
# ID: 7937, Label: Phân loại kết nối
# ID: 5972, Label: Mức độ hài lòng
# ID: 5833, Label: Dịch vụ chi tiết
```

---

### Step 3: Insert Config into Database

#### Using Python (PyIceberg)

```python
import json
from pyiceberg.catalog import load_catalog

# Load catalog
catalog = load_catalog(...)
brand_table = catalog.load_table("caresoft.core_brand")

# Prepare config
brand_1_config = {
    "ghi_chu": 6014,
    "phan_loai_ket_noi": 7937,
    "muc_do_hai_long": 5972,
    "dich_vu_chi_tiet": 5833
}

# Update record
# (Implementation depends on your update logic)
```

#### Using SQL (if supported)

```sql
UPDATE caresoft.core_brand
SET custom_field_mappings = '{"ghi_chu": 6014, "phan_loai_ket_noi": 7937, "muc_do_hai_long": 5972, "dich_vu_chi_tiet": 5833}'
WHERE erp_id = 1;
```

---

## 📊 Config Format

### JSON Structure

```json
{
    "ghi_chu": 6014,
    "phan_loai_ket_noi": 7937,
    "muc_do_hai_long": 5972,
    "dich_vu_chi_tiet": 5833
}
```

### Field Mapping

| Config Key | PostgreSQL Column | Description |
|------------|-------------------|-------------|
| `ghi_chu` | `ghi_chu` | Notes/Comments field |
| `phan_loai_ket_noi` | `id_value_phan_loai_ket_noi` | Connection classification |
| `muc_do_hai_long` | `id_value_muc_do_hai_long` | Satisfaction level |
| `dich_vu_chi_tiet` | `id_value_dich_vu_chi_tiet` | Service details |

---

## 🔄 Extraction Flow

### Step-by-Step

```python
# 1. Load brand config
brand = get_active_brands()[0]
# brand = {
#     'erp_id': 1,
#     'caresoft_domain': 'brand1',
#     'custom_field_mappings': '{"ghi_chu": 6014, ...}'
# }

# 2. Parse mappings
mappings = json.loads(brand['custom_field_mappings'])
# mappings = {'ghi_chu': 6014, 'phan_loai_ket_noi': 7937, ...}

# 3. Extract from custom_fields
custom_fields = [
    {"id": 6014, "label": "Ghi chú", "value": "Khách VIP"},
    {"id": 7937, "label": "Phân loại", "value": "Tư vấn"}
]

result = {}
for item in custom_fields:
    if item['id'] == mappings['ghi_chu']:  # 6014 == 6014 ✅
        result['ghi_chu'] = item['value']  # "Khách VIP"
    
    if item['id'] == mappings['phan_loai_ket_noi']:  # 7937 == 7937 ✅
        result['id_value_phan_loai_ket_noi'] = item['value']  # "Tư vấn"

# 4. Write to Iceberg
record = {
    "ghi_chu": result['ghi_chu'],  # "Khách VIP"
    "id_value_phan_loai_ket_noi": result['id_value_phan_loai_ket_noi'],  # "Tư vấn"
}
```

---

## 📋 Example Configs

### Brand 1 (Kangnam)

```json
{
    "ghi_chu": 6014,
    "phan_loai_ket_noi": 7937,
    "muc_do_hai_long": 5972,
    "dich_vu_chi_tiet": 5833
}
```

### Brand 3 (Another Brand)

```json
{
    "ghi_chu": 6016,
    "phan_loai_ket_noi": 5864,
    "muc_do_hai_long": 5979,
    "dich_vu_chi_tiet": 5873
}
```

### Brand 5 (New Brand)

```json
{
    "ghi_chu": 8888,
    "phan_loai_ket_noi": 9999,
    "muc_do_hai_long": 7777,
    "dich_vu_chi_tiet": 6666
}
```

---

## ✅ Benefits

### Old System (Hardcode)

- ❌ Only works for 2 brands (1 and 3)
- ❌ Need code change for new brands
- ❌ Hard to maintain
- ❌ No flexibility

### New System (Config-Driven)

- ✅ Works for **unlimited brands**
- ✅ **No code change** for new brands
- ✅ Easy to maintain (just update database)
- ✅ Flexible (can add/remove fields)

---

## 🚀 Adding a New Brand

### Old System

```python
# ❌ Need to modify code
if brand_id == 1:
    ghi_chu = 6014
elif brand_id == 3:
    ghi_chu = 6016
elif brand_id == 5:  # ← ADD THIS
    ghi_chu = 8888  # ← ADD THIS
```

### New System

```sql
-- ✅ Just insert config
INSERT INTO caresoft.core_brand VALUES (
    5,  -- erp_id
    'Brand 5',
    'brand5',
    'api_key_here',
    true,
    '{"ghi_chu": 8888, "phan_loai_ket_noi": 9999, ...}'  -- ← Config
);
```

**No code change needed!**

---

## 🔍 Troubleshooting

### Issue: Fields not extracted

**Check 1: Config exists?**
```sql
SELECT erp_id, custom_field_mappings 
FROM caresoft.core_brand 
WHERE erp_id = 1;
```

**Check 2: Config valid JSON?**
```python
import json
config = '{"ghi_chu": 6014}'
json.loads(config)  # Should not error
```

**Check 3: IDs match?**
```python
# Check API response
custom_fields = [{"id": 6014, "label": "Ghi chú", "value": "..."}]

# Check config
mappings = {"ghi_chu": 6014}

# Should match!
custom_fields[0]['id'] == mappings['ghi_chu']  # True
```

---

## 📝 Summary

### What Changed?

| Aspect | Old System | New System |
|--------|------------|------------|
| **Config Location** | Hardcoded in Python | Database (core_brand table) |
| **Scalability** | 2 brands only | Unlimited brands |
| **Maintenance** | Code change required | Database update only |
| **Flexibility** | Fixed fields | Configurable fields |

### How to Use?

1. ✅ Find custom field IDs from CareSoft admin or API
2. ✅ Insert config JSON into `core_brand.custom_field_mappings`
3. ✅ Run ticket ingestion
4. ✅ Fields will be extracted automatically!

**No code change needed for new brands!** 🎉
