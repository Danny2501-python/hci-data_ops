# Custom Fields Extraction Clarification
## Understanding Derived Fields in Old System

---

## 🔍 Important Clarification

**Fields like `id_value_phan_loai_ket_noi`, `ghi_chu`, etc. are NOT in the API response!**

They are **DERIVED FIELDS** extracted from the `custom_fields` JSON array.

---

## 📊 API Response Structure

### GET `/tickets` Response

```json
{
    "ticket_id": 654439211,
    "ticket_no": 3676910,
    "ticket_status": "sale",
    "custom_fields": [  // ← This is what API returns
        {
            "id": 9790,
            "label": "Phân loại công việc",
            "type": "Single drop-down list",
            "value": null
        },
        {
            "id": 9795,
            "label": "Ghi chú",
            "type": "Text Area",
            "value": "Some note here"
        },
        {
            "id": 7937,
            "label": "Phân loại kết nối",
            "type": "Single drop-down list",
            "value": "Tư vấn"
        }
        // ... more custom fields
    ],
    "assignee": {...},
    "requester": {...},
    "tags": [...]
}
```

### What Old System Does

```python
# 1. API returns custom_fields as JSON array
custom_fields = [
    {"id": 9795, "label": "Ghi chú", "value": "Some note"},
    {"id": 7937, "label": "Phân loại kết nối", "value": "Tư vấn"},
    {"id": 5972, "label": "Mức độ hài lòng", "value": "Hài lòng"},
    {"id": 5833, "label": "Dịch vụ chi tiết", "value": "Tư vấn da"}
]

# 2. Old system HARDCODES which IDs to extract (per brand)
if brand_id == 1:
    ghi_chu_id = 6014
    phan_loai_ket_noi_id = 7937
    muc_do_hai_long_id = 5972
    dich_vu_chi_tiet_id = 5833

# 3. Old system EXTRACTS values by ID
for item in custom_fields:
    if item["id"] == ghi_chu_id:
        row["ghi_chu"] = item["value"]  # ← Creates NEW column
    if item["id"] == phan_loai_ket_noi_id:
        row["id_value_phan_loai_ket_noi"] = item["value"]  # ← Creates NEW column
    # ... etc

# 4. Result: PostgreSQL table has BOTH
# - custom_fields (full JSON)
# - ghi_chu (extracted value)
# - id_value_phan_loai_ket_noi (extracted value)
# - id_value_muc_do_hai_long (extracted value)
# - id_value_dich_vu_chi_tiet (extracted value)
```

---

## 🎯 Field Types

### Direct Fields (From API)

| Field | Source | In API Response? |
|-------|--------|------------------|
| `ticket_id` | API | ✅ Yes |
| `ticket_no` | API | ✅ Yes |
| `ticket_status` | API | ✅ Yes |
| `custom_fields` | API | ✅ Yes (JSON array) |
| `assignee` | API | ✅ Yes (JSON object) |
| `requester` | API | ✅ Yes (JSON object) |
| `tags` | API | ✅ Yes (JSON array) |

### Derived Fields (Extracted from `custom_fields`)

| Field | Source | In API Response? | How It's Created |
|-------|--------|------------------|------------------|
| `ghi_chu` | `custom_fields[id=6014].value` | ❌ No | Extracted by old system |
| `id_value_phan_loai_ket_noi` | `custom_fields[id=7937].value` | ❌ No | Extracted by old system |
| `id_value_muc_do_hai_long` | `custom_fields[id=5972].value` | ❌ No | Extracted by old system |
| `id_value_dich_vu_chi_tiet` | `custom_fields[id=5833].value` | ❌ No | Extracted by old system |

### Derived Fields (Extracted from `tags`)

| Field | Source | In API Response? | How It's Created |
|-------|--------|------------------|------------------|
| `ad_id_value` | `tags[name contains "ad_id."].name` | ❌ No | Regex extracted from tags |

---

## 🔧 Old System Extraction Logic

### Code from `cs_extract.py`

```python
# Lines 310-326: Hardcoded custom field IDs per brand
if self.obj_brand.erp_id == 1:
    ghi_chu = 6014
    id_value_phan_loai_ket_noi = 7937
    id_value_muc_do_hai_long = 5972
    id_value_dich_vu_chi_tiet = 5833
elif self.obj_brand.erp_id == 3:
    ghi_chu = 6016
    id_value_phan_loai_ket_noi = 5864
    id_value_muc_do_hai_long = 5979
    id_value_dich_vu_chi_tiet = 5873
else:
    ghi_chu = 0
    id_value_phan_loai_ket_noi = 0
    id_value_muc_do_hai_long = 0
    id_value_dich_vu_chi_tiet = 0

# Lines 338-368: Extract values from custom_fields JSON
for i, row in df.iterrows():
    custom_fields = row["custom_fields"]
    custom_fields_dict = json.loads(custom_fields)
    
    for item in custom_fields_dict:
        if item["id"] == ghi_chu:
            df.at[i, "ghi_chu"] = item["value"]
        
        if item["id"] == id_value_phan_loai_ket_noi:
            df.at[i, "id_value_phan_loai_ket_noi"] = item["value"]
        
        if item["id"] == id_value_muc_do_hai_long:
            df.at[i, "id_value_muc_do_hai_long"] = item["value"]
        
        if item["id"] == id_value_dich_vu_chi_tiet:
            df.at[i, "id_value_dich_vu_chi_tiet"] = item["value"]

# Lines 379-388: Extract ad_id from tags
def extract_ad_id(tags):
    if pd.isna(tags):
        return ''
    all_names = ' '.join(item.get('name', '') for item in tags)
    return re.findall(r"ad_id\.(\w+)", all_names)

df['ad_id_value'] = df['tags'].apply(extract_ad_id).astype(str)
```

---

## 🎯 New System Approach

### ✅ Recommended: Store Full JSON, Extract Later

**Bronze Layer (ticket_ingestion.py):**
```python
# Store FULL JSON, no extraction
record = {
    "custom_fields": json.dumps(ticket.get("custom_fields", []), ensure_ascii=False),
    "assignee": json.dumps(ticket.get("assignee", {}), ensure_ascii=False),
    "requester": json.dumps(ticket.get("requester", {}), ensure_ascii=False),
    "tags": json.dumps(ticket.get("tags", []), ensure_ascii=False),
    
    # Keep these columns for backward compatibility, but set to NULL
    "ghi_chu": None,
    "id_value_phan_loai_ket_noi": None,
    "id_value_muc_do_hai_long": None,
    "id_value_dich_vu_chi_tiet": None,
    "ad_id_value": None,
}
```

**Silver/Gold Layer (Future Processing):**
```python
# Extract when needed, using brand config
def extract_custom_field(custom_fields_json, field_id):
    """Extract value from custom_fields by ID"""
    try:
        custom_fields = json.loads(custom_fields_json)
        for item in custom_fields:
            if item.get("id") == field_id:
                return item.get("value")
    except:
        return None
    return None

# Load brand config from database
brand_config = get_brand_custom_field_config(brand_id)

# Extract fields
df["ghi_chu"] = df["custom_fields"].apply(
    lambda x: extract_custom_field(x, brand_config.get("ghi_chu_id"))
)
df["id_value_phan_loai_ket_noi"] = df["custom_fields"].apply(
    lambda x: extract_custom_field(x, brand_config.get("phan_loai_ket_noi_id"))
)
```

---

## 📋 Summary

### What API Returns

```json
{
    "custom_fields": [...],  // ✅ API returns this
    "assignee": {...},       // ✅ API returns this
    "requester": {...},      // ✅ API returns this
    "tags": [...]            // ✅ API returns this
}
```

### What Old System Creates (Derived)

```python
{
    "ghi_chu": "...",                        // ❌ Extracted from custom_fields
    "id_value_phan_loai_ket_noi": "...",    // ❌ Extracted from custom_fields
    "id_value_muc_do_hai_long": "...",      // ❌ Extracted from custom_fields
    "id_value_dich_vu_chi_tiet": "...",     // ❌ Extracted from custom_fields
    "ad_id_value": "..."                     // ❌ Extracted from tags
}
```

### New System Strategy

1. **Bronze**: Store full JSON (no extraction)
2. **Silver/Gold**: Extract when needed (config-driven)
3. **Benefits**:
   - ✅ No data loss
   - ✅ Flexible (can extract any field later)
   - ✅ No hardcoded IDs
   - ✅ Scalable (works for all brands)

---

## 🔍 Why This Matters

**Old System Problem:**
- Hardcoded custom field IDs for only 2 brands
- Can't add new brands without code changes
- Can't extract new custom fields without code changes

**New System Solution:**
- Store full `custom_fields` JSON
- Extract in downstream processing
- Config-driven (no code changes for new brands/fields)
