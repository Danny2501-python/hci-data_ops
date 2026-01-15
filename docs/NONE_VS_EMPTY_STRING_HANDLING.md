# None vs Empty String Handling
## Custom Fields Extraction Behavior

---

## 🎯 Key Difference: Old System Behavior

### Old System (cs_extract.py)

```python
if item["value"] is None:
    df.at[i, "ghi_chu"] = ''  # ← Convert None to empty string
else:
    df.at[i, "ghi_chu"] = item["value"]
```

**Behavior:** `None` → `''` (empty string)

---

## 📊 Examples

### API Response

```json
{
    "custom_fields": [
        {
            "id": 6014,
            "label": "Ghi chú",
            "value": null  // ← None/null value
        },
        {
            "id": 7937,
            "label": "Phân loại kết nối",
            "value": "Tư vấn"  // ← Has value
        }
    ]
}
```

### Old System Processing

```python
# Item 1: value = null
if item["value"] is None:  # True
    df.at[i, "ghi_chu"] = ''  # ← Store as empty string

# Item 2: value = "Tư vấn"
if item["value"] is None:  # False
    df.at[i, "id_value_phan_loai_ket_noi"] = "Tư vấn"  # ← Store as-is
```

### Result in PostgreSQL

| ticket_id | ghi_chu | id_value_phan_loai_ket_noi |
|-----------|---------|----------------------------|
| 100 | `''` (empty string) | `'Tư vấn'` |

**NOT:**
| ticket_id | ghi_chu | id_value_phan_loai_ket_noi |
|-----------|---------|----------------------------|
| 100 | `NULL` | `'Tư vấn'` |

---

## ✅ New System Implementation

### Code (ticket_ingestion.py)

```python
def extract_custom_fields(custom_fields, brand_config):
    result = {
        'ghi_chu': '',  # ← Default to empty string (not None)
        'id_value_phan_loai_ket_noi': '',
        'id_value_muc_do_hai_long': '',
        'id_value_dich_vu_chi_tiet': '',
    }
    
    for item in custom_fields:
        item_id = item.get('id')
        item_value = item.get('value')
        
        # Convert None to empty string (matching old system)
        if item_value is None:
            item_value = ''  # ← Convert None to ''
        
        # Match and assign
        if item_id == mappings.get('ghi_chu'):
            result['ghi_chu'] = item_value  # Will be '' if was None
```

---

## 📋 Behavior Comparison

### Scenario 1: Value is None/null

| System | API Value | Stored Value | Type |
|--------|-----------|--------------|------|
| **Old** | `null` | `''` | Empty string |
| **New** | `null` | `''` | Empty string ✅ |

### Scenario 2: Value is String

| System | API Value | Stored Value | Type |
|--------|-----------|--------------|------|
| **Old** | `"Tư vấn"` | `"Tư vấn"` | String |
| **New** | `"Tư vấn"` | `"Tư vấn"` | String ✅ |

### Scenario 3: Field Not Found

| System | Scenario | Stored Value | Type |
|--------|----------|--------------|------|
| **Old** | No matching ID | Not set (stays as column default) | - |
| **New** | No matching ID | `''` | Empty string ✅ |

---

## 🔍 Why Empty String Instead of NULL?

### Old System Reasoning

1. **Consistency**: All text fields default to empty string
2. **SQL Queries**: Easier to query (`WHERE field = ''` vs `WHERE field IS NULL`)
3. **Application Logic**: Simpler to check (`if not value:` works for both)
4. **Data Type**: String columns prefer empty string over NULL

### Example Queries

**With Empty String:**
```sql
-- Simple
SELECT * FROM cs_ticket_api WHERE ghi_chu = '';

-- Count non-empty
SELECT COUNT(*) FROM cs_ticket_api WHERE ghi_chu != '';
```

**With NULL (harder):**
```sql
-- More complex
SELECT * FROM cs_ticket_api WHERE ghi_chu IS NULL;

-- Count non-empty (need COALESCE)
SELECT COUNT(*) FROM cs_ticket_api WHERE COALESCE(ghi_chu, '') != '';
```

---

## 📊 Complete Flow

### Input

```json
{
    "custom_fields": [
        {"id": 6014, "label": "Ghi chú", "value": null},
        {"id": 7937, "label": "Phân loại", "value": "Tư vấn"},
        {"id": 5972, "label": "Mức độ", "value": "Hài lòng"}
    ]
}
```

### Processing

```python
# Config
mappings = {
    'ghi_chu': 6014,
    'phan_loai_ket_noi': 7937,
    'muc_do_hai_long': 5972,
    'dich_vu_chi_tiet': 5833  # Not in API response
}

# Extract
result = {
    'ghi_chu': '',  # Default
    'id_value_phan_loai_ket_noi': '',
    'id_value_muc_do_hai_long': '',
    'id_value_dich_vu_chi_tiet': '',
}

# Item 1: id=6014, value=null
item_value = None
if item_value is None:
    item_value = ''  # ← Convert
result['ghi_chu'] = ''  # ← Store empty string

# Item 2: id=7937, value="Tư vấn"
item_value = "Tư vấn"
if item_value is None:  # False
    pass
result['id_value_phan_loai_ket_noi'] = "Tư vấn"  # ← Store as-is

# Item 3: id=5972, value="Hài lòng"
item_value = "Hài lòng"
result['id_value_muc_do_hai_long'] = "Hài lòng"

# Item 4: id=5833 not in API response
# result['id_value_dich_vu_chi_tiet'] stays as ''
```

### Output

```python
{
    'ghi_chu': '',  # ← Was null, converted to ''
    'id_value_phan_loai_ket_noi': 'Tư vấn',
    'id_value_muc_do_hai_long': 'Hài lòng',
    'id_value_dich_vu_chi_tiet': '',  # ← Not found, default ''
}
```

### Database Record

| Field | Value | Notes |
|-------|-------|-------|
| `ghi_chu` | `''` | Was null in API |
| `id_value_phan_loai_ket_noi` | `'Tư vấn'` | From API |
| `id_value_muc_do_hai_long` | `'Hài lòng'` | From API |
| `id_value_dich_vu_chi_tiet` | `''` | Not in API response |

---

## ✅ Summary

### Behavior

| API Value | Old System | New System | Match? |
|-----------|------------|------------|--------|
| `null` | `''` | `''` | ✅ Yes |
| `"text"` | `"text"` | `"text"` | ✅ Yes |
| Missing | Default | `''` | ✅ Yes |

### Key Points

1. **✅ `None` → `''`**: Convert null to empty string
2. **✅ Default `''`**: All fields default to empty string
3. **✅ Preserve strings**: Non-null values stored as-is
4. **✅ Consistent**: Matches old system behavior exactly

### Code Changes

```python
# Before (WRONG)
result = {
    'ghi_chu': None,  # ❌ Would store NULL
}

# After (CORRECT)
result = {
    'ghi_chu': '',  # ✅ Stores empty string
}

# Conversion
if item_value is None:
    item_value = ''  # ✅ Convert None to ''
```

**Result:** Perfect match with old system! 🎉
