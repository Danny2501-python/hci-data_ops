# Issues Found in Old Ticket Flow
## Code Review: cs_extract.py - call_tickets()

---

## 🐛 Issue #1: Loop 3 Never Runs (Indentation Bug)

### Location
**File:** `etl_marketing/app/caresoft/cs_extract.py`  
**Function:** `call_tickets()`  
**Lines:** 211-254

### Problem

```python
for i in range(1, 4):  # Loop 1, 2, 3
    if i == 1:
        # ... setup params for UPDATED tickets
        
    if i == 2:
        # ... setup params for CREATED tickets (today)
        params = {...}
        
        if i == 3:  # ❌ BUG: Nested inside "if i == 2"
            # ... setup params for CREATED tickets (yesterday)
            params = {...}
```

### Why It Never Runs

```python
if i == 2:
    # When i == 2, this block runs
    params = {...}
    
    if i == 3:  # ← This is INSIDE "if i == 2"
        # This can NEVER run because:
        # - We're inside "if i == 2" block
        # - i == 2 (not 3)
        # - So "if i == 3" is always False
```

### Correct Code Should Be

```python
for i in range(1, 4):
    if i == 1:
        # ... setup params for UPDATED tickets
        params = {...}
        
    if i == 2:  # ← NOT elif, because we want all 3 to run
        # ... setup params for CREATED tickets (today)
        params = {...}
        
    if i == 3:  # ← Should be at SAME LEVEL as "if i == 2"
        # ... setup params for CREATED tickets (yesterday)
        params = {...}
```

### Impact

- ❌ **Loop 3 never executes**
- ❌ **Missing CREATED tickets from day before yesterday**
- ❌ **Data loss**: Tickets created 2 days ago are never fetched

---

## 🔧 Issue #2: Hardcoded Brand-Specific Values

### Location
**File:** `etl_marketing/app/caresoft/cs_extract.py`  
**Function:** `call_tickets()`  
**Lines:** 310-326

### Problem

```python
# fix cứng giá trị cho 1 và 3
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
```

### Issues

1. **Hardcoded Brand IDs**: Only works for `erp_id == 1` and `erp_id == 3`
2. **Hardcoded Custom Field IDs**: Magic numbers (6014, 7937, etc.)
3. **Not Scalable**: Adding new brands requires code changes
4. **No Documentation**: What do these numbers mean?

### What These Values Are

These are **custom field IDs** from CareSoft API:

| Field Name | Brand 1 (erp_id=1) | Brand 3 (erp_id=3) |
|------------|--------------------|--------------------|
| `ghi_chu` | 6014 | 6016 |
| `id_value_phan_loai_ket_noi` | 7937 | 5864 |
| `id_value_muc_do_hai_long` | 5972 | 5979 |
| `id_value_dich_vu_chi_tiet` | 5833 | 5873 |

### Better Approach

**Option 1: Database Configuration Table**
```sql
CREATE TABLE caresoft_custom_field_mapping (
    brand_id INT,
    field_name VARCHAR(100),
    custom_field_id INT,
    PRIMARY KEY (brand_id, field_name)
);

INSERT INTO caresoft_custom_field_mapping VALUES
(1, 'ghi_chu', 6014),
(1, 'id_value_phan_loai_ket_noi', 7937),
(1, 'id_value_muc_do_hai_long', 5972),
(1, 'id_value_dich_vu_chi_tiet', 5833),
(3, 'ghi_chu', 6016),
(3, 'id_value_phan_loai_ket_noi', 5864),
(3, 'id_value_muc_do_hai_long', 5979),
(3, 'id_value_dich_vu_chi_tiet', 5873);
```

**Option 2: JSON Configuration in Brand Model**
```python
# In core_brand table
{
    "custom_field_mappings": {
        "ghi_chu": 6014,
        "id_value_phan_loai_ket_noi": 7937,
        "id_value_muc_do_hai_long": 5972,
        "id_value_dich_vu_chi_tiet": 5833
    }
}
```

---

## 📊 Complete Issue Summary

### Issue #1: Loop 3 Bug

| Aspect | Details |
|--------|---------|
| **Severity** | 🔴 **HIGH** - Data loss |
| **Type** | Logic error (indentation) |
| **Impact** | Missing tickets created 2 days ago |
| **Affected** | All brands |
| **Fix Difficulty** | Easy (fix indentation) |

### Issue #2: Hardcoded Values

| Aspect | Details |
|--------|---------|
| **Severity** | 🟡 **MEDIUM** - Maintainability |
| **Type** | Design issue (hardcoded config) |
| **Impact** | Only works for 2 brands, not scalable |
| **Affected** | Brands other than erp_id 1 and 3 |
| **Fix Difficulty** | Medium (requires refactoring) |

---

## 🔍 Code Comparison: Old vs New

### Old System (Buggy)

```python
# ❌ BUG: Loop 3 never runs
for i in range(1, 4):
    if i == 1:
        params = {'updated_since': ..., 'updated_to': ...}
    if i == 2:
        params = {'created_since': ..., 'created_to': ...}
        if i == 3:  # ← NESTED, never runs!
            params = {'created_since': ..., 'created_to': ...}
    
    # API call here
    while record_count != 0:
        response = requests.get(...)

# ❌ HARDCODED: Only works for brand 1 and 3
if self.obj_brand.erp_id == 1:
    ghi_chu = 6014
    id_value_phan_loai_ket_noi = 7937
elif self.obj_brand.erp_id == 3:
    ghi_chu = 6016
    id_value_phan_loai_ket_noi = 5864
```

### New System (Fixed)

```python
# ✅ FIXED: All 3 loops run correctly
# Loop 1: UPDATED tickets (yesterday → today)
tickets_updated = fetch_tickets(brand, 'updated', date_yesterday, date_today)

# Loop 2: CREATED tickets (yesterday → today)
tickets_created = fetch_tickets(brand, 'created', date_yesterday, date_today)

# Loop 3: CREATED tickets (day before yesterday → yesterday)
tickets_created_prev = fetch_tickets(brand, 'created_prev', date_day_before, date_yesterday)

# ✅ NO HARDCODED VALUES: All config from brand table
# Custom field extraction is generic, works for all brands
```

---

## 🎯 Recommendations for New System

### ✅ Already Fixed in New System

1. **Loop 3 Bug**: Fixed by using separate function calls
2. **Hardcoded Values**: Removed, all config from `core_brand` table
3. **Scalability**: Works for any number of brands
4. **Maintainability**: No code changes needed for new brands

### 📝 Additional Improvements

1. **Custom Field Extraction**: 
   - Old: Hardcoded field IDs per brand
   - New: Store all `custom_fields` as JSON, extract in downstream processing

2. **Error Handling**:
   - Old: Mixed success/error logic
   - New: Separate error table (`cs_ticket_source_error_api`)

3. **Data Lineage**:
   - Old: No tracking of data source
   - New: `fetch_type` metadata ('updated', 'created', 'created_prev')

---

## 🚀 Migration Benefits

| Aspect | Old System | New System |
|--------|------------|------------|
| **Loop 3** | ❌ Never runs (bug) | ✅ Runs correctly |
| **Brand Config** | ❌ Hardcoded for 2 brands | ✅ Dynamic from DB |
| **Custom Fields** | ❌ Hardcoded IDs | ✅ Stored as JSON |
| **Scalability** | ❌ Code change per brand | ✅ Config-driven |
| **Maintainability** | ❌ Complex nested logic | ✅ Clean, separate functions |
| **Error Tracking** | ❌ No separate tracking | ✅ Dedicated error table |
| **Data Lineage** | ❌ No metadata | ✅ Full metadata tracking |

---

## 📌 Conclusion

**Old System Issues:**
1. 🐛 **Critical Bug**: Loop 3 never runs due to indentation error
2. 🔧 **Hardcoded Values**: Only works for 2 specific brands
3. 📉 **Not Scalable**: Requires code changes for new brands

**New System Improvements:**
1. ✅ **Bug-Free**: All 3 loops execute correctly
2. ✅ **Config-Driven**: No hardcoded values
3. ✅ **Scalable**: Works for unlimited brands
4. ✅ **Maintainable**: Clean, modular code

**Recommendation:** Migrate to new system ASAP to fix data loss from Loop 3 bug!
