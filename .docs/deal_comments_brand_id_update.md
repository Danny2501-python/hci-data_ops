# Deal Comments Pipeline - Brand ID Support & Bug Fixes

**Date:** 2026-01-23  
**Status:** ✅ Completed

---

## 📋 SUMMARY

Added `brand_id` support to deal comments pipeline and fixed critical PyArrow conversion error.

---

## 🎯 CHANGES MADE

### 1. Added `brand_id` to Comments Schema

**File:** `deal_comments_transformation.py`

**Schema Update:**
```python
# Added field #26
NestedField(26, "brand_id", IntegerType(), required=False),  # Brand identifier
```

**Purpose:** 
- Identify which brand each comment belongs to
- Enable multi-brand data isolation
- Support brand-specific queries and filtering

---

### 2. Updated Data Extraction

**File:** `deal_comments_transformation.py` (lines 143-145)

**Before:**
```python
selected_fields=("comments", "snapshot_date")
```

**After:**
```python
selected_fields=("id", "brand_id", "comments", "snapshot_date")  # Include id and brand_id
```

**Purpose:**
- Extract `id` (deal_id/ticket_id) from source
- Extract `brand_id` from source
- Pass to each comment during parsing

---

### 3. Enhanced Comment Parsing

**File:** `deal_comments_transformation.py` (lines 168-197)

**Added Logic:**
```python
deal_id = row.get('id')  # ticket_id from deal
brand_id = row.get('brand_id')  # brand_id from deal

for comment in comments:
    if isinstance(comment, dict):
        # Add deal context
        if 'ticket_id' not in comment or not comment['ticket_id']:
            comment['ticket_id'] = deal_id  # Fill missing ticket_id
        comment['brand_id'] = brand_id  # Add brand_id
```

**Purpose:**
- Ensure every comment has `ticket_id` (from deal if missing)
- Add `brand_id` to every comment
- Maintain data integrity

---

### 4. Fixed PyArrow Conversion Error ⚠️ → ✅

**Error:**
```
NumPyConverter doesn't implement <large_string> conversion.
Conversion failed for column attack_file_id with type float64
```

**Root Cause:**
- Fields like `attack_file_id`, `file_id`, etc. are numeric in JSON
- Schema defines them as `StringType`
- PyArrow cannot auto-convert `float64` → `string`

**Fix:** (lines 253-263)
```python
# Convert string columns (explicitly convert to string to avoid PyArrow conversion errors)
string_columns = [
    'comment', 'username', 'facebook', 'facebook_name', 'comment_source',
    'attack_file_id', 'file_name', 'file_id', 'download_connection_string',
    'call_id', 'email_receive_id', 'addition_details', 'can_hide', 'can_remove',
    'can_reply_privately', 'avatar', 'path', 'file_path'
]
for col in string_columns:
    if col in df.columns:
        # Convert to string, but keep None/NaN as None (not string 'nan')
        df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else None)
```

**Key Points:**
- ✅ Explicitly convert all string fields to string
- ✅ Use `apply()` with `pd.notna()` to preserve None values
- ✅ Avoid converting None → 'nan' string

---

### 5. Updated Serving Layer for Multi-Brand Support

**File:** `deal_comments_serving_layer.py`

#### A. Composite Index (lines 68-96)

**Before:**
```python
CREATE INDEX idx_table_ticket_id ON table (ticket_id)
```

**After:**
```python
CREATE INDEX idx_table_brand_ticket ON table (brand_id, ticket_id)
```

**Benefits:**
- ✅ Fast lookups by (brand_id, ticket_id)
- ✅ Multi-brand data isolation
- ✅ Efficient DELETE operations

#### B. Brand-Aware DELETE (lines 125-175)

**Before:**
```python
DELETE FROM table WHERE ticket_id IN (...)
```

**After:**
```python
# Build WHERE clause: (brand_id = %s AND ticket_id = %s) OR ...
DELETE FROM table WHERE 
    (brand_id = 1 AND ticket_id = 123) OR
    (brand_id = 1 AND ticket_id = 456) OR
    (brand_id = 2 AND ticket_id = 789)
```

**Benefits:**
- ✅ Delete only specific (brand, ticket) pairs
- ✅ Prevent cross-brand data deletion
- ✅ Proper data isolation

#### C. Brand Distribution Logging

**Added:**
```python
# Log brand distribution
brand_counts = {}
for brand_id, _ in delete_pairs:
    brand_counts[brand_id] = brand_counts.get(brand_id, 0) + 1
logger.info(f"Brand distribution: {brand_counts}")
```

**Output Example:**
```
Brand distribution: {1: 45, 2: 23, 3: 12}
```

**Benefits:**
- Monitor data distribution across brands
- Detect anomalies (e.g., one brand has too many/few comments)
- Debugging support

---

## 🔄 DATA FLOW

### Complete Pipeline

```
1. deal_detail_enrichment.py
   ↓ (writes to cs_deal_detail_api with brand_id)
   
2. deal_comments_transformation.py
   ↓ (reads id, brand_id, comments)
   ↓ (parses JSON, adds brand_id to each comment)
   ↓ (converts string fields properly)
   ↓ (writes to cs_deal_api_trans_comments)
   
3. deal_comments_serving_layer.py
   ↓ (reads from Iceberg Gold)
   ↓ (deletes by (brand_id, ticket_id) pairs)
   ↓ (inserts new data)
   ↓ (syncs to PostgreSQL cs_deal_api_trans_comments_v1)
```

### Data Example

**Source (cs_deal_detail_api):**
```json
{
  "id": 12345,
  "brand_id": 1,
  "comments": [
    {"ticket_comments_id": 1, "comment": "Khách hàng đồng ý"},
    {"ticket_comments_id": 2, "comment": "Đã gửi báo giá"}
  ]
}
```

**After Transformation (cs_deal_api_trans_comments):**
```json
[
  {
    "ticket_comments_id": 1,
    "ticket_id": 12345,        // ← Added from deal.id
    "brand_id": 1,              // ← Added from deal.brand_id
    "comment": "Khách hàng đồng ý",
    "snapshot_date": "2026-01-22"
  },
  {
    "ticket_comments_id": 2,
    "ticket_id": 12345,
    "brand_id": 1,
    "comment": "Đã gửi báo giá",
    "snapshot_date": "2026-01-22"
  }
]
```

**PostgreSQL Serving Layer:**
- Same structure as above
- Composite index on `(brand_id, ticket_id)`
- DELETE by `(brand_id, ticket_id)` pairs before INSERT

---

## ✅ TESTING CHECKLIST

- [ ] **Transformation**: Run with test snapshot_date
  ```bash
  python deal_comments_transformation.py 2026-01-22
  ```
  - [ ] Verify brand_id is populated
  - [ ] Verify ticket_id is filled for all comments
  - [ ] Check string conversion (no 'nan' strings)
  - [ ] Confirm no PyArrow errors

- [ ] **Serving Layer**: Run sync
  ```bash
  python deal_comments_serving_layer.py 2026-01-22
  ```
  - [ ] Verify composite index created
  - [ ] Check brand distribution logging
  - [ ] Verify DELETE by (brand_id, ticket_id)
  - [ ] Confirm row counts match

- [ ] **PostgreSQL Validation**:
  ```sql
  -- Check index
  SELECT * FROM pg_indexes 
  WHERE tablename = 'cs_deal_api_trans_comments_v1';
  
  -- Check brand distribution
  SELECT brand_id, COUNT(*) 
  FROM cs_deal_api_trans_comments_v1 
  GROUP BY brand_id;
  
  -- Verify no null brand_id
  SELECT COUNT(*) 
  FROM cs_deal_api_trans_comments_v1 
  WHERE brand_id IS NULL;
  ```

---

## 🐛 BUGS FIXED

### Bug #1: PyArrow Conversion Error ✅

**Error:**
```
NumPyConverter doesn't implement <large_string> conversion.
Conversion failed for column attack_file_id with type float64
```

**Fix:**
- Added explicit string conversion for all StringType fields
- Used `apply()` with `pd.notna()` to preserve None values

**Impact:** 🔴 Critical - Pipeline was failing completely

---

### Bug #2: Missing brand_id ✅

**Problem:**
- Comments from different brands were mixed
- No way to filter by brand
- Risk of cross-brand data pollution

**Fix:**
- Added brand_id to schema
- Extract from deal and add to each comment
- Composite index on (brand_id, ticket_id)

**Impact:** 🟡 High - Data isolation issue

---

## 📊 PERFORMANCE IMPACT

### Index Performance

**Before:**
```sql
-- Single column index
CREATE INDEX idx_table_ticket_id ON table (ticket_id)

-- Query performance
DELETE FROM table WHERE ticket_id IN (...)  -- OK for single brand
```

**After:**
```sql
-- Composite index
CREATE INDEX idx_table_brand_ticket ON table (brand_id, ticket_id)

-- Query performance
DELETE FROM table WHERE (brand_id = 1 AND ticket_id = 123) OR ...  -- Better for multi-brand
```

**Benefits:**
- ✅ Faster lookups when filtering by brand
- ✅ Better query planning for multi-brand queries
- ✅ Reduced index size (composite vs multiple single indexes)

---

## 🚀 DEPLOYMENT

### Files Modified

1. ✅ `deal_comments_transformation.py`
   - Added brand_id to schema
   - Updated data extraction
   - Enhanced parsing logic
   - Fixed string conversion

2. ✅ `deal_comments_serving_layer.py`
   - Composite index on (brand_id, ticket_id)
   - Brand-aware DELETE
   - Brand distribution logging

### Deployment Steps

1. **Deploy Code**
   ```bash
   git add .
   git commit -m "Add brand_id support and fix PyArrow conversion"
   git push
   ```

2. **Test in Dev**
   - Run transformation for recent snapshot_date
   - Verify no errors
   - Check brand_id populated

3. **Deploy to Production**
   - Update Docker image if needed
   - Trigger Airflow DAG manually for testing
   - Monitor logs

4. **Verify**
   - Check PostgreSQL index created
   - Verify brand distribution
   - Compare row counts

---

## 📝 NOTES

### Why ticket_id Fallback?

```python
if 'ticket_id' not in comment or not comment['ticket_id']:
    comment['ticket_id'] = deal_id
```

**Reason:**
- Some comments in API response don't have `ticket_id`
- Or have `ticket_id: null` or `ticket_id: 0`
- Need to fill from parent deal to maintain referential integrity

### Why String Conversion?

**Problem:**
- JSON API returns numeric values for string fields
- Example: `attack_file_id: 123` (number) but schema expects string
- PyArrow cannot auto-convert `float64` → `string`

**Solution:**
- Explicitly convert all string fields using `apply()`
- Preserve None values (don't convert to 'nan' string)

---

## 🎯 NEXT STEPS

1. ✅ Code deployed
2. ⏳ Test in production
3. ⏳ Monitor first few runs
4. ⏳ Validate data quality
5. ⏳ Update documentation

---

**Status:** ✅ Ready for production testing
