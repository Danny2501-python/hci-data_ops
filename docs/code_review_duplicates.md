# Code Review: Duplicate Detection & Refactoring Recommendation

## Duplicate Found

### **Location: DuckDB SQL Query**

**Duplicate 1:** Lines 295-351 (Main processing loop)
**Duplicate 2:** Lines 375-431 (Final batch processing)

**Total Duplication:** 56 lines × 2 = **112 lines of duplicate code**

---

## Problem Statement

```python
# Lines 295-351 (DUPLICATE #1)
if accumulated_raw_data:
    try:
        raw_arrow = pa.Table.from_pylist(accumulated_raw_data)
        res = con.sql("""
            SELECT
                TRY_CAST(pk_id AS BIGINT) as pk_id,
                id,
                TRY_CAST(deal_id AS INTEGER) as deal_id,
                # ... 50+ lines of identical SQL ...
            FROM raw_arrow
        """).arrow()
        batch_success = res.to_pylist()

# Lines 375-431 (DUPLICATE #2 - EXACT SAME CODE)
if accumulated_raw_data:
    try:
        raw_arrow = pa.Table.from_pylist(accumulated_raw_data)
        res = con.sql("""
            SELECT
                TRY_CAST(pk_id AS BIGINT) as pk_id,
                id,
                TRY_CAST(deal_id AS INTEGER) as deal_id,
                # ... 50+ lines of IDENTICAL SQL ...
            FROM raw_arrow
        """).arrow()
        batch_success = res.to_pylist()
```

---

## Impact

### **Maintenance Risk:**
- ❌ Must update 2 places when changing transformation logic
- ❌ Easy to forget updating one location
- ❌ Inconsistency risk if SQL diverges

### **Code Quality:**
- ❌ Violates DRY (Don't Repeat Yourself) principle
- ❌ Harder to read and understand
- ❌ Increases file size unnecessarily

---

## Recommended Solution

### **Option 1: Extract Helper Method** ⭐ **RECOMMENDED**

```python
def _transform_with_duckdb(self, con, raw_data):
    """
    Helper method: Transform raw data using DuckDB SQL
    
    Args:
        con: DuckDB connection
        raw_data: List of dict records
        
    Returns:
        List of transformed records
    """
    raw_arrow = pa.Table.from_pylist(raw_data)
    
    res = con.sql("""
        SELECT
            TRY_CAST(pk_id AS BIGINT) as pk_id,
            id,
            TRY_CAST(deal_id AS INTEGER) as deal_id,
            CAST(name AS VARCHAR) as name,
            # ... all transformation logic ...
            snapshot_date,
            CAST(now() AS TIMESTAMP) as ingestion_timestamp
        FROM raw_arrow
    """).arrow()
    
    return res.to_pylist()
```

**Usage:**
```python
# Replace lines 290-353 with:
batch_success = []
if accumulated_raw_data:
    try:
        batch_success = self._transform_with_duckdb(con, accumulated_raw_data)
    except Exception as e:
        logger.error(f"DuckDB Transformation Error: {e}")
        for row in accumulated_raw_data:
             accumulated_errors.append((row["pk_id"], row["id"]))

# Replace lines 372-437 with:
batch_success = []
if accumulated_raw_data:
    try:
        batch_success = self._transform_with_duckdb(con, accumulated_raw_data)
    except Exception as e:
        logger.error(f"DuckDB Transformation Error: {e}")
        for row in accumulated_raw_data:
             accumulated_errors.append((row["pk_id"], row["id"]))
```

**Benefits:**
- ✅ Single source of truth for transformation logic
- ✅ Reduces code from 525 lines → ~470 lines (-10%)
- ✅ Easier to maintain and test
- ✅ Clearer intent (method name explains purpose)

---

### **Option 2: Use Closure/Nested Function**

```python
def parse_and_transform_batch(self, bronze_table, processing_batch_size=50000):
    # ... setup code ...
    
    # Define transformation function once
    def transform_batch(raw_data):
        raw_arrow = pa.Table.from_pylist(raw_data)
        res = con.sql("""...""").arrow()
        return res.to_pylist()
    
    # Use in both places
    for arrow_batch in scan.to_arrow_batch_reader():
        # ... accumulation logic ...
        
        if len(accumulated_raw_data) >= processing_batch_size:
            batch_success = transform_batch(accumulated_raw_data)  # Call #1
    
    # Final batch
    if accumulated_raw_data:
        batch_success = transform_batch(accumulated_raw_data)  # Call #2
```

**Benefits:**
- ✅ Scoped to single method
- ✅ Access to closure variables (con, etc.)
- ⚠️ Less reusable (can't use outside this method)

---

## Other Minor Issues Found

### **1. Unused Import (Line 14)**
```python
import hashlib  # ❌ Not used anywhere in the file
```
**Fix:** Remove this import

### **2. Redundant Comment (Lines 493-496)**
```python
# fallback or re-raise? Let's log specifically which column might fail if possible, 
# but for now printing error is critical.
# We might want to move these to error_batch if they fail schema validation?
# For now, let's Raise to not lose data silently.
```
**Fix:** Remove or simplify to single line

### **3. Inconsistent Error Handling**
```python
# Line 206: Silent continue
if not data_dict:
     accumulated_errors.append((pk, row_id))
     continue

# Line 276: Silent append
except Exception:
    accumulated_errors.append((pk, row_id))
```
**Recommendation:** Add logging for debugging:
```python
except Exception as e:
    logger.debug(f"Parse error for pk={pk}: {e}")
    accumulated_errors.append((pk, row_id))
```

---

## Implementation Priority

### **High Priority:**
1. ✅ **Extract DuckDB transformation** (Option 1)
   - **Effort:** 15 minutes
   - **Impact:** -10% code size, better maintainability
   - **Risk:** Very low (pure refactoring)

### **Low Priority:**
2. Remove unused `hashlib` import
   - **Effort:** 1 minute
   - **Impact:** Minimal
   
3. Clean up redundant comments
   - **Effort:** 2 minutes
   - **Impact:** Minimal

4. Add debug logging to error handling
   - **Effort:** 5 minutes
   - **Impact:** Better debugging

---

## Code Metrics

### **Current:**
- Total lines: 525
- Duplicate lines: 112 (21%)
- Methods: 4
- Complexity: Medium-High

### **After Refactoring:**
- Total lines: ~470 (-10%)
- Duplicate lines: 0 (0%)
- Methods: 5 (+1 helper)
- Complexity: Medium (improved)

---

## Testing Checklist

After refactoring:

- [ ] Unit test: Verify `_transform_with_duckdb()` output
- [ ] Integration test: Run full pipeline
- [ ] Compare output with current version
- [ ] Check memory usage (should be identical)
- [ ] Validate error handling still works

---

## Conclusion

**Current code is functionally correct** but has **one significant duplicate** (DuckDB SQL query).

**Recommendation:**
- Implement **Option 1** (Extract helper method)
- Low risk, high benefit refactoring
- Can be done in 15 minutes
- Improves code quality significantly

**Decision:**
- If time permits: Refactor now
- If urgent: Document as tech debt, refactor later
- Either way: Code works correctly as-is

---

## Files to Update

If implementing refactoring:

1. `webhook_transformation_silver.py`
   - Add `_transform_with_duckdb()` method after line 178
   - Replace lines 290-353 with method call
   - Replace lines 372-437 with method call
   - Remove `import hashlib` (line 14)

**Estimated time:** 20 minutes  
**Risk level:** Very low  
**Benefit:** High (maintainability)
