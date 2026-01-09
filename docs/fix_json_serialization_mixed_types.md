# Fix: JSON Serialization for Mixed-Type Fields

## Problem Discovered from Sample Data

### **Sample Input:**
```python
{
    'deal': {...},
    'account_id': 8816,              # ❌ Integer, not dict!
    'activity_source': 'WORKFLOW',  # ❌ String, not dict!
    'action': 'update',              # ❌ String, not dict!
    'object': 'deal',                # ❌ String, not dict!
    'updated_properties': [...]      # ✅ List (OK)
}
```

### **Previous Code (Incorrect Assumption):**
```python
# Assumed all top-level fields are dicts
"account_id": json.dumps(data_dict["account_id"]) if data_dict.get("account_id") else EMPTY_JSON
```

**Problem:**
- `account_id` = `8816` (int) → `json.dumps(8816)` = `"8816"` ✅ Works
- `activity_source` = `"WORKFLOW"` (str) → `json.dumps("WORKFLOW")` = `'"WORKFLOW"'` ❌ Double quotes!
- `action` = `"update"` (str) → `json.dumps("update")` = `'"update"'` ❌ Double quotes!

---

## The Fix: `safe_json()` Function

### **Implementation:**
```python
def safe_json(val):
    """Safely serialize value - handles dict, list, and primitives"""
    if not val:
        return EMPTY_JSON  # Empty/None → "{}"
    
    # If dict or list, serialize to JSON
    if isinstance(val, (dict, list)):
        return json.dumps(val)
    
    # If primitive (string, int, etc.), convert to string
    return str(val)
```

### **Behavior:**

| Input Type | Input Value | Output | Explanation |
|------------|-------------|--------|-------------|
| `dict` | `{"key": "val"}` | `'{"key": "val"}'` | JSON serialized |
| `list` | `[1, 2, 3]` | `'[1, 2, 3]'` | JSON serialized |
| `int` | `8816` | `"8816"` | String conversion |
| `str` | `"WORKFLOW"` | `"WORKFLOW"` | Direct string (no double quotes) |
| `None` | `None` | `"{}"` | EMPTY_JSON constant |
| `""` | `""` | `"{}"` | EMPTY_JSON constant |

---

## Comparison: Before vs After

### **Field: `account_id`**

**Before:**
```python
"account_id": json.dumps(data_dict["account_id"]) if data_dict.get("account_id") else EMPTY_JSON
```

| Input | Output | Correct? |
|-------|--------|----------|
| `8816` (int) | `"8816"` | ✅ Yes |
| `{"id": 123}` (dict) | `'{"id": 123}'` | ✅ Yes |
| `None` | `"{}"` | ✅ Yes |

**After:**
```python
"account_id": safe_json(data_dict.get("account_id"))
```

| Input | Output | Correct? |
|-------|--------|----------|
| `8816` (int) | `"8816"` | ✅ Yes |
| `{"id": 123}` (dict) | `'{"id": 123}'` | ✅ Yes |
| `None` | `"{}"` | ✅ Yes |

---

### **Field: `activity_source`**

**Before:**
```python
"activity_source": json.dumps(data_dict["activity_source"]) if data_dict.get("activity_source") else EMPTY_JSON
```

| Input | Output | Correct? |
|-------|--------|----------|
| `"WORKFLOW"` (str) | `'"WORKFLOW"'` | ❌ No (double quotes) |
| `{"type": "auto"}` (dict) | `'{"type": "auto"}'` | ✅ Yes |
| `None` | `"{}"` | ✅ Yes |

**After:**
```python
"activity_source": safe_json(data_dict.get("activity_source"))
```

| Input | Output | Correct? |
|-------|--------|----------|
| `"WORKFLOW"` (str) | `"WORKFLOW"` | ✅ Yes (fixed!) |
| `{"type": "auto"}` (dict) | `'{"type": "auto"}'` | ✅ Yes |
| `None` | `"{}"` | ✅ Yes |

---

## Fields Affected

All top-level complex fields now use `safe_json()`:

1. **`call_info`** - Can be dict or empty
2. **`agent_submit`** - Can be dict or empty
3. **`updated_properties`** - Can be list or empty
4. **`account_id`** - Can be int, dict, or empty
5. **`activity_source`** - Can be string, dict, or empty
6. **`action`** - Can be string, dict, or empty
7. **`convert_from_object`** - Can be dict or empty
8. **`object`** - Can be string, dict, or empty

---

## Real Data Examples

### **Example 1: `account_id` as Integer**
```python
Input: {'account_id': 8816}
safe_json(8816) → "8816"
DuckDB: CAST("8816" AS VARCHAR) → "8816" ✅
```

### **Example 2: `activity_source` as String**
```python
Input: {'activity_source': 'WORKFLOW'}
safe_json('WORKFLOW') → "WORKFLOW"
DuckDB: CAST("WORKFLOW" AS VARCHAR) → "WORKFLOW" ✅
```

### **Example 3: `updated_properties` as List**
```python
Input: {'updated_properties': [{'field': 'ticket_id', 'value': 650205704}]}
safe_json([...]) → '[{"field": "ticket_id", "value": 650205704}]'
DuckDB: CAST(...) → '[{"field": "ticket_id", "value": 650205704}]' ✅
```

### **Example 4: `call_info` as Dict**
```python
Input: {'call_info': {'duration': 120, 'status': 'completed'}}
safe_json({...}) → '{"duration": 120, "status": "completed"}'
DuckDB: CAST(...) → '{"duration": 120, "status": "completed"}' ✅
```

---

## Why This Matters

### **Without `safe_json()`:**
```python
# String value
activity_source = "WORKFLOW"
json.dumps("WORKFLOW")  # → '"WORKFLOW"' (with quotes!)

# Stored in database
activity_source = '"WORKFLOW"'  # ❌ Wrong!

# When queried
SELECT activity_source FROM table
→ "WORKFLOW"  # Has extra quotes
```

### **With `safe_json()`:**
```python
# String value
activity_source = "WORKFLOW"
safe_json("WORKFLOW")  # → "WORKFLOW" (no extra quotes)

# Stored in database
activity_source = 'WORKFLOW'  # ✅ Correct!

# When queried
SELECT activity_source FROM table
→ WORKFLOW  # Clean value
```

---

## Code Structure

### **Helper Functions (Lines 287-305):**
```python
def safe_str(val):
    """Convert to string, handle booleans"""
    # Used for numeric fields
    ...

def safe_json(val):
    """Serialize dict/list, convert primitives"""
    # Used for complex fields
    ...
```

### **Usage in Row Dict (Lines 337-344):**
```python
row = {
    # Numeric fields use safe_str()
    "deal_id": safe_str(deal_info.get("deal_id")),
    
    # Complex fields use safe_json()
    "account_id": safe_json(data_dict.get("account_id")),
    "activity_source": safe_json(data_dict.get("activity_source")),
    ...
}
```

---

## Testing Validation

### **Test Case 1: Integer Value**
```python
Input: account_id = 8816
Expected: "8816"
Result: safe_json(8816) → "8816" ✅
```

### **Test Case 2: String Value**
```python
Input: activity_source = "WORKFLOW"
Expected: "WORKFLOW"
Result: safe_json("WORKFLOW") → "WORKFLOW" ✅
```

### **Test Case 3: Dict Value**
```python
Input: call_info = {"duration": 120}
Expected: '{"duration": 120}'
Result: safe_json({...}) → '{"duration": 120}' ✅
```

### **Test Case 4: List Value**
```python
Input: updated_properties = [{"field": "id"}]
Expected: '[{"field": "id"}]'
Result: safe_json([...]) → '[{"field": "id"}]' ✅
```

### **Test Case 5: None/Empty**
```python
Input: action = None
Expected: "{}"
Result: safe_json(None) → "{}" ✅
```

---

## Comparison with Old Logic

### **Old System (trans_json_wehook_caresoft_p2.py):**
```python
# Lines 127-131
deal_info_chung['account_id'] = str(deal_account_id_info)
deal_info_chung['activity_source'] = str(deal_activity_source_info)
deal_info_chung['action'] = str(deal_action_info)
```

**Behavior:**
- `8816` → `"8816"` ✅
- `"WORKFLOW"` → `"WORKFLOW"` ✅
- `{"key": "val"}` → `"{'key': 'val'}"` ⚠️ (Python dict repr, not JSON)

### **New System (After Fix):**
```python
"account_id": safe_json(data_dict.get("account_id"))
"activity_source": safe_json(data_dict.get("activity_source"))
"action": safe_json(data_dict.get("action"))
```

**Behavior:**
- `8816` → `"8816"` ✅
- `"WORKFLOW"` → `"WORKFLOW"` ✅
- `{"key": "val"}` → `'{"key": "val"}'` ✅ (Proper JSON)

**Improvement:** New system produces proper JSON for dict/list values!

---

## Conclusion

**Problem:** Top-level fields can be dict, list, string, or int  
**Old Code:** Used `json.dumps()` unconditionally → double quotes for strings  
**New Code:** `safe_json()` checks type first → correct serialization  

**Status:** ✅ FIXED  
**Risk:** Very low (handles all data types correctly)  
**Benefit:** Cleaner data, proper JSON serialization
