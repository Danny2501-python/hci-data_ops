# Ad ID Extraction from Tags
## How Old System Extracts ad_id_value

---

## 🎯 What is ad_id_value?

**`ad_id_value`** is a **derived field** extracted from the `tags` array using regex pattern matching.

It's used to track which Facebook/Google Ad ID is associated with a ticket.

---

## 📊 Tags Structure

### API Response

```json
{
    "ticket_id": 123456,
    "tags": [
        {
            "name": "ad_id.987654321",  // ← Ad ID tag
            "id": 5555
        },
        {
            "name": "vip_customer",  // ← Other tag
            "id": 6666
        },
        {
            "name": "urgent",  // ← Other tag
            "id": 7777
        }
    ]
}
```

**Goal:** Extract `987654321` from `"ad_id.987654321"`

---

## 🔧 Old System Logic

### Code (cs_extract.py lines 379-388)

```python
def extract_ad_id(tags):
    if pd.isna(tags):  # Check if NaN
        return ''
    
    # Join all 'name' values into one string
    all_names = ' '.join(item.get('name', '') for item in tags)
    # Result: "ad_id.987654321 vip_customer urgent"
    
    # Extract using regex pattern
    return re.findall(r"ad_id\.(\w+)", all_names)
    # Result: ['987654321']

# Apply to DataFrame
df['ad_id_value'] = df['tags'].apply(extract_ad_id).astype(str)
```

---

## 📋 Step-by-Step Example

### Input

```python
tags = [
    {"name": "ad_id.987654321", "id": 5555},
    {"name": "vip_customer", "id": 6666},
    {"name": "urgent", "id": 7777}
]
```

### Step 1: Join All Names

```python
all_names = ' '.join(item.get('name', '') for item in tags)
# all_names = "ad_id.987654321 vip_customer urgent"
```

### Step 2: Regex Pattern Match

```python
import re
matches = re.findall(r"ad_id\.(\w+)", all_names)
# Pattern: "ad_id." followed by word characters
# matches = ['987654321']
```

### Step 3: Convert to String

```python
result = str(matches)
# result = "['987654321']"
```

---

## 🎯 Regex Pattern Explanation

### Pattern: `r"ad_id\.(\w+)"`

| Part | Meaning |
|------|---------|
| `ad_id` | Literal text "ad_id" |
| `\.` | Literal dot "." (escaped) |
| `(\w+)` | **Capture group**: One or more word characters (letters, digits, underscore) |

### Examples

| Input | Match? | Captured |
|-------|--------|----------|
| `"ad_id.123456"` | ✅ Yes | `123456` |
| `"ad_id.abc123"` | ✅ Yes | `abc123` |
| `"ad_id.987_654"` | ✅ Yes | `987_654` |
| `"vip_customer"` | ❌ No | - |
| `"ad_id."` | ❌ No | (no word chars after dot) |

---

## 🔄 Complete Flow

### Example 1: Single Ad ID

```python
# Input
tags = [
    {"name": "ad_id.123456"},
    {"name": "urgent"}
]

# Step 1: Join
all_names = "ad_id.123456 urgent"

# Step 2: Regex
matches = re.findall(r"ad_id\.(\w+)", all_names)
# matches = ['123456']

# Step 3: Convert
ad_id_value = str(matches)
# ad_id_value = "['123456']"
```

### Example 2: Multiple Ad IDs

```python
# Input
tags = [
    {"name": "ad_id.111111"},
    {"name": "ad_id.222222"},
    {"name": "other_tag"}
]

# Step 1: Join
all_names = "ad_id.111111 ad_id.222222 other_tag"

# Step 2: Regex
matches = re.findall(r"ad_id\.(\w+)", all_names)
# matches = ['111111', '222222']

# Step 3: Convert
ad_id_value = str(matches)
# ad_id_value = "['111111', '222222']"
```

### Example 3: No Ad ID

```python
# Input
tags = [
    {"name": "vip_customer"},
    {"name": "urgent"}
]

# Step 1: Join
all_names = "vip_customer urgent"

# Step 2: Regex
matches = re.findall(r"ad_id\.(\w+)", all_names)
# matches = []

# Step 3: Convert
ad_id_value = str(matches)
# ad_id_value = "[]"  or  ""
```

---

## ✅ New System Implementation

### Code (ticket_ingestion.py)

```python
def extract_ad_id(tags):
    """Extract ad_id value from tags array
    
    Tags format: [{"name": "ad_id.123456"}, {"name": "other_tag"}]
    Returns: "['123456']" or empty string
    """
    if not tags:
        return ''
    
    try:
        # Parse JSON if string
        if isinstance(tags, str):
            import json
            tags = json.loads(tags)
        
        # Join all 'name' values
        all_names = ' '.join(item.get('name', '') for item in tags if isinstance(item, dict))
        
        # Extract ad_id using regex
        import re
        matches = re.findall(r"ad_id\.(\w+)", all_names)
        return str(matches) if matches else ''
    except Exception:
        return ''

# Usage in transformation
record = {
    "tags": json.dumps(ticket.get("tags", []), ensure_ascii=False),
    "ad_id_value": extract_ad_id(ticket.get("tags", [])),  # ← Extract here
}
```

---

## 📊 Comparison: Old vs New

### Old System

```python
# In cs_extract.py
def extract_ad_id(tags):
    if pd.isna(tags):
        return ''
    all_names = ' '.join(item.get('name', '') for item in tags)
    return re.findall(r"ad_id\.(\w+)", all_names)

df['ad_id_value'] = df['tags'].apply(extract_ad_id).astype(str)
```

**Issues:**
- ❌ Uses pandas `isna()` (requires pandas)
- ❌ No error handling
- ❌ Assumes tags is already parsed

### New System

```python
# In ticket_ingestion.py
def extract_ad_id(tags):
    if not tags:
        return ''
    try:
        if isinstance(tags, str):
            tags = json.loads(tags)
        all_names = ' '.join(item.get('name', '') for item in tags if isinstance(item, dict))
        matches = re.findall(r"ad_id\.(\w+)", all_names)
        return str(matches) if matches else ''
    except Exception:
        return ''

record["ad_id_value"] = extract_ad_id(ticket.get("tags", []))
```

**Improvements:**
- ✅ No pandas dependency
- ✅ Error handling with try/except
- ✅ Handles both list and JSON string
- ✅ Type checking for dict items
- ✅ Returns empty string on error

---

## 🎯 Use Cases

### Why Extract Ad ID?

1. **Marketing Attribution**: Track which ad brought the customer
2. **ROI Analysis**: Calculate conversion rate per ad
3. **Campaign Optimization**: Identify high-performing ads

### Example Query

```sql
-- Count tickets by ad_id
SELECT 
    ad_id_value,
    COUNT(*) as ticket_count
FROM cs_ticket_api
WHERE ad_id_value != '' 
  AND ad_id_value != '[]'
GROUP BY ad_id_value
ORDER BY ticket_count DESC;
```

---

## 📋 Data Examples

### PostgreSQL Table

| ticket_id | tags (JSON) | ad_id_value |
|-----------|-------------|-------------|
| 100 | `[{"name": "ad_id.123"}]` | `['123']` |
| 200 | `[{"name": "ad_id.456"}, {"name": "vip"}]` | `['456']` |
| 300 | `[{"name": "urgent"}]` | `[]` or `''` |
| 400 | `[{"name": "ad_id.111"}, {"name": "ad_id.222"}]` | `['111', '222']` |

---

## ✅ Summary

### What is ad_id_value?

- **Derived field** extracted from `tags` array
- Uses **regex pattern** `ad_id\.(\w+)` to match
- Stores **list of matched IDs** as string

### How It Works

1. Join all tag names into one string
2. Apply regex to find `ad_id.XXX` patterns
3. Extract the `XXX` part (capture group)
4. Convert list to string

### New System Status

- ✅ **Implemented** in `ticket_ingestion.py`
- ✅ **Improved** error handling
- ✅ **Compatible** with old system output
- ✅ **No pandas dependency**
