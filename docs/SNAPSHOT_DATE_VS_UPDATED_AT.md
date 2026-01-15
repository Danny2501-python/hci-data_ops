# snapshot_date vs updated_at - Filter Logic
## Why Old System Uses updated_at

---

## ✅ Bạn Đúng Rồi!

Hệ thống cũ filter theo **`updated_at`**, KHÔNG phải `snapshot_date`!

---

## 📊 Old System Logic

### Code (cs_extract.py)

```python
# Get tickets updated in last 10 days
query = f"""
    WITH a AS (
        SELECT ticket_id, MAX(updated_at) AS updated_at  
        FROM cs_ticket_api_new_day 
        WHERE brand_id = {brand_id} 
        GROUP BY ticket_id
    )
    SELECT ticket_id, updated_at  
    FROM a 
    ORDER BY updated_at DESC 
    LIMIT 300;
"""
```

**Key Point:** Filter by `updated_at` (when ticket was last updated)

---

## 🎯 What's The Difference?

### `updated_at` (Old System)

**Definition:** Timestamp when the **ticket was last updated** in CareSoft

**Example:**
```
Ticket #100:
- Created: 2026-01-01
- Updated: 2026-01-14  ← This is updated_at
- Ingested: 2026-01-15
```

**Logic:** "Get tickets that were **updated** in last 10 days"

---

### `snapshot_date` (New System - WRONG!)

**Definition:** Date when we **ingested** the ticket into our system

**Example:**
```
Ticket #100:
- Created: 2026-01-01
- Updated: 2026-01-14
- Ingested: 2026-01-15  ← This is snapshot_date
```

**Logic:** "Get tickets that were **ingested** in last 10 days"

---

## ❌ Why snapshot_date Was Wrong

### Scenario

**Ticket #100:**
- Created: 2025-12-01
- Last updated: 2025-12-15 (30 days ago)
- Ingested today: 2026-01-15

**Old System (updated_at):**
```sql
WHERE updated_at >= '2026-01-05'  -- Last 10 days
-- Result: NOT selected (updated 30 days ago)
```

**New System (snapshot_date - WRONG):**
```sql
WHERE snapshot_date >= '2026-01-05'  -- Last 10 days
-- Result: SELECTED (ingested today)
```

**Problem:** Ticket hasn't been updated in 30 days, but we'd still enrich it!

---

## ✅ Correct Logic (Fixed)

### New System (updated_at - CORRECT)

```python
# Filter by updated_at (matching old system)
scan = bronze_table.scan(
    row_filter=And(
        GreaterThanOrEqual("updated_at", cutoff_date),  # ← Correct!
        EqualTo("brand_id", brand_erp_id),
        NotNull("ticket_id")
    ),
    selected_fields=("ticket_id", "updated_at")
)
```

**Logic:** "Get tickets that were **updated** in last 10 days"

---

## 📋 Comparison

### Scenario: Ticket Updated 30 Days Ago

| Field | Value | Old System | New (Wrong) | New (Fixed) |
|-------|-------|------------|-------------|-------------|
| `created_at` | 2025-12-01 | - | - | - |
| `updated_at` | 2025-12-15 | ❌ Not selected | - | ❌ Not selected |
| `snapshot_date` | 2026-01-15 | - | ✅ Selected (WRONG!) | - |

**Result:** Fixed version matches old system! ✅

---

### Scenario: Ticket Updated Yesterday

| Field | Value | Old System | New (Wrong) | New (Fixed) |
|-------|-------|------------|-------------|-------------|
| `created_at` | 2026-01-01 | - | - | - |
| `updated_at` | 2026-01-14 | ✅ Selected | - | ✅ Selected |
| `snapshot_date` | 2026-01-15 | - | ✅ Selected | - |

**Result:** Both work, but for different reasons

---

## 🎯 Why Filter by updated_at?

### Business Logic

**Goal:** Enrich tickets that have **recent activity**

**Reasoning:**
1. **Active tickets** = Recently updated
2. **Stale tickets** = Not updated in long time
3. **Priority** = Focus on active tickets

**Example:**
```
Ticket #100: Last updated 6 months ago
→ Low priority, skip enrichment

Ticket #200: Updated yesterday
→ High priority, enrich now!
```

---

### Old System Query Breakdown

```sql
-- Step 1: Get latest updated_at for each ticket (from last 10 days)
WITH a AS (
    SELECT ticket_id, MAX(updated_at) AS updated_at  
    FROM cs_ticket_api_new_day 
    WHERE brand_id = {brand_id} 
    GROUP BY ticket_id
)

-- Step 2: Sort by updated_at and get top 300
SELECT ticket_id, updated_at  
FROM a 
ORDER BY updated_at DESC 
LIMIT 300;
```

**Logic:**
1. Group by `ticket_id` (handle duplicates)
2. Get `MAX(updated_at)` (latest update)
3. Order by `updated_at DESC` (most recent first)
4. `LIMIT 300` (top 300 most recently updated)

---

## ✅ New System (Fixed)

### Code

```python
# Calculate cutoff date (10 days ago)
cutoff_date = self.snapshot_date - timedelta(days=10)

# Filter by updated_at (matching old system)
scan = bronze_table.scan(
    row_filter=And(
        GreaterThanOrEqual("updated_at", cutoff_date),  # ← Filter by updated_at
        EqualTo("brand_id", brand_erp_id),
        NotNull("ticket_id")
    ),
    selected_fields=("ticket_id", "updated_at")
)

# Then in DuckDB: Get top 300 most recent
result = con.execute("""
    SELECT ticket_id
    FROM bronze_tickets
    ORDER BY updated_at DESC
    LIMIT 300
""").fetchall()
```

**Logic:**
1. Filter tickets updated in last 10 days
2. Sort by `updated_at DESC`
3. Take top 300

**Perfect match with old system!** ✅

---

## 📊 Complete Flow Comparison

### Old System

```
1. Filter: WHERE date(updated_at) >= current_date - interval '10 days'
2. Group: GROUP BY ticket_id
3. Aggregate: MAX(updated_at)
4. Sort: ORDER BY updated_at DESC
5. Limit: LIMIT 300
```

### New System (Fixed)

```
1. Filter: WHERE updated_at >= cutoff_date (10 days ago)
2. (Iceberg handles duplicates via partition/merge)
3. Sort: ORDER BY updated_at DESC
4. Limit: LIMIT 300
```

**Same logic, different implementation!** ✅

---

## 🎯 Key Takeaway

### Wrong Approach

```python
# ❌ Filter by snapshot_date
GreaterThanOrEqual("snapshot_date", cutoff_date)
```

**Problem:** Selects tickets based on **when we ingested them**, not **when they were updated**

### Correct Approach

```python
# ✅ Filter by updated_at
GreaterThanOrEqual("updated_at", cutoff_date)
```

**Correct:** Selects tickets based on **when they were last updated** (matching old system)

---

## ✅ Summary

| Aspect | Old System | New (Wrong) | New (Fixed) |
|--------|------------|-------------|-------------|
| **Filter Field** | `updated_at` | `snapshot_date` | `updated_at` ✅ |
| **Logic** | Recent activity | Recent ingestion | Recent activity ✅ |
| **Matches Old** | - | ❌ No | ✅ Yes |

**Result:** Fixed to match old system behavior! 🎉

---

## 📝 Code Changes

### Before (WRONG)

```python
scan = bronze_table.scan(
    row_filter=And(
        GreaterThanOrEqual("snapshot_date", cutoff_date),  # ❌ WRONG
        EqualTo("brand_id", brand_erp_id),
        NotNull("ticket_id")
    )
)
```

### After (CORRECT)

```python
scan = bronze_table.scan(
    row_filter=And(
        GreaterThanOrEqual("updated_at", cutoff_date),  # ✅ CORRECT
        EqualTo("brand_id", brand_erp_id),
        NotNull("ticket_id")
    )
)
```

**Perfect match with old system!** 🎉
