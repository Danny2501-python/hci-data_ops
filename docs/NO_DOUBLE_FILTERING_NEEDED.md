# Why No Double Filtering Needed
## snapshot_date Filter is Sufficient

---

## ❌ Old Logic (Double Filtering - WRONG!)

```python
# Step 1: Filter by snapshot_date
scan = bronze_table.scan(
    row_filter=GreaterThanOrEqual("snapshot_date", cutoff_date)  # Last 10 snapshots
)

# Step 2: Filter by updated_at AGAIN
result = con.execute(f"""
    SELECT ticket_id, MAX(updated_at)
    FROM bronze_tickets
    WHERE updated_at >= '{cutoff_date}'  -- ❌ Double filter!
    GROUP BY ticket_id
""")
```

**Problem:** Filtering twice is redundant and confusing!

---

## ✅ Correct Logic (Single Filter)

```python
# Step 1: Filter by snapshot_date (partition pruning)
scan = bronze_table.scan(
    row_filter=GreaterThanOrEqual("snapshot_date", cutoff_date)  # Last 10 snapshots
)

# Step 2: Just GROUP BY (no additional filter needed)
result = con.execute(f"""
    SELECT ticket_id, MAX(updated_at)
    FROM bronze_tickets
    GROUP BY ticket_id  -- ✅ Deduplicate only
    ORDER BY updated_at DESC
    LIMIT 300
""")
```

**Correct:** Filter once by `snapshot_date`, then deduplicate!

---

## 🎯 Why snapshot_date Filter is Sufficient?

### Ingestion Logic

**Every day, we ingest:**
1. **Updated tickets** (last 3 days)
2. **Created tickets** (today)
3. **Created tickets** (yesterday)

**Example (2026-01-15):**
```python
# Fetch 1: Updated in last 3 days
updated_from = 2026-01-12  # 3 days ago
updated_to = 2026-01-15    # today

# Fetch 2: Created today
created_from = 2026-01-15
created_to = 2026-01-15

# Fetch 3: Created yesterday
created_from = 2026-01-14
created_to = 2026-01-14
```

**Result:** We ingest tickets updated/created in last ~3 days

---

### What's in Last 10 Snapshots?

**Snapshot dates:** 2026-01-06 to 2026-01-15 (10 days)

**Each snapshot contains:**
- Tickets updated in last 3 days from that snapshot date
- Tickets created on that snapshot date

**Combined coverage:**
- Snapshot 2026-01-06: Tickets updated 2026-01-03 to 2026-01-06
- Snapshot 2026-01-07: Tickets updated 2026-01-04 to 2026-01-07
- ...
- Snapshot 2026-01-15: Tickets updated 2026-01-12 to 2026-01-15

**Total coverage:** ~13 days of ticket updates (10 + 3 overlap)

---

## 📊 Example

### Scenario: Ticket #100 Updated Multiple Times

**Timeline:**
- 2026-01-05: Created
- 2026-01-08: Updated
- 2026-01-12: Updated
- 2026-01-15: Updated

**Ingestion:**

**2026-01-06 snapshot:**
```
ticket_id | updated_at  | snapshot_date
----------|-------------|---------------
100       | 2026-01-05  | 2026-01-06  (created yesterday)
```

**2026-01-09 snapshot:**
```
ticket_id | updated_at  | snapshot_date
----------|-------------|---------------
100       | 2026-01-08  | 2026-01-09  (updated in last 3 days)
```

**2026-01-13 snapshot:**
```
ticket_id | updated_at  | snapshot_date
----------|-------------|---------------
100       | 2026-01-12  | 2026-01-13  (updated in last 3 days)
```

**2026-01-15 snapshot:**
```
ticket_id | updated_at  | snapshot_date
----------|-------------|---------------
100       | 2026-01-15  | 2026-01-15  (updated in last 3 days)
```

---

### Query: Last 10 Snapshots

**Filter:** `snapshot_date >= 2026-01-06`

**Result:**
```
ticket_id | updated_at  | snapshot_date
----------|-------------|---------------
100       | 2026-01-05  | 2026-01-06
100       | 2026-01-08  | 2026-01-09
100       | 2026-01-12  | 2026-01-13
100       | 2026-01-15  | 2026-01-15
```

**After GROUP BY ticket_id, MAX(updated_at):**
```
ticket_id | updated_at
----------|------------
100       | 2026-01-15  ← Latest version!
```

**Perfect!** No need for `WHERE updated_at >= cutoff_date`!

---

## 🔍 What if We Add updated_at Filter?

### With Double Filter

```sql
SELECT ticket_id, MAX(updated_at)
FROM bronze_tickets
WHERE updated_at >= '2026-01-06'  -- ❌ Unnecessary!
GROUP BY ticket_id
```

**Result:**
```
ticket_id | updated_at
----------|------------
100       | 2026-01-15  ← Same result!
```

**Conclusion:** `WHERE updated_at >= cutoff_date` doesn't change the result!

---

## 📋 Why No Difference?

### Reason 1: Overlapping Coverage

**Last 10 snapshots** already cover tickets updated in last ~13 days

**Filtering by `updated_at >= cutoff_date` (10 days)** is redundant because:
- All tickets in last 10 snapshots were updated within ~13 days
- Filtering by 10 days doesn't exclude anything meaningful

---

### Reason 2: GROUP BY Handles Everything

**GROUP BY ticket_id, MAX(updated_at)** already:
- Deduplicates tickets
- Gets latest version
- Sorts by updated_at

**No need for additional WHERE clause!**

---

## ✅ Correct Approach

### Single Filter by snapshot_date

```python
# Step 1: Filter by snapshot_date (partition pruning + sufficient coverage)
scan = bronze_table.scan(
    row_filter=GreaterThanOrEqual("snapshot_date", cutoff_date)
)

# Step 2: Deduplicate and sort (no additional filter)
result = con.execute("""
    WITH a AS (
        SELECT 
            ticket_id, 
            MAX(updated_at) AS updated_at
        FROM bronze_tickets
        GROUP BY ticket_id  -- ✅ Deduplicate
    )
    SELECT ticket_id
    FROM a
    ORDER BY updated_at DESC  -- ✅ Sort by latest update
    LIMIT 300
""")
```

**Benefits:**
- ✅ Simpler logic
- ✅ Faster (no redundant filter)
- ✅ Correct result
- ✅ Clear intent

---

## 📊 Performance Comparison

### With Double Filter

```sql
-- Step 1: Scan (read 10 partitions)
-- Step 2: Filter by updated_at (scan all rows)
-- Step 3: GROUP BY
```

**Time:** Scan + Filter + Group = **Slower**

---

### Without Double Filter

```sql
-- Step 1: Scan (read 10 partitions)
-- Step 2: GROUP BY directly
```

**Time:** Scan + Group = **Faster!**

---

## 🎯 Summary

### Question

> Đã lọc theo 10 snapshot_date gần nhất, tại sao lúc query lại lọc theo updated_at 10 ngày gần nhất nữa?

### Answer

**Không cần!** Filter theo `snapshot_date` là đủ!

**Lý do:**

1. **✅ Sufficient coverage:** Last 10 snapshots cover ~13 days of updates
2. **✅ GROUP BY handles dedup:** No need for WHERE clause
3. **✅ Simpler logic:** One filter is clearer
4. **✅ Better performance:** No redundant filtering

### Correct Logic

```python
# Filter by snapshot_date (partition pruning)
scan = bronze_table.scan(
    row_filter=GreaterThanOrEqual("snapshot_date", cutoff_date)
)

# GROUP BY to deduplicate (no WHERE needed)
result = con.execute("""
    SELECT ticket_id, MAX(updated_at)
    FROM bronze_tickets
    GROUP BY ticket_id
    ORDER BY updated_at DESC
    LIMIT 300
""")
```

**Perfect!** ✅
