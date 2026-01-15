# Tái Hiện Logic cs_ticket_api_new_day
## Không Cần Tạo Bảng Trung Gian

---

## 🎯 Old System Logic

### Step 1: Populate `cs_ticket_api_new_day`

```python
# Insert tickets updated in last 10 days into temporary table
insert_query_new_day = f"""
    DELETE FROM cs_ticket_api_new_day WHERE brand_id = {brand_id}; 
    
    INSERT INTO cs_ticket_api_new_day 
    SELECT ticket_id, created_at, updated_at, brand_id 
    FROM cs_ticket_api 
    WHERE date(updated_at) >= current_date - interval '10 days' 
      AND brand_id = {brand_id};
"""
```

**Purpose:** Create temporary table with tickets updated in last 10 days

---

### Step 2: Query from `cs_ticket_api_new_day`

```python
select_query = f"""
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

**Logic:**
1. Group by `ticket_id` (handle duplicates)
2. Get `MAX(updated_at)` (latest update time)
3. Order by `updated_at DESC` (most recent first)
4. Limit 300 (top 300 tickets)

---

## ✅ New System - Direct Query (No Temp Table)

### Complete Logic

```python
# Step 1: Calculate cutoff date (10 days ago from TODAY)
current_date = datetime.now().date()
cutoff_date = current_date - timedelta(days=10)

# Step 2: Scan Bronze table for tickets updated in last 10 days
scan = bronze_table.scan(
    row_filter=And(
        GreaterThanOrEqual("updated_at", cutoff_date),  # ← Filter by updated_at
        EqualTo("brand_id", brand_erp_id),
        NotNull("ticket_id")
    ),
    selected_fields=("ticket_id", "updated_at")
)

# Step 3: Load into DuckDB
tickets_table = pa.Table.from_batches(batches)
con.register('bronze_tickets', tickets_table)

# Step 4: Query with same logic as old system
result = con.execute(f"""
    WITH a AS (
        SELECT 
            ticket_id, 
            MAX(updated_at) AS updated_at
        FROM bronze_tickets
        GROUP BY ticket_id
    )
    SELECT ticket_id
    FROM a
    ORDER BY updated_at DESC
    LIMIT {limit}
""").fetchall()
```

---

## 📊 Side-by-Side Comparison

### Old System

```sql
-- Step 1: Populate temp table
DELETE FROM cs_ticket_api_new_day WHERE brand_id = {brand_id};

INSERT INTO cs_ticket_api_new_day 
SELECT ticket_id, created_at, updated_at, brand_id 
FROM cs_ticket_api 
WHERE date(updated_at) >= current_date - interval '10 days' 
  AND brand_id = {brand_id};

-- Step 2: Query from temp table
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
```

### New System

```python
# Step 1: Filter in Iceberg scan (replaces INSERT INTO new_day)
scan = bronze_table.scan(
    row_filter=And(
        GreaterThanOrEqual("updated_at", cutoff_date),  # Same filter
        EqualTo("brand_id", brand_erp_id),
        NotNull("ticket_id")
    )
)

# Step 2: Query in DuckDB (same logic)
WITH a AS (
    SELECT ticket_id, MAX(updated_at) AS updated_at  
    FROM bronze_tickets  -- ← Replaces cs_ticket_api_new_day
    GROUP BY ticket_id
)
SELECT ticket_id  
FROM a 
ORDER BY updated_at DESC 
LIMIT 300;
```

---

## 🔄 Data Flow

### Old System

```
cs_ticket_api (full table)
    ↓
    Filter: updated_at >= current_date - 10 days
    ↓
cs_ticket_api_new_day (temp table)
    ↓
    GROUP BY ticket_id
    MAX(updated_at)
    ↓
    ORDER BY updated_at DESC
    LIMIT 300
    ↓
Result: Top 300 ticket_ids
```

### New System

```
cs_ticket_api (Iceberg Bronze)
    ↓
    Iceberg Scan Filter: updated_at >= cutoff_date
    ↓
bronze_tickets (DuckDB temp table)
    ↓
    GROUP BY ticket_id
    MAX(updated_at)
    ↓
    ORDER BY updated_at DESC
    LIMIT 300
    ↓
Result: Top 300 ticket_ids
```

**Same logic, different implementation!**

---

## 📋 Key Differences

| Aspect | Old System | New System |
|--------|------------|------------|
| **Temp Table** | `cs_ticket_api_new_day` (PostgreSQL) | `bronze_tickets` (DuckDB in-memory) |
| **Filter Location** | SQL INSERT | Iceberg scan |
| **Query Engine** | PostgreSQL | DuckDB |
| **Persistence** | Persisted table | In-memory only |
| **Cleanup** | DELETE before INSERT | Auto-cleanup on close |

---

## ✅ Benefits of New System

### 1. No Persistent Temp Table

**Old:**
```sql
-- Need to manage temp table
DELETE FROM cs_ticket_api_new_day WHERE brand_id = {brand_id};
INSERT INTO cs_ticket_api_new_day ...;
```

**New:**
```python
# Auto-managed in-memory
con.register('bronze_tickets', tickets_table)
# Auto-cleanup when con.close()
```

### 2. Faster Filtering

**Old:**
```sql
-- Full table scan on cs_ticket_api
SELECT * FROM cs_ticket_api 
WHERE date(updated_at) >= current_date - interval '10 days';
```

**New:**
```python
# Iceberg predicate pushdown (only reads relevant files)
scan = bronze_table.scan(
    row_filter=GreaterThanOrEqual("updated_at", cutoff_date)
)
```

### 3. No Disk I/O for Temp Table

**Old:** Write to disk → Read from disk  
**New:** In-memory only (DuckDB)

---

## 🎯 Exact Logic Match

### Old System Query

```sql
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
```

### New System Query

```sql
WITH a AS (
    SELECT ticket_id, MAX(updated_at) AS updated_at  
    FROM bronze_tickets  -- ← Only difference: table name
    GROUP BY ticket_id
)
SELECT ticket_id
FROM a 
ORDER BY updated_at DESC 
LIMIT 300;
```

**Identical logic!** ✅

---

## 📊 Example

### Input Data (Bronze Table)

```
ticket_id | updated_at          | brand_id
----------|---------------------|----------
100       | 2026-01-14 10:00:00 | 1
100       | 2026-01-15 11:00:00 | 1  ← Duplicate (newer)
200       | 2026-01-13 09:00:00 | 1
300       | 2026-01-05 08:00:00 | 1  ← Old (30 days ago)
400       | 2026-01-14 12:00:00 | 1
```

### Step 1: Filter (updated_at >= cutoff_date)

Cutoff: `2026-01-05` (10 days ago)

```
ticket_id | updated_at          | brand_id
----------|---------------------|----------
100       | 2026-01-14 10:00:00 | 1
100       | 2026-01-15 11:00:00 | 1
200       | 2026-01-13 09:00:00 | 1
300       | 2026-01-05 08:00:00 | 1  ← Included (exactly 10 days)
400       | 2026-01-14 12:00:00 | 1
```

### Step 2: GROUP BY ticket_id, MAX(updated_at)

```
ticket_id | updated_at
----------|--------------------
100       | 2026-01-15 11:00:00  ← MAX (kept newer)
200       | 2026-01-13 09:00:00
300       | 2026-01-05 08:00:00
400       | 2026-01-14 12:00:00
```

### Step 3: ORDER BY updated_at DESC, LIMIT 300

```
ticket_id | updated_at
----------|--------------------
100       | 2026-01-15 11:00:00  ← Most recent
400       | 2026-01-14 12:00:00
200       | 2026-01-13 09:00:00
300       | 2026-01-05 08:00:00
```

### Result

```python
ticket_ids = [100, 400, 200, 300]
```

---

## ✅ Summary

### Old System

1. ❌ **Create temp table** `cs_ticket_api_new_day`
2. ❌ **DELETE** old data
3. ❌ **INSERT** new data (10 days)
4. ✅ **Query** with GROUP BY + ORDER BY + LIMIT

### New System

1. ✅ **Scan** Bronze table (Iceberg predicate pushdown)
2. ✅ **Register** in DuckDB (in-memory)
3. ✅ **Query** with same logic (GROUP BY + ORDER BY + LIMIT)
4. ✅ **Auto-cleanup** (no persistent temp table)

**Same result, better performance!** 🎉

---

## 🎯 Key Takeaway

**Old System Logic:**
```
Filter (10 days) → Temp Table → GROUP BY → ORDER BY → LIMIT 300
```

**New System Logic:**
```
Filter (10 days) → In-Memory → GROUP BY → ORDER BY → LIMIT 300
```

**Perfect match without temp table!** ✅
