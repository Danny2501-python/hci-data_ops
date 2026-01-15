# UPSERT vs APPEND - Critical Difference
## Old System vs New System Architecture

---

## 🎯 Câu Hỏi Cốt Lõi

**Old system:** UPSERT liên tục vào `cs_ticket_api` → Lọc -10 days → `new_day`

**New system:** APPEND theo snapshot_date → Có duplicates

**Liệu có đáp ứng được logic cũ không?**

---

## 📊 Old System Logic

### Step 1: UPSERT vào `cs_ticket_api`

```python
# process_table_2() - UPSERT logic
def process_table_2(self, db, table_temp, table, key_columns):
    """
    UPSERT: Update existing, Insert new
    Key: ticket_no, updated_at
    """
    query = f"""
        -- Delete existing records with same key
        DELETE FROM {table} a
        USING {table_temp} b
        WHERE a.ticket_no = b.ticket_no 
          AND a.updated_at = b.updated_at;
        
        -- Insert all records from temp
        INSERT INTO {table}
        SELECT * FROM {table_temp};
    """
```

**Result:** `cs_ticket_api` has **ONLY ONE** record per `(ticket_no, updated_at)`

**Example:**
```
ticket_id | ticket_no | updated_at  | data
----------|-----------|-------------|------
100       | 1001      | 2026-01-14  | v1
200       | 1002      | 2026-01-15  | v1
```

**Next day, ticket #100 updated:**
```
ticket_id | ticket_no | updated_at  | data
----------|-----------|-------------|------
100       | 1001      | 2026-01-14  | v1  ← Old version
100       | 1001      | 2026-01-16  | v2  ← New version (NEW RECORD!)
200       | 1002      | 2026-01-15  | v1
```

**Key point:** **2 separate records** for ticket #100 (different `updated_at`)

---

### Step 2: Filter -10 days → `new_day`

```sql
DELETE FROM cs_ticket_api_new_day WHERE brand_id = {brand_id};

INSERT INTO cs_ticket_api_new_day 
SELECT ticket_id, created_at, updated_at, brand_id 
FROM cs_ticket_api 
WHERE date(updated_at) >= current_date - interval '10 days' 
  AND brand_id = {brand_id};
```

**Result:** `new_day` has tickets updated in last 10 days

**Example (cutoff = 2026-01-06):**
```
ticket_id | updated_at
----------|------------
100       | 2026-01-14  ✅ (within 10 days)
100       | 2026-01-16  ✅ (within 10 days)
200       | 2026-01-15  ✅ (within 10 days)
```

---

### Step 3: GROUP BY to get latest

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

**Result:**
```
ticket_id | updated_at
----------|------------
100       | 2026-01-16  ← MAX (latest version)
200       | 2026-01-15
```

---

## 📊 New System Logic

### Step 1: APPEND theo snapshot_date

```python
# ticket_ingestion.py
for ticket in all_records:
    record = {
        "ticket_id": ticket.get("ticket_id"),
        "updated_at": ticket.get("updated_at"),
        "snapshot_date": snapshot_date,  # ← Today's date
        # ... other fields
    }
    transformed_records.append(record)

# Append to Iceberg (NO UPSERT!)
ticket_table.append(df)
```

**Result:** Bronze table has **MULTIPLE** records per ticket (one per snapshot_date)

**Example:**

**Day 1 (2026-01-15):**
```
ticket_id | updated_at  | snapshot_date
----------|-------------|---------------
100       | 2026-01-14  | 2026-01-15
200       | 2026-01-15  | 2026-01-15
```

**Day 2 (2026-01-16) - Ticket #100 updated:**
```
ticket_id | updated_at  | snapshot_date
----------|-------------|---------------
100       | 2026-01-14  | 2026-01-15  ← Old snapshot
100       | 2026-01-16  | 2026-01-16  ← New snapshot
200       | 2026-01-15  | 2026-01-15
```

**Key point:** **2 records** for ticket #100 with **different snapshot_date**

---

### Step 2: Filter by snapshot_date (partition pruning)

```python
scan = bronze_table.scan(
    row_filter=GreaterThanOrEqual("snapshot_date", cutoff_date)
)
```

**Result:** Get all records from last 10 days of snapshots

**Example (cutoff = 2026-01-06):**
```
ticket_id | updated_at  | snapshot_date
----------|-------------|---------------
100       | 2026-01-14  | 2026-01-15  ✅
100       | 2026-01-16  | 2026-01-16  ✅
200       | 2026-01-15  | 2026-01-15  ✅
```

---

### Step 3: Filter by updated_at + GROUP BY

```sql
WITH a AS (
    SELECT ticket_id, MAX(updated_at) AS updated_at
    FROM bronze_tickets
    WHERE updated_at >= '{cutoff_date}'  -- ← Filter by updated_at
    GROUP BY ticket_id
)
SELECT ticket_id
FROM a
ORDER BY updated_at DESC
LIMIT 300
```

**Result:**
```
ticket_id | updated_at
----------|------------
100       | 2026-01-16  ← MAX (latest version)
200       | 2026-01-15
```

---

## ✅ Comparison: Same Result!

### Old System

```
cs_ticket_api (UPSERT)
    ↓ Multiple records per ticket (different updated_at)
    ↓ Filter: updated_at >= -10 days
cs_ticket_api_new_day
    ↓ GROUP BY ticket_id, MAX(updated_at)
Result: Latest version of each ticket
```

### New System

```
cs_ticket_api (APPEND by snapshot_date)
    ↓ Multiple records per ticket (different snapshot_date)
    ↓ Filter: snapshot_date >= -10 days
bronze_tickets (in-memory)
    ↓ Filter: updated_at >= -10 days
    ↓ GROUP BY ticket_id, MAX(updated_at)
Result: Latest version of each ticket
```

**Same result!** ✅

---

## 🎯 Key Insight

### Old System: Multiple Records by `updated_at`

```
Ticket #100:
- Record 1: updated_at = 2026-01-14
- Record 2: updated_at = 2026-01-16
```

**Deduplicate:** `GROUP BY ticket_id, MAX(updated_at)` → Get record 2

---

### New System: Multiple Records by `snapshot_date`

```
Ticket #100:
- Record 1: updated_at = 2026-01-14, snapshot_date = 2026-01-15
- Record 2: updated_at = 2026-01-16, snapshot_date = 2026-01-16
```

**Deduplicate:** `GROUP BY ticket_id, MAX(updated_at)` → Get record 2

**Same logic!** ✅

---

## 📋 Critical Difference

### Old System

**Storage:** UPSERT (replace old versions)
- ✅ No duplicates by `(ticket_no, updated_at)`
- ✅ Smaller storage (only latest versions)
- ❌ Lose history (can't see old versions)

### New System

**Storage:** APPEND (keep all versions)
- ✅ Full history (can see all versions)
- ✅ Time travel (query any snapshot_date)
- ❌ More storage (all versions kept)
- ✅ Deduplication in query (GROUP BY)

---

## ✅ Does New System Meet Old System Logic?

### Answer: **YES!** ✅

**Why:**

1. **Same deduplication logic:** `GROUP BY ticket_id, MAX(updated_at)`
2. **Same filter logic:** `updated_at >= cutoff_date`
3. **Same result:** Latest version of each ticket updated in last 10 days

**Bonus benefits:**

1. ✅ **Time travel:** Can query historical snapshots
2. ✅ **Audit trail:** Full history of all changes
3. ✅ **Partition pruning:** Faster queries

---

## 🔍 Detailed Example

### Scenario: Ticket #100 Updated Multiple Times

**Timeline:**
- 2026-01-10: Created (updated_at = 2026-01-10)
- 2026-01-14: Updated (updated_at = 2026-01-14)
- 2026-01-16: Updated (updated_at = 2026-01-16)

---

### Old System Storage

**cs_ticket_api:**
```
ticket_id | updated_at  | data
----------|-------------|------
100       | 2026-01-10  | v1  ← Old version (still there if > -10 days)
100       | 2026-01-14  | v2  ← Old version
100       | 2026-01-16  | v3  ← Latest version
```

**cs_ticket_api_new_day (filtered by updated_at >= 2026-01-06):**
```
ticket_id | updated_at
----------|------------
100       | 2026-01-10  ✅
100       | 2026-01-14  ✅
100       | 2026-01-16  ✅
```

**After GROUP BY:**
```
ticket_id | updated_at
----------|------------
100       | 2026-01-16  ← MAX
```

---

### New System Storage

**cs_ticket_api (Bronze):**
```
ticket_id | updated_at  | snapshot_date | data
----------|-------------|---------------|------
100       | 2026-01-10  | 2026-01-10    | v1
100       | 2026-01-14  | 2026-01-14    | v2
100       | 2026-01-14  | 2026-01-15    | v2  ← Re-ingested
100       | 2026-01-16  | 2026-01-16    | v3
```

**After filter (snapshot_date >= 2026-01-06):**
```
ticket_id | updated_at  | snapshot_date
----------|-------------|---------------
100       | 2026-01-10  | 2026-01-10    ✅
100       | 2026-01-14  | 2026-01-14    ✅
100       | 2026-01-14  | 2026-01-15    ✅ (duplicate)
100       | 2026-01-16  | 2026-01-16    ✅
```

**After filter (updated_at >= 2026-01-06) + GROUP BY:**
```
ticket_id | updated_at
----------|------------
100       | 2026-01-16  ← MAX
```

**Same result!** ✅

---

## 🎯 Conclusion

### Question

> Logic cũ UPSERT liên tục vào `cs_ticket_api`, rồi lọc -10 để đổ vào `new_day`.
> Hệ thống mới APPEND theo snapshot_date.
> Liệu có đáp ứng được logic cũ không?

### Answer

**CÓ! Hoàn toàn đáp ứng được!** ✅

**Lý do:**

1. **Cùng deduplication logic:** `GROUP BY ticket_id, MAX(updated_at)`
2. **Cùng filter logic:** `updated_at >= cutoff_date`
3. **Cùng kết quả:** Latest version của mỗi ticket

**Khác biệt:**

| Aspect | Old System | New System |
|--------|------------|------------|
| **Storage** | UPSERT (replace) | APPEND (keep all) |
| **Duplicates** | By `updated_at` | By `snapshot_date` |
| **History** | ❌ Lost | ✅ Kept |
| **Query** | Same logic | Same logic |
| **Result** | ✅ Latest version | ✅ Latest version |

**Bonus của new system:**
- ✅ Time travel
- ✅ Audit trail
- ✅ Partition pruning

**Perfect match with better features!** 🎉
