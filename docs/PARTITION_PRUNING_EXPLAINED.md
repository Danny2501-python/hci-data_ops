# Why Filter by snapshot_date in Iceberg Scan
## Partition Pruning for Performance

---

## ✅ Bạn Đúng Rồi!

**Iceberg scan nên filter theo `snapshot_date`, KHÔNG phải `updated_at`!**

---

## 🎯 Lý Do

### 1. Iceberg Partitioned by `snapshot_date`

```python
# Schema definition (ticket_ingestion.py)
partition_spec = PartitionSpec(
    PartitionField(
        source_id=28,  # snapshot_date field
        field_id=1000,
        transform=DayTransform(),
        name="snapshot_date_day"
    )
)
```

**Iceberg table is partitioned by `snapshot_date`!**

---

### 2. Partition Pruning

**Filter by `snapshot_date` (FAST):**
```python
scan = bronze_table.scan(
    row_filter=GreaterThanOrEqual("snapshot_date", cutoff_date)  # ✅ Partition pruning!
)
```

**What happens:**
- Iceberg reads **partition metadata**
- Skips partitions where `snapshot_date < cutoff_date`
- Only reads **relevant data files**
- **FAST!** ⚡

---

**Filter by `updated_at` (SLOW):**
```python
scan = bronze_table.scan(
    row_filter=GreaterThanOrEqual("updated_at", cutoff_date)  # ❌ No partition pruning!
)
```

**What happens:**
- Iceberg **cannot** use partition metadata (not partitioned by `updated_at`)
- Must scan **ALL data files**
- Filter row-by-row
- **SLOW!** 🐌

---

## 📊 Performance Comparison

### Scenario: 100 days of data, want last 10 days

**Filter by `snapshot_date`:**
```
Total partitions: 100 (one per day)
Partitions to read: 10 (last 10 days)
Files to read: 10 files
Performance: ⚡ FAST (90% reduction!)
```

**Filter by `updated_at`:**
```
Total partitions: 100
Partitions to read: 100 (must read all!)
Files to read: 100 files
Performance: 🐌 SLOW (no reduction)
```

---

## ✅ Correct Approach: Two-Stage Filter

### Stage 1: Iceberg Scan (Partition Pruning)

```python
# Filter by snapshot_date for partition pruning
scan = bronze_table.scan(
    row_filter=GreaterThanOrEqual("snapshot_date", cutoff_date)  # ✅ Fast!
)
```

**Result:** Only read partitions from last 10 days

---

### Stage 2: DuckDB Query (Exact Filter)

```python
# Filter by updated_at for exact logic
result = con.execute(f"""
    WITH a AS (
        SELECT ticket_id, MAX(updated_at) AS updated_at
        FROM bronze_tickets
        WHERE updated_at >= '{cutoff_date}'  -- ✅ Exact filter!
        GROUP BY ticket_id
    )
    SELECT ticket_id
    FROM a
    ORDER BY updated_at DESC
    LIMIT 300
""").fetchall()
```

**Result:** Only tickets actually updated in last 10 days

---

## 🔄 Complete Flow

```
1. Iceberg Scan (Partition Pruning)
   ↓
   Filter: snapshot_date >= cutoff_date
   ↓
   Read only 10 partitions (last 10 days)
   ↓
   Load ~10 days of data into DuckDB

2. DuckDB Query (Exact Filter)
   ↓
   Filter: updated_at >= cutoff_date
   ↓
   GROUP BY ticket_id, MAX(updated_at)
   ↓
   ORDER BY updated_at DESC
   ↓
   LIMIT 300
   ↓
   Result: Top 300 tickets actually updated in last 10 days
```

---

## 📋 Why Two Filters?

### Question

"Why filter twice? Why not just filter by `updated_at` in Iceberg scan?"

### Answer

**Because `snapshot_date` ≈ `updated_at` for recent data!**

**Example:**
- Ticket updated on 2026-01-14
- Ingested on 2026-01-15 (snapshot_date)
- Difference: 1 day

**For last 10 days:**
- `snapshot_date >= 2026-01-05` → Includes tickets updated ~2026-01-04 to 2026-01-15
- Then filter `updated_at >= 2026-01-05` → Exact match

**Trade-off:**
- Read slightly more data (1-2 extra days)
- But **90% faster** due to partition pruning!

---

## 📊 Example

### Data in Iceberg

```
Partition: snapshot_date=2026-01-05
  - Ticket 100: updated_at=2026-01-04 (old!)
  - Ticket 200: updated_at=2026-01-05 (good)

Partition: snapshot_date=2026-01-14
  - Ticket 300: updated_at=2026-01-14 (good)
  - Ticket 400: updated_at=2026-01-13 (good)
```

### Stage 1: Iceberg Scan

```python
# Filter: snapshot_date >= 2026-01-05
# Result: Read both partitions (2026-01-05 and 2026-01-14)
```

**Loaded into DuckDB:**
```
ticket_id | updated_at  | snapshot_date
----------|-------------|---------------
100       | 2026-01-04  | 2026-01-05  ← Will be filtered out
200       | 2026-01-05  | 2026-01-05
300       | 2026-01-14  | 2026-01-14
400       | 2026-01-13  | 2026-01-14
```

### Stage 2: DuckDB Query

```sql
WHERE updated_at >= '2026-01-05'
```

**Result:**
```
ticket_id | updated_at
----------|------------
200       | 2026-01-05  ✅
300       | 2026-01-14  ✅
400       | 2026-01-13  ✅
-- Ticket 100 filtered out! ❌
```

**Perfect!** Only tickets actually updated in last 10 days.

---

## ✅ Summary

### Wrong Approach

```python
# ❌ Filter by updated_at in Iceberg scan
scan = bronze_table.scan(
    row_filter=GreaterThanOrEqual("updated_at", cutoff_date)
)
```

**Problem:** No partition pruning → Read all 100 partitions → SLOW!

---

### Correct Approach

```python
# ✅ Stage 1: Filter by snapshot_date (partition pruning)
scan = bronze_table.scan(
    row_filter=GreaterThanOrEqual("snapshot_date", cutoff_date)
)

# ✅ Stage 2: Filter by updated_at (exact logic)
result = con.execute("""
    SELECT ticket_id, MAX(updated_at) AS updated_at
    FROM bronze_tickets
    WHERE updated_at >= '{cutoff_date}'
    GROUP BY ticket_id
    ORDER BY updated_at DESC
    LIMIT 300
""").fetchall()
```

**Benefits:**
- ✅ **Partition pruning** (90% faster!)
- ✅ **Exact logic** (matches old system)
- ✅ **Best performance**

---

## 🎯 Key Takeaway

| Filter Location | Filter Field | Purpose |
|----------------|--------------|---------|
| **Iceberg Scan** | `snapshot_date` | Partition pruning (performance) |
| **DuckDB Query** | `updated_at` | Exact logic (correctness) |

**Two-stage filtering = Fast + Correct!** 🎉
