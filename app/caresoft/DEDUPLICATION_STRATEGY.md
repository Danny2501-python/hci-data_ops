# Deduplication Strategy in Serving Layer

## Problem

When reading from Iceberg and upserting to PostgreSQL, if the same `(id, brand_id)` appears **multiple times** in the batch:

```python
# Example data from Iceberg
rows = [
    (1, 'Source A', ..., '2026-01-15 10:00:00', 1),  # older
    (1, 'Source B', ..., '2026-01-17 11:00:00', 1),  # newer ✅
]
```

**PostgreSQL UPSERT behavior:**
```sql
INSERT INTO table VALUES (...), (...)
ON CONFLICT (id, brand_id) DO UPDATE SET ...
WHERE table.updated_at < EXCLUDED.updated_at
```

PostgreSQL will process rows **sequentially**:
1. Insert row 1 (id=1, updated_at='2026-01-15')
2. Conflict with row 2 (id=1) → Check WHERE clause
3. **Keep row 2** (last in batch)

**Problem:** PostgreSQL keeps the **LAST row in batch**, NOT the row with **MAX(updated_at)**!

---

## Solution

**Deduplicate in Python BEFORE UPSERT:**

```python
def get_ticket_sources_for_date(self, snapshot_date: date):
    # Read from Iceberg
    rows = [...]
    
    # Deduplicate: Keep only MAX(updated_at) per (id, brand_id)
    if rows:
        id_idx = columns.index('id')
        brand_id_idx = columns.index('brand_id')
        updated_at_idx = columns.index('updated_at')
        
        # Group by (id, brand_id)
        from collections import defaultdict
        grouped = defaultdict(list)
        for row in rows:
            key = (row[id_idx], row[brand_id_idx])
            grouped[key].append(row)
        
        # Keep only MAX(updated_at) for each key
        deduped_rows = []
        for key, group in grouped.items():
            sorted_group = sorted(group, key=lambda r: r[updated_at_idx] or '', reverse=True)
            deduped_rows.append(sorted_group[0])  # Take newest
        
        rows = deduped_rows
    
    return columns, rows
```

---

## Why This Happens

### Scenario 1: Multiple snapshots in Bronze

If Bronze table has multiple snapshots for the same `(id, brand_id)`:

```
snapshot_date | id | brand_id | updated_at
2026-01-15    | 1  | 1        | 2026-01-15 10:00:00
2026-01-17    | 1  | 1        | 2026-01-17 11:00:00  ✅ Latest
```

When reading `snapshot_date = '2026-01-17'`, we might get **both rows** if partition contains historical data.

### Scenario 2: API returns duplicates

CareSoft API might return the same source multiple times with different `updated_at`.

### Scenario 3: Race condition

Multiple brands calling API simultaneously might create duplicate entries.

---

## Implementation

### ✅ With Deduplication (CORRECT):

```python
# ticket_source_metadata_serving_layer.py
def get_ticket_sources_for_date(self, snapshot_date: date):
    rows = [...]  # Read from Iceberg
    
    # Deduplicate before UPSERT
    rows = self.deduplicate_by_max_updated_at(rows, columns)
    
    return columns, rows
```

**Result:** PostgreSQL receives **only 1 row** per `(id, brand_id)` → No conflict → Correct data

### ❌ Without Deduplication (WRONG):

```python
def get_ticket_sources_for_date(self, snapshot_date: date):
    rows = [...]  # Read from Iceberg
    return columns, rows  # Might have duplicates
```

**Result:** PostgreSQL receives **multiple rows** per `(id, brand_id)` → Keeps LAST in batch → **Wrong data if last is not newest**

---

## Performance Impact

| Metric | Without Dedup | With Dedup | Impact |
|--------|---------------|------------|--------|
| **Read Time** | Fast | Fast | Same |
| **Dedup Time** | 0ms | ~10-50ms | Minimal |
| **UPSERT Time** | Slower (conflicts) | Faster (no conflicts) | Better |
| **Data Quality** | ❌ Wrong | ✅ Correct | Critical |

**Deduplication is CHEAP and NECESSARY!**

---

## When to Deduplicate

### ✅ Always deduplicate if:

1. Source table might have duplicates
2. Reading from Bronze/Silver (not Gold final)
3. UPSERT uses `updated_at` for conflict resolution
4. Data quality is critical

### ⚠️ Might skip if:

1. Source table is **guaranteed unique** (e.g., Gold final snapshot)
2. Performance is critical AND duplicates are impossible
3. UPSERT doesn't care about order

---

## All Serving Layers

| Script | Deduplication | Reason |
|--------|---------------|--------|
| `deal_detail_serving_layer.py` | ❌ No | Reads from Gold (already deduped) |
| `ticket_api_serving_layer.py` | ❓ TBD | Should check |
| `ticket_source_serving_layer.py` | ❓ TBD | Should check |
| `ticket_source_metadata_serving_layer.py` | ✅ **Yes** | Reads from Bronze (might have dups) |

---

## Testing

### Test Case 1: No Duplicates

```python
# Input
rows = [(1, 'A', '2026-01-17', 1)]

# Output (same)
rows = [(1, 'A', '2026-01-17', 1)]
```

### Test Case 2: Duplicates (Different updated_at)

```python
# Input
rows = [
    (1, 'A', '2026-01-15', 1),  # older
    (1, 'B', '2026-01-17', 1),  # newer
]

# Output (keep newer)
rows = [(1, 'B', '2026-01-17', 1)]
```

### Test Case 3: Duplicates (Same updated_at)

```python
# Input
rows = [
    (1, 'A', '2026-01-17', 1),
    (1, 'B', '2026-01-17', 1),
]

# Output (keep first after sort - deterministic)
rows = [(1, 'B', '2026-01-17', 1)]
```

---

## Conclusion

**Always deduplicate when reading from Bronze/Silver before UPSERT to PostgreSQL!**

This ensures:
- ✅ Correct data (MAX updated_at)
- ✅ Faster UPSERT (no conflicts)
- ✅ Data quality guaranteed
