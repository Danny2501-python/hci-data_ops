# ✅ Serving Layer Pattern - CORRECTED

## Pattern Summary

All serving layer scripts follow the **SAME pattern**:

```python
# ✅ CORRECT PATTERN
def get_data_for_date(self, snapshot_date: date):
    """Read ENTIRE partition for snapshot_date (ALL brands)"""
    scan = table.scan(row_filter=EqualTo("snapshot_date", snapshot_date_str))
    # Returns ALL brands in one read
    
def upsert_serving_table(self, columns, rows):
    """UPSERT all data in ONE batch operation"""
    execute_values(cursor, upsert_sql, rows)
    # Single UPSERT for all brands

def run(self):
    """Main execution - NO brand loop"""
    columns, rows = self.get_data_for_date(self.snapshot_date)
    self.upsert_serving_table(columns, rows)
```

---

## Why This Pattern?

### ✅ Benefits:

1. **Performance**: Single partition scan (fast)
2. **Simplicity**: No brand loop needed
3. **Atomicity**: One transaction for all brands
4. **Consistency**: All brands updated together

### ❌ Old Pattern (WRONG):

```python
# ❌ WRONG - Don't do this
for brand in brands:
    records = get_data_for_brand(brand_id)  # Multiple scans
    upsert(records)  # Multiple transactions
```

**Problems:**
- Multiple partition scans (slow)
- Multiple transactions (not atomic)
- More complex code
- Higher memory usage

---

## All Serving Layer Scripts

| Script | Source | Target | Pattern |
|--------|--------|--------|---------|
| `deal_detail_serving_layer.py` | Gold → cs_deal_detail_api | PostgreSQL | ✅ Single scan |
| `ticket_api_serving_layer.py` | Bronze → cs_ticket_api | PostgreSQL | ✅ Single scan |
| `ticket_source_serving_layer.py` | Silver → cs_ticket_source_api | PostgreSQL | ✅ Single scan |
| `ticket_source_metadata_serving_layer.py` | Bronze → cs_ticket_source_metadata | PostgreSQL | ✅ Single scan |

---

## Code Comparison

### ✅ CORRECT (Current):

```python
class TicketSourceMetadataServingLayer:
    def get_ticket_sources_for_date(self, snapshot_date: date):
        # Read ENTIRE partition (all brands)
        scan = table.scan(row_filter=EqualTo("snapshot_date", snapshot_date_str))
        result = scan.to_arrow()
        return columns, rows  # All brands
    
    def run(self):
        # Single read, single UPSERT
        columns, rows = self.get_ticket_sources_for_date(self.snapshot_date)
        self.upsert_serving_table(columns, rows)
```

### ❌ WRONG (Previous version):

```python
class TicketSourceMetadataServingLayer:
    def get_ticket_sources_from_iceberg(self, brand_id: int):
        # Read ONE brand at a time
        scan = table.scan(
            row_filter=And(
                EqualTo("snapshot_date", self.snapshot_date),
                EqualTo("brand_id", brand_id)  # ❌ Filter by brand
            )
        )
        return records
    
    def run(self):
        # ❌ Loop through brands
        for brand in brands:
            records = self.get_ticket_sources_from_iceberg(brand["erp_id"])
            self.upsert_to_postgres(conn, records)
```

---

## Performance Comparison

| Metric | Wrong Pattern | Correct Pattern | Improvement |
|--------|---------------|-----------------|-------------|
| **Partition Scans** | N (# of brands) | 1 | N times faster |
| **Transactions** | N | 1 | N times faster |
| **Memory** | Low per iteration | Higher peak | More efficient |
| **Code Complexity** | High | Low | Simpler |

**Example with 5 brands:**
- Wrong: 5 scans + 5 transactions
- Correct: 1 scan + 1 transaction

---

## Key Takeaways

1. ✅ **Always read entire partition** (all brands)
2. ✅ **Single UPSERT operation** for all data
3. ✅ **No brand loop** in serving layer
4. ✅ **PostgreSQL handles deduplication** via Primary Key
5. ✅ **Iceberg partition scan is fast** (metadata-based)

---

## Reference

See these files for correct implementation:
- `app/caresoft/deal/deal_detail_serving_layer.py` (reference implementation)
- `app/caresoft/ticket/ticket_source_metadata_serving_layer.py` (corrected)
