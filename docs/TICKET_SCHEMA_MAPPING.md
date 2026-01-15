# Schema Mapping: Old PostgreSQL → New Iceberg
## cs_ticket_api Table

---

## ✅ Complete Field Mapping

### Old PostgreSQL Table: `cs_ticket_api` (29 fields)

| # | Old Field | New Iceberg Field | Type | Status | Notes |
|---|-----------|-------------------|------|--------|-------|
| 1 | `incident_id` | `incident_id` | LongType | ✅ | Mapped |
| 2 | `ticket_no` | `ticket_no` | IntegerType | ✅ | Mapped |
| 3 | `ticket_status` | `ticket_status` | StringType | ✅ | Mapped |
| 4 | `ticket_subject` | `ticket_subject` | StringType | ✅ | Mapped |
| 5 | `created_at` | `created_at` | TimestampType | ✅ | Mapped |
| 6 | `updated_at` | `updated_at` | TimestampType | ✅ | Mapped |
| 7 | `ticket_source_end_status` | `ticket_source_end_status` | StringType | ✅ | Mapped |
| 8 | `ticket_source` | `ticket_source` | StringType | ✅ | Mapped |
| 9 | `ticket_priority` | `ticket_priority` | StringType | ✅ | Mapped |
| 10 | `service_id` | `service_id` | LongType | ✅ | Mapped |
| 11 | `ticket_id` | `ticket_id` | LongType | ✅ | Mapped |
| 12 | `requester_id` | `requester_id` | LongType | ✅ | Mapped |
| 13 | `assignee` | `assignee` | StringType (JSON) | ✅ | Mapped as JSON string |
| 14 | `assignee_id` | `assignee_id` | LongType | ✅ | Mapped |
| 15 | `campaign_id` | `campaign_id` | StringType | ✅ | Mapped |
| 16 | `campaign` | `campaign` | StringType | ✅ | Mapped |
| 17 | `duedate` | `duedate` | StringType | ✅ | Mapped |
| 18 | `custom_fields` | `custom_fields` | StringType (JSON) | ✅ | Mapped as JSON string |
| 19 | `requester` | `requester` | StringType (JSON) | ✅ | Mapped as JSON string |
| 20 | `tags` | `tags` | StringType (JSON) | ✅ | Mapped as JSON string |
| 21 | `brand_id` | `brand_id` | IntegerType | ✅ | Mapped |
| 22 | `raw_data` | `raw_data` | StringType | ✅ | Mapped |
| 23 | `ghi_chu` | `ghi_chu` | StringType | ✅ | Mapped |
| 24 | `id_value_phan_loai_ket_noi` | `id_value_phan_loai_ket_noi` | StringType | ✅ | Mapped |
| 25 | `id_value_muc_do_hai_long` | `id_value_muc_do_hai_long` | StringType | ✅ | Mapped |
| 26 | `ad_id_value` | `ad_id_value` | StringType | ✅ | Mapped |
| 27 | `id_value_dich_vu_chi_tiet` | `id_value_dich_vu_chi_tiet` | StringType | ✅ | Mapped |
| - | N/A | `snapshot_date` | DateType | ✨ **NEW** | Partition key |
| - | N/A | `ingestion_timestamp` | TimestampType | ✨ **NEW** | Metadata |
| - | N/A | `fetch_type` | StringType | ✨ **NEW** | Metadata |

---

## 📊 Summary

### ✅ Coverage: 100%

- **Old Fields**: 27 fields (all mapped)
- **New Fields**: 27 + 3 metadata = **30 fields total**
- **Missing**: 0 fields
- **Extra**: 3 metadata fields (snapshot_date, ingestion_timestamp, fetch_type)

### 🎯 Key Points

1. **Backward Compatible**: Tất cả fields từ hệ thống cũ đều có trong Iceberg
2. **JSON Fields**: `custom_fields`, `assignee`, `requester`, `tags` được lưu dưới dạng JSON string (giống hệ thống cũ)
3. **Custom Fields**: Các fields custom như `ghi_chu`, `id_value_*`, `ad_id_value` đều được giữ nguyên
4. **Metadata**: Thêm 3 fields mới cho Lakehouse (snapshot_date, ingestion_timestamp, fetch_type)

---

## 🔄 Data Transformation

### API Response → Iceberg Record

```python
# API returns:
{
    "ticket_id": 654439211,
    "ticket_no": 3676910,
    "ticket_status": "sale",
    "ticket_subject": "Chat với khách hàng",
    "ticket_priority": "Normal",
    "ticket_source": "Inbox Facebook",
    "ticket_source_end_status": 0,
    "created_at": "2026-01-12T23:58:32Z",
    "updated_at": "2026-01-13T17:13:17Z",
    "requester_id": 248279329,
    "assignee_id": 240857594,
    "service_id": 61075829,
    "incident_id": null,
    "campaign_id": null,
    "campaign": null,
    "duedate": null,
    "custom_fields": [...],  // Array
    "assignee": {...},       // Object
    "requester": {...},      // Object
    "tags": [...],           // Array
}

# Transformed to Iceberg:
{
    "ticket_id": 654439211,
    "ticket_no": 3676910,
    "ticket_status": "sale",
    "ticket_subject": "Chat với khách hàng",
    "ticket_priority": "Normal",
    "ticket_source": "Inbox Facebook",
    "ticket_source_end_status": "0",  // Converted to string
    "created_at": datetime(2026, 1, 12, 23, 58, 32),
    "updated_at": datetime(2026, 1, 13, 17, 13, 17),
    "requester_id": 248279329,
    "assignee_id": 240857594,
    "service_id": 61075829,
    "incident_id": null,
    "campaign_id": null,
    "campaign": null,
    "duedate": null,
    "custom_fields": "[{...}]",  // JSON string
    "assignee": "{...}",          // JSON string
    "requester": "{...}",         // JSON string
    "tags": "[...]",              // JSON string
    "brand_id": 3,
    "raw_data": "{...}",          // Full JSON
    "ghi_chu": null,
    "id_value_phan_loai_ket_noi": null,
    "id_value_muc_do_hai_long": null,
    "ad_id_value": null,
    "id_value_dich_vu_chi_tiet": null,
    "snapshot_date": date(2026, 1, 15),
    "ingestion_timestamp": datetime.now(),
    "fetch_type": "updated",
}
```

---

## 📝 Iceberg Schema Definition

```python
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, IntegerType, StringType, TimestampType, 
    DateType, LongType
)

schema = Schema(
    # Core IDs
    NestedField(1, "ticket_id", LongType(), required=False),
    NestedField(2, "ticket_no", IntegerType(), required=False),
    NestedField(3, "requester_id", LongType(), required=False),
    NestedField(4, "assignee_id", LongType(), required=False),
    NestedField(5, "incident_id", LongType(), required=False),
    NestedField(6, "service_id", LongType(), required=False),
    NestedField(7, "campaign_id", StringType(), required=False),
    
    # Status and classification
    NestedField(10, "ticket_status", StringType(), required=False),
    NestedField(11, "ticket_priority", StringType(), required=False),
    NestedField(12, "ticket_source", StringType(), required=False),
    NestedField(13, "ticket_source_end_status", StringType(), required=False),
    
    # Text fields
    NestedField(20, "ticket_subject", StringType(), required=False),
    NestedField(21, "campaign", StringType(), required=False),
    NestedField(22, "duedate", StringType(), required=False),
    
    # Timestamps
    NestedField(30, "created_at", TimestampType(), required=False),
    NestedField(31, "updated_at", TimestampType(), required=False),
    
    # JSON fields (stored as text)
    NestedField(40, "custom_fields", StringType(), required=False),
    NestedField(41, "assignee", StringType(), required=False),
    NestedField(42, "requester", StringType(), required=False),
    NestedField(43, "tags", StringType(), required=False),
    
    # Brand and metadata
    NestedField(50, "brand_id", IntegerType(), required=False),
    NestedField(51, "raw_data", StringType(), required=False),
    NestedField(52, "ghi_chu", StringType(), required=False),
    
    # Custom extracted fields
    NestedField(60, "id_value_phan_loai_ket_noi", StringType(), required=False),
    NestedField(61, "id_value_muc_do_hai_long", StringType(), required=False),
    NestedField(62, "ad_id_value", StringType(), required=False),
    NestedField(63, "id_value_dich_vu_chi_tiet", StringType(), required=False),
    
    # NEW: Metadata for Lakehouse
    NestedField(100, "snapshot_date", DateType(), required=False),
    NestedField(101, "ingestion_timestamp", TimestampType(), required=False),
    NestedField(102, "fetch_type", StringType(), required=False),
)
```

---

## 🎯 Migration Validation

### SQL Query to Compare

```sql
-- Old PostgreSQL
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT ticket_id) as unique_tickets,
    COUNT(DISTINCT brand_id) as unique_brands,
    MIN(created_at) as earliest_ticket,
    MAX(created_at) as latest_ticket
FROM cs_ticket_api
WHERE brand_id = 3;

-- New Iceberg (via DuckDB/Trino)
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT ticket_id) as unique_tickets,
    COUNT(DISTINCT brand_id) as unique_brands,
    MIN(created_at) as earliest_ticket,
    MAX(created_at) as latest_ticket
FROM caresoft.cs_ticket_api
WHERE brand_id = 3;
```

### Field-by-Field Validation

```python
# Compare specific ticket
old_ticket = pg_cursor.execute(
    "SELECT * FROM cs_ticket_api WHERE ticket_id = 654439211"
).fetchone()

new_ticket = iceberg_table.scan(
    row_filter=EqualTo("ticket_id", 654439211)
).to_pandas().iloc[0]

# Validate all fields match
assert old_ticket['ticket_id'] == new_ticket['ticket_id']
assert old_ticket['ticket_status'] == new_ticket['ticket_status']
assert old_ticket['ticket_subject'] == new_ticket['ticket_subject']
# ... etc for all fields
```

---

## ✅ Conclusion

**Schema Migration: COMPLETE**

- ✅ All 27 fields from old `cs_ticket_api` table are mapped
- ✅ JSON fields preserved as strings (matching old system)
- ✅ Custom fields (`ghi_chu`, `id_value_*`) preserved
- ✅ 3 new metadata fields added for Lakehouse
- ✅ Backward compatible with old queries (field names match)
- ✅ Ready for production migration
