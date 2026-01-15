# Schema Field Order Reference
## cs_ticket_api - PostgreSQL DDL Order

---

## 📋 Field Order Mapping

### PostgreSQL DDL → Iceberg Schema

| Field ID | PostgreSQL Column | Iceberg Field | Type | Notes |
|----------|-------------------|---------------|------|-------|
| **1** | `incident_id` | `incident_id` | LongType | ✅ |
| **2** | `ticket_no` | `ticket_no` | IntegerType | ✅ |
| **3** | `ticket_status` | `ticket_status` | StringType | ✅ |
| **4** | `ticket_subject` | `ticket_subject` | StringType | ✅ |
| **5** | `created_at` | `created_at` | TimestampType | ✅ |
| **6** | `updated_at` | `updated_at` | TimestampType | ✅ |
| **7** | `ticket_source_end_status` | `ticket_source_end_status` | StringType | ✅ |
| **8** | `ticket_source` | `ticket_source` | StringType | ✅ |
| **9** | `ticket_priority` | `ticket_priority` | StringType | ✅ |
| **10** | `service_id` | `service_id` | LongType | ✅ |
| **11** | `ticket_id` | `ticket_id` | LongType | ✅ Primary Key |
| **12** | `requester_id` | `requester_id` | LongType | ✅ |
| **13** | `assignee` | `assignee` | StringType | ✅ JSON object |
| **14** | `assignee_id` | `assignee_id` | LongType | ✅ |
| **15** | `campaign_id` | `campaign_id` | StringType | ✅ |
| **16** | `campaign` | `campaign` | StringType | ✅ |
| **17** | `duedate` | `duedate` | StringType | ✅ |
| **18** | `custom_fields` | `custom_fields` | StringType | ✅ JSON array |
| **19** | `requester` | `requester` | StringType | ✅ JSON object |
| **20** | `tags` | `tags` | StringType | ✅ JSON array |
| **21** | `brand_id` | `brand_id` | IntegerType | ✅ |
| **22** | `raw_data` | `raw_data` | StringType | ✅ Full JSON |
| **23** | `ghi_chu` | `ghi_chu` | StringType | ✅ |
| **24** | `id_value_phan_loai_ket_noi` | `id_value_phan_loai_ket_noi` | StringType | ✅ |
| **25** | `id_value_muc_do_hai_long` | `id_value_muc_do_hai_long` | StringType | ✅ |
| **26** | `ad_id_value` | `ad_id_value` | StringType | ✅ |
| **27** | `id_value_dich_vu_chi_tiet` | `id_value_dich_vu_chi_tiet` | StringType | ✅ |
| **28** | N/A (NEW) | `snapshot_date` | DateType | ✨ Partition key |
| **29** | N/A (NEW) | `ingestion_timestamp` | TimestampType | ✨ Metadata |
| **30** | N/A (NEW) | `fetch_type` | StringType | ✨ Metadata |

---

## ✅ Verification

### Sequential Field IDs: 1-30

```python
# Field IDs are sequential and match DDL order
NestedField(1, "incident_id", ...),        # ← DDL line 1
NestedField(2, "ticket_no", ...),          # ← DDL line 2
NestedField(3, "ticket_status", ...),      # ← DDL line 3
NestedField(4, "ticket_subject", ...),     # ← DDL line 4
# ... continues sequentially ...
NestedField(27, "id_value_dich_vu_chi_tiet", ...),  # ← DDL line 27
NestedField(28, "snapshot_date", ...),     # ← NEW (Lakehouse metadata)
NestedField(29, "ingestion_timestamp", ...), # ← NEW (Lakehouse metadata)
NestedField(30, "fetch_type", ...),        # ← NEW (Lakehouse metadata)
```

### Partition Spec

```python
partition_spec = PartitionSpec(
    PartitionField(
        source_id=28,  # ← Field ID of snapshot_date
        field_id=1000,
        transform=IdentityTransform(),
        name="snapshot_date"
    )
)
```

---

## 📝 PostgreSQL DDL Reference

```sql
CREATE TABLE public.cs_ticket_api (
    incident_id text NULL,                    -- Field 1
    ticket_no numeric NULL,                   -- Field 2
    ticket_status text NULL,                  -- Field 3
    ticket_subject text NULL,                 -- Field 4
    created_at timestamp NULL,                -- Field 5
    updated_at timestamp NULL,                -- Field 6
    ticket_source_end_status text NULL,       -- Field 7
    ticket_source text NULL,                  -- Field 8
    ticket_priority text NULL,                -- Field 9
    service_id numeric NULL,                  -- Field 10
    ticket_id numeric NULL,                   -- Field 11
    requester_id numeric NULL,                -- Field 12
    assignee text NULL,                       -- Field 13
    assignee_id text NULL,                    -- Field 14
    campaign_id text NULL,                    -- Field 15
    campaign text NULL,                       -- Field 16
    duedate text NULL,                        -- Field 17
    custom_fields text NULL,                  -- Field 18
    requester text NULL,                      -- Field 19
    tags text NULL,                           -- Field 20
    brand_id int4 NULL,                       -- Field 21
    raw_data text NULL,                       -- Field 22
    ghi_chu text NULL,                        -- Field 23
    id_value_phan_loai_ket_noi text NULL,    -- Field 24
    id_value_muc_do_hai_long text NULL,      -- Field 25
    ad_id_value text NULL,                    -- Field 26
    id_value_dich_vu_chi_tiet text NULL      -- Field 27
);
```

---

## 🎯 Benefits of Sequential Ordering

1. **✅ Easy to Compare**: Field IDs match DDL line numbers
2. **✅ Maintainable**: Clear 1:1 mapping with old schema
3. **✅ Predictable**: No gaps in numbering (1-30 sequential)
4. **✅ Migration-Friendly**: Easy to validate field-by-field

---

## 🔍 Quick Lookup

### By Field ID
- **1-10**: Core identifiers and status
- **11**: `ticket_id` (Primary Key)
- **12-20**: Additional identifiers and JSON fields
- **21-27**: Brand and custom fields
- **28-30**: Lakehouse metadata (NEW)

### By Category
- **IDs**: 1, 10, 11, 12, 14, 15, 21
- **Status**: 3, 7, 8, 9
- **Text**: 4, 16, 17, 23
- **Timestamps**: 5, 6, 29
- **JSON**: 13, 18, 19, 20
- **Raw Data**: 22
- **Custom**: 24, 25, 26, 27
- **Metadata**: 28, 29, 30
