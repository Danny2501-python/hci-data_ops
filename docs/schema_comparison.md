# Schema Comparison: Legacy vs New Lakehouse

## core_brand

### ✅ Field Mapping (9 fields)

| # | Legacy Field | PostgreSQL Type | Iceberg Field | Iceberg Type | Match |
|---|-------------|-----------------|---------------|--------------|-------|
| 1 | `id` | `int8` (IDENTITY) | `id` | LongType | ✅ |
| 2 | `name` | `varchar(255)` | `name` | StringType | ✅ |
| 3 | `erp_id` | `int4` (UNIQUE) | `erp_id` | IntegerType | ✅ |
| 4 | `etl_job_name` | `varchar(255)` | `etl_job_name` | StringType | ✅ |
| 5 | `is_active` | `bool` | `is_active` | BooleanType | ✅ |
| 6 | `created` | `timestamptz` | `created` | TimestampType | ✅ |
| 7 | `updated` | `timestamptz` | `updated` | TimestampType | ✅ |
| 8 | `caresoft_domain` | `varchar(255)` | `caresoft_domain` | StringType | ✅ |
| 9 | `caresoft_key` | `varchar(50)` | `caresoft_key` | StringType | ✅ |

**Notes:**
- No partitioning (small reference table)
- IDENTITY and UNIQUE constraints not enforced in Iceberg (application-level)
- All fields preserved with exact same names

---


## cs_deal_detail_api → cs_deal_detail

### ✅ Data Type Mapping

| PostgreSQL Type | Iceberg Type | Notes |
|----------------|--------------|-------|
| `numeric(20)` | `LongType()` | For large IDs (id, requester_id, assignee_id, service_id, convert_by) |
| `int4` | `IntegerType()` | Standard integers |
| `text` | `StringType()` | All text fields |
| `timestamp` | `TimestampType()` | Timestamps without timezone |

### 📋 Field Mapping (56 fields total)

| # | Legacy Field | Iceberg Field | Type | Match |
|---|-------------|---------------|------|-------|
| 1 | `id` (numeric(20)) | `id` | LongType | ✅ |
| 2 | `account_id` | `account_id` | IntegerType | ✅ |
| 3 | `deal_no` | `deal_no` | IntegerType | ✅ |
| 4 | `requester_id` (numeric(20)) | `requester_id` | LongType | ✅ |
| 5 | `group_id` | `group_id` | IntegerType | ✅ |
| 6 | `ticket_source_end_status` | `ticket_source_end_status` | IntegerType | ✅ |
| 7 | `assignee_id` (numeric(20)) | `assignee_id` | LongType | ✅ |
| 8 | `ticket_source` | `ticket_source` | StringType | ✅ |
| 9 | `merge_status` | `merge_status` | StringType | ✅ |
| 10 | `merge_to` | `merge_to` | StringType | ✅ |
| 11 | `clone_from` | `clone_from` | StringType | ✅ |
| 12 | `subject` | `subject` | StringType | ✅ |
| 13 | `created_at` | `created_at` | TimestampType | ✅ |
| 14 | `updated_at` | `updated_at` | TimestampType | ✅ |
| 15 | `satisfaction_at` | `satisfaction_at` | TimestampType | ✅ |
| 16 | `convert_at` | `convert_at` | TimestampType | ✅ |
| 17 | `estimated_closed_date` | `estimated_closed_date` | TimestampType | ✅ |
| 18 | `closed_at` | `closed_at` | TimestampType | ✅ |
| 19 | `duedate` | `duedate` | StringType | ✅ |
| 20 | `satisfaction` | `satisfaction` | StringType | ✅ |
| 21 | `satisfaction_content` | `satisfaction_content` | StringType | ✅ |
| 22 | `satisfaction_send` | `satisfaction_send` | StringType | ✅ |
| 23 | `campaign_id` | `campaign_id` | StringType | ✅ |
| 24 | `campaign_action_id` | `campaign_action_id` | StringType | ✅ |
| 25 | `campaign_status` | `campaign_status` | StringType | ✅ |
| 26 | `automessage_id` | `automessage_id` | StringType | ✅ |
| 27 | `manualmessage_id` | `manualmessage_id` | StringType | ✅ |
| 28 | `incident_id` | `incident_id` | IntegerType | ✅ |
| 29 | `service_id` (numeric(20)) | `service_id` | LongType | ✅ |
| 30 | `qa_script_id` | `qa_script_id` | StringType | ✅ |
| 31 | `qa_agent` | `qa_agent` | StringType | ✅ |
| 32 | `current_agent` | `current_agent` | StringType | ✅ |
| 33 | `ticket_source_detail_id` | `ticket_source_detail_id` | IntegerType | ✅ |
| 34 | `convert_by` (numeric(20)) | `convert_by` | LongType | ✅ |
| 35 | `convert_type` | `convert_type` | IntegerType | ✅ |
| 36 | `unqualified_reasons` | `unqualified_reasons` | StringType | ✅ |
| 37 | `last_lead_status_id` | `last_lead_status_id` | IntegerType | ✅ |
| 38 | `lead_status_id` | `lead_status_id` | IntegerType | ✅ |
| 39 | `closed_by` | `closed_by` | StringType | ✅ |
| 40 | `closed_type` | `closed_type` | StringType | ✅ |
| 41 | `pipeline_id` | `pipeline_id` | IntegerType | ✅ |
| 42 | `pipeline_stage_id` | `pipeline_stage_id` | IntegerType | ✅ |
| 43 | `value` | `value` | IntegerType | ✅ |
| 44 | `probability` | `probability` | IntegerType | ✅ |
| 45 | `last_deal_stage_id` | `last_deal_stage_id` | IntegerType | ✅ |
| 46 | `lost_reasons` | `lost_reasons` | StringType | ✅ |
| 47 | `comments` | `comments` | StringType | ✅ |
| 48 | `custom_fields` | `custom_fields` | StringType | ✅ |
| 49 | `tags` | `tags` | StringType | ✅ |
| 50 | `ccs` | `ccs` | StringType | ✅ |
| 51 | `follows` | `follows` | StringType | ✅ |
| 52 | `labels` | `labels` | StringType | ✅ |
| 53 | `assignee` | `assignee` | StringType | ✅ |
| 54 | `requester` | `requester` | StringType | ✅ |
| 55 | `brand_id` | `brand_id` | IntegerType | ✅ |
| 56 | `raw_data` | `raw_data` | StringType | ✅ |
| 100 | N/A (new) | `snapshot_date` | DateType | ➕ Partition key |
| 101 | N/A (new) | `ingestion_timestamp` | TimestampType | ➕ Metadata |

## cs_deal_detail_error_api → cs_deal_detail_error

| # | Legacy Field | Iceberg Field | Type | Match |
|---|-------------|---------------|------|-------|
| 1 | `id` (numeric(20)) | `id` | LongType | ✅ |
| 100 | N/A (new) | `snapshot_date` | DateType | ➕ Partition key |
| 101 | N/A (new) | `ingestion_timestamp` | TimestampType | ➕ Metadata |

## Summary

✅ **100% field coverage** - All legacy fields mapped  
✅ **Correct data types** - numeric(20) → LongType, int4 → IntegerType  
✅ **Partitioning added** - snapshot_date for efficient queries  
✅ **Metadata tracking** - ingestion_timestamp for audit trail  

## Notes

1. **LongType for large IDs**: PostgreSQL `numeric(20)` mapped to Iceberg `LongType()` to handle large ID values
2. **Partitioning**: Added `snapshot_date` partition for query performance
3. **Metadata**: Added `ingestion_timestamp` for data lineage tracking
4. **Backward compatible**: All legacy fields preserved with same names
