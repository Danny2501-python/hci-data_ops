# Ticket Flow Migration Guide
## Hệ Thống Cũ → Lakehouse Architecture

---

## 📋 Tổng Quan

Tài liệu này mô tả chi tiết cách **tái hiện luồng Ticket** từ hệ thống cũ (PostgreSQL) sang Lakehouse mới (Iceberg).

### Hệ Thống Cũ (Old System)

```
CareSoft API
     │
     ▼
┌─────────────────────────┐
│  cs_ticket_api          │  ← Bảng chính (list tickets)
│  - ticket_id            │
│  - ticket_no            │
│  - ticket_status        │
│  - ticket_source        │
│  - custom_fields (JSON) │
│  - assignee (JSON)      │
│  - requester (JSON)     │
│  - tags (JSON)          │
│  - brand_id             │
│  - raw_data             │
└─────────────────────────┘
     │
     ▼
┌─────────────────────────┐
│ cs_ticket_api_new_day   │  ← Helper table (temp)
│  - ticket_id            │
│  - created_at           │
│  - updated_at           │
│  - brand_id             │
└─────────────────────────┘
     │
     ▼
┌─────────────────────────┐
│ cs_ticket_source_api    │  ← Detail table (from API /tickets/{id})
│  - ticket_id            │
│  - account_id           │
│  - comments (JSON)      │
│  - ticket_source_detail_id │
│  - brand_id             │
│  - raw_data             │
└─────────────────────────┘
```

### Hệ Thống Mới (Lakehouse)

```
CareSoft API
     │
     ▼
┌──────────────────────────────────┐
│  BRONZE: cs_ticket_api           │  ← Raw data (Iceberg) - SAME NAME as old system
│  Partition: snapshot_date        │
│  - ticket_id                     │
│  - ticket_no                     │
│  - ticket_status                 │
│  - ticket_source                 │
│  - brand_id                      │
│  - raw_data                      │
│  - fetch_type (updated/created)  │
└──────────────────────────────────┘
     │
     ▼
┌──────────────────────────────────┐
│  SILVER: cs_ticket_source_api    │  ← Enriched data (Iceberg) - SAME NAME as old system
│  Partition: snapshot_date        │
│  - ticket_id                     │
│  - account_id                    │
│  - comments (JSON)               │
│  - ticket_source_detail_id       │
│  - brand_id                      │
│  - raw_data                      │
└──────────────────────────────────┘
     │
     ▼
┌──────────────────────────────────┐
│  SILVER: cs_ticket_source_error_api │  ← Error tracking (NEW)
│  Partition: snapshot_date        │
│  - ticket_id                     │
│  - brand_id                      │
│  - error_message                 │
└──────────────────────────────────┘
```

---

## 🔄 Flow 2: Ticket Source Detail

### Step 1: Ticket Ingestion (Bronze)

**API Endpoint:** `GET /api/v1/tickets`

**Hệ Thống Cũ:**
```python
# File: cs_extract.py, method: call_tickets()
for i in range(1, 4):
    if i == 1:
        params = {'updated_since': yesterday, 'updated_to': today}
    if i == 2:
        params = {'created_since': yesterday, 'created_to': today}
    if i == 3:
        params = {'created_since': day_before_yesterday, 'created_to': yesterday}
    
    # Pagination
    while record_count != 0:
        response = requests.get(api_url + "tickets", params=params)
        tickets = response.json()['tickets']
        
        # Save to PostgreSQL: cs_ticket_api
        df.to_sql('cs_ticket_api', ...)
```

**Hệ Thống Mới:**
```python
# File: ticket_ingestion.py
# Loop 1: UPDATED tickets (yesterday → today)
tickets_updated = fetch_tickets(brand, 'updated', date_yesterday, date_today)

# Loop 2: CREATED tickets (yesterday → today)
tickets_created = fetch_tickets(brand, 'created', date_yesterday, date_today)

# Loop 3: CREATED tickets (day before yesterday → yesterday)
tickets_created_prev = fetch_tickets(brand, 'created_prev', date_day_before, date_yesterday)

# Save to Iceberg: caresoft.cs_ticket_api
ticket_table.append(df)
```

**Mapping:**

| Hệ Thống Cũ | Hệ Thống Mới | Ghi Chú |
|--------------|--------------|---------|
| `cs_ticket_api` | `caresoft.cs_ticket_api` | Iceberg table, partitioned by `snapshot_date` - SAME NAME |
| `brand_id` | `brand_id` | Same |
| `raw_data` | `raw_data` | Full JSON response |
| N/A | `fetch_type` | NEW: 'updated', 'created', 'created_prev' |
| N/A | `snapshot_date` | NEW: Partition key |

---

### Step 2: Get Ticket IDs to Enrich

**Hệ Thống Cũ:**
```sql
-- Create helper table
DELETE FROM cs_ticket_api_new_day WHERE brand_id = {brand_id};
INSERT INTO cs_ticket_api_new_day 
SELECT ticket_id, created_at, updated_at, brand_id 
FROM cs_ticket_api 
WHERE date(updated_at) >= current_date - interval '10 days' 
  AND brand_id = {brand_id};

-- Get top 300 tickets
SELECT ticket_id FROM cs_ticket_api_new_day 
ORDER BY updated_at DESC 
LIMIT 300;
```

**Hệ Thống Mới:**
```python
# File: ticket_detail_enrichment.py
def get_ticket_ids_to_enrich(brand_erp_id, limit=300):
    # Scan Bronze table (last 10 days)
    scan = bronze_table.scan(
        row_filter=And(
            GreaterThanOrEqual("snapshot_date", cutoff_date),
            EqualTo("brand_id", brand_erp_id),
            NotNull("ticket_id")
        ),
        selected_fields=("ticket_id", "updated_at")
    )
    
    # Use DuckDB to sort and limit
    con.execute("""
        SELECT DISTINCT ticket_id
        FROM bronze_tickets
        ORDER BY updated_at DESC
        LIMIT {limit}
    """)
    
    return ticket_ids
```

**Cải Tiến:**
- ❌ **Không cần** helper table `cs_ticket_api_new_day`
- ✅ **Dùng DuckDB** để sort và limit (hiệu quả hơn)
- ✅ **Scan trực tiếp** từ Iceberg Bronze

---

### Step 3: Call API for Each Ticket

**API Endpoint:** `GET /api/v1/tickets/{ticket_id}`

**Hệ Thống Cũ:**
```python
# (Logic này không có trong cs_extract.py, nhưng suy luận từ schema)
for ticket_id in ticket_ids:
    response = requests.get(f"{api_url}/tickets/{ticket_id}")
    ticket_data = response.json()['ticket']
    
    # Save to PostgreSQL: cs_ticket_source_api
    df.to_sql('cs_ticket_source_api', ...)
```

**Hệ Thống Mới:**
```python
# File: ticket_detail_enrichment.py
for ticket_id in ticket_ids:
    ticket_data, error = call_caresoft_api(brand, ticket_id)
    
    if ticket_data:
        # Save to Iceberg: caresoft.cs_ticket_source_api
        detail_table.append(df)
    else:
        # Save error to: caresoft.cs_ticket_source_error_api
        error_table.append(df_err)
```

**Cải Tiến:**
- ✅ **Error tracking** riêng biệt
- ✅ **Batch insert** (1000 records/batch)
- ✅ **Rate limiting** (0.5s/request)

---

## 📊 Schema Mapping

### Bronze Table: `cs_ticket_api`

| Old Field | New Field | Type | Notes |
|-----------|-----------|------|-------|
| `ticket_id` | `ticket_id` | LongType | Primary key |
| `ticket_no` | `ticket_no` | IntegerType | |
| `ticket_status` | `status` | StringType | |
| `ticket_subject` | `subject` | StringType | |
| `ticket_source` | `ticket_source` | StringType | e.g. "Inbox Facebook" |
| `ticket_priority` | `priority` | StringType | |
| `service_id` | `service_id` | LongType | |
| `requester_id` | `requester_id` | LongType | |
| `assignee_id` | `assignee_id` | LongType | |
| `created_at` | `created_at` | TimestampType | |
| `updated_at` | `updated_at` | TimestampType | |
| `custom_fields` (JSON) | ❌ Removed | - | Too large, not needed in Bronze |
| `assignee` (JSON) | ❌ Removed | - | Too large, not needed in Bronze |
| `requester` (JSON) | ❌ Removed | - | Too large, not needed in Bronze |
| `tags` (JSON) | ❌ Removed | - | Too large, not needed in Bronze |
| `brand_id` | `brand_id` | IntegerType | |
| `raw_data` | `raw_data` | StringType | Full JSON |
| N/A | `snapshot_date` | DateType | **NEW: Partition key** |
| N/A | `ingestion_timestamp` | TimestampType | **NEW: Metadata** |
| N/A | `fetch_type` | StringType | **NEW: 'updated', 'created', 'created_prev'** |

### Silver Table: `cs_ticket_source_api`

| Old Field | New Field | Type | Notes |
|-----------|-----------|------|-------|
| `account_id` | `account_id` | IntegerType | |
| `ticket_id` | `ticket_id` | LongType | Primary key |
| `sla_id` | `sla_id` | IntegerType | |
| `ticket_no` | `ticket_no` | IntegerType | |
| `requester_id` | `requester_id` | LongType | |
| `group_id` | `group_id` | IntegerType | |
| `created_at` | `created_at` | TimestampType | |
| `updated_at` | `updated_at` | TimestampType | |
| `duedate` | `duedate` | StringType | |
| `satisfaction` | `satisfaction` | StringType | |
| `satisfaction_at` | `satisfaction_at` | TimestampType | |
| `satisfaction_send` | `satisfaction_send` | StringType | |
| `satisfaction_content` | `satisfaction_content` | StringType | |
| `automessage_id` | `automessage_id` | StringType | |
| `feedback_status` | `feedback_status` | StringType | |
| `is_overdue` | `is_overdue` | BooleanType | |
| `incident_id` | `incident_id` | IntegerType | |
| `service_id` | `service_id` | LongType | |
| `qa_script_id` | `qa_script_id` | StringType | |
| `qa_agent` | `qa_agent` | StringType | |
| `current_agent` | `current_agent` | StringType | |
| `ticket_source_detail_id` | `ticket_source_detail_id` | IntegerType | **KEY FIELD** |
| `comments` (JSON) | `comments` | StringType | JSON array |
| `brand_id` | `brand_id` | IntegerType | |
| `raw_data` | `raw_data` | StringType | Full JSON |
| N/A | `snapshot_date` | DateType | **NEW: Partition key** |
| N/A | `ingestion_timestamp` | TimestampType | **NEW: Metadata** |

---

## 🔍 API Response Examples

### GET `/tickets` (List Endpoint)

```json
{
    "code": "ok",
    "numFound": 53820,
    "tickets": [
        {
            "ticket_id": 654439211,
            "ticket_no": 3676910,
            "requester_id": 248279329,
            "ticket_source_end_status": 0,
            "assignee_id": 240857594,
            "ticket_priority": "Normal",
            "ticket_source": "Inbox Facebook",
            "ticket_status": "sale",
            "sla_id": 8742,
            "is_overdue": null,
            "ticket_subject": "Chat với khách hàng: តាវឿន យាយហាន",
            "created_at": "2026-01-12T23:58:32Z",
            "updated_at": "2026-01-13T17:13:17Z",
            "duedate": null,
            "service_id": 61075829,
            "incident_id": null,
            "ticket_type": "CHAT",
            "satisfaction": null,
            "satisfaction_at": null,
            "satisfaction_content": null,
            "campaign_id": null,
            "automessage_id": null,
            "custom_fields": [...],  // Array of custom field objects
            "assignee": {...},       // Assignee object
            "requester": {...},      // Requester object
            "tags": [],              // Array of tag objects
            "ccs": [],
            "follows": []
        }
    ]
}
```

**Fields Used in Bronze:**
- ✅ `ticket_id`, `ticket_no`, `requester_id`, `assignee_id`
- ✅ `ticket_priority`, `ticket_source`, `ticket_status`
- ✅ `created_at`, `updated_at`, `service_id`
- ✅ `subject` (from `ticket_subject`)
- ❌ `custom_fields`, `assignee`, `requester`, `tags` → Stored in `raw_data` only

### GET `/tickets/{ticket_id}` (Detail Endpoint)

```json
{
    "ticket": {
        "account_id": 8816,
        "ticket_id": 654439211,
        "sla_id": 8742,
        "ticket_no": 3676910,
        "requester_id": 248279329,
        "group_id": 15915,
        "ticket_source_end_status": 0,
        "assignee_id": 240857594,
        "ticket_priority": "Normal",
        "ticket_source": "Inbox Facebook",
        "ticket_status": "sale",
        "ticket_subject": "Chat với khách hàng: តាវឿន យាយហាន",
        "created_at": "2026-01-12 23:58:32",
        "updated_at": "2026-01-13 17:13:17",
        "duedate": null,
        "satisfaction": null,
        "satisfaction_at": null,
        "satisfaction_send": null,
        "satisfaction_content": null,
        "feedback_status": "CHAT",
        "is_overdue": null,
        "incident_id": null,
        "service_id": 61075829,
        "qa_script_id": null,
        "qa_agent": null,
        "current_agent": null,
        "ticket_source_detail_id": 7054738,  // ← KEY FIELD (only in detail API)
        "comments": [...]  // ← Array of comment objects (only in detail API)
    }
}
```

**Fields Only in Detail API:**
- ✅ `ticket_source_detail_id` → **ID của nguồn ticket cụ thể** (e.g. Facebook Page ID)
- ✅ `comments` → **Lịch sử tương tác** (chat messages, status changes)
- ✅ `account_id` → **CareSoft account ID**

---

## 🚀 Migration Checklist

### ✅ Đã Hoàn Thành

- [x] **Bronze Table**: `cs_ticket_api` (Iceberg) - SAME NAME as old system
- [x] **Silver Table**: `cs_ticket_source_api` (Iceberg)
- [x] **Error Table**: `cs_ticket_source_error_api` (Iceberg)
- [x] **Ingestion Script**: `ticket_ingestion.py`
- [x] **Enrichment Script**: `ticket_detail_enrichment.py`
- [x] **3-loop logic**: Updated, Created, Created_prev
- [x] **DuckDB optimization**: Sort and limit without helper table
- [x] **Error tracking**: Separate error table
- [x] **Batch processing**: 1000 records/batch
- [x] **Rate limiting**: 0.5s/request

### ⏳ Cần Làm Tiếp

- [ ] **Add to Airflow DAG**: Create tasks for ticket ingestion and enrichment
- [ ] **Test with real data**: Run for all brands
- [ ] **Monitor performance**: Check memory usage and execution time
- [ ] **Data validation**: Compare old vs new system results
- [ ] **Documentation**: Update README with ticket flow

---

## 📝 Airflow DAG Integration

### Task Structure

```python
# Task 1: Ticket Ingestion (Bronze)
task_ticket_ingestion = KubernetesPodOperator(
    task_id='ticket_ingestion',
    cmds=['python'],
    arguments=['/app/caresoft/ticket_ingestion.py', '{{ ds }}'],
    env_vars={
        **common_env_vars,
        'CARESOFT_API_URL': 'https://api.caresoft.vn',
        'CARESOFT_PAGE_SIZE': '100',
    },
)

# Task 2: Ticket Detail Enrichment (Silver)
task_ticket_detail_enrichment = KubernetesPodOperator(
    task_id='ticket_detail_enrichment',
    cmds=['python'],
    arguments=['/app/caresoft/ticket_detail_enrichment.py', '{{ ds }}'],
    env_vars={
        **common_env_vars,
        'CARESOFT_API_URL': 'https://api.caresoft.vn',
    },
)

# Dependencies
task_ticket_ingestion >> task_ticket_detail_enrichment
```

---

## 🔧 Testing Commands

### Test Ticket Ingestion

```bash
export ICEBERG_REST_URI_BRONZE="http://iceberg-rest-bronze-zone:8181"
export CATALOG_NAME_BRONZE="bronze"
export S3_ENDPOINT="http://10.152.183.65:9000"
export AWS_ACCESS_KEY_ID="admin"
export AWS_SECRET_ACCESS_KEY="password"
export AWS_REGION="us-east-1"

python ticket_ingestion.py 2026-01-15
```

### Test Ticket Detail Enrichment

```bash
export ICEBERG_REST_URI_BRONZE="http://iceberg-rest-bronze-zone:8181"
export ICEBERG_REST_URI_SILVER="http://iceberg-rest-silver-zone:8181"
export CATALOG_NAME_BRONZE="bronze"
export CATALOG_NAME_SILVER="silver"
export S3_ENDPOINT="http://10.152.183.65:9000"
export AWS_ACCESS_KEY_ID="admin"
export AWS_SECRET_ACCESS_KEY="password"
export AWS_REGION="us-east-1"

python ticket_detail_enrichment.py 2026-01-15
```

### Query Bronze Table

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog('bronze', uri='http://iceberg-rest-bronze-zone:8181')
table = catalog.load_table('caresoft.cs_ticket_api')

# Count tickets by brand
df = table.scan().to_pandas()
print(df.groupby('brand_id')['ticket_id'].count())
```

### Query Silver Table

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog('silver', uri='http://iceberg-rest-silver-zone:8181')
table = catalog.load_table('caresoft.cs_ticket_source_api')

# Count enriched tickets
df = table.scan().to_pandas()
print(f"Total enriched tickets: {len(df)}")
```

---

## 📌 Key Differences: Old vs New

| Aspect | Old System | New System |
|--------|------------|------------|
| **Storage** | PostgreSQL | Iceberg (S3) |
| **Helper Table** | `cs_ticket_api_new_day` | ❌ Not needed |
| **Sorting/Limiting** | SQL on PostgreSQL | DuckDB in-memory |
| **Error Tracking** | ❌ No | ✅ Separate error table |
| **Partitioning** | ❌ No | ✅ By `snapshot_date` |
| **Time-Travel** | ❌ No | ✅ Iceberg supports |
| **Scalability** | Limited by PostgreSQL | Unlimited (S3) |
| **Data Lineage** | ❌ No | ✅ `fetch_type` metadata |

---

## 🎯 Summary

Luồng Ticket đã được **tái hiện thành công** trong Lakehouse với các cải tiến:

1. **Bronze Layer**: Lưu raw tickets từ API `/tickets` (3 loops)
2. **Silver Layer**: Enriched tickets từ API `/tickets/{id}` (top 300 mỗi brand)
3. **Error Tracking**: Riêng biệt cho failed API calls
4. **No Helper Tables**: Dùng DuckDB thay vì temp tables
5. **Partitioning**: Theo `snapshot_date` cho time-travel
6. **Metadata**: `fetch_type` để track nguồn gốc data

**Next Steps:**
- Add to Airflow DAG
- Test with production data
- Monitor and optimize
