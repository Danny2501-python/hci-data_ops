# Contact Flow Summary
## CareSoft Contact Pipeline Architecture

---

## 📊 Overview

Contact flow follows **the same pattern** as Deal and Ticket flows:
- Loop through brands from `core_brand` table
- Use `caresoft_domain` + `caresoft_key` for API authentication
- Bronze → Silver → Serving layer architecture

---

## 🔄 Architecture

### Contact Flow (Flow 3)
```
┌─────────────────────────────────────────────────────────────┐
│  core_brand (Gold)                                          │
│  - caresoft_domain: "kangnam2", "sci", etc.                 │
│  - caresoft_key: API key                                    │
└─────────────────────────────────────────────────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │  get_active_brands()  │
        └───────────────────────┘
                    │
        ┌───────────┴───────────┐
        │                       │
        ▼                       ▼
┌───────────────────┐   ┌───────────────────────┐
│ Contact           │   │ Contact Detail        │
│ Ingestion         │   │ Enrichment            │
│ (Bronze)          │   │ (Bronze → Silver)     │
│                   │   │                       │
│ Loop all brands   │   │ Loop all brands       │
│ ↓                 │   │ ↓                     │
│ Write to:         │   │ Call API:             │
│ cs_contacts_api   │   │ /contacts/{id}        │
└───────────────────┘   └───────────────────────┘
        │                       │
        │                       ▼
        │               ┌───────────────────────┐
        │               │ Contact Detail        │
        │               │ Serving Layer         │
        │               │ (Silver → PostgreSQL) │
        │               │                       │
        │               │ UPSERT to:            │
        │               │ cs_contact_api_v1     │
        │               └───────────────────────┘
        │                       │
        └───────────┬───────────┘
                    ▼
            ┌───────────────┐
            │  PostgreSQL   │
            │  DWH          │
            └───────────────┘
```

---

## 🎯 Key Similarities with Deal & Ticket Flows

| Aspect | Deal Flow | Ticket Flow | Contact Flow |
|--------|-----------|-------------|--------------|
| **Brand Loading** | `get_active_brands()` | `get_active_brands()` | `get_active_brands()` ✅ |
| **Loop Pattern** | `for brand in brands:` | `for brand in brands:` | `for brand in brands:` ✅ |
| **API URL** | `/deal/{id}` | `/tickets/{id}` | `/contacts/{id}` ✅ |
| **API Key** | `brand["caresoft_key"]` | `brand["caresoft_key"]` | `brand["caresoft_key"]` ✅ |
| **Subdomain** | `brand["caresoft_domain"]` | `brand["caresoft_domain"]` | `brand["caresoft_domain"]` ✅ |
| **Error Tracking** | Separate error table | Separate error table | Separate error table ✅ |
| **Batch Processing** | 1000 records/batch | 1000 records/batch | 1000 records/batch ✅ |

---

## 📝 Pipeline Steps

### Step 1: Contact Ingestion (Bronze Layer)

**File:** `contact_ingestion.py`

**Purpose:** Pull contact list from CareSoft API

**OLD SYSTEM:**
- Function: `call_cs_contact_ttc()`
- Table: `cs_contacts_API_TTC` (PostgreSQL)
- Temp table: `cs_contacts_API_TTC_test_YYYYMMDD`

**NEW SYSTEM:**
- Table: `caresoft.cs_contacts_api` (Iceberg Bronze)
- Partitioned by: `snapshot_date`

**API Call:**
```python
GET /contacts?updated_since=YYYY-MM-DDTHH:MM:SSZ&updated_to=YYYY-MM-DDTHH:MM:SSZ
```

**Date Range:** Last 3 days (configurable)

**Schema:**
```python
- id (int)
- created_at (timestamp)
- updated_at (timestamp)
- phone_no (string)
- username (string)
- email (string)
- brand_id (int)
- raw_data (string - JSON)
- snapshot_date (string)
- ingestion_timestamp (timestamp)
```

---

### Step 2: Contact Detail Enrichment (Silver Layer)

**File:** `contact_detail_enrichment.py`

**Purpose:** Pull full contact details for each contact ID

**OLD SYSTEM:**
- File: `cs_contacts_kangnam.py`
- Source: `cs_contacts_API_TTC` (PostgreSQL)
- Target: `cs_contact_api` (PostgreSQL)
- Limit: 499 contacts per run

**NEW SYSTEM:**
- Source: `caresoft.cs_contacts_api` (Iceberg Bronze)
- Target: `caresoft.cs_contact_detail_api` (Iceberg Silver)
- Error table: `caresoft.cs_contact_detail_error_api`
- Limit: 300 contacts per run (configurable)

**Selection Logic:**
```sql
-- Read TODAY's snapshot from Bronze
-- Deduplicate by contact_id (MAX updated_at)
-- Order by updated_at DESC
-- LIMIT 300
```

**API Call:**
```python
GET /contacts/{id}
```

**Schema:**
```python
- account_id (int)
- id (int)
- username (string)
- email (string)
- email2 (string)
- phone_no (string)
- phone_no2 (string)
- phone_no3 (string)
- facebook (string)
- gender (string)
- organization_id (int)
- address (string)
- city_id (int)
- created_from (string)
- district_id (int)
- created_at (timestamp)
- updated_at (timestamp)
- role_id (int)
- campaign_handler_id (int)
- take_phone_at (timestamp)
- take_email_at (timestamp)
- custom_fields (string - JSON)
- organization (string)
- psid (string - JSON)
- brand_id (int)
- raw_data (string - JSON)
- snapshot_date (string)
- enrichment_timestamp (timestamp)
```

---

### Step 3: Serving Layer Refresh (PostgreSQL)

**File:** `contact_detail_serving_layer.py`

**Purpose:** UPSERT contact details into PostgreSQL DWH

**OLD SYSTEM:**
- Direct write from API to PostgreSQL
- No deduplication
- No serving layer separation

**NEW SYSTEM:**
- Source: `caresoft.cs_contact_detail_api` (Iceberg Silver)
- Target: `cs_contact_api_v1` (PostgreSQL)
- Pattern: Single partition scan + UPSERT

**Logic:**
```python
1. Read TODAY's snapshot from Silver (single partition scan)
2. Deduplicate by contact_id (MAX updated_at) using DuckDB
3. UPSERT into PostgreSQL:
   - INSERT new contacts
   - UPDATE existing if updated_at is newer
```

**UPSERT Strategy:**
```sql
INSERT INTO cs_contact_api_v1 (...)
VALUES (...)
ON CONFLICT (id) 
DO UPDATE SET
    ... = EXCLUDED...,
    last_synced_at = CURRENT_TIMESTAMP
WHERE cs_contact_api_v1.updated_at < EXCLUDED.updated_at
```

---

## 📊 Data Flow Diagram

```
┌──────────────────────────────────────────────────────────────┐
│  FOR EACH brand IN get_active_brands():                     │
│                                                              │
│  STEP 1: Contact Ingestion (Bronze)                         │
│  ────────────────────────────────────────────────────────   │
│    1. Get API config from brand dict                        │
│       - subdomain = brand["caresoft_domain"]                │
│       - api_key = brand["caresoft_key"]                     │
│                                                              │
│    2. Construct API URL                                     │
│       - /contacts?updated_since=...&updated_to=...          │
│                                                              │
│    3. Paginate through API results                          │
│       - page_size = 100                                     │
│       - Loop until no more data                             │
│                                                              │
│    4. Write to Bronze Iceberg table                         │
│       - Table: cs_contacts_api                              │
│       - Partition by: snapshot_date                         │
│       - Add metadata: brand_id, ingestion_timestamp         │
│                                                              │
│  STEP 2: Contact Detail Enrichment (Silver)                 │
│  ────────────────────────────────────────────────────────   │
│    1. Get contact IDs from Bronze (today's snapshot)        │
│       - Deduplicate by contact_id (MAX updated_at)          │
│       - Limit to 300 contacts                               │
│                                                              │
│    2. FOR EACH contact_id:                                  │
│                                                              │
│         a. Call API for detail                              │
│            - /contacts/{id}                                 │
│                                                              │
│         b. If success:                                      │
│            - Write to Silver table                          │
│            - Table: cs_contact_detail_api                   │
│                                                              │
│         c. If error:                                        │
│            - Write to error table                           │
│            - Table: cs_contact_detail_error_api             │
│                                                              │
│       END FOR (contact_ids)                                 │
│                                                              │
│  STEP 3: Serving Layer Refresh (PostgreSQL)                 │
│  ────────────────────────────────────────────────────────   │
│    1. Read TODAY's snapshot from Silver                     │
│       - Single partition scan (all brands)                  │
│                                                              │
│    2. Deduplicate using DuckDB                              │
│       - By contact_id (MAX updated_at)                      │
│                                                              │
│    3. UPSERT into PostgreSQL                                │
│       - Table: cs_contact_api_v1                            │
│       - Strategy: INSERT or UPDATE if newer                 │
│                                                              │
│  END FOR (brands)                                           │
└──────────────────────────────────────────────────────────────┘
```

---

## 🔧 Environment Variables

### ✅ Required
```bash
# Iceberg configs
ICEBERG_REST_URI_BRONZE=http://iceberg-rest-bronze-zone:8181
ICEBERG_REST_URI_SILVER=http://iceberg-rest-silver-zone:8181
ICEBERG_REST_URI_GOLD=http://iceberg-rest-gold-zone:8181
CATALOG_NAME_BRONZE=bronze
CATALOG_NAME_SILVER=silver
CATALOG_NAME_GOLD=gold

# S3 configs
S3_ENDPOINT=http://10.152.183.65:9000
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=password
AWS_REGION=us-east-1

# CareSoft API config
CARESOFT_PAGE_SIZE=100

# PostgreSQL config
POSTGRES_URI=postgresql://user:pass@host:5432/dbname
```

---

## 📅 Scheduling Recommendations

Based on old system patterns:

| Job | Old Cron | Recommended New Cron | Frequency |
|-----|----------|---------------------|-----------|
| **Contact Ingestion** | 01:45 daily | `45 1 * * *` | Once daily |
| **Contact Detail Enrichment** | Every 15 min | `*/15 * * * *` | 96 times/day |
| **Serving Layer Refresh** | - | `0 */2 * * *` | Every 2 hours |

**Rationale:**
- **Ingestion:** Run once daily to capture last 3 days of updates
- **Enrichment:** Run frequently (every 15 min) to keep details fresh
- **Serving:** Run every 2 hours to sync to PostgreSQL DWH

---

## 🎯 Key Differences from Old System

| Aspect | Old System | New System | Improvement |
|--------|-----------|------------|-------------|
| **Data Storage** | PostgreSQL only | Iceberg + PostgreSQL | Scalable data lake |
| **Partitioning** | None | By snapshot_date | Fast queries |
| **Deduplication** | Manual SQL | DuckDB | Efficient |
| **Error Handling** | Limited | Separate error table | Better tracking |
| **Brand Config** | Hardcoded | Dynamic from core_brand | Flexible |
| **Temp Tables** | Many temp tables | No temp tables | Cleaner |
| **API Limits** | 499 contacts | 300 contacts (configurable) | Tunable |

---

## 📊 Table Mapping

| Old System | New System | Layer |
|-----------|------------|-------|
| `cs_contacts_API_TTC` | `caresoft.cs_contacts_api` | Bronze |
| `cs_contact_api` | `caresoft.cs_contact_detail_api` | Silver |
| `cs_contact_api` | `cs_contact_api_v1` | Serving (PostgreSQL) |
| - | `caresoft.cs_contact_detail_error_api` | Error tracking |

---

## 🚀 Next Steps

1. ✅ **Completed**: Create contact flow scripts
2. ⏳ **TODO**: Test contact ingestion with real brands
3. ⏳ **TODO**: Test contact detail enrichment
4. ⏳ **TODO**: Test serving layer refresh
5. ⏳ **TODO**: Create Airflow DAG for contact flow
6. ⏳ **TODO**: Deploy to production

---

## 📝 Notes

- Contact flow is **simpler** than Ticket flow (no custom fields extraction needed in Bronze)
- Contact detail enrichment **limit is 300** (vs 499 in old system) for better rate limiting
- Serving layer follows **standard pattern** (single partition scan + UPSERT)
- Error tracking is **built-in** from the start
