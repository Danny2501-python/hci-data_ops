# Contact Flow Migration Guide
## From Old System (etl_marketing) to New System (data-ops)

---

## 📊 Overview

This document maps the Contact flow from the old Django-based ETL system to the new Lakehouse architecture.

---

## 🗺️ File Mapping

### Old System Files
```
etl_marketing/app/caresoft/
├── cs_extract.py                    # Contains call_cs_contact_ttc()
└── cs_contacts_kangnam.py           # Contact detail enrichment
```

### New System Files
```
data-ops/app/caresoft/contact/
├── __init__.py
├── contact_ingestion.py             # Bronze layer (replaces call_cs_contact_ttc)
├── contact_detail_enrichment.py     # Silver layer (replaces cs_contacts_kangnam.py)
└── contact_detail_serving_layer.py  # PostgreSQL serving layer
```

---

## 🔄 Function Mapping

### Step 1: Contact List Ingestion

| Aspect | Old System | New System |
|--------|-----------|------------|
| **File** | `cs_extract.py` | `contact_ingestion.py` |
| **Function** | `call_cs_contact_ttc()` | `ContactIngestion.run()` |
| **API Endpoint** | `GET /contacts` | `GET /contacts` ✅ |
| **Date Range** | Last 3 days | Last 3 days ✅ |
| **Output Table** | `cs_contacts_API_TTC` (PostgreSQL) | `caresoft.cs_contacts_api` (Iceberg) |
| **Temp Table** | `cs_contacts_API_TTC_test_YYYYMMDD` | None (no temp tables) |
| **Partitioning** | None | By `snapshot_date` |

**OLD CODE:**
```python
def call_cs_contact_ttc(self, days_for_ago=3):
    # Calculate date range
    days_for_ago = 3
    now = datetime.now()
    this_day = now
    this_day_1 = this_day - timedelta(days=days_for_ago)
    updated_since = this_day_1.replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + 'Z'
    updated_to = this_day.replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + 'Z'
    
    # API call
    params = {
        'updated_to': updated_to,
        'page': page,
        'count': settings.ETL_CARESOFT_NUMBER_ITEM_IN_PAGES,
        'updated_since': updated_since
    }
    
    response = requests.get(
        self.get_api_url(path='contacts'), 
        params=params, 
        headers=self.get_api_headers()
    )
    
    # Write to PostgreSQL with temp table
    table = 'cs_contacts_API_TTC_test_' + str(updated_since[:10]).replace('-', '')
    # ... complex temp table logic ...
```

**NEW CODE:**
```python
def run(self, brand: dict, days_ago: int = 3):
    # Fetch contacts from API
    contacts = self.fetch_contacts(brand, days_ago)
    
    # Transform to Iceberg records
    records = []
    for contact in contacts:
        record = {
            "id": safe_int(contact.get("id")),
            "created_at": safe_ts(contact.get("created_at")),
            "updated_at": safe_ts(contact.get("updated_at")),
            "phone_no": safe_str(contact.get("phone_no")),
            "username": safe_str(contact.get("username")),
            "email": safe_str(contact.get("email")),
            "brand_id": brand_erp_id,
            "raw_data": json.dumps(contact, ensure_ascii=False),
            "snapshot_date": snapshot_date_str,
            "ingestion_timestamp": ingestion_timestamp,
        }
        records.append(record)
    
    # Write to Iceberg Bronze (no temp tables!)
    table = self.bronze_catalog.load_table(CONTACT_BRONZE_TABLE)
    table.append(records)
```

---

### Step 2: Contact Detail Enrichment

| Aspect | Old System | New System |
|--------|-----------|------------|
| **File** | `cs_contacts_kangnam.py` | `contact_detail_enrichment.py` |
| **Class** | `CsCall` | `ContactDetailEnrichment` |
| **Source Query** | `SELECT id FROM cs_contacts_api_ttc WHERE NOT EXISTS (SELECT id FROM cs_contact_api) LIMIT 499` | Scan Bronze Iceberg, deduplicate, LIMIT 300 |
| **API Endpoint** | `GET /contacts/{id}` | `GET /contacts/{id}` ✅ |
| **Output Table** | `cs_contact_api` (PostgreSQL) | `caresoft.cs_contact_detail_api` (Iceberg) |
| **Error Tracking** | None | `caresoft.cs_contact_detail_error_api` |
| **Limit** | 499 contacts | 300 contacts (configurable) |

**OLD CODE:**
```python
class CsCall:
    def fetch_ids(self):
        # Hardcoded PostgreSQL connection
        conn = psycopg2.connect(
            host="10.10.10.214",
            port="5432",
            database="staging_sci",
            user="admin",
            password="dqibcujfvpos"
        )
        
        # Simple NOT EXISTS query
        select_query = """
        SELECT id
        FROM cs_contacts_api_ttc
        WHERE NOT EXISTS (SELECT id FROM cs_contact_api)
        LIMIT 499
        """
        cursor.execute(select_query)
        data = cursor.fetchall()
        
        new_ids = [contact[0] for contact in data]
        self.ids.extend(new_ids)
    
    def fetch_contact_details(self, id):
        # Hardcoded URL
        url = f'https://api.caresoft.vn/kangnam/api/v1/contacts/{id}'
        response = requests.get(url, params=self.params, headers=self.headers)
        
        if response.status_code == 200:
            data = response.json()
            new_calls_data = data['contact']
            self.calls_data.append(new_calls_data)
            self.processed_ids.append(id)
        else:
            print("Yêu cầu API không thành công. Mã trạng thái:", response.status_code)
```

**NEW CODE:**
```python
class ContactDetailEnrichment:
    def get_contact_ids_to_enrich(self, brand_erp_id: int, limit: int = 300) -> list:
        # Scan Bronze Iceberg table (today's snapshot only)
        bronze_table = self.bronze_catalog.load_table(CONTACT_BRONZE_TABLE)
        scan = bronze_table.scan(
            row_filter=EqualTo("snapshot_date", snapshot_date_str)
        )
        
        # Deduplicate using DuckDB
        query = f"""
        SELECT id as contact_id
        FROM (
            SELECT 
                id,
                brand_id,
                updated_at,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
            FROM bronze_contacts
            WHERE brand_id = {brand_erp_id}
        ) t
        WHERE rn = 1
        ORDER BY updated_at DESC
        LIMIT {limit}
        """
        
        result = conn.execute(query).fetchall()
        contact_ids = [row[0] for row in result]
        
        return contact_ids
    
    def call_caresoft_api(self, brand: dict, contact_id: int) -> tuple:
        # Dynamic URL from brand config
        subdomain = brand["caresoft_domain"]
        api_key = brand["caresoft_key"]
        
        url = f"https://api.caresoft.vn/{subdomain}/api/v1/contacts/{contact_id}"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return data.get("contact"), None
            else:
                return None, f"HTTP {response.status_code}: {response.text[:500]}"
                
        except Exception as e:
            return None, str(e)
```

---

## 📊 Schema Comparison

### Bronze Layer (Contact List)

**OLD:** `cs_contacts_API_TTC` (PostgreSQL)
```sql
CREATE TABLE cs_contacts_API_TTC (
    id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    phone_no TEXT,
    username TEXT,
    email TEXT,
    brand_id INTEGER,
    raw_data TEXT
);
```

**NEW:** `caresoft.cs_contacts_api` (Iceberg)
```python
Schema(
    NestedField(1, "id", IntegerType(), required=True),
    NestedField(2, "created_at", TimestampType(), required=False),
    NestedField(3, "updated_at", TimestampType(), required=False),
    NestedField(4, "phone_no", StringType(), required=False),
    NestedField(5, "username", StringType(), required=False),
    NestedField(6, "email", StringType(), required=False),
    NestedField(7, "brand_id", IntegerType(), required=True),
    NestedField(8, "raw_data", StringType(), required=False),
    
    # NEW: Metadata fields
    NestedField(9, "snapshot_date", StringType(), required=True),
    NestedField(10, "ingestion_timestamp", TimestampType(), required=True),
)
# Partitioned by snapshot_date
```

---

### Silver Layer (Contact Details)

**OLD:** `cs_contact_api` (PostgreSQL)
```sql
CREATE TABLE cs_contact_api (
    account_id INTEGER,
    id INTEGER PRIMARY KEY,
    username TEXT,
    email TEXT,
    email2 TEXT,
    phone_no TEXT,
    phone_no2 TEXT,
    phone_no3 TEXT,
    facebook TEXT,
    gender TEXT,
    organization_id INTEGER,
    address TEXT,
    city_id INTEGER,
    created_from TEXT,
    district_id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    role_id INTEGER,
    campaign_handler_id INTEGER,
    take_phone_at TIMESTAMP,
    take_email_at TIMESTAMP,
    custom_fields TEXT,
    organization TEXT,
    psid TEXT,
    brand_id INTEGER,
    raw_data TEXT
);
```

**NEW:** `caresoft.cs_contact_detail_api` (Iceberg Silver)
```python
Schema(
    # Same fields as old system
    NestedField(1, "account_id", IntegerType(), required=False),
    NestedField(2, "id", IntegerType(), required=True),
    # ... all other fields ...
    NestedField(25, "brand_id", IntegerType(), required=True),
    NestedField(26, "raw_data", StringType(), required=False),
    
    # NEW: Metadata fields
    NestedField(27, "snapshot_date", StringType(), required=True),
    NestedField(28, "enrichment_timestamp", TimestampType(), required=True),
)
# Partitioned by snapshot_date
```

---

## 🔧 Configuration Changes

### Old System
```python
# settings.py
ETL_CARESOFT_URL = "https://api.caresoft.vn/%(domain)s/api/v1/"
ETL_CARESOFT_NUMBER_ITEM_IN_PAGES = 100

# Hardcoded in cs_contacts_kangnam.py
self.headers = {
    'Authorization': 'Bearer UZDhVsGJXMK0LoQ'  # Hardcoded!
}
```

### New System
```bash
# Environment variables
CARESOFT_PAGE_SIZE=100

# Brand config loaded from core_brand table
# No hardcoded credentials!
```

---

## 🚀 Execution Comparison

### Old System
```python
# Manual execution per brand
from app.caresoft.cs_extract import CsExtract
from app.caresoft.cs_contacts_kangnam import CsCall

# Step 1: Contact list
obj_brand = Brand.objects.get(id=1)  # Hardcoded brand
extractor = CsExtract(obj_brand)
extractor.call_cs_contact_ttc(days_for_ago=3)

# Step 2: Contact details
contact_job = CsCall()
contact_job.run()
```

### New System
```bash
# Automatic execution for all brands

# Step 1: Contact ingestion (Bronze)
python contact_ingestion.py 2026-01-17

# Step 2: Contact detail enrichment (Silver)
python contact_detail_enrichment.py 2026-01-17

# Step 3: Serving layer refresh (PostgreSQL)
python contact_detail_serving_layer.py 2026-01-17
```

---

## 📈 Improvements

| Feature | Old System | New System | Benefit |
|---------|-----------|------------|---------|
| **Scalability** | PostgreSQL only | Iceberg data lake | Handle TB+ data |
| **Partitioning** | None | By snapshot_date | Fast queries |
| **Multi-brand** | Manual per brand | Auto loop all brands | Less manual work |
| **Error Tracking** | Print to console | Dedicated error table | Better debugging |
| **Temp Tables** | Many temp tables | No temp tables | Cleaner |
| **Deduplication** | Manual SQL | DuckDB | Faster |
| **API Config** | Hardcoded | Dynamic from core_brand | Flexible |
| **Limit** | 499 (hardcoded) | 300 (configurable) | Tunable |
| **Credentials** | Hardcoded in code | Environment variables | Secure |

---

## 🎯 Migration Checklist

- [x] Create Bronze layer script (`contact_ingestion.py`)
- [x] Create Silver layer script (`contact_detail_enrichment.py`)
- [x] Create Serving layer script (`contact_detail_serving_layer.py`)
- [x] Create documentation (`CONTACT_FLOW_SUMMARY.md`)
- [x] Create migration guide (this file)
- [ ] Populate `core_brand` table with contact API configs
- [ ] Test Bronze layer ingestion
- [ ] Test Silver layer enrichment
- [ ] Test Serving layer refresh
- [ ] Create Airflow DAG
- [ ] Deploy to production
- [ ] Monitor and validate data quality

---

## 📝 Notes

1. **No Custom Fields Extraction:** Unlike Ticket flow, Contact flow doesn't need custom field extraction in Bronze layer (it's done in Silver)
2. **Simpler Schema:** Contact schema is simpler than Ticket schema
3. **Lower Limit:** Changed from 499 to 300 contacts per run for better rate limiting
4. **Error Tracking:** Added from the start (old system had none)
5. **Serving Layer:** Added new step to sync to PostgreSQL DWH
