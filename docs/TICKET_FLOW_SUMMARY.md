# Ticket Flow Summary
## CareSoft Ticket Pipeline Architecture

---

## 📊 Overview

Ticket flow **giống hệt** Deal flow về cách loop qua brands và lấy API config từ `core_brand` table.

---

## 🔄 Architecture Comparison

### Deal Flow (Flow 1)
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
│ ERP Webhook       │   │ Deal Detail           │
│ Ingestion         │   │ Enrichment            │
│ (Bronze)          │   │ (Silver → Gold)       │
│                   │   │                       │
│ Loop all brands   │   │ Loop all brands       │
│ ↓                 │   │ ↓                     │
│ Write to:         │   │ Call API:             │
│ cs_deal_wehook    │   │ /deal/{id}            │
└───────────────────┘   └───────────────────────┘
```

### Ticket Flow (Flow 2) - **SAME PATTERN**
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
│ Ticket            │   │ Ticket Detail         │
│ Ingestion         │   │ Enrichment            │
│ (Bronze)          │   │ (Bronze → Silver)     │
│                   │   │                       │
│ Loop all brands   │   │ Loop all brands       │
│ ↓                 │   │ ↓                     │
│ Write to:         │   │ Call API:             │
│ cs_ticket_api     │   │ /tickets/{id}         │
└───────────────────┘   └───────────────────────┘
```

---

## 🎯 Key Similarities

| Aspect | Deal Flow | Ticket Flow |
|--------|-----------|-------------|
| **Brand Loading** | `get_active_brands()` | `get_active_brands()` ✅ |
| **Loop Pattern** | `for brand in brands:` | `for brand in brands:` ✅ |
| **API URL** | `https://api.caresoft.vn/{subdomain}/api/v1/deal/{id}` | `https://api.caresoft.vn/{subdomain}/api/v1/tickets/{id}` ✅ |
| **API Key** | `brand["caresoft_key"]` | `brand["caresoft_key"]` ✅ |
| **Subdomain** | `brand["caresoft_domain"]` | `brand["caresoft_domain"]` ✅ |
| **Error Tracking** | Separate error table | Separate error table ✅ |
| **Batch Processing** | 1000 records/batch | 1000 records/batch ✅ |

---

## 📝 Code Examples

### 1. Brand Loading (Both Flows)

```python
# File: brand_config.py
def get_active_brands():
    """
    Load active brands from Gold layer core_brand table
    
    Returns:
        List of brand dicts with keys: id, name, erp_id, caresoft_domain, caresoft_key
    """
    brand_table = ensure_brand_table_exists()
    
    scan = brand_table.scan(
        row_filter=EqualTo("is_active", True),
        selected_fields=("id", "name", "erp_id", "caresoft_domain", "caresoft_key")
    )
    
    brands = []
    for batch in scan.to_arrow_batch_reader():
        for i in range(len(batch)):
            brand = {
                "id": batch["id"][i].as_py(),
                "name": batch["name"][i].as_py(),
                "erp_id": batch["erp_id"][i].as_py(),
                "caresoft_domain": batch["caresoft_domain"][i].as_py(),
                "caresoft_key": batch["caresoft_key"][i].as_py(),
            }
            brands.append(brand)
    
    return brands
```

### 2. Main Loop (Ticket Ingestion)

```python
# File: ticket_ingestion.py
def main():
    snapshot_date = date.today()
    
    # Load all active brands from Gold layer
    brands = get_active_brands()
    
    if not brands:
        logger.error("No active brands found in core_brand table")
        sys.exit(1)
    
    logger.info(f"Found {len(brands)} active brands to process")
    
    # Initialize job once
    job = TicketIngestion(snapshot_date)
    
    # Process each brand sequentially
    total_success = 0
    total_errors = 0
    
    for brand in brands:
        logger.info(f"Processing brand: {brand['name']} (erp_id={brand['erp_id']})")
        
        try:
            job.run(brand=brand, days_ago=1)
            total_success += 1
        except Exception as e:
            logger.error(f"Failed to process brand {brand['erp_id']}: {e}")
            total_errors += 1
    
    logger.success(f"Completed. Success: {total_success}/{len(brands)}, Errors: {total_errors}")
```

### 3. API Call (Ticket Detail Enrichment)

```python
# File: ticket_detail_enrichment.py
def call_caresoft_api(self, brand: dict, ticket_id: int):
    """Call CareSoft API to get ticket details"""
    subdomain = brand["caresoft_domain"]  # e.g. "kangnam2"
    api_key = brand["caresoft_key"]
    
    # API URL pattern: https://api.caresoft.vn/{subdomain}/api/v1/tickets/{id}
    url = f"https://api.caresoft.vn/{subdomain}/api/v1/tickets/{ticket_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            return response.json().get("ticket"), None
        else:
            return None, f"HTTP {response.status_code}: {response.text[:500]}"
            
    except Exception as e:
        return None, str(e)
```

### 4. Main Loop (Ticket Detail Enrichment)

```python
# File: ticket_detail_enrichment.py
def main():
    snapshot_date = date.today()
    
    # Load all active brands
    brands = get_active_brands()
    
    if not brands:
        logger.error("No active brands found")
        sys.exit(1)
    
    logger.info(f"Found {len(brands)} active brands")
    
    # Initialize job once
    job = TicketDetailEnrichment(snapshot_date)
    
    # Process each brand
    for brand in brands:
        logger.info(f"Processing brand: {brand['name']}")
        
        try:
            job.run(brand=brand, limit=300)
        except Exception as e:
            logger.error(f"Failed to process brand {brand['erp_id']}: {e}")
```

---

## 🔧 Environment Variables

### ❌ NOT Needed (Removed)
```bash
CARESOFT_API_URL=https://api.caresoft.vn  # Hard-coded in scripts now
```

### ✅ Still Needed
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
```

---

## 📊 Data Flow

### Bronze Ingestion (Both Flows)

```
┌──────────────────────────────────────────────────────────────┐
│  FOR EACH brand IN get_active_brands():                     │
│                                                              │
│    1. Get API config from brand dict                        │
│       - subdomain = brand["caresoft_domain"]                │
│       - api_key = brand["caresoft_key"]                     │
│                                                              │
│    2. Construct API URL                                     │
│       - Deal: /deal?updated_since=...                       │
│       - Ticket: /tickets?updated_since=...                  │
│                                                              │
│    3. Paginate through API results                          │
│       - page_size = 100                                     │
│       - Loop until no more data                             │
│                                                              │
│    4. Write to Bronze Iceberg table                         │
│       - Deal: cs_deal_wehook                                │
│       - Ticket: cs_ticket_api                               │
│       - Partition by: snapshot_date                         │
│       - Add metadata: brand_id, fetch_type, etc.            │
│                                                              │
│  END FOR                                                     │
└──────────────────────────────────────────────────────────────┘
```

### Silver Enrichment (Both Flows)

```
┌──────────────────────────────────────────────────────────────┐
│  FOR EACH brand IN get_active_brands():                     │
│                                                              │
│    1. Get IDs to enrich from Bronze table                   │
│       - Deal: Compare today vs yesterday webhooks           │
│       - Ticket: Get top 300 by updated_at (last 10 days)    │
│                                                              │
│    2. FOR EACH id IN ids_to_enrich:                         │
│                                                              │
│         a. Call API for detail                              │
│            - Deal: /deal/{id}                               │
│            - Ticket: /tickets/{id}                          │
│                                                              │
│         b. If success:                                      │
│            - Write to Silver/Gold table                     │
│            - Deal: cs_deal_detail_api (Gold)                │
│            - Ticket: cs_ticket_source_api (Silver)          │
│                                                              │
│         c. If error:                                        │
│            - Write to error table                           │
│            - Deal: cs_deal_detail_error_api                 │
│            - Ticket: cs_ticket_source_error_api             │
│                                                              │
│       END FOR (ids)                                         │
│                                                              │
│  END FOR (brands)                                           │
└──────────────────────────────────────────────────────────────┘
```

---

## 🎯 Summary

### ✅ Ticket Flow = Deal Flow Pattern

| Feature | Status |
|---------|--------|
| Loop through brands | ✅ Yes |
| Load from `core_brand` table | ✅ Yes |
| Use `caresoft_domain` + `caresoft_key` | ✅ Yes |
| Construct API URL dynamically | ✅ Yes |
| No hardcoded `CARESOFT_API_URL` env var | ✅ Yes |
| Separate error tracking | ✅ Yes |
| Batch processing | ✅ Yes |
| Iceberg partitioning | ✅ Yes |

### 🔄 Execution Flow

1. **Airflow triggers task**
2. **Script loads brands** from `core_brand` table via `get_active_brands()`
3. **Loop through each brand**:
   - Get `caresoft_domain` (e.g. "kangnam2")
   - Get `caresoft_key` (API key)
   - Construct URL: `https://api.caresoft.vn/{subdomain}/api/v1/...`
   - Call API with brand-specific credentials
   - Write results to Iceberg
4. **Log summary**: Success/Error counts per brand

### 📌 Key Advantage

**Single Task, Multiple Brands**: Không cần tạo dynamic tasks cho mỗi brand. Script tự loop qua tất cả active brands, giống như Deal flow.

---

## 🚀 Next Steps

1. ✅ **Đã hoàn thành**: Remove `CARESOFT_API_URL` env var
2. ✅ **Đã hoàn thành**: Update scripts to use `brand["caresoft_domain"]`
3. ✅ **Đã hoàn thành**: Update DAG to remove `CARESOFT_API_URL`
4. ⏳ **Cần làm**: Populate `core_brand` table with brand data
5. ⏳ **Cần làm**: Test ticket flow with real brands
6. ⏳ **Cần làm**: Deploy to Airflow
