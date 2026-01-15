# Flow 2: Ticket Source Detail - Giải Thích Chi Tiết

## 📌 Tổng Quan

**Flow 2** là luồng xử lý để lấy **chi tiết đầy đủ** của từng ticket từ CareSoft API.

### Tại sao cần Flow 2?

- **Flow 1** (Ticket Ingestion) chỉ lấy danh sách tickets (metadata cơ bản).
- **Flow 2** (Ticket Detail Enrichment) gọi API riêng lẻ cho từng `ticket_id` để lấy **thông tin chi tiết** (comments, custom_fields, ticket_source_detail_id, v.v.).

---

## 🔄 So Sánh Hệ Thống Cũ vs Mới

### Hệ Thống Cũ (`cs_extract.py`)

#### Bước 1: Lấy danh sách Ticket IDs
```python
# File: cs_extract.py, method: call_tickets()
# Vòng lặp 1, 2, 3: Gọi API /tickets với params updated_since/created_since
for i in range(1, 4):
    if i == 1:
        params = {'updated_to': updated_to, 'updated_since': updated_since}
    if i == 2:
        params = {'created_to': updated_to, 'created_since': updated_since}
    if i == 3:
        params = {'created_to': updated_to_2, 'created_since': updated_since_2}
    
    # Pagination loop
    while record_count != 0:
        response = requests.get(api_url + "tickets", params=params)
        tickets = response.json()['tickets']
        all_tickets.extend(tickets)
```

**Kết quả:** Lưu vào bảng `cs_ticket_API` (PostgreSQL).

#### Bước 2: Tạo Helper Table
```sql
-- Tạo bảng tạm cs_ticket_api_new_day
DELETE FROM cs_ticket_api_new_day WHERE brand_id = {brand_id};
INSERT INTO cs_ticket_api_new_day 
SELECT ticket_id, created_at, updated_at, brand_id 
FROM cs_ticket_api 
WHERE date(updated_at) >= current_date - interval '10 days' 
  AND brand_id = {brand_id};
```

**Vấn đề:**
- Phải tạo bảng tạm phức tạp.
- Logic hard-code 10 ngày.
- Không có cơ chế partition.

#### Bước 3: Lấy Chi Tiết Ticket (Ticket Source Detail)
```python
# File: cs_extract.py (phần này không có trong đoạn code bạn gửi)
# Nhưng logic sẽ là:
# 1. SELECT ticket_id FROM cs_ticket_api_new_day LIMIT 300
# 2. Loop qua từng ticket_id
# 3. Gọi API: GET /tickets/{ticket_id}
# 4. Lưu vào cs_ticket_source_api (PostgreSQL)
```

**Vấn đề:**
- LIMIT 300 cố định (không linh hoạt).
- Không có error tracking.
- Temp table operations phức tạp.

---

### Hệ Thống Mới (Lakehouse)

#### Bước 1: Ticket Ingestion (Bronze)
```python
# File: ticket_ingestion.py
# Gọi API /tickets với 3 vòng lặp (giống hệ thống cũ)
# Loop 1: UPDATED tickets (yesterday → today)
# Loop 2: CREATED tickets (yesterday → today)
# Loop 3: CREATED tickets (day before yesterday → yesterday)

# Lưu vào: caresoft.cs_ticket_bronze (Iceberg)
# Partition by: snapshot_date
```

**Cải tiến:**
- Lưu raw data vào Bronze (Iceberg).
- Partition theo `snapshot_date` → Time-travel.
- Metadata: `fetch_type` ('updated', 'created', 'created_prev').

#### Bước 2: Ticket Detail Enrichment (Silver)
```python
# File: ticket_detail_enrichment.py

# 2.1. Lấy danh sách Ticket IDs từ Bronze
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
        WHERE ticket_id IS NOT NULL AND ticket_id != 0
        ORDER BY updated_at DESC
        LIMIT {limit}
    """)
    
    return ticket_ids

# 2.2. Gọi API cho từng ticket_id
for ticket_id in ticket_ids:
    url = f"{CARESOFT_API_URL}/{subdomain}/api/v1/tickets/{ticket_id}"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        ticket_data = response.json()['ticket']
        # Lưu vào: caresoft.cs_ticket_source_api (Silver)
    else:
        # Lưu error vào: caresoft.cs_ticket_source_error_api (Silver)
```

**Cải tiến:**
- **Không cần helper table** (`cs_ticket_api_new_day`).
- **DuckDB** để sort và limit (hiệu quả hơn).
- **Error tracking** riêng biệt.
- **Partition** theo `snapshot_date`.

---

## 🎯 Logic Chi Tiết: `get_ticket_ids_to_enrich`

Đây là phần **khó hiểu nhất** trong Flow 2. Tôi sẽ giải thích từng bước:

### Mục Đích
Lấy **top N tickets** (mặc định 300) có `updated_at` gần nhất từ Bronze table.

### Tại Sao Cần DuckDB?

**Vấn đề:** PyIceberg không hỗ trợ `ORDER BY` + `LIMIT` trực tiếp trong `scan()`.

**Giải pháp:** 
1. Scan toàn bộ tickets (last 10 days) từ Bronze.
2. Load vào DuckDB.
3. Dùng SQL để `ORDER BY updated_at DESC LIMIT 300`.

### Code Breakdown

```python
def get_ticket_ids_to_enrich(self, brand_erp_id: int, limit: int = 300):
    # 1. Load Bronze table
    bronze_table = self.bronze_catalog.load_table(TICKET_BRONZE_TABLE)
    
    # 2. Calculate cutoff date (10 days ago)
    cutoff_date = self.snapshot_date - timedelta(days=10)
    
    # 3. Initialize DuckDB
    con = duckdb.connect()
    con.execute(f"SET temp_directory='{temp_dir}'")
    con.execute("SET max_memory='3GB'")
    
    # 4. Scan Bronze table with filters
    scan = bronze_table.scan(
        row_filter=And(
            GreaterThanOrEqual("snapshot_date", cutoff_date),  # Last 10 days
            EqualTo("brand_id", brand_erp_id),                 # Specific brand
            NotNull("ticket_id")                               # Valid ticket_id
        ),
        selected_fields=("ticket_id", "updated_at")            # Only need these 2 fields
    )
    
    # 5. Collect batches into Arrow Table
    batches = []
    for batch in scan.to_arrow_batch_reader():
        if len(batch) > 0:
            batches.append(batch)
    
    tickets_table = pa.Table.from_batches(batches)
    
    # 6. Register with DuckDB
    con.register('bronze_tickets', tickets_table)
    
    # 7. Execute SQL to get top N
    result = con.execute(f"""
        SELECT DISTINCT ticket_id
        FROM bronze_tickets
        WHERE ticket_id IS NOT NULL AND ticket_id != 0
        ORDER BY updated_at DESC
        LIMIT {limit}
    """).fetchall()
    
    # 8. Extract ticket_ids
    ticket_ids = [row[0] for row in result]
    
    return ticket_ids
```

### Tại Sao Không Dùng Pandas?

**DuckDB** hiệu quả hơn Pandas khi:
- Xử lý data lớn (millions of rows).
- Cần SQL-like operations (ORDER BY, GROUP BY).
- Tự động spill to disk nếu RAM không đủ.

---

## 📊 Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ FLOW 2: TICKET SOURCE DETAIL                                │
└─────────────────────────────────────────────────────────────┘

Step 1: Ticket Ingestion (Bronze)
┌──────────────┐
│ CareSoft API │
│  /tickets    │
└──────┬───────┘
       │ (3 loops: updated, created, created_prev)
       ▼
┌──────────────────────────────┐
│ cs_ticket_bronze (Iceberg)   │
│ - ticket_id                  │
│ - ticket_no                  │
│ - subject                    │
│ - status                     │
│ - ticket_source              │  ◄── Basic info only
│ - updated_at                 │
│ - brand_id                   │
│ - snapshot_date (partition)  │
└──────────────────────────────┘

Step 2: Get Ticket IDs to Enrich
┌──────────────────────────────┐
│ Scan Bronze (last 10 days)   │
└──────┬───────────────────────┘
       │
       ▼
┌──────────────────────────────┐
│ DuckDB: ORDER BY updated_at  │
│         LIMIT 300             │
└──────┬───────────────────────┘
       │
       ▼
┌──────────────────────────────┐
│ List of ticket_ids           │
│ [12345, 12346, 12347, ...]   │
└──────────────────────────────┘

Step 3: Call API for Each Ticket
┌──────────────────────────────┐
│ Loop: for ticket_id in list  │
└──────┬───────────────────────┘
       │
       ▼
┌──────────────────────────────┐
│ CareSoft API                 │
│ GET /tickets/{ticket_id}     │
└──────┬───────────────────────┘
       │
       ├─── Success ──────────────────────────────┐
       │                                          │
       ▼                                          ▼
┌──────────────────────────────┐    ┌──────────────────────────────┐
│ cs_ticket_source_api (Silver)│    │ cs_ticket_source_error_api   │
│ - ticket_id                  │    │ - ticket_id                  │
│ - comments (JSON)            │    │ - error_message              │
│ - ticket_source_detail_id    │    │ - snapshot_date              │
│ - qa_agent                   │    └──────────────────────────────┘
│ - satisfaction_content       │
│ - raw_data (full JSON)       │
│ - snapshot_date (partition)  │
└──────────────────────────────┘
```

---

## 🔍 Giải Thích Các Field Quan Trọng

### `ticket_source` vs `ticket_source_detail_id`

**`ticket_source`** (có trong Bronze):
- Kiểu: `StringType`
- Giá trị: "Email", "Phone", "Web", "Facebook", v.v.
- Nguồn: API `/tickets` (list endpoint).

**`ticket_source_detail_id`** (chỉ có trong Silver):
- Kiểu: `IntegerType`
- Giá trị: ID cụ thể của nguồn (ví dụ: Facebook Page ID, Email Account ID).
- Nguồn: API `/tickets/{ticket_id}` (detail endpoint).

**Ví dụ:**
```json
// Bronze (from /tickets)
{
  "ticket_id": 12345,
  "ticket_source": "Facebook"
}

// Silver (from /tickets/12345)
{
  "ticket_id": 12345,
  "ticket_source": "Facebook",
  "ticket_source_detail_id": 789  // ← ID của Facebook Page cụ thể
}
```

---

## ⚙️ Tại Sao Cần 3 Vòng Lặp?

Hệ thống cũ (và mới) đều dùng **3 vòng lặp** khi gọi API `/tickets`:

### Loop 1: UPDATED tickets (yesterday → today)
```python
params = {
    'updated_since': '2026-01-13T00:00:00Z',
    'updated_to': '2026-01-14T00:00:00Z'
}
```
**Mục đích:** Lấy tickets đã được **cập nhật** trong ngày hôm qua.

### Loop 2: CREATED tickets (yesterday → today)
```python
params = {
    'created_since': '2026-01-13T00:00:00Z',
    'created_to': '2026-01-14T00:00:00Z'
}
```
**Mục đích:** Lấy tickets **mới tạo** trong ngày hôm qua.

### Loop 3: CREATED tickets (day before yesterday → yesterday)
```python
params = {
    'created_since': '2026-01-12T00:00:00Z',
    'created_to': '2026-01-13T00:00:00Z'
}
```
**Mục đích:** Lấy tickets **mới tạo** trong ngày hôm kia (để bù đắp nếu có delay).

### Tại Sao Cần Loop 3?

**Trường hợp thực tế:**
- Ngày 12/01: Ticket được tạo lúc 23:59.
- Ngày 13/01: Job chạy lúc 00:01 → Có thể miss ticket này.
- Ngày 14/01: Job chạy lại → Loop 3 sẽ catch được ticket này.

---

## 🚀 Cải Tiến Của Hệ Thống Mới

| Tiêu Chí | Hệ Thống Cũ | Hệ Thống Mới |
|----------|--------------|--------------|
| **Helper Table** | Cần `cs_ticket_api_new_day` | Không cần (dùng Iceberg scan) |
| **Sorting/Limiting** | SQL trên PostgreSQL | DuckDB (hiệu quả hơn) |
| **Error Tracking** | Không có | Riêng biệt (`cs_ticket_source_error_api`) |
| **Partitioning** | Không có | Partition theo `snapshot_date` |
| **Time-Travel** | Không có | Iceberg hỗ trợ |
| **Scalability** | Giới hạn bởi PostgreSQL | Lakehouse (S3 + Iceberg) |

---

## 📝 Tóm Tắt

**Flow 2** gồm 2 bước chính:

1. **Ticket Ingestion (Bronze):**
   - Gọi API `/tickets` với 3 vòng lặp.
   - Lưu metadata cơ bản vào `cs_ticket_bronze`.

2. **Ticket Detail Enrichment (Silver):**
   - Scan Bronze để lấy top 300 tickets mới nhất.
   - Gọi API `/tickets/{ticket_id}` cho từng ticket.
   - Lưu chi tiết vào `cs_ticket_source_api`.
   - Lưu lỗi vào `cs_ticket_source_error_api`.

**Điểm Mấu Chốt:**
- Dùng **DuckDB** để sort và limit (vì PyIceberg không hỗ trợ).
- **Không cần helper table** như hệ thống cũ.
- **Error tracking** riêng biệt để dễ debug.
