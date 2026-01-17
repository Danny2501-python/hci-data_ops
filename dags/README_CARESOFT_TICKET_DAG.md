# CareSoft Ticket ETL DAG - Hướng dẫn sử dụng

## Tổng quan

DAG `caresoft_ticket_etl` xử lý dữ liệu Ticket từ CareSoft API qua các tầng Bronze → Silver → Serving.

**Lịch chạy mặc định:** 5:00 AM hàng ngày (UTC+7)

## Cơ chế xử lý ngày (Snapshot Date)

### Logic mặc định: T-1 (Ngày hôm qua)

Tất cả các task trong DAG sẽ **mặc định xử lý dữ liệu của ngày hôm qua** (T-1), tính từ thời điểm chạy thực tế.

**Ví dụ:**
- DAG chạy lúc **5:00 AM ngày 17/1/2026**
- Snapshot date được tính: **16/1/2026** (T-1)
- Tất cả task sẽ xử lý dữ liệu của ngày **16/1**

### Override snapshot_date qua "Run with Config"

Bạn có thể chạy thủ công với ngày tùy chỉnh:

1. Vào Airflow UI → DAG `caresoft_ticket_etl`
2. Click **"Trigger DAG w/ config"** (biểu tượng ▶️ với dấu +)
3. Nhập JSON config:

```json
{
  "snapshot_date": "2026-01-15"
}
```

4. Click **Trigger**

DAG sẽ chạy và xử lý dữ liệu cho ngày **15/1/2026** thay vì T-1.

## Kiến trúc Pipeline

```
CareSoft API (Ticket List)
    ↓
[Task 1] ticket_ingestion_bronze
    ↓
Bronze Layer (Iceberg: caresoft.cs_ticket_api)
    ↓                                    ↓
[Task 2] ticket_enrichment_silver    [Task 3] ticket_api_serving_layer
    ↓                                    ↓
Silver Layer                         PostgreSQL DWH
(Iceberg: caresoft.cs_ticket_source_api)  (cs_ticket_api_v1)
    ↓
[Task 4] ticket_source_serving_layer
    ↓
PostgreSQL DWH (cs_ticket_source_api_v1)
```

## Các Task trong DAG

| Task ID | Mô tả | Input | Output |
|---------|-------|-------|--------|
| `ticket_ingestion_bronze` | Lấy danh sách ticket từ CareSoft API | CareSoft API | Bronze Iceberg |
| `ticket_enrichment_silver` | Gọi API chi tiết cho từng ticket | Bronze Iceberg | Silver Iceberg |
| `ticket_api_serving_layer` | Đẩy dữ liệu ticket list vào DWH | Bronze Iceberg | PostgreSQL (cs_ticket_api_v1) |
| `ticket_source_serving_layer` | Đẩy dữ liệu ticket detail vào DWH | Silver Iceberg | PostgreSQL (cs_ticket_source_api_v1) |

### Luồng xử lý song song

```
ticket_ingestion_bronze
    ├─→ ticket_enrichment_silver → ticket_source_serving_layer
    └─→ ticket_api_serving_layer
```

Task 2 và Task 3 chạy **song song** sau khi Task 1 hoàn thành để tối ưu thời gian.

## Cấu hình quan trọng

### Start Date & Catchup
```python
start_date=datetime(2026, 1, 1)
catchup=False
max_active_runs=1
```

- `catchup=False`: Không chạy lại các ngày trong quá khứ
- `max_active_runs=1`: Chỉ cho phép 1 DAG run cùng lúc

### Schedule Interval
```python
schedule_interval='0 5 * * *'  # 5:00 AM UTC+7 hàng ngày
```

### Resource Requirements

**Bronze Ingestion:**
- CPU Request: 1000m, Memory Request: 1Gi
- CPU Limit: 2000m, Memory Limit: 2Gi

**Silver Enrichment & Serving:**
- CPU Request: 1000m, Memory Request: 1Gi
- CPU Limit: 2000m, Memory Limit: 2Gi

## API Configuration

### CareSoft API Settings
```python
CARESOFT_PAGE_SIZE: '500'  # Số lượng ticket mỗi page
```

### Ticket Selection Logic (Enrichment)

Task `ticket_enrichment_silver` chỉ lấy chi tiết cho:
- **Tickets được cập nhật trong 10 ngày gần nhất**
- **Top 300 tickets** (sắp xếp theo `updated_at` DESC)
- **Nhóm theo ticket_id**, lấy bản ghi mới nhất

## Troubleshooting

### Vấn đề: DAG chạy sai ngày

**Triệu chứng:** DAG chạy lúc 5h sáng 17/1 nhưng xử lý dữ liệu ngày 15/1

**Nguyên nhân:** DAG bị pause hoặc chưa chạy lần nào, Airflow tìm lần chạy gần nhất chưa thực thi

**Giải pháp:**
1. Xóa DAG Run cũ trong Airflow UI
2. Hoặc chạy thủ công với config:
   ```json
   {
     "snapshot_date": "2026-01-16"
   }
   ```

### Vấn đề: API Rate Limit

**Triệu chứng:** Task `ticket_enrichment_silver` bị lỗi 429 Too Many Requests

**Nguyên nhân:** Gọi API CareSoft quá nhanh

**Giải pháp:**
- Giảm số lượng ticket xử lý (hiện tại: 300)
- Thêm delay giữa các API call trong code Python
- Tăng `retry_delay` trong DAG config

### Vấn đề: Ticket bị thiếu trong Serving Layer

**Triệu chứng:** PostgreSQL có ít ticket hơn Iceberg

**Nguyên nhân:** UPSERT logic chỉ update khi `updated_at` mới hơn

**Giải pháp:**
- Kiểm tra logic WHERE clause trong serving layer script
- Xem log để xác nhận số lượng rows được upsert
- Nếu cần, truncate table và chạy lại

### Vấn đề: Bronze Ingestion timeout

**Triệu chứng:** Task `ticket_ingestion_bronze` timeout sau 1-2 giờ

**Nguyên nhân:** Quá nhiều ticket cần lấy từ API

**Giải pháp:**
- Tăng `execution_timeout` trong task config
- Giảm `CARESOFT_PAGE_SIZE` để tránh timeout mỗi request
- Kiểm tra network connectivity đến CareSoft API

## Monitoring

### Xem logs
```bash
# Xem logs của task Bronze Ingestion
kubectl logs -n default -l airflow-task-id=ticket_ingestion_bronze

# Xem logs của task Silver Enrichment
kubectl logs -n default -l airflow-task-id=ticket_enrichment_silver -f
```

### Kiểm tra dữ liệu Iceberg (Bronze)
```sql
-- Kết nối Trino hoặc Spark
SELECT 
    snapshot_date,
    brand_id,
    COUNT(*) as ticket_count
FROM bronze.caresoft.cs_ticket_api
GROUP BY snapshot_date, brand_id
ORDER BY snapshot_date DESC, brand_id
LIMIT 20;
```

### Kiểm tra dữ liệu Iceberg (Silver)
```sql
-- Ticket detail (enriched)
SELECT 
    snapshot_date,
    brand_id,
    COUNT(*) as enriched_count
FROM silver.caresoft.cs_ticket_source_api
GROUP BY snapshot_date, brand_id
ORDER BY snapshot_date DESC, brand_id
LIMIT 20;
```

### Kiểm tra PostgreSQL Serving Layer
```sql
-- Ticket List
SELECT 
    snapshot_date,
    COUNT(*) as total_tickets,
    COUNT(DISTINCT brand_id) as brands
FROM cs_ticket_api_v1
GROUP BY snapshot_date
ORDER BY snapshot_date DESC
LIMIT 10;

-- Ticket Detail
SELECT 
    snapshot_date,
    COUNT(*) as total_details,
    COUNT(DISTINCT brand_id) as brands
FROM cs_ticket_source_api_v1
GROUP BY snapshot_date
ORDER BY snapshot_date DESC
LIMIT 10;
```

## Best Practices

1. **Kiểm tra API connectivity** trước khi chạy DAG
2. **Monitor API rate limits** để tránh bị block
3. **Sử dụng "Clear" thay vì "Delete"** khi muốn chạy lại task
4. **Backfill dữ liệu** bằng cách chạy thủ công với config cho từng ngày
5. **Monitor resource usage** để tối ưu hóa memory/CPU limits
6. **Kiểm tra data quality** sau mỗi lần chạy (so sánh số lượng records)

## Data Quality Checks

### Kiểm tra tính toàn vẹn dữ liệu

```sql
-- So sánh Bronze vs Serving (Ticket List)
SELECT 
    'Bronze' as layer,
    snapshot_date,
    COUNT(*) as count
FROM bronze.caresoft.cs_ticket_api
WHERE snapshot_date = '2026-01-16'
GROUP BY snapshot_date

UNION ALL

SELECT 
    'Serving' as layer,
    snapshot_date::date,
    COUNT(*) as count
FROM cs_ticket_api_v1
WHERE snapshot_date = '2026-01-16'
GROUP BY snapshot_date;
```

### Kiểm tra tickets bị thiếu detail

```sql
-- Tickets trong Bronze nhưng không có detail trong Silver
SELECT b.ticket_id, b.brand_id
FROM bronze.caresoft.cs_ticket_api b
LEFT JOIN silver.caresoft.cs_ticket_source_api s
    ON b.ticket_id = s.ticket_id 
    AND b.brand_id = s.brand_id
    AND b.snapshot_date = s.snapshot_date
WHERE b.snapshot_date = '2026-01-16'
    AND s.ticket_id IS NULL
LIMIT 100;
```

## Liên hệ

- **Owner:** Data Engineering Team
- **Email:** data-eng@example.com
- **Slack:** #data-engineering
