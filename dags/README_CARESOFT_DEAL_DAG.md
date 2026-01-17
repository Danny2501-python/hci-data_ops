# CareSoft Deal ETL DAG - Hướng dẫn sử dụng

## Tổng quan

DAG `caresoft_deal_etl` xử lý dữ liệu Deal từ ERP qua các tầng Bronze → Silver → Gold → Serving.

**Lịch chạy mặc định:** 2:00 AM hàng ngày (UTC+7)

## Cơ chế xử lý ngày (Snapshot Date)

### Logic mặc định: T-1 (Ngày hôm qua)

Tất cả các task trong DAG sẽ **mặc định xử lý dữ liệu của ngày hôm qua** (T-1), tính từ thời điểm chạy thực tế.

**Ví dụ:**
- DAG chạy lúc **2:00 AM ngày 17/1/2026**
- Snapshot date được tính: **16/1/2026** (T-1)
- Tất cả task sẽ xử lý dữ liệu của ngày **16/1**

### Override snapshot_date qua "Run with Config"

Bạn có thể chạy thủ công với ngày tùy chỉnh:

1. Vào Airflow UI → DAG `caresoft_deal_etl`
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
ERP (PostgreSQL)
    ↓
[Task 1] erp_direct_ingestion
    ↓
Bronze Layer (Iceberg: caresoft.wehook_api_caresoft)
    ↓
[Task 2] webhook_transformation_silver
    ↓
Silver Layer (Iceberg: caresoft.cs_deal_wehook)
    ↓
[Task 3] deal_detail_enrichment (API Call)
    ↓
Gold Layer - Incremental (Iceberg: caresoft.cs_deal_detail_api)
    ↓
[Task 4] deal_snapshot_gold
    ↓
Gold Layer - Final Snapshot (Iceberg: caresoft.cs_deal_detail_api_final)
    ↓
[Task 5 & 6] refresh_serving_layer + refresh_serving_layer_sandbox
    ↓
PostgreSQL DWH (Production + Sandbox)
```

## Các Task trong DAG

| Task ID | Mô tả | Input | Output |
|---------|-------|-------|--------|
| `erp_direct_ingestion` | Lấy dữ liệu từ ERP (SCI, KN, PR, HH) | ERP PostgreSQL | Bronze Iceberg |
| `webhook_transformation_silver` | Transform và làm sạch dữ liệu | Bronze Iceberg | Silver Iceberg |
| `deal_detail_enrichment` | Gọi API CareSoft để lấy chi tiết Deal | Silver Iceberg | Gold Iceberg (Incremental) |
| `deal_snapshot_gold` | Tạo snapshot cuối cùng (latest state per deal) | Gold Incremental | Gold Final |
| `refresh_serving_layer` | Đẩy dữ liệu vào PostgreSQL Production | Gold Final | DWH Product |
| `refresh_serving_layer_sandbox` | Đẩy dữ liệu vào PostgreSQL Sandbox | Gold Final | DWH Sandbox |

## Cấu hình quan trọng

### Start Date & Catchup
```python
start_date=datetime(2026, 1, 1)
catchup=False
```

- `catchup=False`: Không chạy lại các ngày trong quá khứ
- Khi unpause DAG, nó sẽ chạy cho lần gần nhất theo lịch

### Schedule Interval
```python
schedule_interval='0 2 * * *'  # 2:00 AM UTC+7 hàng ngày
```

### Resource Requirements
- **CPU Request:** 1000m (1 core)
- **Memory Request:** 2Gi
- **CPU Limit:** 2000m (2 cores)
- **Memory Limit:** 3Gi

## Troubleshooting

### Vấn đề: DAG chạy sai ngày

**Triệu chứng:** DAG chạy lúc 2h sáng 17/1 nhưng xử lý dữ liệu ngày 15/1

**Nguyên nhân:** DAG bị pause hoặc chưa chạy lần nào, Airflow tìm lần chạy gần nhất chưa thực thi

**Giải pháp:**
1. Xóa DAG Run cũ trong Airflow UI
2. Hoặc chạy thủ công với config:
   ```json
   {
     "snapshot_date": "2026-01-16"
   }
   ```

### Vấn đề: Task OOMKilled

**Nguyên nhân:** DuckDB hoặc PyArrow xử lý quá nhiều dữ liệu trong RAM

**Giải pháp:**
- Kiểm tra `DUCKDB_TEMP_DIR` đã được mount đúng chưa
- Tăng memory limit trong `resource_requirements`
- Giảm batch size trong script Python

### Vấn đề: Connection timeout đến ERP

**Nguyên nhân:** Network policy hoặc firewall chặn kết nối từ Kubernetes Pod

**Giải pháp:**
- Kiểm tra Airflow Connection đã cấu hình đúng chưa
- Test kết nối từ Pod: `kubectl run -it --rm debug --image=postgres:13 --restart=Never -- psql <connection_string>`

## Monitoring

### Xem logs
```bash
# Xem logs của task cụ thể
kubectl logs -n default -l airflow-task-id=erp_direct_ingestion

# Xem logs real-time
kubectl logs -n default -l airflow-task-id=deal_snapshot_gold -f
```

### Kiểm tra dữ liệu Iceberg
```sql
-- Kết nối Trino hoặc Spark
SELECT snapshot_date, COUNT(*) as deal_count
FROM gold.caresoft.cs_deal_detail_api_final
GROUP BY snapshot_date
ORDER BY snapshot_date DESC
LIMIT 10;
```

### Kiểm tra PostgreSQL Serving Layer
```sql
-- Kết nối DWH Product
SELECT 
    snapshot_date,
    COUNT(*) as total_deals,
    COUNT(DISTINCT brand_id) as brands
FROM cs_deal_detail_api_v1
GROUP BY snapshot_date
ORDER BY snapshot_date DESC
LIMIT 10;
```

## Best Practices

1. **Luôn kiểm tra logs** trước khi re-run task thất bại
2. **Sử dụng "Clear" thay vì "Delete"** khi muốn chạy lại task
3. **Backfill dữ liệu** bằng cách chạy thủ công với config cho từng ngày
4. **Monitor resource usage** để tối ưu hóa memory/CPU limits
5. **Backup Iceberg metadata** định kỳ (MinIO snapshots)

## Liên hệ

- **Owner:** Data Engineering Team
- **Email:** data-eng@example.com
- **Slack:** #data-engineering
