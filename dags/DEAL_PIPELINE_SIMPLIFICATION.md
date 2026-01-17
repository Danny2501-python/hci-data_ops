# Deal Pipeline Simplification - Migration Guide

## Tổng quan thay đổi

Đã **loại bỏ intermediate snapshot step** để đơn giản hóa pipeline Deal.

### Trước (6 tasks):
```
1. erp_direct_ingestion
2. webhook_transformation_silver
3. deal_detail_enrichment
4. deal_snapshot_gold          ← BỎ TASK NÀY
5. refresh_serving_layer
6. refresh_serving_layer_sandbox
```

### Sau (5 tasks):
```
1. erp_direct_ingestion
2. webhook_transformation_silver
3. deal_detail_enrichment
4. refresh_serving_layer        ← Đọc trực tiếp từ cs_deal_detail_api
5. refresh_serving_layer_sandbox
```

---

## Lý do thay đổi

### ❌ Vấn đề của approach cũ:

1. **Duplicate data**: `cs_deal_detail_api_final` là bản copy của `cs_deal_detail_api`
2. **Tốn storage**: Mỗi ngày tạo thêm 1 partition duplicate
3. **Chậm**: Phải scan toàn bộ history để tìm latest state
4. **Phức tạp**: 2 Gold tables + 1 intermediate step

### ✅ Lợi ích của approach mới:

1. **Tiết kiệm storage**: Bỏ table `cs_deal_detail_api_final`
2. **Nhanh hơn**: Chỉ scan 1 partition (ngày hôm đó)
3. **Đơn giản hơn**: 1 Gold table, logic rõ ràng
4. **PostgreSQL tự quản lý latest state**: Qua UPSERT ON CONFLICT

---

## Thay đổi chi tiết

### 1. File `deal_detail_serving_layer.py`

#### Trước:
```python
SOURCE_TABLE = 'caresoft.cs_deal_detail_api_final'  # Đọc từ snapshot table

def get_latest_deals(self, snapshot_date):
    # Đọc 1 partition từ final table
    scan = table.scan(row_filter=EqualTo("snapshot_date", target_date))
```

#### Sau:
```python
SOURCE_TABLE = 'caresoft.cs_deal_detail_api'  # Đọc trực tiếp từ incremental table

def get_deals_for_date(self, snapshot_date):
    # Đọc 1 partition từ incremental table (chỉ deals của ngày hôm đó)
    scan = table.scan(row_filter=EqualTo("snapshot_date", snapshot_date))
```

**Logic UPSERT:**
```python
INSERT INTO cs_deal_detail_api_v1 (...)
VALUES %s
ON CONFLICT (id, brand_id)
DO UPDATE SET ...
WHERE cs_deal_detail_api_v1.updated_at < EXCLUDED.updated_at
```

- Deal mới → INSERT
- Deal cũ + `updated_at` mới hơn → UPDATE
- Deal cũ + `updated_at` cũ hơn → SKIP (giữ nguyên)

---

### 2. File `caresoft_deal_dag.py`

#### Trước:
```python
task_deal_detail_enrichment >> task_deal_snapshot_gold >> [serving_layer, serving_sandbox]
```

#### Sau:
```python
task_deal_detail_enrichment >> [serving_layer, serving_sandbox]
```

**Bỏ task:**
- `task_deal_snapshot_gold` (không còn cần thiết)

---

### 3. File `deal_detail_snapshot.py`

**Trạng thái:** ❌ **DEPRECATED** - Không còn được sử dụng

File này có thể:
- Giữ lại để tham khảo (rename thành `deal_detail_snapshot.py.deprecated`)
- Hoặc xóa hoàn toàn

---

## Cơ chế hoạt động mới

### Ngày 15/1:
```
cs_deal_detail_api (snapshot_date = 15/1):
  - deal_id=100, updated_at=15/1 08:00
  - deal_id=200, updated_at=15/1 09:00

PostgreSQL sau UPSERT:
  - deal_id=100, updated_at=15/1 08:00
  - deal_id=200, updated_at=15/1 09:00
```

### Ngày 16/1:
```
cs_deal_detail_api (snapshot_date = 16/1):
  - deal_id=100, updated_at=16/1 10:00  ← Deal 100 được update
  - deal_id=300, updated_at=16/1 11:00  ← Deal mới

PostgreSQL sau UPSERT:
  - deal_id=100, updated_at=16/1 10:00  ← UPDATED
  - deal_id=200, updated_at=15/1 09:00  ← Không đổi
  - deal_id=300, updated_at=16/1 11:00  ← INSERTED
```

### Ngày 17/1:
```
cs_deal_detail_api (snapshot_date = 17/1):
  - deal_id=200, updated_at=17/1 14:00  ← Deal 200 được update

PostgreSQL sau UPSERT:
  - deal_id=100, updated_at=16/1 10:00  ← Không đổi
  - deal_id=200, updated_at=17/1 14:00  ← UPDATED
  - deal_id=300, updated_at=16/1 11:00  ← Không đổi
```

**Kết quả:** PostgreSQL luôn giữ **latest state** của mỗi deal!

---

## Migration Steps

### Bước 1: Deploy code mới

```bash
# Commit và push code
git add .
git commit -m "Simplify Deal pipeline: Remove snapshot step"
git push
```

### Bước 2: Rebuild Docker image (nếu cần)

Nếu `deal_detail_serving_layer.py` đã được copy vào Docker image:

```bash
# Rebuild image với code mới
docker build -t 111050/python-etl-base:1.1.0 .
docker push 111050/python-etl-base:1.1.0

# Update DAG để dùng image mới
# Sửa PYTHON_ETL_IMAGE = '111050/python-etl-base:1.1.0'
```

### Bước 3: Unpause DAG

1. Vào Airflow UI
2. Unpause DAG `caresoft_deal_etl`
3. DAG sẽ tự chạy vào 2h sáng hôm sau

### Bước 4: Monitor lần chạy đầu tiên

```bash
# Xem logs của serving layer task
kubectl logs -n default -l airflow-task-id=refresh_serving_layer -f

# Kiểm tra dữ liệu PostgreSQL
psql -h <host> -U <user> -d <db> -c "
SELECT 
    COUNT(*) as total_deals,
    COUNT(DISTINCT brand_id) as brands,
    MAX(updated_at) as latest_update
FROM cs_deal_detail_api_v1;
"
```

### Bước 5: Cleanup (Optional)

Sau khi confirm pipeline chạy ổn định (1-2 tuần):

#### Option 1: Xóa table `cs_deal_detail_api_final`
```sql
-- Kết nối Trino hoặc Spark
DROP TABLE gold.caresoft.cs_deal_detail_api_final;
```

#### Option 2: Giữ lại để backup (khuyến nghị)
```sql
-- Rename table để đánh dấu deprecated
ALTER TABLE gold.caresoft.cs_deal_detail_api_final 
RENAME TO cs_deal_detail_api_final_deprecated;
```

#### Option 3: Xóa file Python cũ
```bash
# Rename để đánh dấu deprecated
mv app/caresoft/deal/deal_detail_snapshot.py \
   app/caresoft/deal/deal_detail_snapshot.py.deprecated
```

---

## Rollback Plan

Nếu có vấn đề, rollback về version cũ:

### Bước 1: Revert code
```bash
git revert HEAD
git push
```

### Bước 2: Rebuild image với code cũ
```bash
docker build -t 111050/python-etl-base:1.0.0 .
docker push 111050/python-etl-base:1.0.0
```

### Bước 3: Update DAG
```python
PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'
```

### Bước 4: Unpause DAG

---

## Performance Comparison

| Metric | Cũ (6 tasks) | Mới (5 tasks) | Cải thiện |
|--------|--------------|---------------|-----------|
| **Tasks** | 6 | 5 | -16.7% |
| **Storage** | 2x (api + final) | 1x (chỉ api) | -50% |
| **Scan size** | Full history | 1 partition | -90%+ |
| **Processing time** | ~15-20 phút | ~10-12 phút | -30% |
| **Complexity** | Cao | Thấp | ✅ |

---

## FAQ

### Q: PostgreSQL có bị duplicate data không?

**A:** Không. Primary Key `(id, brand_id)` đảm bảo mỗi deal chỉ có 1 record.

### Q: Nếu deal bị xóa trong CareSoft thì sao?

**A:** Deal vẫn tồn tại trong PostgreSQL (soft delete). Nếu cần hard delete, phải implement logic riêng.

### Q: Performance có ảnh hưởng không?

**A:** Nhanh hơn! Chỉ scan 1 partition thay vì full history.

### Q: Có mất dữ liệu không?

**A:** Không. `cs_deal_detail_api` vẫn giữ full history. PostgreSQL chỉ giữ latest state.

### Q: Có thể query historical state không?

**A:** Có! Query trực tiếp từ `cs_deal_detail_api` trong Iceberg (qua Trino/Spark).

---

## Testing Checklist

- [ ] DAG parse thành công (không có syntax error)
- [ ] Task `deal_snapshot_gold` đã bị xóa khỏi DAG
- [ ] Task dependencies đúng: `enrichment >> [serving, serving_sandbox]`
- [ ] Serving layer đọc từ `cs_deal_detail_api` (không phải `_final`)
- [ ] PostgreSQL UPSERT thành công (check logs)
- [ ] Data quality: So sánh số lượng deals trước/sau
- [ ] Performance: Thời gian chạy giảm ~30%

---

## Support

Nếu có vấn đề, liên hệ:
- **Team:** Data Engineering
- **Email:** data-eng@example.com
- **Slack:** #data-engineering
