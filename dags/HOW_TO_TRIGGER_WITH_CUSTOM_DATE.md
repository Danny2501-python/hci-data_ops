# Hướng dẫn Trigger DAG với Custom Date trong Airflow 2.9.1

## ✅ Đã Fix: Bây giờ có Form khi Trigger!

Sau khi cập nhật DAG, khi bạn click nút **Play (▶️)** trong Airflow UI, sẽ hiện **popup form** với ô nhập `snapshot_date`.

---

## Cách Trigger DAG với Custom Date

### 1. Từ Airflow UI (Khuyến nghị)

#### Bước 1: Mở DAG
- Truy cập Airflow UI
- Click vào tên DAG (`caresoft_deal_etl` hoặc `caresoft_ticket_etl`)

#### Bước 2: Click nút Trigger
- Ở góc trên bên phải, click nút **Play (▶️)**
- Sẽ hiện popup **"Trigger DAG"**

#### Bước 3: Nhập snapshot_date
- Trong popup, bạn sẽ thấy form với field:
  ```
  snapshot_date: [2026-01-16]  ← Giá trị mặc định (T-1)
  ```
- **Để mặc định:** Giữ nguyên giá trị → Xử lý dữ liệu ngày hôm qua
- **Thay đổi ngày:** Nhập ngày khác (format: `YYYY-MM-DD`)
  - Ví dụ: `2026-01-10` để xử lý dữ liệu ngày 10/1

#### Bước 4: Trigger
- Click nút **"Trigger"**
- DAG sẽ chạy với ngày bạn đã chọn

---

### 2. Ví dụ minh họa UI

```
┌────────────────────────────────────────────────────┐
│ Trigger DAG: caresoft_deal_etl                     │
├────────────────────────────────────────────────────┤
│                                                    │
│ Configuration:                                     │
│                                                    │
│ snapshot_date *                                    │
│ ┌────────────────────────────────────────────┐    │
│ │ 2026-01-16                                 │    │  ← Mặc định T-1
│ └────────────────────────────────────────────┘    │
│                                                    │
│ Snapshot date to process (YYYY-MM-DD).             │
│ Default: T-1 (yesterday)                           │
│                                                    │
│                                                    │
│                        [Cancel]  [Trigger]         │
└────────────────────────────────────────────────────┘
```

---

### 3. Xem Params sau khi Trigger

Sau khi DAG chạy, kiểm tra params đã được apply:

1. Vào DAG Run (click vào hình vuông màu xanh/đỏ trong Grid view)
2. Tab **"Details"** → Section **"Configuration"**
3. Bạn sẽ thấy:
   ```json
   {
     "snapshot_date": "2026-01-16"
   }
   ```

---

## Cách Trigger qua CLI (Alternative)

Nếu không dùng UI, có thể trigger qua kubectl:

### PowerShell
```powershell
# Trigger với ngày mặc định (T-1)
kubectl exec -n default deployment/airflow-scheduler -- `
    airflow dags trigger caresoft_deal_etl

# Trigger với ngày cụ thể
kubectl exec -n default deployment/airflow-scheduler -- `
    airflow dags trigger caresoft_deal_etl `
    --conf '{\"snapshot_date\": \"2026-01-10\"}'
```

### Bash
```bash
# Trigger với ngày mặc định (T-1)
kubectl exec -n default deployment/airflow-scheduler -- \
    airflow dags trigger caresoft_deal_etl

# Trigger với ngày cụ thể
kubectl exec -n default deployment/airflow-scheduler -- \
    airflow dags trigger caresoft_deal_etl \
    --conf '{"snapshot_date": "2026-01-10"}'
```

---

## Cách Trigger qua REST API

```python
import requests

AIRFLOW_URL = "http://your-airflow-url:8080"
USERNAME = "admin"
PASSWORD = "your-password"

# Trigger với ngày cụ thể
response = requests.post(
    f"{AIRFLOW_URL}/api/v1/dags/caresoft_deal_etl/dagRuns",
    json={"conf": {"snapshot_date": "2026-01-10"}},
    auth=(USERNAME, PASSWORD)
)

print(response.json())
```

---

## Kiểm tra Logs để Verify

Sau khi trigger, xem logs để confirm ngày đã được apply:

```bash
# Xem logs của task đầu tiên
kubectl logs -n default -l airflow-task-id=erp_direct_ingestion --tail=50

# Tìm dòng log:
# "Processing snapshot for date: 2026-01-10"
```

---

## Troubleshooting

### Vấn đề: Vẫn không thấy form khi click Trigger

**Nguyên nhân:** DAG chưa được reload sau khi update

**Giải pháp:**
1. Unpause → Pause lại DAG trong UI
2. Hoặc restart Airflow Scheduler:
   ```bash
   kubectl rollout restart deployment/airflow-scheduler -n default
   ```
3. Refresh trang web (Ctrl+F5)

### Vấn đề: Form hiện nhưng không có giá trị mặc định

**Nguyên nhân:** Airflow chưa evaluate `datetime.now()` đúng cách

**Giải pháp:** Giá trị mặc định vẫn hoạt động khi trigger, chỉ là UI không hiển thị. Bạn có thể:
- Để trống → Sẽ dùng T-1
- Nhập ngày cụ thể

### Vấn đề: Task chạy với ngày sai

**Nguyên nhân:** Params không được truyền đúng

**Giải pháp:**
1. Kiểm tra DAG Run → Details → Configuration
2. Xem logs của task để confirm ngày
3. Nếu sai, clear task và trigger lại với params đúng

---

## Best Practices

1. **Luôn kiểm tra ngày trước khi trigger** để tránh xử lý sai dữ liệu
2. **Sử dụng format YYYY-MM-DD** (ISO 8601) cho snapshot_date
3. **Backfill nhiều ngày:** Trigger từng ngày một, không nên chạy song song
4. **Monitor logs** sau khi trigger để confirm ngày đã đúng
5. **Document lý do** khi backfill dữ liệu (comment trong Airflow UI)

---

## Tóm tắt

| Phương thức | Ưu điểm | Nhược điểm |
|-------------|---------|------------|
| **UI Form** | Trực quan, dễ dùng, có validation | Cần access UI |
| **CLI** | Tự động hóa được, script được | Cần kubectl access |
| **REST API** | Tích hợp được vào hệ thống khác | Cần setup authentication |

**Khuyến nghị:** Dùng UI Form cho manual trigger, dùng CLI/API cho automation.
