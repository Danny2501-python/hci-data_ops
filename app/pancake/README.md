# Pancake – lakehouse pipeline

Kéo dữ liệu từ [Pancake API](https://developer.pancake.biz) theo kiến trúc lakehouse của hệ thống:

```
Pancake API ─► BRONZE (Iceberg, JSON gốc) ─► SILVER (Iceberg, có kiểu, khử trùng) ─► GOLD (Iceberg, sẵn sàng báo cáo)
                                                                                         │
                                                       Postgres mart (dwh_product_new) ◄─┘ ─► dbt (data_serving) ─► Tableau
```

Lakehouse dùng MinIO + Iceberg REST catalog (`bronze` / `silver` / `gold`), namespace `pancake`. Đọc/ghi bằng PyIceberg trong pod Airflow (`KubernetesPodOperator`), giống pipeline CareSoft.

## File

| File | Vai trò |
|---|---|
| `app/common/pancake_api.py` | Client API: token qua query param, giãn cách ≤ 5 req/giây/page, retry 429/5xx, raise khi 4xx |
| `app/pancake/pancake_lakehouse.py` | Cấu hình catalog, **schema mọi bảng Bronze/Silver/Gold**, ghi đè theo partition `snapshot_date` |
| `app/pancake/pancake_transforms.py` | Logic biến đổi thuần Python (Silver, Gold), không I/O, dễ test |
| `app/pancake/pancake_bronze_ingestion.py` | API → `bronze.pancake.*_api` |
| `app/pancake/pancake_silver_transformation.py` | Bronze → `silver.pancake.*` |
| `app/pancake/pancake_gold_transformation.py` | Silver → `gold.pancake.*` |
| `app/pancake/pancake_serving_layer.py` | Gold → Postgres `public.pancake_*` (UPSERT, cột có kiểu đúng) |
| `dags/pancake/pancake_etl_dag.py` | DAG `pancake_etl`, 02:30 hằng ngày, mỗi entity là 1 TaskGroup |

## Bảng theo tầng

| Entity | Bronze | Silver | Gold | Postgres mart |
|---|---|---|---|---|
| conversations | `conversations_api` | `conversation` | `fct_conversation` | `pancake_conversation` |
| messages | `messages_api` | `message` | `fct_message` | `pancake_message` |
| customers | `page_customers_api` | `page_customer` | `dim_page_customer` | `pancake_page_customer` |
| statistics | `statistics_api` | `statistics`, `stat_page_hourly`, `stat_user_daily` | `fct_statistics`, `fct_stat_page_hourly`, **`agg_stat_page_daily`**, `fct_stat_user_daily` | `pancake_statistics`, `pancake_stat_page_hourly`, `pancake_stat_page_daily`, `pancake_stat_user_daily` |

- **Bronze:** giữ nguyên JSON trả về trong cột `payload`, kèm `page_id`, `record_id`, cửa sổ thời gian đã gọi và `brand_id`.
- **Silver:** tách JSON ra thành các cột có kiểu, giờ **UTC**, khử trùng theo khoá (giữ bản `updated_at` mới nhất).
- **Gold:** thêm giờ VN (`*_vn`, `inserted_date_vn`), các cờ nghiệp vụ (`is_from_ad`, `first_ad_id`, `first_phone`, `sender_role`) và bảng tổng hợp thống kê page theo ngày VN.
- **Postgres:** khoá chính theo từng bảng; cột JSON dùng `jsonb`. Lakehouse chỉ giữ dữ liệu ngắn hạn, còn Postgres tích luỹ toàn bộ lịch sử.

Mọi bảng Iceberg đều partition theo `snapshot_date` (ngày dữ liệu, giờ VN). Mỗi lần chạy sẽ xoá partition cũ rồi ghi lại, nên chạy lại cùng một ngày không bị trùng dữ liệu.

## DAG `pancake_etl`

```
conversations: bronze ─► silver ─► gold ─► serving (prod) + serving_sandbox
      │
      └─► messages: bronze ─► silver ─► gold ─► serving + serving_sandbox
customers:     bronze ─► silver ─► gold ─► serving + serving_sandbox
statistics:    bronze ─► silver ─► gold ─► serving + serving_sandbox
```

Params:
- `snapshot_date`: bỏ trống thì lấy hôm qua.
- `days`: số ngày lùi lại khi gọi API, mặc định 3, để bắt các dữ liệu cập nhật trễ.
- `page_id`: chỉ chạy Bronze cho 1 page.

Page nào lỗi (token sai, thiếu quyền…) thì task **FAILED**, nhưng dữ liệu của các page khác vẫn được ghi.

## Cài đặt lần đầu

1. **Khai báo page và token.** Lần đầu chạy, job tự tạo bảng `public.core_pancake_page`. Lấy `page_access_token` trong Pancake ở **Cài đặt → Công cụ** của từng page; token này không hết hạn.

```sql
INSERT INTO public.core_pancake_page (page_id, page_name, platform, brand_id, page_access_token, is_active)
VALUES ('<page_id>', '<tên page>', 'facebook', 1, '<page_access_token>', true)
ON CONFLICT (page_id) DO UPDATE
SET page_access_token = EXCLUDED.page_access_token, is_active = true, updated_at = now();
```

`brand_id` dùng mã ERP: 1 = KN, 3 = PR, 4 = HH, 8 = THV. **Không** gọi API "generate page_access_token", vì gọi API đó sẽ vô hiệu hoá token cũ của page.

2. **MinIO.** DAG đọc access key từ Airflow Connection `minio_logs_conn` (login/password, `extra.endpoint_url`), là connection Airflow đang dùng để ghi log. Không cần hardcode key trong code.

3. **Retention.** DAG `maintenance_iceberg_retention` hiện chỉ dọn namespace `caresoft`. Cần thêm namespace `pancake` vào DAG đó để lakehouse không phình mãi. Postgres vẫn giữ đủ lịch sử nên không mất dữ liệu.

## Chạy tay / backfill

Trigger DAG với `snapshot_date` và `days` (tối đa 31), hoặc chạy trong pod:

```
python /app/pancake/pancake_bronze_ingestion.py --entity conversations --snapshot-date 2026-09-30 --days 30
python /app/pancake/pancake_silver_transformation.py --entity conversations --snapshot-date 2026-09-30
python /app/pancake/pancake_gold_transformation.py --entity conversations --snapshot-date 2026-09-30
python /app/pancake/pancake_serving_layer.py --entity conversations --snapshot-date 2026-09-30
```

## Cần kiểm tra khi chạy với dữ liệu thật

- Cách phân trang tin nhắn (`current_count`) đang làm theo mô tả trong tài liệu. Cần so số tin lấy được với `message_count` của vài hội thoại.
- Các API thống kê `ads`, `pages_campaigns`, `users`… cần token có quyền xem báo cáo. Loại nào bị từ chối sẽ có log cảnh báo và task báo FAILED, nhưng các loại khác vẫn được lưu.
- Định dạng thời gian thật của Pancake (UTC hay giờ local): `parse_ts` đang coi chuỗi không có múi giờ là UTC.
