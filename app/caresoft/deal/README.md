# CareSoft — Deal Pipeline

Tài liệu mô tả luồng ETL "Deal" (cơ hội bán hàng) lấy từ CareSoft CRM, nằm ở `app/caresoft/deal/` (code) và `dags/caresoft/` (orchestration). Đây là 2 pipeline độc lập chạy trên cùng schema Iceberg `caresoft`:

- **Pipeline A — Deal record ETL**: webhook deal → enrich chi tiết qua API → serving Postgres. DAG: `caresoft_deal_etl` (brand thường: sci/kn/pr/hh) và `caresoft_deal_thv_etl` (brand THV).
- **Pipeline B — Deal Pipelines (stage metadata) ETL**: danh sách pipeline/stage bán hàng. DAG: `caresoft_deal_pipelines_etl` và `caresoft_deal_pipelines_thv_etl`.

Toàn bộ brand thường (sci/kn/pr/hh) chạy chung 1 DAG, loop qua `get_active_brands()`. **THV luôn là DAG + script riêng** (hậu tố `_thv`), không đi qua vòng loop chung — đây là điểm quan trọng cần nhớ khi debug.

## Sơ đồ luồng — Pipeline A (Deal record)

DAG `caresoft_deal_etl` / `caresoft_deal_thv_etl`, lịch chạy **01:30 hàng ngày** (giờ VN), xử lý dữ liệu của **T-1** (`snapshot_date` mặc định = hôm qua).

**Brand thường (sci/kn/pr/hh)** — vẫn qua webhook:
```
CareSoft (webhook) → bảng api_log trong Postgres ERP (sci/kn/pr/hh)
        │
        │  erp_webhook_ingestion.py
        │  - Đọc TOÀN BỘ bảng api_log (không filter theo ngày/loại), stream → PyArrow
        │  - Full load: xoá partition snapshot_date rồi append lại
        ▼
BRONZE   caresoft.wehook_api_caresoft   (brand_etl = 7/1/3/4)
        │
        │  webhook_transformation_silver.py
        │  - Lọc row: snapshot_date = ngày chạy AND input LIKE "{'deal'"
        │  - Parse input (Python-repr, dùng ast.literal_eval) → ép kiểu qua DuckDB
        ▼
SILVER   caresoft.cs_deal_wehook  + caresoft.cs_deal_wehook_error
        │
        │  deal_detail_enrichment.py
        │  - Lấy brand active: get_active_brands()
        │  - So sánh deal_id/updated_at hôm nay vs hôm qua trong Silver → chỉ enrich deal mới/đổi
        │  - GET https://api.caresoft.vn/{brand.caresoft_domain}/api/v1/deal/{deal_id}
        ▼
GOLD     caresoft.cs_deal_detail_api  + caresoft.cs_deal_detail_error_api
        ├─ deal_detail_serving_layer.py → UPSERT (chỉ ghi nếu updated_at mới hơn)
        │        ▼ Postgres: cs_deal_detail_api_v2 (dwh_product_new + dwh_sandbox)
        └─ deal_comments_transformation.py (bóc tách mảng comments trong deal detail)
                 ▼
        GOLD    caresoft.cs_deal_api_trans_comments
                 │  deal_comments_serving_layer.py → DELETE+INSERT theo (brand_id, ticket_id)
                 ▼ Postgres: cs_deal_api_trans_comments
```

**Brand THV** — **không dùng webhook** (xem mục "Sự cố đã xử lý" bên dưới). Deal_id cần enrich được phát hiện trực tiếp qua CareSoft `/deals` list API:
```
CareSoft API: GET https://api.caresoft.vn/{brand.caresoft_domain}/api/v1/deals
              ?updated_since=...&updated_to=...  (+ 2 cửa sổ created_since/created_to để vét late-arrival)
              phân trang page/count (count=500, env CARESOFT_PAGE_SIZE)
        │  deal_detail_enrichment_thv.py :: get_deal_ids_to_enrich() (override)
        │  - Gom deal_id từ /deals theo 3 cửa sổ thời gian (giống pattern ticket_ingestion.py)
        │  - Với từng deal_id: GET /deal/{deal_id} (API chi tiết, lấy đủ comments/tags/custom_fields...)
        ▼
GOLD     caresoft.cs_deal_detail_api_thv  + caresoft.cs_deal_detail_error_api_thv
        ├─ deal_detail_serving_layer_thv.py → UPSERT (chỉ ghi nếu updated_at mới hơn)
        │        ▼ Postgres: cs_deal_detail_api_v2_thv (chỉ dwh_product_new)
        └─ deal_comments_transformation_thv.py (bóc tách mảng comments trong deal detail)
                 ▼
        GOLD    caresoft.cs_deal_api_trans_comments_thv
                 │  deal_comments_serving_layer_thv.py → DELETE+INSERT theo (brand_id, ticket_id)
                 ▼ Postgres: cs_deal_api_trans_comments_thv
```

Task graph Airflow:
```
# Prod (caresoft_deal_etl)
erp_direct_ingestion >> webhook_transformation_silver >> deal_detail_enrichment
    >> [refresh_serving_layer_v2, refresh_serving_layer_v2_sandbox]
    >> deal_comments_transformation
    >> [deal_comments_serving_layer, deal_comments_serving_layer_sandbox]

# THV (caresoft_deal_thv_etl) — không còn task webhook, bắt đầu thẳng ở enrichment
deal_detail_enrichment_thv
    >> [refresh_serving_layer_v2_thv, refresh_serving_layer_v2_sandbox_thv]
    >> deal_comments_transformation_thv
    >> [deal_comments_serving_layer_thv, deal_comments_serving_layer_sandbox_thv]
```
(DAG THV không có nhánh sandbox cho serving layer comment — chỉ prod `dwh_product_new`.)

## Sơ đồ luồng — Pipeline B (Deal Pipelines / stage metadata)

DAG `caresoft_deal_pipelines_etl` / `_thv`, chạy **00:45 hàng ngày** (trước Pipeline A 45 phút, không phụ thuộc nhau).

```
CareSoft API: GET https://api.caresoft.vn/{brand.caresoft_domain}/api/v1/deal/pipelines
        │  deal_pipelines_ingestion.py / _thv.py — lưu nguyên trạng (stages = JSON blob), theo từng brand active
        ▼
BRONZE   caresoft.cs_deal_pipelines_api      (prod)
         caresoft.cs_deal_pipelines_api_thv  (THV)
        │  deal_pipelines_serving_layer.py / _thv.py — DuckDB dedup theo (id, brand_id) lấy bản mới nhất, upsert
        ▼
Postgres (chỉ dwh_product_new, không có sandbox): cs_deal_pipelines / cs_deal_pipelines_thv
```

## Bảng tra cứu tên bảng theo brand

| Layer | Prod (sci/kn/pr/hh) | THV |
|---|---|---|
| Bronze (webhook) | `caresoft.wehook_api_caresoft` | `caresoft.wehook_api_caresoft_thv` |
| Silver (deal) | `caresoft.cs_deal_wehook` (+`_error`) | `caresoft.cs_deal_wehook_thv` (+`_error_thv`) |
| Gold (deal detail) | `caresoft.cs_deal_detail_api` (+`_error_api`) | `caresoft.cs_deal_detail_api_thv` (+`_error_api_thv`) |
| Gold (comments) | `caresoft.cs_deal_api_trans_comments` | `caresoft.cs_deal_api_trans_comments_thv` |
| Serving (deal detail) | Postgres `cs_deal_detail_api_v2` | Postgres `cs_deal_detail_api_v2_thv` |
| Serving (comments) | Postgres `cs_deal_api_trans_comments` | Postgres `cs_deal_api_trans_comments_thv` |
| Bronze (pipelines) | `caresoft.cs_deal_pipelines_api` | `caresoft.cs_deal_pipelines_api_thv` |
| Serving (pipelines) | Postgres `cs_deal_pipelines` | Postgres `cs_deal_pipelines_thv` |

Brand config (`caresoft.core_brand`, Gold): THV = `erp_id = 8`.

---

## ✅ Sự cố đã xử lý: DWH không có dữ liệu deal của THV (2026-07-28)

### Root cause (đã xác nhận qua Trino)

Điều tra theo từng layer: Bronze `caresoft.wehook_api_caresoft_thv` có dữ liệu đều đặn (~9000 dòng/ngày), nhưng khi lọc theo filter mà `webhook_transformation_silver_thv.py` dùng (`input LIKE '{'deal'...'`) thì **khớp 0 dòng**. Kiểm tra sâu hơn: toàn bộ dữ liệu Bronze của THV chỉ gồm 1 loại webhook (`model_id = 509`, nội dung số điện thoại/contact — không phải deal).

→ **CareSoft chưa được cấu hình gửi webhook loại "deal" về ERP của brand THV.** Đây không phải bug trong `data_ops`, mà là thiếu cấu hình ở phía tích hợp CareSoft/ERP cho THV — pipeline `erp_webhook_ingestion_thv.py` → `webhook_transformation_silver_thv.py` → `deal_detail_enrichment_thv.py` (dựa vào `deal_id` từ Silver webhook) do đó không bao giờ có việc để làm cho THV, dù chạy "thành công" mỗi ngày.

Một yếu tố khiến sự cố không bị phát hiện sớm: cả `erp_webhook_ingestion_thv.py` và `deal_detail_enrichment_thv.py::main()` đều **không** `sys.exit(1)` khi không có dữ liệu/lỗi — Airflow luôn hiện task "success" bất kể có ghi được dữ liệu hay không.

### Fix đã triển khai

CareSoft cung cấp thêm endpoint list `GET /{domain}/api/v1/deals` hỗ trợ `updated_since/updated_to`, `created_since/created_to`, phân trang (`page`, `count` tối đa 500) — cùng pattern đã dùng ở `ticket_ingestion.py`. Đã sửa để THV **không phụ thuộc webhook nữa**:

1. **`deal_detail_enrichment_thv.py`** — override `get_deal_ids_to_enrich()`: thay vì đọc Silver webhook (luôn rỗng), gọi trực tiếp `GET /deals` theo 3 cửa sổ thời gian (giống `ticket_ingestion.py`) để tự phát hiện deal_id đã thay đổi:
   - `updated_since/updated_to` = hôm qua → hôm nay
   - `created_since/created_to` = hôm qua → hôm nay
   - `created_since/created_to` = hôm kia → hôm qua (vét late-arrival)

   Sau khi có tập `deal_id`, phần còn lại **giữ nguyên**: vẫn gọi `GET /deal/{deal_id}` cho từng ID để lấy đủ chi tiết (response của `/deals` list không có `comments`/`tags`/`custom_fields`/`assignee`/`requester` object — các trường này chỉ có ở API chi tiết, cần cho `deal_comments_transformation_thv.py` ở bước sau).

2. **`dags/caresoft/caresoft_deal_thv_dag.py`** — bỏ 2 task `erp_direct_ingestion_thv` và `webhook_transformation_silver_thv` (không còn tác dụng), DAG giờ bắt đầu thẳng ở `deal_detail_enrichment_thv`. Thêm env `CARESOFT_PAGE_SIZE=500` cho task này. Bỏ luôn phần load Airflow Connection `sci_erp_vde` (không còn dùng tới).

Task graph mới của DAG THV: xem mục "Sơ đồ luồng" phía trên (đã cập nhật).

### Việc cần làm thêm (không thuộc phạm vi code)

- **Chưa deploy**: thay đổi trên nằm trong working tree, cần review + `git push` để trigger `.gitea/workflows/deploy.yaml` rsync lên NFS, rồi Airflow mới nhận DAG mới.
- **Backfill dữ liệu cũ**: cách fix mới chỉ phát hiện deal thay đổi trong cửa sổ vài ngày gần nhất từ lần chạy đầu tiên trở đi. Deal cũ hơn (tạo/cập nhật trước khi fix được deploy) sẽ **không tự động có trong DWH**.
  Đã cấu hình sẵn `dags/caresoft/caresoft_thv_backfill_dag.py` (DAG chung, trigger tuần tự 1 DAG đích theo từng ngày trong khoảng) với `START_DATE_STR=2026-01-01`, `END_DATE_STR=2026-07-27` (208 ngày). Sau khi deploy, trigger DAG `caresoft_thv_backfill_dag` thủ công với param:
  ```json
  {"target_dag_id": "caresoft_deal_thv_etl"}
  ```
  DAG sẽ tự trigger `caresoft_deal_thv_etl` cho từng ngày trong khoảng, chờ (`wait_for_completion=True`) từng ngày chạy xong mới sang ngày kế — chạy tuần tự nên sẽ mất nhiều thời gian (mỗi ngày = đủ 4 task: enrichment gọi `/deals` + `/deal/{id}` → serving → comments transform → comments serving). Nên theo dõi log để phát hiện sớm nếu có ngày bị lỗi/API rate-limit.
- Nên cân nhắc báo lại đội quản lý tích hợp CareSoft để bật webhook "deal" cho THV nếu muốn dữ liệu near-real-time thay vì chờ lịch chạy DAG (01:30 hàng ngày).
- Vấn đề nền "script không exit(1) khi lỗi/rỗng" vẫn còn tồn tại ở các module CareSoft khác (không chỉ deal) — nên cân nhắc dọn sau, ngoài phạm vi fix lần này.

---

## Các helper dùng chung (`app/common/`)

- `brand_config.py`: `get_active_brands()` (brand thường), `get_brand_by_erp_id(erp_id)` (THV, bỏ qua `is_active`) — đọc từ `caresoft.core_brand` (Gold).
- `caresoft_api.py`: `get_api_url()`, `get_api_headers()`, `call_caresoft_api()` — build URL `https://api.caresoft.vn/{domain}/api/v1/{path}` + header `Authorization: Bearer {key}`.
- `utils.py`: `get_current_timestamp_naive()` (UTC+7), `bulk_load()` — COPY + temp-table upsert/delete-insert dùng ở mọi serving layer.

## Ghi chú chung

- Mọi bảng Iceberg partition theo `snapshot_date`, ghi theo kiểu "xoá partition (+brand_id nếu có) rồi append" — idempotent nhưng không phải append-only; chạy lại cùng ngày sẽ ghi đè, không cộng dồn.
- Toàn bộ code brand THV là **subclass/monkeypatch** của code prod (không tham số hoá qua config), nghĩa là mỗi lần sửa logic chung phải sửa cả 2 nơi — dễ lệch nhau (rủi ro kỹ thuật đã biết, xem `data_ops/README.md`).
