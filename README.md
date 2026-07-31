# HciCode

## data_ops

`data_ops` là dự án **data engineering / ELT** chịu trách nhiệm ingest và xử lý dữ liệu cho hệ thống data lakehouse của tập đoàn (nhiều brand/công ty như Kangnam, SCI, Huong Hoa, PR, VDE...). Đây là tập hợp các **Airflow DAG + Python ETL scripts**, không phải một ứng dụng deploy độc lập: code được rsync lên NFS share và chạy dưới dạng Kubernetes pod bởi một Airflow instance bên ngoài repo.

### Cấu trúc thư mục

```
data_ops/
├── .gitea/workflows/deploy.yaml   # CI/CD: rsync dags/ và app/ lên NFS khi push vào main
├── app/                           # Logic ETL thực thi trong K8s pod (mount qua NFS tại /app)
│   ├── call_functions/            # Đồng bộ trực tiếp ERP Postgres → DWH Postgres, Google Sheets, tỷ giá Techcombank...
│   ├── caresoft/                  # Tích hợp CareSoft CRM/helpdesk theo mô hình Bronze → Silver → Gold
│   ├── common/                    # Thư viện dùng chung: DB helper, bulk-load, API client, config loader
│   ├── credentials/               # Service-account key cho Google Sheets API
│   ├── facebook/                  # Ingest Facebook Ads (campaign, adset, ad, insight, token refresh)
│   ├── jupyter_sync/              # Script đồng bộ Postgres-to-Postgres (khởi nguồn từ notebook cá nhân)
│   ├── maintenance/                # Dọn dẹp/retention cho Iceberg lakehouse (qua Trino)
│   └── scd_type2/                 # Engine Slowly Changing Dimension Type 2, cấu hình qua Postgres
└── dags/                          # Airflow DAG (mirror cấu trúc app/), mỗi DAG orchestrate các bước ETL tương ứng
    ├── call_functions/, caresoft/, facebook/, jupyter_sync/, maintenance/, scd_type2/
    └── dbt/                       # DAG mỏng chỉ gọi `dbt run --select tag:X` vào project dbt bên ngoài (xem data_serving)
```

### Công nghệ sử dụng

- **Orchestration**: Apache Airflow, chạy task qua `KubernetesPodOperator` trên image `python-etl-base`.
- **Lakehouse**: Apache Iceberg (qua PyIceberg, REST catalog theo zone Bronze/Silver/Gold) trên storage MinIO/S3.
- **Query engine**: Trino — chỉ dùng cho maintenance (expire snapshots, remove orphan files).
- **Transform**: dbt (project nằm ở thư mục `data_serving` bên cạnh, `data_ops` chỉ gọi qua DAG).
- **Database**: PostgreSQL cho DWH (`dwh_product_new`, `dwh_sandbox`) và các DB nguồn ERP theo từng brand.
- **API bên ngoài**: CareSoft, Facebook Marketing API, Google Sheets, Techcombank (tỷ giá).

### Luồng dữ liệu

Hai luồng ingest song song:

1. **Lakehouse (Bronze → Silver → Gold → Serving)** — chủ yếu cho CareSoft: dữ liệu API/webhook → Bronze Iceberg → làm sạch/parse → Silver → enrich → Gold → upsert vào PostgreSQL DWH để phục vụ báo cáo (dbt project `data_serving`).
2. **Direct path** (`call_functions`, `facebook`, `jupyter_sync`) — bỏ qua lakehouse, đồng bộ thẳng Postgres-to-Postgres hoặc từ API/Google Sheets vào DWH bằng `COPY` + upsert.

Cấu hình (danh sách brand, bảng ETL, Google Sheet, dimension SCD2) được lưu dưới dạng dữ liệu (Iceberg/Postgres config table) thay vì hardcode, nên thêm brand/bảng mới chỉ cần thêm config, không cần sửa code.

### Deploy

Không có build/test step. Push vào `main` sẽ trigger Gitea Actions rsync `dags/` và `app/` lên NFS server, Airflow tự đọc DAG mới từ NFS mount.

### Lưu ý / hạn chế đã biết

- File credential GCP service-account (`app/credentials/*.json`) đang được commit thẳng vào repo — cần chuyển sang secret management.
- Một số endpoint hạ tầng (IP NFS, MinIO, mật khẩu) bị hardcode trực tiếp trong DAG source.
- Chưa có test tự động; một số script CareSoft có bản trùng lặp theo brand (hậu tố `_thv`) thay vì tham số hoá.

---