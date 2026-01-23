# Tài Liệu Giải Thích: Bronze Ingestion Script

## 1. Tổng Quan
Script `bronze_ingestion.py` là một **ETL Job** có nhiệm vụ trích xuất dữ liệu từ các hệ thống nguồn (PostgreSQL) và nạp vào **Bronze Layer** (Tầng dữ liệu thô) của Data Lakehouse dưới định dạng **Iceberg Table**.

### Điểm nổi bật:
- **Generic Engine**: Một script duy nhất xử lý cho nhiều bảng khác nhau dựa trên cấu hình (Metadata Driven).
- **Streaming Ingestion**: Sử dụng cơ chế Streaming và PyArrow để xử lý lượng dữ liệu lớn mà không tốn RAM (tránh lỗi Out Of Memory).
- **Schema Evolution**: Tự động phát hiện và cập nhật schema của bảng đích dựa trên dữ liệu nguồn.
- **Idempotent**: Có thể chạy lại nhiều lần mà không gây trùng lặp dữ liệu (nhờ cơ chế Partition Overwrite).

---

## 2. Luồng Xử Lý Chính (Workflow)
Quy trình nạp dữ liệu cho một bảng diễn ra qua các bước sau:

1.  **Đọc Cấu Hình**: Lấy thông tin các bảng cần chạy từ `common.etl_table_config` (được lưu trong Gold Layer).
2.  **Kết Nối Nguồn**: Tạo kết nối an toàn đến Database nguồn.
3.  **Lấy Schema**: Truy vấn metadata (tên cột, kiểu dữ liệu) từ nguồn (không tải dữ liệu).
4.  **Chuẩn Bị Bảng Đích**:
    *   Kiểm tra bảng Iceberg đích đã tồn tại chưa.
    *   Nếu chưa -> Tạo mới với schema tương ứng.
    *   Nếu mode là `truncate` -> Xóa dữ liệu cũ của ngày đang chạy.
5.  **Streaming Data**:
    *   Mở kết nối dạng Stream đến Database nguồn.
    *   Lấy từng lô (batch) 50,000 dòng.
    *   Chuyển đổi sang định dạng PyArrow (tối ưu bộ nhớ).
    *   Ghi trực tiếp vào Iceberg.
    *   Lặp lại đến khi hết dữ liệu.

---

## 3. Giải Thích Chi Tiết Từng Hàm

### 3.1. `process_table` (Hàm Điều Phối)
Đây là "nhạc trưởng", điều phối toàn bộ các bước cho một bảng cụ thể.
- **Input**: Config của bảng (tên bảng, nguồn, điều kiện lọc...).
- **Logic**:
    1.  Gọi `create_source_connection` để kết nối DB.
    2.  Chạy query `SELECT ... WHERE 1=0` để lấy **Schema** (DataFrame rỗng).
    3.  Gọi `ensure_bronze_table_exists` để tạo bảng nếu cần.
    4.  Gọi `stream_and_ingest` để bắt đầu nạp dữ liệu thật.

### 3.2. `stream_and_ingest` (Trái Tim Của Script)
Hàm này thực hiện việc chuyển dữ liệu khối lượng lớn (Heavy Lifting).
- **Cơ chế**: Thay vì tải 1 triệu dòng vào RAM (như Pandas thường làm), hàm này mở một "vòi nước" (cursor) từ Database.
- **Batch Processing**: Hứng từng xô nước (50k dòng), đóng gói lại và cất vào kho (Iceberg), sau đó đổ xô đi hứng tiếp.
- **Ưu điểm**:
    - RAM sử dụng ổn định, thấp (chỉ tốn khoảng vài trăm MB dù bảng nặng hàng chục GB).
    - Tốc độ cao nhờ PyArrow (xử lý dạng cột - Columnar).

### 3.3. `ensure_bronze_table_exists`
Đảm bảo bảng đích luôn sẵn sàng.
- **Tự động Map kiểu**:
    - `Integer` (SQL) -> `Long` (Iceberg)
    - `Datetime` (SQL) -> `Timestamp` (Iceberg)
    - Còn lại -> `String` (để an toàn, tránh lỗi parse).
- **Metadata Columns**: Tự động thêm 3 cột quản lý:
    - `snapshot_date`: Ngày của dữ liệu (Dùng để Partition).
    - `ingestion_timestamp`: Thời điểm chạy.
    - `source_server`: Nguồn dữ liệu.

### 3.4. `create_source_connection`
- **Bảo mật**: Đọc cấu hình JSON từ biến môi trường.
- **An toàn**: Sử dụng `SQLAlchemy URL.create` để xử lý các ký tự đặc biệt trong mật khẩu (như `@`, `!`), tránh lỗi kết nối phổ biến.

---

## 4. Các Chế Độ Chạy (Load Types)

Script hỗ trợ 2 chế độ, được điều khiển bởi tham số `load_type`:

| Chế Độ | Logic | Ứng Dụng |
| :--- | :--- | :--- |
| **Incremental** (Mặc định) | Lọc dữ liệu theo ngày: `date(col) >= T-10`. <br> Nếu bảng không có cột ngày -> Tự động chuyển về Full Load. | Chạy hàng ngày để lấy dữ liệu mới sinh ra. |
| **Full** | Lấy toàn bộ dữ liệu thỏa mãn điều kiện `consider`. | Chạy định kỳ (ví dụ cuối tuần) hoặc khi cần reload lại toàn bộ lịch sử. |

---

## 5. Metadata Driven
Script này không chứa logic cứng ("hardcode") cho từng bảng. Mọi thứ được điều khiển bởi bảng cấu hình `etl_metadata.table_config`.
Muốn thêm bảng mới? Chỉ cần INSERT thêm 1 dòng vào bảng cấu hình, không cần sửa code!

**Ví dụ cấu hình 1 dòng:**
```json
{
  "table_name": "crm_call_log",
  "source_db": "ERP_SCI",
  "source_table": "public.call_logs",
  "keyword": "created_at",  // Cột dùng để lọc Incremental
  "rename_column": {"id": "call_id"} // Đổi tên cột
}
```
