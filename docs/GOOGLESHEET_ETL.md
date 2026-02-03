# Google Sheet ETL - Dedicated Configuration

## Overview

Luồng Google Sheet ETL sử dụng **bảng cấu hình riêng biệt** `declare_api_googlesheet` để tuân thủ logic gốc của hệ thống cũ.

## Architecture

```
Google Sheets → Bronze (Iceberg) → [Manual/Scheduled] → Serving (PostgreSQL)
                     ↑
         declare_api_googlesheet (Gold Iceberg)
```

## Configuration Table

### Schema: `call_functions.declare_api_googlesheet`

Bảng này được lưu trong **Iceberg Gold layer** và giữ nguyên schema từ PostgreSQL cũ:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `id` | int | Primary Key | 1 |
| `name` | string | Tên hiển thị | "Budget 2024" |
| `spreadsheet_id` | string | Google Sheet ID | "1ABC...XYZ" |
| `sheet` | string | Tên sheet | "Sheet1" |
| `table_data` | string | Tên bảng đích | "budget_2024" |
| `method_data` | boolean | True=Truncate, False=Upsert | false |
| `remove_column` | string | Cột cần xóa (CSV) | "col1,col2" |
| `rename_column` | string | Đổi tên cột (JSON) | `{"old":"new"}` |
| `schedule` | string | Lịch chạy | "0 1 * * *" |
| `active_schedule` | boolean | Chạy theo lịch? | true |
| `active_daily` | boolean | Chạy hàng ngày? | true |
| `active` | boolean | Kích hoạt | true |
| `key` | string | Primary key (CSV) | "id,month" |
| `loai` | string | Hướng: IN/OUT | "IN" |
| `query` | string | Custom query (for OUT) | null |
| `creator_id` | int | Người tạo | 1 |
| `updater_id` | int | Người cập nhật | 1 |
| `ghi_chu` | string | Ghi chú | "" |
| `result` | string | Kết quả chạy | "Success" |
| `update_time` | timestamp | Thời gian chạy | 2026-02-03 10:00:00 |

### Field Details

#### `loai` (Direction)
- **`IN`**: Google Sheet → Database (Import)
- **`OUT`**: Database → Google Sheet (Export - not implemented yet)

#### `method_data` (Load Method)
- **`true`**: Truncate toàn bộ dữ liệu trước khi load
- **`false`**: Upsert dựa trên `key`

#### `key` (Primary Key)
- Dùng để xác định duplicate rows khi upsert
- Format: CSV string, ví dụ: `"id"` hoặc `"id,month"`

## Migration from Old System

### 1. Run Migration Script

```bash
# Set environment variable
export OLD_POSTGRES_URI="postgresql://user:password@old-db:5432/etl_marketing"

# Run migration
python scripts/migrate_googlesheet_config.py
```

Script sẽ:
- Đọc toàn bộ records từ `declare_api_googsheet` (PostgreSQL)
- Ghi vào `call_functions.declare_api_googlesheet` (Iceberg Gold)
- Giữ nguyên 100% schema và data

### 2. Verify Migration

```python
from common.googlesheet_config import get_active_googlesheet_configs

# Get all IN configs
configs = get_active_googlesheet_configs(loai='IN')
print(f"Found {len(configs)} configurations")

# Get specific sheet
config = get_active_googlesheet_configs(sheet_id=123)
```

## Usage

### Manual Run

```bash
# Run Bronze ingestion for all active sheets
python app/call_functions/googlesheet_ingestion.py "2026-02-03"

# Run for specific sheet ID
python app/call_functions/googlesheet_ingestion.py "2026-02-03" "123"
```

### Airflow DAG

```bash
# Trigger via Airflow UI
# DAG: googlesheet_lakehouse_etl

# Or via CLI
airflow dags trigger googlesheet_lakehouse_etl \
  --conf '{"snapshot_date": "2026-02-03", "sheet_id": 123}'
```

## Logic Comparison

### Old System (Django)

```python
# declare_api_googlesheet.py
def call_api_googlesheet(self):
    # 1. Read Google Sheet
    data_matrix = sheet.get_all_values()
    df = pd.DataFrame(data_matrix[1:], columns=data_matrix[0])
    
    # 2. Transform
    if self.remove_column:
        df = df.drop(columns=self.remove_column.split(','))
    if self.rename_column:
        df = df.rename(columns=json.loads(self.rename_column))
    df.columns = df.columns.str.lower()
    
    # 3. Load to PostgreSQL
    if self.method_data:  # Truncate
        cursor.execute(f"DELETE FROM {self.table_data}")
    if self.key:  # Upsert
        cursor.execute(f"DELETE FROM {self.table_data} WHERE key IN ...")
    cursor.execute(f"INSERT INTO {self.table_data} SELECT * FROM staging")
```

### New System (Lakehouse)

```python
# googlesheet_ingestion.py
def process_sheet(self, config):
    # 1. Read Google Sheet (same logic)
    data_matrix = worksheet.get_all_values()
    df = pd.DataFrame(data_matrix[1:], columns=data_matrix[0])
    
    # 2. Transform (same logic)
    if config['remove_column']:
        df = df.drop(columns=config['remove_column'].split(','))
    if config['rename_column']:
        df = df.rename(columns=json.loads(config['rename_column']))
    df.columns = df.columns.str.lower()
    
    # 3. Load to Iceberg Bronze
    df['snapshot_date'] = self.snapshot_date
    if config['method_data']:  # Truncate
        table.delete(delete_filter=AlwaysTrue())
    else:  # Incremental
        table.delete(delete_filter=EqualTo("snapshot_date", self.snapshot_date))
    table.append(arrow_table)
```

## Key Differences

| Aspect | Old System | New System |
|--------|-----------|------------|
| **Config Table** | PostgreSQL | Iceberg Gold |
| **Target Storage** | PostgreSQL directly | Iceberg Bronze → PostgreSQL |
| **Versioning** | No | Yes (Iceberg snapshots) |
| **Partitioning** | No | By snapshot_date |
| **Truncate** | DELETE FROM table | Iceberg delete(AlwaysTrue()) |
| **Upsert** | Staging table + DELETE + INSERT | Handled in Serving layer |

## Benefits of New Architecture

1. **Data Versioning**: Iceberg snapshots allow time travel
2. **Separation of Concerns**: Bronze (raw) vs Serving (transformed)
3. **Scalability**: Iceberg handles large datasets better
4. **Auditability**: All changes tracked in Iceberg metadata
5. **Flexibility**: Can rebuild Serving layer from Bronze anytime

## Limitations

1. **No OUT direction yet**: Export (DB → Google Sheet) not implemented
2. **Two-step process**: Bronze → Serving (vs direct load in old system)
3. **Additional storage**: Data stored in both Iceberg and PostgreSQL

## Troubleshooting

### Issue: Config not found

```bash
# Check if migration ran
python -c "from common.googlesheet_config import get_active_googlesheet_configs; print(len(get_active_googlesheet_configs()))"
```

## Configuration

### 1. Google Sheets Credentials

Credentials file đã được đặt trong NFS volume:

```bash
# File location in NFS
/mnt/nfs/data-ops/app/credentials/google-credentials.json

# Mounted in pod as
/app/credentials/google-credentials.json
```

**Note:** Không cần tạo Kubernetes Secret vì credentials đã nằm trong NFS volume được mount vào pod.

### Issue: Table not created in Iceberg

```bash
# Check Bronze catalog
python -c "from pyiceberg.catalog import load_catalog; catalog = load_catalog('bronze', uri='http://iceberg-rest:8181'); print(catalog.list_tables('call_functions'))"
```

## Next Steps

1. ✅ Migrate configurations
2. ✅ Test Bronze ingestion
3. ⬜ Implement Serving layer sync
4. ⬜ Add OUT direction support (DB → Google Sheet)
5. ⬜ Add scheduling based on `active_schedule` and `schedule` fields
