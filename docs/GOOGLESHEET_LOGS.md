# Google Sheet Ingestion - Log Output Example

## Sample Log Output

Khi chạy `googlesheet_ingestion.py`, bạn sẽ thấy log như sau:

```
2026-02-03 14:30:00.123 | INFO     | Starting Google Sheet Ingestion for 2026-02-03
2026-02-03 14:30:00.456 | INFO     | Loaded 3 Google Sheet configurations

2026-02-03 14:30:00.789 | INFO     | 📊 Processing Google Sheet: Budget 2024 -> budget_2024
2026-02-03 14:30:00.790 | INFO     | 🔗 Sheet Link: https://docs.google.com/spreadsheets/d/1ABC...XYZ
2026-02-03 14:30:00.791 | INFO     | 📄 Sheet Name: Sheet1
2026-02-03 14:30:00.792 | INFO     | 🗑️  Remove columns: notes,comments
2026-02-03 14:30:00.793 | INFO     | ✏️  Rename columns: {"Month": "month", "Amount": "amount"}
2026-02-03 14:30:00.794 | INFO     | Connected to Google Sheets API
2026-02-03 14:30:01.234 | INFO     | Reading Google Sheet: 1ABC...XYZ/Sheet1
2026-02-03 14:30:02.567 | INFO     | Removed columns: ['notes', 'comments']
2026-02-03 14:30:02.568 | INFO     | Renamed columns: {'Month': 'month', 'Amount': 'amount'}
2026-02-03 14:30:02.569 | INFO     | Read 120 rows with columns: ['month', 'department', 'amount']
2026-02-03 14:30:02.789 | INFO     | Table call_functions.budget_2024 already exists
2026-02-03 14:30:02.790 | INFO     | Deleting existing data for 2026-02-03
2026-02-03 14:30:03.123 | INFO     | Writing 120 rows to budget_2024
2026-02-03 14:30:04.567 | SUCCESS  | ✅ Successfully wrote 120 rows to budget_2024

2026-02-03 14:30:04.890 | INFO     | 📊 Processing Google Sheet: Sales Report -> sales_report
2026-02-03 14:30:04.891 | INFO     | 🔗 Sheet Link: https://docs.google.com/spreadsheets/d/1DEF...UVW
2026-02-03 14:30:04.892 | INFO     | 📄 Sheet Name: Daily Sales
2026-02-03 14:30:04.893 | WARNING  | ⚠️  Truncate mode: Will delete all existing data
2026-02-03 14:30:05.234 | INFO     | Reading Google Sheet: 1DEF...UVW/Daily Sales
2026-02-03 14:30:06.567 | INFO     | Read 500 rows with columns: ['date', 'product', 'quantity', 'revenue']
2026-02-03 14:30:06.789 | INFO     | Creating table call_functions.sales_report
2026-02-03 14:30:07.123 | INFO     | Created table call_functions.sales_report
2026-02-03 14:30:07.456 | WARNING  | Truncating table sales_report (method_data=True)
2026-02-03 14:30:08.123 | INFO     | Writing 500 rows to sales_report
2026-02-03 14:30:10.567 | SUCCESS  | ✅ Successfully wrote 500 rows to sales_report
```

## Log Icons Meaning

| Icon | Meaning | Example |
|------|---------|---------|
| 📊 | Processing sheet | `📊 Processing Google Sheet: Budget 2024 -> budget_2024` |
| 🔗 | Google Sheet link | `🔗 Sheet Link: https://docs.google.com/...` |
| 📄 | Sheet name | `📄 Sheet Name: Sheet1` |
| 🗑️ | Remove columns | `🗑️  Remove columns: notes,comments` |
| ✏️ | Rename columns | `✏️  Rename columns: {"old": "new"}` |
| ⚠️ | Truncate warning | `⚠️  Truncate mode: Will delete all existing data` |
| ✅ | Success | `✅ Successfully wrote 120 rows to budget_2024` |

## Clickable Links

Trong terminal/console, link Google Sheet sẽ **clickable** (tùy terminal):

- **VS Code Terminal**: Ctrl+Click
- **iTerm2**: Cmd+Click
- **Windows Terminal**: Ctrl+Click
- **Standard terminals**: Copy/paste link

## Benefits

1. **Quick Access**: Click link để mở Google Sheet ngay
2. **Debugging**: Biết chính xác sheet nào đang process
3. **Transparency**: Thấy rõ transformations được apply
4. **Warning**: Cảnh báo khi truncate mode
5. **Audit Trail**: Log đầy đủ cho troubleshooting

## Comparison with Old System

### Old System (Django Admin)
```html
<!-- HTML link in admin interface -->
<a href='https://docs.google.com/spreadsheets/d/1ABC...XYZ' target='_blank'>Link</a>
```
- ✅ Clickable in browser
- ❌ Only in admin UI
- ❌ No logs

### New System (Airflow/CLI)
```
🔗 Sheet Link: https://docs.google.com/spreadsheets/d/1ABC...XYZ
```
- ✅ Visible in logs
- ✅ Works in terminal
- ✅ Captured in Airflow logs
- ✅ Easy to copy/paste

## Airflow UI

Trong Airflow task logs, bạn sẽ thấy:

```
[2026-02-03, 14:30:00 +07] {googlesheet_ingestion.py:296} INFO - 📊 Processing Google Sheet: Budget 2024 -> budget_2024
[2026-02-03, 14:30:00 +07] {googlesheet_ingestion.py:297} INFO - 🔗 Sheet Link: https://docs.google.com/spreadsheets/d/1ABC...XYZ
[2026-02-03, 14:30:00 +07] {googlesheet_ingestion.py:298} INFO - 📄 Sheet Name: Sheet1
```

Link sẽ **clickable** trong Airflow UI! 🎉
