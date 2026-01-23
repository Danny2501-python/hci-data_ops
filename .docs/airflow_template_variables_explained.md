# Airflow Template Variable Test

## Daily Schedule (schedule_interval='30 1 * * *')

### Run on: 2026-01-23 01:30 AM

| Variable | Value | Meaning |
|----------|-------|---------|
| `ds` | `2026-01-22` | Execution date (T-1) |
| `logical_date` | `2026-01-22` | Same as execution_date |
| `data_interval_start` | `2026-01-22` | Start of data interval (T-1) |
| `data_interval_end` | `2026-01-23` | End of data interval (T) |
| `macros.ds_add(ds, -1)` | `2026-01-21` | T-2 ❌ |
| `macros.ds_add(logical_date \| ds, -1)` | `2026-01-21` | T-2 ❌ |

---

## Recommended Templates

### For T-1 (yesterday's data)

```python
# Option 1: Simple and clear (RECOMMENDED)
'{{ dag_run.conf.get("snapshot_date") or (data_interval_start | ds) }}'

# Option 2: Using .get() with default
'{{ dag_run.conf.get("snapshot_date", data_interval_start | ds) }}'
```

**Result:**
- Auto run: `2026-01-22` ✅
- Manual with config: `2026-01-20` ✅
- Manual without config: `2026-01-22` ✅

---

### For T-2 (2 days ago)

```python
'{{ dag_run.conf.get("snapshot_date") or macros.ds_add(data_interval_start | ds, -1) }}'
```

**Result:**
- Auto run: `2026-01-21` (T-2)

---

## Why NOT use `macros.ds_add(logical_date | ds, -1)`?

For daily schedule:
- `logical_date` is already T-1 (execution_date)
- Adding `-1` makes it T-2
- Only correct if you want 2 days ago

---

## Test in Airflow

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

dag = DAG(
    'test_template_variables',
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    schedule_interval='30 1 * * *',
    catchup=False,
)

test_task = BashOperator(
    task_id='test',
    bash_command='''
        echo "ds: {{ ds }}"
        echo "logical_date: {{ logical_date | ds }}"
        echo "data_interval_start: {{ data_interval_start | ds }}"
        echo "data_interval_end: {{ data_interval_end | ds }}"
        echo "ds_add(ds, -1): {{ macros.ds_add(ds, -1) }}"
        echo "Recommended: {{ dag_run.conf.get('snapshot_date') or (data_interval_start | ds) }}"
    ''',
    dag=dag,
)
```

Run this DAG and check logs to see actual values!
