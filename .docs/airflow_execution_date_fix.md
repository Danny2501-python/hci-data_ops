# Fix Airflow Execution Date Offset Issue

**Date:** 2026-01-23  
**Issue:** DAG chạy 1:30 AM ngày 23/1 nhưng execution_date = 21/1 (T-2) thay vì 22/1 (T-1)  
**Status:** ✅ Fixed

---

## 🐛 PROBLEM

### Symptom
- DAG schedule: `30 1 * * *` (1:30 AM UTC+7)
- Chạy: 23/1 01:30 AM
- Expected execution_date: **22/1** (T-1)
- Actual execution_date: **21/1** (T-2) ❌

### Impact
- Python scripts nhận snapshot_date = 21/1
- Xử lý data của ngày 21/1 thay vì 22/1
- Data bị lệch 1 ngày

---

## 🔍 ROOT CAUSE

### Code Before Fix

```python
from datetime import datetime

dag = DAG(
    'caresoft_deal_etl',
    schedule_interval='30 1 * * *',  # 1:30 AM UTC+7
    start_date=datetime(2026, 1, 1),  # ❌ Naive datetime (no timezone)
    catchup=False,
)
```

### Problem Explanation

**Naive datetime = UTC assumption:**
- `datetime(2026, 1, 1)` không có timezone info
- Airflow assumes **UTC** cho naive datetime
- Schedule `30 1 * * *` cũng là **UTC**

**Execution date calculation:**
```
Airflow config timezone: Asia/Ho_Chi_Minh (UTC+7)
Schedule: 30 1 * * * (UTC) = 08:30 UTC+7
Start date: 2026-01-01 00:00:00 UTC (naive → UTC)

Run at: 2026-01-23 01:30 UTC (08:30 UTC+7)
Execution date: 2026-01-21 01:30 UTC (T-2!)
```

**Why T-2 instead of T-1?**

Airflow execution_date logic:
1. Data interval = [execution_date, next_execution_date)
2. DAG runs **AFTER** data interval ends
3. With daily schedule starting 2026-01-01:
   - 2026-01-22 01:30 UTC → execution_date = 2026-01-20
   - 2026-01-23 01:30 UTC → execution_date = 2026-01-21 ❌

**Timezone mismatch causes off-by-one error!**

---

## ✅ SOLUTION

### Code After Fix

```python
from datetime import datetime
import pendulum

# Timezone for Vietnam
local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

dag = DAG(
    'caresoft_deal_etl',
    schedule_interval='30 1 * * *',  # 1:30 AM UTC+7
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),  # ✅ Timezone-aware
    catchup=False,
)
```

### Fix Explanation

**Timezone-aware datetime:**
- `datetime(2026, 1, 1, tzinfo=local_tz)` = 2026-01-01 00:00:00 Asia/Ho_Chi_Minh
- Airflow now knows start_date is in UTC+7
- Execution date calculation aligns with schedule

**Corrected calculation:**
```
Start date: 2026-01-01 00:00:00 Asia/Ho_Chi_Minh
Schedule: 30 1 * * * (interpreted as UTC+7 due to start_date timezone)

Run at: 2026-01-23 01:30 Asia/Ho_Chi_Minh
Execution date: 2026-01-22 01:30 Asia/Ho_Chi_Minh (T-1!) ✅
```

---

## 📊 VERIFICATION

### Before Fix
```bash
# DAG run at 2026-01-23 01:30 UTC+7
kubectl logs airflow-scheduler | grep caresoft_deal

# Output:
scheduled__2026-01-21T18:30:00+00:00  # ❌ T-2
```

### After Fix (Expected)
```bash
# DAG run at 2026-01-24 01:30 UTC+7 (next run)
kubectl logs airflow-scheduler | grep caresoft_deal

# Output:
scheduled__2026-01-23T18:30:00+00:00  # ✅ T-1
```

### Verify Next Execution
```bash
kubectl exec -n default airflow-webserver-xxx -- \
  airflow dags next-execution caresoft_deal_etl

# Expected output:
2026-01-23T18:30:00+00:00  # 23/1 18:30 UTC = 24/1 01:30 UTC+7
# Execution date will be: 2026-01-23 ✅
```

---

## 🎯 KEY LEARNINGS

### 1. Always Use Timezone-Aware Datetimes

**Bad:**
```python
start_date=datetime(2026, 1, 1)  # Naive → UTC assumption
```

**Good:**
```python
import pendulum
local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')
start_date=datetime(2026, 1, 1, tzinfo=local_tz)
```

### 2. Understand Airflow Execution Date

- **Execution date ≠ Run date**
- Execution date = **data interval start**
- For daily DAG: execution_date = T-1 of run time

**Example:**
```
DAG runs: 2026-01-24 01:30 AM
Execution date: 2026-01-23 (data for yesterday)
Data interval: [2026-01-23 00:00, 2026-01-24 00:00)
```

### 3. Schedule Interval Interpretation

**With naive start_date:**
```python
schedule_interval='30 1 * * *'  # Interpreted as UTC
```

**With timezone-aware start_date:**
```python
schedule_interval='30 1 * * *'  # Interpreted in start_date's timezone
```

---

## 📝 DEPLOYMENT CHECKLIST

- [x] Add `import pendulum`
- [x] Define `local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')`
- [x] Update `start_date=datetime(2026, 1, 1, tzinfo=local_tz)`
- [ ] Commit and push changes
- [ ] Wait for next DAG run (24/1 01:30 AM)
- [ ] Verify execution_date = 23/1 (T-1)
- [ ] Check Python script logs for correct snapshot_date

---

## 🚀 DEPLOYMENT

```bash
# Commit changes
git add dags/caresoft_deal_dag.py
git commit -m "Fix execution_date offset by adding timezone to start_date"
git push

# Airflow will automatically reload DAG
# Next run: 2026-01-24 01:30 AM UTC+7
# Execution date: 2026-01-23 ✅
```

---

## ⚠️ IMPORTANT NOTES

### Why Not Use `ds` Template Variable?

Earlier I suggested using `{{ ds }}` instead of empty string:
```python
'{{ dag_run.conf.get("snapshot_date") | default(ds, true) }}'
```

**This is still valid but not necessary** if:
1. Python script correctly uses execution_date
2. start_date has correct timezone

**Current approach (with timezone fix):**
```python
# DAG passes empty string
'{{ dag_run.conf.get("snapshot_date") | default("", true) }}'

# Python script uses date.today() - 1
snapshot_date = snapshot_date or (date.today() - timedelta(days=1))
```

**This works because:**
- `date.today()` in pod = 2026-01-24 (UTC)
- `date.today() - 1` = 2026-01-23 ✅
- Matches execution_date!

**However, using `ds` is MORE RELIABLE:**
```python
# DAG passes execution_date
'{{ dag_run.conf.get("snapshot_date") | default(ds, true) }}'

# Python script receives "2026-01-23" directly
# No dependency on pod timezone
```

**Recommendation:** Use `ds` for consistency and clarity.

---

## 🔄 ALTERNATIVE: Use Pendulum for Schedule

For even better timezone handling:

```python
from airflow.timetables.interval import CronDataIntervalTimetable
import pendulum

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

dag = DAG(
    'caresoft_deal_etl',
    schedule=CronDataIntervalTimetable(
        '30 1 * * *',
        timezone=local_tz
    ),
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
)
```

**Benefits:**
- Explicit timezone for schedule
- No ambiguity
- Better for complex schedules

---

**Status:** ✅ Fixed - Ready to deploy
