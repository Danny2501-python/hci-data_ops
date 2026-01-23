# Airflow Template Variables & Caching

**Question:** Airflow có cache biến `snapshot_date` không?  
**Answer:** **KHÔNG** - Airflow không cache template variables.

---

## 🔍 AIRFLOW TEMPLATE RENDERING

### **Mỗi Task Instance = Render Mới**

```python
arguments=[
    '/app/script.py',
    '{{ dag_run.conf.get("snapshot_date") | default("", true) }}'
]
```

**Khi task chạy:**
1. Airflow render template với context hiện tại
2. Evaluate `dag_run.conf.get("snapshot_date")`
3. Pass giá trị cho script
4. **KHÔNG cache** - mỗi lần chạy đều render lại

---

## ❓ TẠI SAO VẪN THẤY NGÀY CŨ?

### **Lý do 1: Task Retry**

```python
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
```

- Task fail → Airflow retry
- **Retry dùng CÙNG execution_date** với lần chạy đầu
- Nếu task của execution_date = 21/1 đang retry → vẫn thấy 21/1

**Kiểm tra:**
```bash
# Xem task instances
kubectl exec airflow-webserver -- \
  airflow tasks states-for-dag-run caresoft_deal_etl scheduled__2026-01-21T18:30:00+00:00

# Output:
task_id                          | state
---------------------------------|--------
erp_direct_ingestion             | success
webhook_transformation_silver    | failed  # ← Đang retry?
deal_detail_enrichment           | queued
```

---

### **Lý do 2: Multiple DAG Runs**

Airflow có thể có **nhiều DAG runs** cùng lúc:

```bash
# List all active runs
kubectl exec airflow-webserver -- \
  airflow dags list-runs -d caresoft_deal_etl --state running

# Output:
run_id                              | execution_date
------------------------------------|------------------
scheduled__2026-01-21T18:30:00      | 2026-01-21  # ← Run cũ
scheduled__2026-01-22T18:30:00      | 2026-01-22  # ← Run mới
```

**Nguyên nhân:**
- Run cũ (21/1) chưa finish
- Run mới (22/1) đã start
- Cả 2 đang chạy song song

---

### **Lý do 3: Queued Tasks**

```bash
# Check task queue
kubectl exec airflow-scheduler -- \
  airflow tasks list caresoft_deal_etl --tree

# Tasks có thể ở trạng thái:
- queued    # Đang chờ
- running   # Đang chạy
- success   # Hoàn thành
- failed    # Lỗi
- retry     # Đang retry
```

---

### **Lý do 4: Manual Trigger với Config Cũ**

Nếu bạn trigger manual:

```bash
airflow dags trigger caresoft_deal_etl \
  --conf '{"snapshot_date": "2026-01-21"}'  # ← Hardcode ngày cũ
```

→ Task sẽ nhận `snapshot_date = 2026-01-21`

---

## 🔍 CÁCH KIỂM TRA

### **1. Xem DAG Runs Đang Chạy**

```bash
kubectl exec -n default airflow-webserver-xxx -- \
  airflow dags list-runs -d caresoft_deal_etl --state running --output table
```

### **2. Xem Task Instances**

```bash
kubectl exec -n default airflow-webserver-xxx -- \
  airflow tasks states-for-dag-run caresoft_deal_etl scheduled__2026-01-21T18:30:00+00:00
```

### **3. Xem Logs của Task**

```bash
# Xem log của task cụ thể
kubectl logs -n default <pod-name> | grep snapshot_date

# Hoặc từ Airflow UI
# → DAG → Task → Logs
```

### **4. Kiểm tra Execution Date**

```python
# Trong Python script, log execution_date
import sys
from datetime import datetime, date, timedelta

snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else ""
logger.info(f"Received snapshot_date argument: '{snapshot_date_str}'")

if snapshot_date_str:
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date()
    logger.info(f"Using provided snapshot_date: {snapshot_date}")
else:
    snapshot_date = date.today() - timedelta(days=1)
    logger.info(f"Using calculated snapshot_date (today-1): {snapshot_date}")
```

---

## ✅ GIẢI PHÁP

### **Option 1: Clear Failed Tasks**

```bash
# Clear failed tasks để chúng không retry với execution_date cũ
kubectl exec -n default airflow-webserver-xxx -- \
  airflow tasks clear caresoft_deal_etl \
  --start-date 2026-01-21 \
  --end-date 2026-01-21 \
  --yes
```

### **Option 2: Mark Failed Tasks as Success**

```bash
# Nếu không muốn retry, mark as success
kubectl exec -n default airflow-webserver-xxx -- \
  airflow tasks set-state caresoft_deal_etl \
  erp_direct_ingestion \
  scheduled__2026-01-21T18:30:00+00:00 \
  --state success
```

### **Option 3: Đợi Run Mới**

- Fix đã apply (timezone-aware start_date)
- Đợi lần chạy tiếp theo (24/1 01:30 AM)
- Execution_date sẽ là 23/1 ✅

### **Option 4: Trigger Manual với Ngày Đúng**

```bash
kubectl exec -n default airflow-webserver-xxx -- \
  airflow dags trigger caresoft_deal_etl \
  --conf '{"snapshot_date": "2026-01-22"}' \
  --exec-date 2026-01-22
```

---

## 📊 TEMPLATE VARIABLE LIFECYCLE

```
DAG Definition (Python)
  ↓
Airflow Scheduler parses DAG
  ↓
Create DAG Run (execution_date = 2026-01-22)
  ↓
Create Task Instances
  ↓
Task queued → Task running
  ↓
**RENDER TEMPLATES** ← Happens HERE, every time!
  ↓
  - Evaluate {{ dag_run.conf.get("snapshot_date") }}
  - Evaluate {{ ds }}
  - Evaluate {{ execution_date }}
  ↓
Execute task with rendered values
  ↓
Task success/fail
```

**Key Point:** Template rendering happens **AT TASK EXECUTION TIME**, not at DAG parse time.

---

## 🎯 TÓM TẮT

| Question | Answer |
|----------|--------|
| Airflow cache `snapshot_date`? | ❌ KHÔNG |
| Template render khi nào? | ✅ Mỗi lần task chạy |
| Tại sao vẫn thấy ngày cũ? | Task retry / Multiple runs / Queued tasks |
| Fix như thế nào? | Clear failed tasks / Đợi run mới / Trigger manual |

---

**Khuyến nghị:** 
1. Check Airflow UI → DAG Runs → Xem execution_date của run đang chạy
2. Check task states → Xem task nào đang retry
3. Đợi run mới (24/1 01:30 AM) với fix đã apply
4. Verify execution_date = 23/1 ✅
