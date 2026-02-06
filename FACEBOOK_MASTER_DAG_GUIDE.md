# Facebook Ads Master DAG - Documentation

## 📋 **Overview**

Master DAG để orchestrate toàn bộ Facebook Ads ETL pipeline bằng cách trigger 3 DAGs tuần tự.

---

## 🎯 **DAG Flow**

```
facebook_ads_master_etl
    ↓
    ├─ 1. trigger_base_entities (facebook_ads_base_entities_etl)
    │      ├─ Campaigns
    │      ├─ AdSets  
    │      └─ Creatives
    │      (Sequential execution)
    ↓
    ├─ 2. trigger_ads (facebook_ads_ads_etl)
    │      └─ Ads
    ↓
    └─ 3. trigger_insights (facebook_ads_insights_etl)
           ├─ Insights
           └─ Message Insights
           (Sequential execution)
```

---

## ⚙️ **Configuration**

### **DAG ID**: `facebook_ads_master_etl`

### **Schedule**: `0 2 * * *` (Daily at 2 AM)

### **Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `date_preset` | string | `last_3d` | Date preset cho Base Entities & Ads<br>Options: `last_3d`, `last_7d`, `last_30d`, `last_90d` |
| `days_for_ago` | integer | `1` | Số ngày trước đó cho Insights<br>`1` = yesterday, `3` = 3 days ago |

---

## 🔧 **How It Works**

### **1. TriggerDagRunOperator**

Mỗi task sử dụng `TriggerDagRunOperator` để trigger DAG con:

```python
trigger_base_entities = TriggerDagRunOperator(
    task_id='trigger_base_entities',
    trigger_dag_id='facebook_ads_base_entities_etl',
    conf={'date_preset': '{{ params.date_preset }}'},
    wait_for_completion=True,  # ← Chờ DAG hoàn thành
    poke_interval=30,          # ← Check mỗi 30 giây
)
```

### **2. Parameter Passing**

Parameters được pass từ Master DAG → Child DAGs:

```python
# Master DAG params
params = {
    'date_preset': 'last_3d',
    'days_for_ago': 1
}

# Passed to child DAGs via conf
conf = {
    'date_preset': '{{ params.date_preset }}'  # → Base Entities & Ads
}
conf = {
    'days_for_ago': '{{ params.days_for_ago }}'  # → Insights
}
```

### **3. Sequential Execution**

```python
trigger_base_entities >> trigger_ads >> trigger_insights
```

**Flow**:
1. Trigger Base Entities → **Wait** for completion
2. Trigger Ads → **Wait** for completion
3. Trigger Insights → **Wait** for completion

---

## 📊 **Execution Timeline**

### **Example: Daily Run at 2 AM**

```
02:00 - Master DAG starts
02:00 - Trigger Base Entities DAG
    02:00 - Campaigns starts
    02:15 - Campaigns completes → AdSets starts
    02:30 - AdSets completes → Creatives starts
    02:45 - Creatives completes
02:45 - Base Entities DAG completes
02:45 - Trigger Ads DAG
    02:45 - Ads starts
    03:15 - Ads completes
03:15 - Ads DAG completes
03:15 - Trigger Insights DAG
    03:15 - Insights starts
    03:45 - Insights completes → Message Insights starts
    04:15 - Message Insights completes
04:15 - Insights DAG completes
04:15 - Master DAG completes ✅
```

**Total Duration**: ~2 hours 15 minutes

---

## 🎛️ **Usage**

### **1. Manual Trigger (Default Parameters)**

```bash
# Via Airflow UI
DAG: facebook_ads_master_etl
Click: "Trigger DAG"
# Uses default: date_preset=last_3d, days_for_ago=1
```

### **2. Manual Trigger (Custom Parameters)**

```bash
# Via Airflow UI
DAG: facebook_ads_master_etl
Click: "Trigger DAG w/ config"
Config:
{
  "date_preset": "last_7d",
  "days_for_ago": 3
}
```

### **3. Scheduled Run**

```bash
# Runs automatically daily at 2 AM
# Uses default parameters
```

---

## 🔍 **Monitoring**

### **Check Master DAG Status**

```bash
# Airflow UI
DAGs → facebook_ads_master_etl
```

### **Check Child DAG Status**

```bash
# Airflow UI
DAGs → facebook_ads_base_entities_etl
DAGs → facebook_ads_ads_etl
DAGs → facebook_ads_insights_etl
```

### **Logs**

```bash
# Master DAG logs
Task: trigger_base_entities → Logs
Task: trigger_ads → Logs
Task: trigger_insights → Logs

# Child DAG logs
Navigate to child DAG → Task → Logs
```

---

## ⚠️ **Important Notes**

### **1. Rate Limit Handling**

- ✅ **Sequential execution** prevents rate limit conflicts
- ✅ Each DAG gets **full quota** (200 calls/hour)
- ✅ **Outer try/except** handles iterator rate limits gracefully

### **2. Dependencies**

```
Base Entities (Campaigns, AdSets, Creatives)
    ↓ (Required by)
Ads (needs Campaigns, AdSets, Creatives)
    ↓ (Required by)
Insights (needs Ads)
```

**Must run in order!**

### **3. Failure Handling**

```python
# If Base Entities fails
→ Ads will NOT run
→ Insights will NOT run
→ Master DAG fails

# If Ads fails
→ Insights will NOT run
→ Master DAG fails

# If Insights fails
→ Master DAG fails
```

**Retry**: Each DAG has `retries=2` (except Master has `retries=1`)

---

## 🚀 **Deployment**

### **1. Deploy DAG**

```bash
# Copy to Airflow DAGs folder
cp facebook_ads_master_dag.py /path/to/airflow/dags/
```

### **2. Verify DAG**

```bash
# Check DAG is loaded
airflow dags list | grep facebook_ads_master_etl
```

### **3. Test Run**

```bash
# Manual trigger via UI
# Or CLI:
airflow dags trigger facebook_ads_master_etl \
  --conf '{"date_preset": "last_3d", "days_for_ago": 1}'
```

---

## 📝 **Files**

### **Master DAG**:
- `dags/facebook_ads_master_dag.py`

### **Child DAGs**:
- `dags/facebook_ads_base_entities_dag.py`
- `dags/facebook_ads_ads_dag.py`
- `dags/facebook_ads_insights_dag.py`

### **Scripts**:
- `app/facebook/facebook_ads_campaigns_ingestion.py`
- `app/facebook/facebook_ads_adsets_ingestion.py`
- `app/facebook/facebook_ads_creatives_ingestion.py`
- `app/facebook/facebook_ads_ads_ingestion.py`
- `app/facebook/facebook_ads_insights_ingestion.py`
- `app/facebook/facebook_ads_message_insights_ingestion.py`

---

## ✅ **Summary**

| Aspect | Value |
|--------|-------|
| **DAG ID** | `facebook_ads_master_etl` |
| **Schedule** | Daily at 2 AM |
| **Execution** | Sequential (3 DAGs) |
| **Default date_preset** | `last_3d` (3 days) |
| **Default days_for_ago** | `1` (yesterday) |
| **Total Duration** | ~2-3 hours |
| **Rate Limit Safe** | ✅ Yes (sequential) |
| **Retry** | 1 (Master), 2 (Child DAGs) |

---

## 🎯 **Benefits**

1. ✅ **Single entry point** - Trigger 1 DAG instead of 3
2. ✅ **Sequential execution** - No rate limit conflicts
3. ✅ **Configurable** - Easy to change date ranges
4. ✅ **Monitored** - Single DAG to watch
5. ✅ **Reliable** - Wait for completion before next step
6. ✅ **Maintainable** - Clear dependencies

**Ready to use!** 🚀
