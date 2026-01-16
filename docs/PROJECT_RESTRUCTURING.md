# Project Restructuring & Naming Convention

## ✅ New Project Structure

```
data-ops/
├── app/
│   ├── common/                      # Shared utilities
│   │   ├── __init__.py
│   │   └── brand_config.py          # Brand configuration loader
│   │
│   └── caresoft/
│       ├── __init__.py
│       │
│       ├── deal/                    # Deal pipeline
│       │   ├── __init__.py
│       │   ├── deal_detail_enrichment.py
│       │   ├── deal_detail_snapshot.py      # NEW - Create _final table
│       │   └── deal_detail_serving_layer.py
│       │
│       ├── ticket/                  # Ticket pipeline
│       │   ├── __init__.py
│       │   ├── ticket_ingestion.py
│       │   ├── ticket_detail_enrichment.py
│       │   ├── ticket_api_serving_layer.py
│       │   └── ticket_source_serving_layer.py
│       │
│       ├── erp_webhook_ingestion.py         # Shared webhook ingestion
│       └── webhook_transformation_silver.py # Shared transformation
│
└── dags/
    ├── caresoft_deal_pipeline_dag.py        # NEW - Deal pipeline DAG
    ├── caresoft_ticket_pipeline_dag.py      # Ticket pipeline DAG
    └── caresoft_webhook_ingestion_dag.py    # NEW - Webhook ingestion DAG
```

---

## 📋 DAG & Task Naming Convention

### **Principle: `{system}_{entity}_{layer}_{action}`**

**Examples:**
- `caresoft_deal_bronze_ingest`
- `caresoft_deal_gold_enrich`
- `caresoft_deal_gold_snapshot`
- `caresoft_deal_serving_upsert`

---

## 🎯 New DAG Structure

### **1. caresoft_webhook_ingestion_dag.py**

**Purpose:** Ingest raw webhooks from ERP systems

**Tasks:**
```python
erp_webhook_ingest = KubernetesPodOperator(
    task_id='erp_webhook_ingest',
    arguments=['/app/caresoft/erp_webhook_ingestion.py', '{{ ds }}'],
)

webhook_transform_silver = KubernetesPodOperator(
    task_id='webhook_transform_silver',
    arguments=['/app/caresoft/webhook_transformation_silver.py', '{{ ds }}'],
)

erp_webhook_ingest >> webhook_transform_silver
```

**Schedule:** `@hourly` or `@continuous`

---

### **2. caresoft_deal_pipeline_dag.py**

**Purpose:** Process deal data from Silver → Gold → Serving

**Tasks:**
```python
# GOLD: Enrichment (incremental)
deal_gold_enrich = KubernetesPodOperator(
    task_id='deal_gold_enrich',
    arguments=['/app/caresoft/deal/deal_detail_enrichment.py', '{{ ds }}'],
)

# GOLD: Create snapshot (_final table)
deal_gold_snapshot = KubernetesPodOperator(
    task_id='deal_gold_snapshot',
    arguments=['/app/caresoft/deal/deal_detail_snapshot.py', '{{ ds }}'],
)

# SERVING: Upsert to PostgreSQL
deal_serving_upsert = KubernetesPodOperator(
    task_id='deal_serving_upsert',
    arguments=['/app/caresoft/deal/deal_detail_serving_layer.py', '{{ ds }}'],
)

deal_gold_enrich >> deal_gold_snapshot >> deal_serving_upsert
```

**Schedule:** `@daily` at 01:00

---

### **3. caresoft_ticket_pipeline_dag.py**

**Purpose:** Process ticket data from Bronze → Silver → Serving

**Tasks:**
```python
# BRONZE: Ingestion
ticket_bronze_ingest = KubernetesPodOperator(
    task_id='ticket_bronze_ingest',
    arguments=['/app/caresoft/ticket/ticket_ingestion.py', '{{ ds }}'],
)

# SILVER: Enrichment
ticket_silver_enrich = KubernetesPodOperator(
    task_id='ticket_silver_enrich',
    arguments=['/app/caresoft/ticket/ticket_detail_enrichment.py', '{{ ds }}'],
)

# SERVING: API layer
ticket_api_serving_upsert = KubernetesPodOperator(
    task_id='ticket_api_serving_upsert',
    arguments=['/app/caresoft/ticket/ticket_api_serving_layer.py', '{{ ds }}'],
)

# SERVING: Source layer
ticket_source_serving_upsert = KubernetesPodOperator(
    task_id='ticket_source_serving_upsert',
    arguments=['/app/caresoft/ticket/ticket_source_serving_layer.py', '{{ ds }}'],
)

ticket_bronze_ingest >> ticket_silver_enrich
ticket_bronze_ingest >> ticket_api_serving_upsert
ticket_silver_enrich >> ticket_source_serving_upsert
```

**Schedule:** `@daily` at 02:00

---

## 🏗️ Deal Pipeline Architecture (New)

```
┌─────────────────────────────────────────────────────────────┐
│ SILVER: cs_deal_wehook (Full snapshot)                     │
└─────────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────────┐
│ GOLD: deal_gold_enrich                                      │
├─────────────────────────────────────────────────────────────┤
│ cs_deal_detail_api (Incremental enrichment)                │
│ - Compare today vs yesterday webhooks                      │
│ - Enrich only NEW/UPDATED deals                            │
│ - Partition by snapshot_date                               │
└─────────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────────┐
│ GOLD: deal_gold_snapshot (NEW!)                            │
├─────────────────────────────────────────────────────────────┤
│ cs_deal_detail_api_final (Latest snapshot)                 │
│ - Read ALL snapshots from cs_deal_detail_api               │
│ - Find MAX(snapshot_date) per deal_id                      │
│ - Write to _final table (no partitioning)                  │
│ - Small, fast, latest state only                           │
└─────────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────────┐
│ SERVING: deal_serving_upsert                                │
├─────────────────────────────────────────────────────────────┤
│ cs_deal_detail_api_v1 (PostgreSQL)                         │
│ - Read from cs_deal_detail_api_final                       │
│ - UPSERT to PostgreSQL                                     │
│ - Simple, fast, no DuckDB needed                           │
└─────────────────────────────────────────────────────────────┘
```

---

## 📝 Task Naming Pattern

| Layer | Action | Task Name Pattern | Example |
|-------|--------|-------------------|---------|
| Bronze | Ingest | `{entity}_bronze_ingest` | `ticket_bronze_ingest` |
| Silver | Transform | `{entity}_silver_transform` | `webhook_silver_transform` |
| Silver | Enrich | `{entity}_silver_enrich` | `ticket_silver_enrich` |
| Gold | Enrich | `{entity}_gold_enrich` | `deal_gold_enrich` |
| Gold | Snapshot | `{entity}_gold_snapshot` | `deal_gold_snapshot` |
| Serving | Upsert | `{entity}_serving_upsert` | `deal_serving_upsert` |
| Serving | Refresh | `{entity}_serving_refresh` | (for full refresh) |

---

## 🎯 Benefits of New Structure

1. ✅ **Clear separation** - Deal vs Ticket vs Common
2. ✅ **Scalable** - Easy to add new entities
3. ✅ **Maintainable** - Each module is self-contained
4. ✅ **Consistent naming** - Easy to understand task flow
5. ✅ **Testable** - Can test each module independently

---

## 📦 Next Steps

1. ✅ Move files to new structure
2. ✅ Update imports (caresoft.brand_config → common.brand_config)
3. ⏳ Create `deal_detail_snapshot.py`
4. ⏳ Split `caresoft_lakehouse_dag.py` into 3 DAGs
5. ⏳ Update DAG arguments to use new paths
6. ⏳ Test imports and DAG parsing

---

## 🚀 Implementation Order

1. **Phase 1: Restructure** (DONE)
   - ✅ Create folders
   - ✅ Move files
   - ✅ Update imports

2. **Phase 2: Create snapshot job**
   - ⏳ Implement `deal_detail_snapshot.py`
   - ⏳ Update `deal_detail_serving_layer.py` to read from _final

3. **Phase 3: Split DAGs**
   - ⏳ Create 3 separate DAGs
   - ⏳ Update task names
   - ⏳ Update file paths

4. **Phase 4: Test**
   - ⏳ Test imports
   - ⏳ Test DAG parsing
   - ⏳ Test end-to-end flow
