# CareSoft Job Schedule Summary
## All CareSoft-related Scheduled Jobs

---

## 📊 Tổng Quan

### Hệ Thống Cũ (Django Crontab)
- **Tổng số jobs**: 14 jobs
- **Job muộn nhất**: **23:00** (11:00 PM)
- **Job sớm nhất**: 01:30 (1:30 AM)

### Hệ Thống Mới (Airflow)
- **Tổng số DAGs**: 2 DAGs
- **Job muộn nhất**: **02:00** (2:00 AM)
- **Job sớm nhất**: 01:30 (1:30 AM)

---

## 🕐 Hệ Thống Cũ (Django Crontab)

### Daily Jobs (Chạy 1 lần/ngày)

| Time | Cron | Function | Description |
|------|------|----------|-------------|
| **01:30** | `30 1 * * *` | `update_caresoft_tickets_data_day_by_day_ago_from_settings` | Update tickets data |
| **01:45** | `45 1 * * *` | `update_caresoft_contacts_ttc_field_data_day_by_day_ago_from_settings` | Update contacts TTC |
| **02:00** | `0 2 * * *` | `update_caresoft_deal_field_data_day_by_day_ago_from_settings` | Update deal data (run 1) |
| **02:30** | `30 2 * * *` | `update_caresoft_chats_field_data_day_by_day_ago_from_settings` | Update chats data |
| **03:15** | `15 3 * * *` | `update_caresoft_ticket_source_detail_field_data_by_day_ago_from_settings` | Update ticket source detail |
| **03:30** | `30 3 * * *` | `update_caresoft_call_data_day_by_day_ago_from_settings` | Update call data |
| **04:10** | `10 4 * * *` | `update_caresoft_services_field_data_day_by_day_ago_from_settings` | Update services data |
| **05:30** | `30 5 * * *` | `update_caresoft_custom_field_data_day_by_day_ago_from_settings` | Update custom fields |
| **05:45** | `45 5 * * *` | `update_caresoft_custom_field_khach_hang_data_day_by_day_ago_from_settings` | Update customer custom fields |
| **06:15** | `15 6 * * *` | `update_caresoft_groups_field_data_day_by_day_ago_from_settings` | Update groups data |
| **08:35** | `35 08 * * *` | `update_caresoft_tickets_data_day_by_day_ago_from_settings` | Update tickets data (run 2) |
| **09:00** | `00 09 * * *` | `update_caresoft_deal_field_data_day_by_day_ago_from_settings` | Update deal data (run 2) |
| **23:00** | `00 23 * * *` | `update_caresoft_agents_field_data_day_by_day_ago_from_settings` | **Update agents data** ⭐ **MUỘN NHẤT** |

### High-Frequency Jobs (Chạy nhiều lần/ngày)

| Frequency | Cron | Function | Description |
|-----------|------|----------|-------------|
| **Every 12 min** | `*/12 * * * *` | `update_caresoft_ticket_source_field_data_day_by_day_ago_from_settings` | Update ticket source (120 runs/day) |
| **Every 15 min** | `*/15 * * * *` | `update_caresoft_contact_field_data_day_by_day_ago_from_settings` | Update contact data (96 runs/day) |
| **Every hour :45** | `45 * * * *` | `update_caresoft_agent_from_cs_chat_data_day_by_day_ago_from_settings` | Update agent from chat (24 runs/day) |
| **Every hour :50** | `50 * * * *` | `update_caresoft_contact_error_field_data_day_by_day_ago_from_settings` | Update contact errors (24 runs/day) |

---

## 🕐 Hệ Thống Mới (Airflow)

### DAG 1: `caresoft_lakehouse_etl`

| Schedule | Description | Tasks |
|----------|-------------|-------|
| **02:00 daily** | `0 2 * * *` | Deal flow: ERP → Bronze → Silver → Gold → Serving |

**Tasks:**
1. `erp_webhook_ingestion` - Ingest from ERP systems
2. `webhook_transformation_silver` - Transform to Silver
3. `deal_detail_enrichment` - Enrich with API details (Gold)
4. `refresh_serving_layer` - Write to PostgreSQL (Production)
5. `refresh_serving_layer_sandbox` - Write to PostgreSQL (Sandbox)

### DAG 2: `caresoft_ticket_bronze_ingestion`

| Schedule | Description | Tasks |
|----------|-------------|-------|
| **01:30 and 08:35 daily** | `30 1,8 * * *` | Ticket Bronze ingestion (API → Iceberg) |

**Tasks:**
1. `ticket_ingestion` - Fetch tickets from CareSoft API

### DAG 3: `caresoft_ticket_silver_enrichment`

| Schedule | Description | Tasks |
|----------|-------------|-------|
| **Every 12 minutes** | `*/12 * * * *` | Ticket Silver enrichment (Scan ID → API Detail) |

**Tasks:**
1. `ticket_detail_enrichment` - Get top 300 tickets and enrich

---

## ⏰ Timeline Comparison

### Hệ Thống Cũ (Django)
```
00:00 ─────────────────────────────────────────────────────────── 24:00
  │                                                                 │
  01:30 ── Tickets (1)
  01:45 ── Contacts TTC
  02:00 ── Deal (1)
  02:30 ── Chats
  03:15 ── Ticket Source Detail
  03:30 ── Calls
  04:10 ── Services
  05:30 ── Custom Fields
  05:45 ── Customer Custom Fields
  06:15 ── Groups
  08:35 ── Tickets (2)
  09:00 ── Deal (2)
  23:00 ── Agents ⭐ MUỘN NHẤT
  
  + Every 12 min: Ticket Source
  + Every 15 min: Contacts
  + Every hour :45: Agent from Chat
  + Every hour :50: Contact Errors
```

### Hệ Thống Mới (Airflow)
```
00:00 ─────────────────────────────────────────────────────────── 24:00
  │                                                                 │
  01:30 ── Ticket Bronze (1)
  02:00 ── Deal Lakehouse ⭐ MUỘN NHẤT (daily)
  08:35 ── Ticket Bronze (2)
  
  + Every 12 min: Ticket Silver Enrichment
```

---

## 📌 Answer: Job Muộn Nhất

### Hệ Thống Cũ (Django Crontab)
**⭐ 23:00 (11:00 PM)** - `update_caresoft_agents_field_data_day_by_day_ago_from_settings`

### Hệ Thống Mới (Airflow)
**⭐ 02:00 (2:00 AM)** - `caresoft_lakehouse_etl` DAG

---

## 🔍 Chi Tiết Job 23:00 (Hệ Thống Cũ)

### Function: `update_caresoft_agents_field_data_day_by_day_ago_from_settings`

**Purpose**: Update CareSoft agents data

**Cron Schedule**: `00 23 * * *` (Every day at 23:00)

**File**: `caresoft/cron_jobs.py`

**Tương đương trong hệ thống mới**: Chưa có (agents data chưa được migrate sang Lakehouse)

---

## 📊 Statistics

### Hệ Thống Cũ
- **Daily jobs**: 13 jobs
- **High-frequency jobs**: 4 jobs (every 12-15 min or hourly)
- **Total executions/day**: ~13 + 120 + 96 + 24 + 24 = **~277 executions/day**
- **Peak time**: 01:00 - 09:00 (most jobs)
- **Latest job**: 23:00 (agents)

### Hệ Thống Mới
- **Daily jobs**: 2 jobs (ticket bronze x2, deal lakehouse x1)
- **High-frequency jobs**: 1 job (ticket silver every 12 min)
- **Total executions/day**: ~3 + 120 = **~123 executions/day**
- **Peak time**: 01:00 - 02:00
- **Latest job**: 02:00 (deal lakehouse)

---

## 🎯 Recommendations

### Migration Priority

1. **✅ Migrated**: 
   - Deal flow (02:00)
   - Ticket bronze (01:30, 08:35)
   - Ticket silver (every 12 min)

2. **⏳ Not Yet Migrated**:
   - Agents data (23:00) ← **MUỘN NHẤT**
   - Contacts data (every 15 min)
   - Chats data (02:30)
   - Calls data (03:30)
   - Services data (04:10)
   - Custom fields (05:30, 05:45)
   - Groups data (06:15)
   - Contact errors (every hour :50)

### Suggested Schedule for New System

If migrating all jobs to Airflow, consider:
- Keep high-frequency jobs (every 12-15 min) as separate DAGs
- Group daily jobs by logical flow
- Latest daily job should be before 23:00 to avoid conflicts with next day's jobs
