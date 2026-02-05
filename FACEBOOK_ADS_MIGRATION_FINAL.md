# Facebook Ads ETL Migration - Final Summary

## Overview
Complete migration of Facebook Ads ETL from `etl_marketing` Django app to `data-ops` Airflow architecture.

**Strategy**: Simplified direct-write approach - no dependency creation, no foreign keys, just API → Database.

## Architecture

### 3 Airflow DAGs (Independent Execution)
1. **`facebook_ads_base_entities_dag.py`**: Campaigns, Ad Sets, Creatives (parallel)
2. **`facebook_ads_ads_dag.py`**: Ads
3. **`facebook_ads_insights_dag.py`**: Insights + Message Insights (parallel)

**Note**: All DAGs run independently. No `ExternalTaskSensor` dependencies.

### 6 Python Ingestion Scripts
All scripts follow the same pattern: API → Direct Write → No Dependencies

| Script | Table | Primary Key | Notes |
|--------|-------|-------------|-------|
| `facebook_ads_campaigns_ingestion.py` | `core_facebook_ad_campaign_v1` | `fb_ad_campaign_id` | Base entity |
| `facebook_ads_adsets_ingestion.py` | `core_facebook_ad_set_v1` | `fb_ad_set_id` | Base entity |
| `facebook_ads_creatives_ingestion.py` | `core_facebook_ad_creative_v1` | `fb_ad_creative_id` | Base entity |
| `facebook_ads_ads_ingestion.py` | `core_facebook_ad_ad_v1` | `fb_ad_ad_id` | References adset & creative |
| `facebook_ads_insights_ingestion.py` | `core_facebook_ad_ad_insight_v1` | `(fb_ad_ad_id, date, region)` | Metrics data |
| `facebook_ads_message_insights_ingestion.py` | `core_facebook_ad_ad_insight_message_v1` | `(fb_ad_ad_id, date, region)` | Message metrics |

## Database Schema Alignment

### Column Naming Convention
All scripts updated to match actual DB schema (`*_v1` tables):

| Old Code | New DB Schema |
|----------|---------------|
| `account_id` | `fb_ad_account_id` |
| `current_brand_id` | `company_id` (Campaigns) / `brand_id` (others) |
| `campaign_id` | `fb_ad_campaign_id` |
| `adset_id` | `fb_ad_set_id` |
| `creative_id` | `fb_creative_id` |
| `ad_id` | `fb_ad_ad_id` |
| `object_id` | `fb_object_id` |
| `object_story_id` | `fb_object_story_id` |
| `link_url` | `fb_link_url` |
| `effective_object_story_id` | `fb_effective_object_story_id` |
| `account_currency` | `fb_account_currency` |
| `etl_inserted` | `etl_created` |

### Removed Columns
- ❌ `raw_json` (removed from all tables)
- ❌ `optimization_goal` (AdSets)
- ❌ `actor_id` (Creatives)
- ❌ `targeting` (Ads)
- ❌ `campaign_id` (Ads - not in schema)
| ❌ `account_id`, `campaign_id`, `adset_id` (Insights - not in schema)

## Key Simplifications

### 1. No Dependency Creation
**Before**: Ads/Insights scripts would create missing Campaigns/AdSets/Creatives via API calls.

**After**: Direct write only. If foreign key references are missing, they'll fail (handled by DB constraints).

### 2. No Raw JSON Storage
**Before**: All tables had `raw_json JSONB` column storing full API response.

**After**: Only structured columns. Simpler schema, smaller storage.

### 3. Argument Parsing
All scripts use `sys.argv` (not `argparse`) to match `call_functions` pattern:

```python
# Campaigns, AdSets, Creatives, Ads
date_preset = sys.argv[1] if len(sys.argv) > 1 else 'last_90d'

# Insights, Message Insights
days_for_ago = int(sys.argv[1]) if len(sys.argv) > 1 else 1
```

DAGs pass arguments as positional parameters:
```python
arguments=[
    '/app/facebook/facebook_ads_campaigns_ingestion.py',
    '{{ params.date_preset }}',  # No --flags
]
```

## Shared Utilities

### `common/facebook_api.py`
- `FacebookRateLimitTracker`: Smart rate limit detection via header changes
- `get_active_facebook_accounts(db)`: Fetch active accounts from DB
- `init_facebook_api(account, version)`: Initialize API with token validation

### `common/database.py`
- `PostgreSQLHelper`: Robust PostgreSQL wrapper (connection, execute, fetchone, close)

## Execution Flow

### Base Entities DAG
```
Start → [Campaigns, AdSets, Creatives] (parallel) → End
```

### Ads DAG
```
Start → Ads → End
```

### Insights DAG
```
Start → [Insights, Message Insights] (parallel) → End
```

**All DAGs are independent** - can run in any order or simultaneously.

## DAG Parameters

### Base Entities & Ads DAGs
```python
params = {
    'date_preset': 'last_90d'  # Facebook date preset
}
```

### Insights DAG
```python
params = {
    'days_for_ago': 1  # How many days back to fetch
}
```

## Code Metrics

| Metric | Count |
|--------|-------|
| Total Python Scripts | 6 |
| Total Airflow DAGs | 3 |
| Total Lines of Code | ~1,200 |
| Shared Utilities | 2 files |
| Database Tables | 6 tables |

## Migration Checklist

### ✅ Completed
- [x] All 6 ingestion scripts created
- [x] All 3 Airflow DAGs created
- [x] Shared utilities (`facebook_api.py`, `database.py`)
- [x] Database schema alignment (column names, types)
- [x] Removed dependency creation logic
- [x] Removed `raw_json` columns
- [x] Updated argument parsing to `sys.argv`
- [x] Rate limiting implementation
- [x] Error handling and logging

### 🔄 Next Steps
1. **Create DB Tables**: Run DDL to create all `*_v1` tables
2. **Deploy to Airflow**: Copy DAGs to Airflow `dags/` directory
3. **Build Docker Image**: Include new scripts in container
4. **Test Execution**: Run each DAG manually to verify
5. **Monitor Logs**: Check for any API/DB errors
6. **Schedule DAGs**: Set up cron schedules if needed

## File Locations

### Python Scripts
```
data-ops/app/facebook/
├── facebook_ads_campaigns_ingestion.py
├── facebook_ads_adsets_ingestion.py
├── facebook_ads_creatives_ingestion.py
├── facebook_ads_ads_ingestion.py
├── facebook_ads_insights_ingestion.py
└── facebook_ads_message_insights_ingestion.py
```

### Airflow DAGs
```
data-ops/dags/
├── facebook_ads_base_entities_dag.py
├── facebook_ads_ads_dag.py
└── facebook_ads_insights_dag.py
```

### Shared Utilities
```
data-ops/app/common/
├── facebook_api.py
└── database.py
```

## Environment Variables Required

```bash
POSTGRES_URI=postgresql://user:pass@host:port/dbname
FB_API_VERSION=v18.0  # Optional, defaults to v18.0
```

## Notes

1. **No Foreign Keys**: Tables don't enforce FK constraints, so orphaned records are possible
2. **Idempotent**: All scripts use `ON CONFLICT ... DO UPDATE`, safe to re-run
3. **Rate Limiting**: Automatic detection and backoff when hitting FB API limits
4. **Error Handling**: Individual record failures don't stop the entire job
5. **Logging**: Comprehensive logging with loguru for debugging

---

**Migration Date**: 2026-02-05  
**Status**: ✅ Complete  
**Approach**: Simplified Direct Write (No Dependencies)
