# Facebook Ads ETL Migration

Migration of Facebook Ads data pipeline from `etl_marketing` (Django) to `data-ops` (Airflow + PostgreSQL).

## 📋 Overview

**Old Architecture:**
```
Facebook Ads API → Django Cron Job → PostgreSQL (custom_db_dev)
```

**New Architecture:**
```
Facebook Ads API → Standalone Script → PostgreSQL (dwh_dev)
```

**Key Difference:** 
- ✅ **No Iceberg layer** - Data goes directly to PostgreSQL serving
- ✅ **100% same business logic** - Exact copy from Django app
- ✅ **No Django dependencies** - Pure Python + psycopg2

## 🎯 What Was Migrated

### Core Functionality:
1. **Facebook Ads Insights Ingestion** ✅
   - Fetches ad insights from Facebook Ads API
   - Supports both "by account" and "by campaign" modes
   - Includes rate limit handling with exponential backoff
   - Stores to `core_facebook_ad_ad_insight` table

### Business Logic Preserved:
- ✅ Rate limit detection (`x-business-use-case-usage`, `x-app-usage`, `x-fb-ads-insights-throttle`)
- ✅ Exponential backoff with jitter
- ✅ Auto-creation of missing Ad Ads
- ✅ Upsert strategy (UPDATE if exists, INSERT if new)
- ✅ Region breakdown support
- ✅ Script type selection (account vs campaign)

## 🚀 Quick Start

### 1. Prerequisites

Ensure PostgreSQL tables exist in `dwh_dev`:
- `core_facebook_ad_account` - Ad account configurations
- `core_facebook_ad_ad` - Ad ads
- `core_facebook_ad_ad_insight` - Insights data (target table)

### 2. Deploy DAG

```bash
# Copy DAG to Airflow
cp dags/call_functions_facebook_ads_dag.py /path/to/airflow/dags/

# Verify DAG
airflow dags list | grep facebook_ads
```

### 3. Trigger Manual Run

```bash
# Run with default (T-1)
airflow dags trigger call_functions_facebook_ads_etl

# Run with custom days_for_ago
airflow dags trigger call_functions_facebook_ads_etl \
  --conf '{"days_for_ago": 5}'
```

### 4. Monitor Execution

```bash
# Check DAG runs
airflow dags list-runs -d call_functions_facebook_ads_etl

# View logs
airflow tasks logs call_functions_facebook_ads_etl facebook_ads_insights_t1 <execution_date>
```

## 📊 Data Flow

### 1. Account Configuration

Facebook Ad accounts are configured in `core_facebook_ad_account`:

| Column | Description |
|--------|-------------|
| ad_account_id | Facebook Ad Account ID |
| app_id | Facebook App ID |
| app_secret | Facebook App Secret |
| access_token | Long-lived access token |
| access_token_expires | Token expiration datetime |
| script_type_to_run | `account` or `campaign` |
| is_active | Enable/disable account |
| current_brand_id | Brand association |

### 2. Insights Data

Stored in `core_facebook_ad_ad_insight`:

| Column | Type | Description |
|--------|------|-------------|
| fb_ad_ad_id | FK | Reference to Ad Ad |
| date | DATE | Insight date |
| spend | FLOAT | Ad spend |
| fb_account_currency | TEXT | Currency code |
| region | TEXT | Geographic region |
| brand_id | FK | Brand association |
| etl_created | TIMESTAMP | Created timestamp |
| etl_updated | TIMESTAMP | Updated timestamp |

**Unique Key:** `(fb_ad_ad_id, date, region)`

## 🔧 Configuration

### Environment Variables

Set in Airflow DAG or as Airflow Variables:

```bash
# PostgreSQL (from Airflow connection 'dwh_dev')
POSTGRES_URI=postgresql://user:pass@host:5432/dbname

# Facebook API
FB_API_VERSION=v18.0
```

### Account Setup

1. **Add Facebook Ad Account** to `core_facebook_ad_account`:
   ```sql
   INSERT INTO core_facebook_ad_account 
   (name, ad_account_id, app_id, app_secret, access_token, access_token_expires, script_type_to_run, is_active, current_brand_id)
   VALUES 
   ('My Ad Account', '123456789', 'app_id', 'app_secret', 'token', '2026-12-31', 'account', TRUE, 1);
   ```

2. **Get Long-Lived Token**: Use Facebook's token debugger or Graph API Explorer

3. **Set Script Type**:
   - `account`: Fetch all ads in account (faster, may hit rate limits)
   - `campaign`: Fetch by campaign (slower, more reliable)

## 📝 Migration Notes

### Differences from Old System

| Aspect | Old (etl_marketing) | New (data-ops) |
|--------|---------------------|----------------|
| **Framework** | Django | Standalone Python |
| **ORM** | Django ORM | Direct psycopg2 |
| **Orchestration** | Django Cron | Airflow |
| **Database** | custom_db_dev | dwh_dev |
| **Logging** | Django logging | Loguru |
| **Deployment** | Django server | Kubernetes Pods |

### What's Exactly the Same

1. **Facebook Business SDK** - Same version, same API calls
2. **Rate Limit Logic** - Exact copy of `check_limit()` function
3. **Upsert Strategy** - Same UPDATE/INSERT logic
4. **Field Definitions** - Same fields fetched from API
5. **Error Handling** - Same try/except patterns

### What's Different

1. **No Django Models** - Replaced with direct SQL
2. **Connection Pooling** - Uses psycopg2 with keepalive
3. **Logging** - Uses loguru instead of Django logging
4. **Configuration** - Environment variables instead of Django settings

## 🐛 Troubleshooting

### Token Expired

```
ERROR: Token expired for account: My Account
```

**Solution:** Refresh long-lived token in `core_facebook_ad_account`

### Rate Limit Reached

```
WARNING: Rate limit high. Cooling 120s | source=x-business-use-case-usage | usage=85%
```

**Solution:** This is normal. Script will auto-wait and retry.

### Missing Ad Ad

```
ERROR: Failed to create Ad Ad 123456: ...
```

**Solution:** Check if ad exists in Facebook. May be deleted or archived.

### Connection Timeout

```
ERROR: Failed to connect to PostgreSQL
```

**Solution:** Check `dwh_dev` Airflow connection configuration

## 📈 Performance

### Resource Usage

- **CPU**: 1-2 cores per task
- **Memory**: 1-2 GB per task
- **Duration**: 5-30 minutes depending on account size

### Rate Limits

Facebook Ads API has strict rate limits:
- **App-level**: 200 calls/hour
- **Account-level**: Varies by tier
- **Business-level**: Shared across apps

**Mitigation:**
- Sequential task execution (T-1 → T-2 → T-3)
- Exponential backoff with jitter
- Campaign mode for large accounts

## 🔗 References

- [Facebook Marketing API](https://developers.facebook.com/docs/marketing-apis)
- [Facebook Business SDK](https://github.com/facebook/facebook-python-business-sdk)
- [Rate Limiting](https://developers.facebook.com/docs/graph-api/overview/rate-limiting)
- [Long-Lived Tokens](https://developers.facebook.com/docs/facebook-login/guides/access-tokens/get-long-lived)

## ✅ Checklist for Killing etl_marketing

Before shutting down `etl_marketing` Django app:

- [ ] Verify all Facebook Ad accounts migrated to `core_facebook_ad_account`
- [ ] Confirm tokens are valid and not expired
- [ ] Run DAG manually for 3 days to verify data quality
- [ ] Compare data between old and new system
- [ ] Update any downstream dependencies (dashboards, reports)
- [ ] Archive old Django cron jobs
- [ ] Document any custom logic not migrated

## 🚨 Important Notes

1. **Token Management**: Long-lived tokens expire after 60 days. Set up monitoring!
2. **Rate Limits**: Facebook can throttle aggressively. Don't run too frequently.
3. **Data Consistency**: Always verify data matches between old and new system before killing old app.
4. **Brand Association**: Ensure `current_brand_id` is set correctly for all accounts.
