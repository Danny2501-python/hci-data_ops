# Facebook Ads ETL - Quick Start Guide (SIMPLIFIED VERSION)

## 🚀 Quick Start

### 1. Create Table

```bash
# Connect to PostgreSQL
psql -h <host> -U <user> -d custom_db_dev

# Run DDL
\i sql/facebook_ads_tables_simple.sql

# Verify table created
\d core_facebook_ad_ad_insight_simple
```

### 2. Verify Account Configuration

```sql
-- Check active Facebook Ad accounts
SELECT id, name, ad_account_id, is_active, current_brand_id, script_type_to_run
FROM core_facebook_ad_account
WHERE is_active = TRUE;

-- If no accounts, insert one:
INSERT INTO core_facebook_ad_account 
(name, ad_account_id, app_id, app_secret, access_token, access_token_expires, 
 script_type_to_run, is_active, current_brand_id)
VALUES 
('My Ad Account', '123456789', 'your_app_id', 'your_app_secret', 
 'your_long_lived_token', '2026-12-31 23:59:59', 'account', TRUE, 1);
```

### 3. Setup Airflow Connection

```bash
# In Airflow UI: Admin → Connections → Add Connection
Conn Id: custom_db_dev
Conn Type: Postgres
Host: <your_postgres_host>
Database: custom_db_dev
Login: <username>
Password: <password>
Port: 5432
```

### 4. Deploy DAG

```bash
# Copy DAG to Airflow
cp dags/call_functions_facebook_ads_simple_dag.py /path/to/airflow/dags/

# Verify DAG appears
airflow dags list | grep facebook_ads_simple
```

### 5. Test Run

```bash
# Trigger DAG manually
airflow dags trigger call_functions_facebook_ads_simple_etl

# Or with custom days_for_ago
airflow dags trigger call_functions_facebook_ads_simple_etl \
  --conf '{"days_for_ago": 2}'

# Check DAG status
airflow dags list-runs -d call_functions_facebook_ads_simple_etl

# View logs
airflow tasks logs call_functions_facebook_ads_simple_etl \
  facebook_ads_insights_simple_t1 <execution_date>
```

### 6. Verify Data

```sql
-- Check if data was inserted
SELECT 
    date,
    COUNT(*) as total_insights,
    COUNT(DISTINCT ad_id) as unique_ads,
    COUNT(DISTINCT campaign_id) as unique_campaigns,
    SUM(spend) as total_spend,
    currency
FROM core_facebook_ad_ad_insight_simple
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date, currency
ORDER BY date DESC;

-- Check specific day
SELECT *
FROM core_facebook_ad_ad_insight_simple
WHERE date = CURRENT_DATE - INTERVAL '1 day'
LIMIT 10;
```

---

## 📋 Files Overview

| File | Purpose |
|------|---------|
| **`facebook_ads_insights_ingestion_simple.py`** | Main ingestion script (400 lines) |
| **`call_functions_facebook_ads_simple_dag.py`** | Airflow DAG |
| **`facebook_ads_tables_simple.sql`** | Table DDL |
| **`facebook_ads_migration.md`** | Migration guide |
| **`facebook_ads_comparison.md`** | Comparison: Original vs Simplified |

---

## 🎯 What This Does

1. **Reads** active accounts from `core_facebook_ad_account`
2. **Calls** Facebook Ads API to get insights
3. **Writes** directly to `core_facebook_ad_ad_insight_simple` (no foreign keys!)
4. **Handles** rate limiting with exponential backoff
5. **Runs** T-1, T-2, T-3 sequentially to avoid rate limits

---

## 📊 Table Schema

```sql
core_facebook_ad_ad_insight_simple (
    id SERIAL PRIMARY KEY,
    ad_id VARCHAR(255),           -- Facebook Ad ID (string, not FK)
    campaign_id VARCHAR(255),     -- Campaign ID (string, not FK)
    adset_id VARCHAR(255),        -- AdSet ID (string, not FK)
    account_id VARCHAR(255),      -- Account ID (string, not FK)
    date DATE,                    -- Insight date
    spend DOUBLE PRECISION,       -- Ad spend
    currency VARCHAR(10),         -- Currency code (VND, USD, etc.)
    region VARCHAR(255),          -- Geographic region
    brand_id INTEGER,             -- Brand association
    etl_created TIMESTAMP,        -- Created timestamp
    etl_updated TIMESTAMP,        -- Updated timestamp
    UNIQUE (ad_id, date, region)  -- Prevent duplicates
)
```

---

## 🔧 Troubleshooting

### Issue: Token Expired

```
ERROR: Token expired for account: My Account
```

**Solution:**
```sql
-- Update access token
UPDATE core_facebook_ad_account
SET access_token = 'new_long_lived_token',
    access_token_expires = '2026-12-31 23:59:59'
WHERE ad_account_id = '123456789';
```

### Issue: No Data Inserted

```sql
-- Check if accounts are active
SELECT * FROM core_facebook_ad_account WHERE is_active = TRUE;

-- Check Airflow logs
airflow tasks logs call_functions_facebook_ads_simple_etl \
  facebook_ads_insights_simple_t1 <execution_date>
```

### Issue: Rate Limit Reached

```
WARNING: Rate limit high. Cooling 120s
```

**Solution:** This is normal! Script will auto-wait and retry.

### Issue: Connection Error

```
ERROR: Failed to connect to PostgreSQL
```

**Solution:** Check Airflow connection `custom_db_dev` configuration.

---

## 📈 Performance

| Metric | Value |
|--------|-------|
| **Speed** | ~5 seconds per 1000 insights |
| **CPU** | 1-2 cores |
| **Memory** | 1-2 GB |
| **Duration** | 5-30 minutes (depends on account size) |

---

## ✅ Success Checklist

- [ ] Table `core_facebook_ad_ad_insight_simple` created
- [ ] At least 1 active account in `core_facebook_ad_account`
- [ ] Access token is valid (not expired)
- [ ] Airflow connection `custom_db_dev` configured
- [ ] DAG appears in Airflow UI
- [ ] Manual trigger successful
- [ ] Data appears in table
- [ ] Logs show no errors

---

## 🎉 Next Steps

Once this works:

1. ✅ Let it run for 3-5 days
2. ✅ Verify data quality
3. ✅ Compare with old Django system
4. ✅ Kill Django app! 🎯

---

## 📞 Support

If you encounter issues:

1. Check Airflow logs
2. Check PostgreSQL logs
3. Verify account configuration
4. Check token expiration
5. Review `facebook_ads_comparison.md` for architecture details

---

## 🔗 Related Files

- **Migration Guide**: `docs/facebook_ads_migration.md`
- **Comparison**: `docs/facebook_ads_comparison.md`
- **DDL**: `sql/facebook_ads_tables_simple.sql`
- **Script**: `app/call_functions/facebook_ads_insights_ingestion_simple.py`
- **DAG**: `dags/call_functions_facebook_ads_simple_dag.py`
