# Facebook Business SDK Fields Verification

## ✅ Verified Against Original Code

Tất cả fields đã được verify với code gốc `etl_marketing/app/facebook_ads/fb_business.py`

## 📋 Field Mapping

### 1. Campaigns (Lines 37-43)
**Original Code:**
```python
campaigns_fields = [
    Campaign.Field.name,
    Campaign.Field.account_id,
    Campaign.Field.created_time,
    Campaign.Field.updated_time,
    Campaign.Field.start_time,
    Campaign.Field.stop_time,
    Campaign.Field.status
]
```

**New Code (data-ops):**
```python
fields = [
    Campaign.Field.name,
    Campaign.Field.account_id,
    Campaign.Field.created_time,
    Campaign.Field.updated_time,
    Campaign.Field.start_time,
    Campaign.Field.stop_time,
    Campaign.Field.status
]
```

**Status:** ✅ **MATCH 100%**

---

### 2. Ad Sets (Lines 45-51)
**Original Code:**
```python
adset_fields = [
    AdSet.Field.name,
    AdSet.Field.campaign_id,
    AdSet.Field.created_time,
    AdSet.Field.updated_time,
    AdSet.Field.start_time,
    AdSet.Field.status,
    AdSet.Field.optimization_goal  # ❌ NOT in DB schema
]
```

**New Code (data-ops):**
```python
fields = [
    AdSet.Field.name,
    AdSet.Field.campaign_id,
    AdSet.Field.created_time,
    AdSet.Field.updated_time,
    AdSet.Field.start_time,
    AdSet.Field.status
]
```

**Status:** ✅ **CORRECT** (removed `optimization_goal` - not in DB schema)

---

### 3. Creatives (Lines 53-62)
**Original Code:**
```python
ad_creative_fields = [
    AdCreative.Field.name,
    AdCreative.Field.account_id,
    AdCreative.Field.object_id,
    AdCreative.Field.object_story_id,
    AdCreative.Field.link_url,
    AdCreative.Field.effective_object_story_id,
    AdCreative.Field.status,
    AdCreative.Field.actor_id,  # ❌ NOT in DB schema
]
```

**New Code (data-ops):**
```python
fields = [
    AdCreative.Field.name,
    AdCreative.Field.account_id,
    AdCreative.Field.object_id,
    AdCreative.Field.object_story_id,
    AdCreative.Field.link_url,
    AdCreative.Field.effective_object_story_id,
    AdCreative.Field.status
]
```

**Status:** ✅ **CORRECT** (removed `actor_id` - not in DB schema)

---

### 4. Ads (Lines 64-73)
**Original Code:**
```python
ad_ad_fields = [
    Ad.Field.account_id,
    Ad.Field.name,
    Ad.Field.campaign_id,
    Ad.Field.adset_id,
    Ad.Field.creative,
    Ad.Field.created_time,
    Ad.Field.updated_time,
    Ad.Field.status,
    Ad.Field.targeting  # ❌ Removed - not needed
]
```

**New Code (data-ops):**
```python
fields = [
    Ad.Field.account_id,      # Fetched but not saved (for validation/logging)
    Ad.Field.name,
    Ad.Field.campaign_id,     # Fetched but not saved (for reference)
    Ad.Field.adset_id,
    Ad.Field.creative,
    Ad.Field.created_time,
    Ad.Field.updated_time,
    Ad.Field.status
]
```

**Status:** ✅ **SAFE** (fetch same fields as original, only save what's in DB schema)

**Note:** We fetch `account_id` and `campaign_id` from API (same as original) but don't save to DB since they're not in schema. Only `targeting` was removed.

---

### 5. Insights (Lines 75-85)
**Original Code:**
```python
ad_ad_insight_fields = [
    AdsInsights.Field.account_id,  # ❌ NOT in DB schema
    AdsInsights.Field.campaign_id,  # ❌ NOT in DB schema
    AdsInsights.Field.adset_id,  # ❌ NOT in DB schema
    AdsInsights.Field.ad_id,
    AdsInsights.Field.spend,
    AdsInsights.Field.date_start,
    AdsInsights.Field.date_stop,  # ❌ NOT in DB schema
    AdsInsights.Field.account_currency,
]
```

**New Code (data-ops):**
```python
fields = [
    AdsInsights.Field.ad_id,
    AdsInsights.Field.spend,
    AdsInsights.Field.date_start,
    AdsInsights.Field.account_currency,
    AdsInsights.Field.region
]
```

**Status:** ✅ **CORRECT** (removed unused fields, added `region` for breakdown)

---

### 6. Message Insights (Lines 87-97)
**Original Code:**
```python
ad_ad_insight_message_fields = [
    AdsInsights.Field.account_id,  # ❌ NOT in DB schema
    AdsInsights.Field.campaign_id,  # ❌ NOT in DB schema
    AdsInsights.Field.adset_id,  # ❌ NOT in DB schema
    AdsInsights.Field.ad_id,
    AdsInsights.Field.spend,
    AdsInsights.Field.date_start,
    AdsInsights.Field.date_stop,  # ❌ NOT in DB schema
    AdsInsights.Field.account_currency,
    AdsInsights.Field.unique_actions
]
```

**New Code (data-ops):**
```python
fields = [
    AdsInsights.Field.ad_id,
    AdsInsights.Field.spend,
    AdsInsights.Field.date_start,
    AdsInsights.Field.account_currency,
    AdsInsights.Field.region,
    AdsInsights.Field.unique_actions
]
```

**Status:** ✅ **CORRECT** (removed unused fields, added `region` for breakdown)

---

## 🔍 Key Differences Summary

### Fields REMOVED (Not in DB Schema)
| Entity | Removed Fields | Reason |
|--------|---------------|--------|
| AdSets | `optimization_goal` | Not in `core_facebook_ad_set` table |
| Creatives | `actor_id` | Not in `core_facebook_ad_creative` table |
| Ads | `targeting` | Not needed (fetched `account_id`, `campaign_id` but not saved) |
| Insights | `account_id`, `campaign_id`, `adset_id`, `date_stop` | Not in `core_facebook_ad_ad_insight` table |
| Message Insights | `account_id`, `campaign_id`, `adset_id`, `date_stop` | Not in `core_facebook_ad_ad_insight_message` table |

### Fields ADDED
| Entity | Added Fields | Reason |
|--------|-------------|--------|
| Insights | `region` | Required for breakdown (composite key) |
| Message Insights | `region` | Required for breakdown (composite key) |

---

## 📊 DB Schema vs API Fields

### Campaigns
```sql
-- DB Columns
fb_ad_campaign_id, name, fb_ad_account_id, created_time, updated_time,
start_time, stop_time, status, company_id

-- API Fields (mapped)
id → fb_ad_campaign_id
name → name
account_id → fb_ad_account_id
created_time → created_time
updated_time → updated_time
start_time → start_time
stop_time → stop_time
status → status
[brand_id from account] → company_id
```

### Ad Sets
```sql
-- DB Columns
fb_ad_set_id, name, fb_ad_campaign_id, created_time, updated_time,
start_time, status

-- API Fields (mapped)
id → fb_ad_set_id
name → name
campaign_id → fb_ad_campaign_id
created_time → created_time
updated_time → updated_time
start_time → start_time
status → status
```

### Creatives
```sql
-- DB Columns
fb_ad_creative_id, name, fb_ad_account_id, fb_object_id, fb_object_story_id,
fb_link_url, fb_effective_object_story_id, status, brand_id

-- API Fields (mapped)
id → fb_ad_creative_id
name → name
account_id → fb_ad_account_id
object_id → fb_object_id
object_story_id → fb_object_story_id
link_url → fb_link_url
effective_object_story_id → fb_effective_object_story_id
status → status
[brand_id from account] → brand_id
```

### Ads
```sql
-- DB Columns
fb_ad_ad_id, name, fb_ad_set_id, fb_creative_id, created_time,
updated_time, status, brand_id

-- API Fields (mapped)
id → fb_ad_ad_id
name → name
adset_id → fb_ad_set_id
creative.id → fb_creative_id
created_time → created_time
updated_time → updated_time
status → status
[brand_id from account] → brand_id
```

### Insights
```sql
-- DB Columns
date, fb_account_currency, fb_ad_ad_id, spend, region, brand_id

-- API Fields (mapped)
date_start → date
account_currency → fb_account_currency
ad_id → fb_ad_ad_id
spend → spend
region → region (from breakdown)
[brand_id from account] → brand_id
```

### Message Insights
```sql
-- DB Columns
date, fb_account_currency, fb_ad_ad_id, spend, region, brand_id, unique_actions

-- API Fields (mapped)
date_start → date
account_currency → fb_account_currency
ad_id → fb_ad_ad_id
spend → spend
region → region (from breakdown)
[brand_id from account] → brand_id
unique_actions → unique_actions (JSON)
```

---

## ✅ Verification Result

**All fields in new code are CORRECT and match:**
1. ✅ Facebook Business SDK field names
2. ✅ Database schema columns
3. ✅ Original code logic (with unnecessary fields removed)

**No changes needed!** 🎉

---

## 📝 Notes

1. **Breakdowns**: Both Insights scripts use `breakdowns=['region']` which is correct for composite key `(fb_ad_ad_id, date, region)`

2. **Creative Field**: In Ads, `creative` returns a dict with `id` field, extracted as `creative.get('id')` or `creative['id']`

3. **Brand ID**: Not from API, injected from account config (`account.get('current_brand_id')`)

4. **Timestamp Fields**: All timestamp fields (`created_time`, `updated_time`, etc.) are strings in ISO format from API, PostgreSQL handles conversion

5. **NULL Handling**: CSV writer converts `None` to empty string for proper COPY handling

---

**Verified Date**: 2026-02-06  
**Status**: ✅ All fields verified and correct  
**Source**: `etl_marketing/app/facebook_ads/fb_business.py` (lines 37-97)
