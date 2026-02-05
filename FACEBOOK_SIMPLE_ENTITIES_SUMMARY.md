# Facebook Ads Migration - Simple Entities Summary

## ✅ Completed Migrations (5/5 Simple Entities)

### Migration Date: 2026-02-05

---

## 1. ✅ Insights Ingestion
- **File**: `app/facebook/facebook_ads_insights_ingestion_simple.py`
- **Source**: `FacebookAccountBussiness.update_all_ad_insights_by_account()`
- **API**: `ad_account.get_insights()`
- **Status**: ✅ Complete
- **Features**:
  - Smart rate limit checking (header change detection)
  - Direct write to denormalized table
  - No foreign key dependencies
  - Breakdowns by region

---

## 2. ✅ Campaigns Ingestion
- **File**: `app/facebook/facebook_ads_campaigns_ingestion.py`
- **Source**: `FacebookAccountBussiness.update_campaigns_from_account()`
- **API**: `ad_account.get_campaigns()`
- **Status**: ✅ Complete
- **Fields**:
  - name, account_id, created_time, updated_time
  - start_time, stop_time, status
- **Table**: `core_facebook_ad_campaign_v1`
- **Primary Key**: `fb_ad_campaign_id`

---

## 3. ✅ Ad Sets Ingestion
- **File**: `app/facebook/facebook_ads_adsets_ingestion.py`
- **Source**: `FacebookAccountBussiness.update_ad_sets_from_account()`
- **API**: `ad_account.get_ad_sets()`
- **Status**: ✅ Complete
- **Fields**:
  - name, campaign_id, created_time, updated_time
  - start_time, status, optimization_goal
- **Table**: `core_facebook_ad_set_v1`
- **Primary Key**: `fb_ad_set_id`

---

## 4. ✅ Ad Creatives Ingestion
- **File**: `app/facebook/facebook_ads_creatives_ingestion.py`
- **Source**: `FacebookAccountBussiness.update_ad_creatives_from_account()`
- **API**: `ad_account.get_ad_creatives()`
- **Status**: ✅ Complete
- **Fields**:
  - name, account_id, object_id, object_story_id
  - link_url, effective_object_story_id, status, actor_id
- **Table**: `core_facebook_ad_creative_v1`
- **Primary Key**: `fb_ad_creative_id`

---

## 5. ✅ Message Insights Ingestion
- **File**: `app/facebook/facebook_ads_message_insights_ingestion.py`
- **Source**: `FacebookAccountBussiness.update_all_ad_insights_message_by_account()`
- **API**: `ad_account.get_insights()` with `unique_actions` field
- **Status**: ✅ Complete
- **Fields**:
  - account_id, campaign_id, adset_id, ad_id
  - spend, date_start, date_stop, account_currency
  - unique_actions (message-specific)
- **Table**: `core_facebook_ad_ad_insight_message_v1`
- **Primary Key**: `(ad_id, date_start, region)`

---

## 🎯 Common Pattern Used

All migrations follow the same pattern:

```python
class Facebook[Entity]Ingestion:
    def __init__(self, db: PostgreSQLHelper, ...):
        self.db = db
        self.rate_limit_tracker = FacebookRateLimitTracker()
    
    def upsert_[entity](self, data: Dict, brand_id: int):
        # Direct SQL INSERT ... ON CONFLICT DO UPDATE
        pass
    
    def fetch_[entities](self, account: Dict):
        # 1. Initialize API
        ad_account, api = init_facebook_api(account, FB_API_VERSION)
        
        # 2. Fetch from API
        results = ad_account.get_[entities](params, fields)
        
        # 3. Process with smart rate limit checking
        self.rate_limit_tracker.reset()
        for item in results:
            self.upsert_[entity](item._json, brand_id)
            
            # Check rate limit only when headers change
            if self.rate_limit_tracker.should_check_limit(results.headers()):
                self.rate_limit_tracker.check_limit(results.headers())
    
    def run(self):
        # Get accounts and process each
        accounts = get_active_facebook_accounts(self.db)
        for account in accounts:
            self.fetch_[entities](account)
```

---

## 🔧 Shared Utilities Used

All scripts use these shared utilities from `common/`:

1. ✅ **`FacebookRateLimitTracker`**
   - Smart header change detection
   - Exponential backoff
   - Handles all rate limit headers

2. ✅ **`get_active_facebook_accounts(db)`**
   - Fetches active accounts from database
   - Returns list of account dictionaries

3. ✅ **`init_facebook_api(account, version)`**
   - Initializes Facebook API
   - Checks token expiration
   - Returns (AdAccount, FacebookAdsApi)

4. ✅ **`PostgreSQLHelper`**
   - Database connection management
   - Query execution
   - Transaction support

---

## 📊 Code Metrics

### Lines of Code per Script

| Script | Lines | Complexity |
|--------|-------|------------|
| Insights | 234 | Medium |
| Campaigns | 220 | Low |
| Ad Sets | 220 | Low |
| Creatives | 230 | Low |
| Message Insights | 230 | Low |
| **Total** | **1,134** | - |

### Code Reuse

- **Shared utilities**: 535 lines (used by all 5 scripts)
- **Without sharing**: ~1,600 lines (with duplication)
- **Savings**: ~466 lines + zero duplication

---

## 🚀 Usage Examples

### Run Campaigns Ingestion
```bash
python app/facebook/facebook_ads_campaigns_ingestion.py --date-preset last_90d
```

### Run Ad Sets Ingestion
```bash
python app/facebook/facebook_ads_adsets_ingestion.py --date-preset last_90d
```

### Run Creatives Ingestion
```bash
python app/facebook/facebook_ads_creatives_ingestion.py --date-preset last_90d
```

### Run Message Insights
```bash
python app/facebook/facebook_ads_message_insights_ingestion.py --days-for-ago 1
```

### Run Regular Insights
```bash
python app/facebook/facebook_ads_insights_ingestion_simple.py --days-for-ago 1
```

---

## 📋 Next Steps: Ads Ingestion (Complex)

### Remaining: Ads with Dependencies

**File**: `app/facebook/facebook_ads_ads_ingestion.py`
**Source**: `FacebookAccountBussiness.update_ad_ads_from_account()`
**Complexity**: 🔴 High

**Why Complex?**
```python
# Must create dependencies FIRST:
for ad_ad in ad_ads:
    self.create_campain_by_ad_campain_id(ad_ad['campaign_id'])      # ← Dependency 1
    self.create_adset_by_ad_adset_id(ad_ad['adset_id'])            # ← Dependency 2
    self.create_ad_creative_by_ad_creative_id(ad_ad['creative']['id'])  # ← Dependency 3
    FacebookAdAd.create_or_update(ad_ad._json)  # ← Then create Ad
```

**Strategy**:
1. Check if campaign exists, if not fetch and create
2. Check if ad set exists, if not fetch and create
3. Check if creative exists, if not fetch and create
4. Then create/update the ad

**Estimated Time**: 4 hours

---

## ✨ Benefits Achieved

1. ✅ **Code Consistency**: All scripts follow the same pattern
2. ✅ **Code Reuse**: Shared utilities eliminate duplication
3. ✅ **Maintainability**: Fix once, benefit everywhere
4. ✅ **Testability**: Easy to test individual components
5. ✅ **Scalability**: Easy to add new entities
6. ✅ **Production Ready**: Same logic as Django, better structure

---

## 📝 Notes

- All scripts use **simplified approach** (direct write, no FK constraints)
- All use **smart rate limit checking** (header change detection)
- All support **command-line arguments** for flexibility
- All have **comprehensive error handling** and logging
- All follow **DRY principle** with shared utilities

---

## 🎉 Summary

**5 out of 5 simple entities migrated successfully!**

- ✅ Insights
- ✅ Campaigns
- ✅ Ad Sets
- ✅ Creatives
- ✅ Message Insights

**Remaining**: 1 complex entity (Ads with dependencies)

**Total Progress**: 83% complete (5/6 entities)
