# Facebook Ads Migration Plan

## Overview
Migrate all Facebook Ads ETL processes from Django (`etl_marketing/fb_business.py`) to new Airflow architecture (`data-ops`).

## ✅ Completed Migrations

### 1. Insights Ingestion (DONE)
- **File**: `app/facebook/facebook_ads_insights_ingestion_simple.py`
- **Status**: ✅ Complete and tested
- **Features**:
  - Smart rate limit checking (headers change detection)
  - Direct write to denormalized table
  - No foreign key dependencies
  - Uses shared utilities

---

## 🔄 Pending Migrations

### 2. Campaigns Ingestion
- **Source**: `FacebookAccountBussiness.update_campaigns_from_account()`
- **Target**: `app/facebook/facebook_ads_campaigns_ingestion.py`
- **API Endpoint**: `ad_account.get_campaigns()`
- **Fields**:
  ```python
  Campaign.Field.name
  Campaign.Field.account_id
  Campaign.Field.created_time
  Campaign.Field.updated_time
  Campaign.Field.status
  Campaign.Field.objective
  Campaign.Field.daily_budget
  Campaign.Field.lifetime_budget
  ```
- **Logic**:
  - Fetch campaigns with `date_preset='last_90d'`
  - Create or update in database
  - Check rate limit after each batch
- **Complexity**: Low (similar to insights)

---

### 3. Ad Sets Ingestion
- **Source**: `FacebookAccountBussiness.update_ad_sets_from_account()`
- **Target**: `app/facebook/facebook_ads_adsets_ingestion.py`
- **API Endpoint**: `ad_account.get_ad_sets()`
- **Fields**:
  ```python
  AdSet.Field.name
  AdSet.Field.campaign_id
  AdSet.Field.created_time
  AdSet.Field.updated_time
  AdSet.Field.status
  AdSet.Field.targeting
  AdSet.Field.daily_budget
  AdSet.Field.lifetime_budget
  ```
- **Logic**:
  - Fetch ad sets with `date_preset='last_90d'`
  - Create or update in database
  - Check rate limit after each batch
- **Complexity**: Low

---

### 4. Ad Creatives Ingestion
- **Source**: `FacebookAccountBussiness.update_ad_creatives_from_account()`
- **Target**: `app/facebook/facebook_ads_creatives_ingestion.py`
- **API Endpoint**: `ad_account.get_ad_creatives()`
- **Fields**:
  ```python
  AdCreative.Field.name
  AdCreative.Field.title
  AdCreative.Field.body
  AdCreative.Field.image_url
  AdCreative.Field.video_id
  AdCreative.Field.object_story_spec
  AdCreative.Field.link_url
  ```
- **Logic**:
  - Fetch creatives with `date_preset='last_90d'`
  - Create or update in database
  - Check rate limit after each batch
- **Complexity**: Medium (complex JSON structure)

---

### 5. Ads (Ad Ads) Ingestion
- **Source**: `FacebookAccountBussiness.update_ad_ads_from_account()`
- **Target**: `app/facebook/facebook_ads_ads_ingestion.py`
- **API Endpoint**: `ad_account.get_ads()`
- **Fields**:
  ```python
  Ad.Field.name
  Ad.Field.campaign_id
  Ad.Field.adset_id
  Ad.Field.creative
  Ad.Field.status
  Ad.Field.created_time
  Ad.Field.updated_time
  ```
- **Logic**:
  - Fetch ads with `date_preset='last_90d'` or time range
  - **IMPORTANT**: Create dependencies first:
    - Create campaign if not exists
    - Create ad set if not exists
    - Create creative if not exists
  - Then create/update ad
  - Check rate limit after each batch
- **Complexity**: High (has dependencies)

---

### 6. Insights by Campaigns
- **Source**: `FacebookAccountBussiness.update_all_ad_insights_by_campaigns()`
- **Target**: `app/facebook/facebook_ads_insights_by_campaigns_ingestion.py`
- **API Endpoint**: `campaign.get_insights()`
- **Logic**:
  - Loop through campaigns
  - Get insights for each campaign
  - More granular than account-level insights
- **Complexity**: Medium
- **Note**: May be redundant if account-level insights are sufficient

---

### 7. Message Insights
- **Source**: `FacebookAccountBussiness.update_all_ad_insights_message_by_account()`
- **Target**: `app/facebook/facebook_ads_message_insights_ingestion.py`
- **API Endpoint**: `ad_account.get_insights()` with message actions
- **Fields**: Message-specific metrics
- **Complexity**: Low

---

### 8. Page Conversations
- **Source**: `FacebookPageBussiness.get_page_converstation()`
- **Target**: `app/facebook/facebook_page_conversations_ingestion.py`
- **API Endpoint**: `page.get_conversations()`
- **Logic**: Different from Ads API, uses Page API
- **Complexity**: Medium

---

## 📋 Migration Strategy

### Phase 1: Simple Entities (Week 1)
1. ✅ Insights (DONE)
2. 🔄 Campaigns
3. 🔄 Ad Sets
4. 🔄 Ad Creatives

### Phase 2: Complex Dependencies (Week 2)
5. 🔄 Ads (with dependency creation)
6. 🔄 Message Insights

### Phase 3: Alternative Approaches (Week 3)
7. 🔄 Insights by Campaigns (evaluate if needed)
8. 🔄 Page Conversations

---

## 🎯 Common Pattern for All Migrations

```python
"""
Facebook [Entity] Ingestion
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List
from loguru import logger

from facebook_business.adobjects.[entity] import [Entity]

from common.facebook_api import (
    FacebookRateLimitTracker,
    get_active_facebook_accounts,
    init_facebook_api
)
from common.database import PostgreSQLHelper

POSTGRES_URI = os.getenv('POSTGRES_URI')
FB_API_VERSION = os.getenv('FB_API_VERSION', 'v18.0')


class Facebook[Entity]Ingestion:
    def __init__(self, db: PostgreSQLHelper):
        self.db = db
        self.rate_limit_tracker = FacebookRateLimitTracker()
    
    def fetch_[entities](self, account: Dict):
        """Fetch [entities] from Facebook API"""
        ad_account, api = init_facebook_api(account, FB_API_VERSION)
        
        if not ad_account:
            return
        
        params = {'date_preset': 'last_90d'}
        fields = [...]  # Entity-specific fields
        
        try:
            results = ad_account.get_[entities](params=params, fields=fields)
        except Exception as e:
            logger.error(f"Failed to get [entities]: {e}")
            return
        
        self.rate_limit_tracker.reset()
        count = 0
        
        for item in results:
            try:
                data = item._json
                self.upsert_[entity](data, account.get('current_brand_id'))
                count += 1
            except Exception as e:
                logger.error(f"Error processing [entity]: {e}")
            
            # Smart rate limit checking
            try:
                current_headers = results.headers()
                if self.rate_limit_tracker.should_check_limit(current_headers):
                    logger.debug(f"Headers changed, checking rate limit")
                    self.rate_limit_tracker.check_limit(current_headers)
            except Exception as e:
                logger.error(f"Error checking limit: {e}")
        
        logger.success(f"✅ Processed {count} [entities]")
    
    def upsert_[entity](self, data: Dict, brand_id: int):
        """Upsert [entity] to database"""
        # Implementation here
        pass
    
    def run(self):
        """Execute [entity] ingestion"""
        accounts = get_active_facebook_accounts(self.db)
        
        for account in accounts:
            try:
                logger.info(f"Processing account: {account['name']}")
                self.fetch_[entities](account)
            except Exception as e:
                logger.error(f"Failed to process account: {e}")
                continue


if __name__ == "__main__":
    db = PostgreSQLHelper(POSTGRES_URI)
    job = Facebook[Entity]Ingestion(db)
    job.run()
    db.close()
```

---

## 🔧 Shared Utilities Already Available

1. ✅ `FacebookRateLimitTracker` - Smart rate limit checking
2. ✅ `get_active_facebook_accounts()` - Get accounts from DB
3. ✅ `init_facebook_api()` - Initialize FB API
4. ✅ `PostgreSQLHelper` - Database operations

---

## 📊 Estimated Effort

| Entity | Complexity | Estimated Time | Dependencies |
|--------|-----------|----------------|--------------|
| Campaigns | Low | 2 hours | None |
| Ad Sets | Low | 2 hours | None |
| Creatives | Medium | 3 hours | Complex JSON |
| Ads | High | 4 hours | Campaigns, AdSets, Creatives |
| Message Insights | Low | 2 hours | None |
| Insights by Campaigns | Medium | 3 hours | Campaigns |
| Page Conversations | Medium | 3 hours | Different API |

**Total**: ~19 hours (~2.5 days)

---

## 🎯 Next Steps

1. **Immediate**: Start with Campaigns (simplest, no dependencies)
2. **Then**: Ad Sets and Creatives (parallel development possible)
3. **Finally**: Ads (requires all dependencies)
4. **Evaluate**: Message Insights and alternative approaches

---

## 📝 Notes

- All migrations will use the **simplified approach** (direct write, no foreign keys)
- All will use **shared utilities** from `common/`
- All will follow the **same pattern** for consistency
- All will have **smart rate limit checking**
- Consider creating **Airflow DAGs** for each entity after migration
