"""
Deal Detail Enrichment THV: Pull full deal details for brand erp_id=8

NOTE: CareSoft is not configured to send 'deal' webhooks to THV's ERP,
so the Silver webhook table (caresoft.cs_deal_wehook_thv) never has any
data and the base class's webhook-based discovery (get_deal_ids_to_enrich)
always returns an empty list. This subclass instead discovers changed
deal_ids directly via the CareSoft `GET /{domain}/api/v1/deals` list
endpoint (supports updated_since/updated_to + created_since/created_to +
pagination), then reuses the base class's per-id `GET /deal/{id}` call to
fetch full detail (comments, tags, custom_fields, etc. are only present
on the detail endpoint, not on the list endpoint).
"""

import os
import sys
import time
from datetime import date, datetime, timedelta
import requests
from loguru import logger

# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

import deal_detail_enrichment
from common.brand_config import get_brand_by_erp_id
from common.caresoft_api import get_api_url, get_api_headers

# Override globals for THV
deal_detail_enrichment.WEBHOOK_TABLE = 'caresoft.cs_deal_wehook_thv'  # unused by this subclass, kept for reference
deal_detail_enrichment.DEAL_DETAIL_TABLE = 'caresoft.cs_deal_detail_api_thv'
deal_detail_enrichment.DEAL_DETAIL_ERROR_TABLE = 'caresoft.cs_deal_detail_error_api_thv'

CARESOFT_PAGE_SIZE = int(os.getenv('CARESOFT_PAGE_SIZE', '500'))


class DealDetailEnrichmentThv(deal_detail_enrichment.DealDetailEnrichment):
    """
    Deal detail enrichment class specialized for THV (erp_id=8).
    Discovers deal_ids to enrich via CareSoft's /deals list API instead
    of the (unused, always-empty) webhook Silver table.
    """

    def __init__(self, snapshot_date: date = None):
        super().__init__(snapshot_date)
        self.brand = get_brand_by_erp_id(8)

    def _fetch_deal_ids_page(self, since_key: str, to_key: str, date_since: str, date_to: str) -> set:
        """Page through GET /deals for one (since,to) window, return set of deal ids"""
        deal_ids = set()
        page = 1

        while True:
            params = {since_key: date_since, to_key: date_to, 'page': page, 'count': CARESOFT_PAGE_SIZE}
            try:
                response = requests.get(
                    get_api_url(self.brand, 'deals'),
                    params=params,
                    headers=get_api_headers(self.brand),
                    timeout=30
                )
            except Exception as e:
                logger.error(f"❌ Exception calling /deals page {page} ({since_key}={date_since}): {e}")
                break

            if response.status_code != 200:
                logger.error(f"❌ /deals API error {response.status_code}: {response.text[:200]}")
                break

            data = response.json()
            deals = data.get('deals', [])
            if not deals:
                break

            for d in deals:
                if d.get('id'):
                    deal_ids.add(d['id'])

            logger.info(f"[{since_key}={date_since}] page {page}: {len(deals)} deals (numFound={data.get('numFound')})")

            if len(deals) < CARESOFT_PAGE_SIZE:
                break

            page += 1
            time.sleep(1)  # rate limiting

        return deal_ids

    def get_deal_ids_to_enrich(self, brand_erp_id: int):
        """
        Override: discover changed deal_ids via CareSoft /deals list API
        (updated + created windows) instead of reading webhook Silver.
        """
        if not self.brand:
            logger.warning("Brand THV (erp_id=8) not found in core_brand, cannot fetch deal list")
            return []

        today = self.snapshot_date + timedelta(days=1)
        yesterday = today - timedelta(days=1)
        day_before_yesterday = yesterday - timedelta(days=1)

        def fmt(d):
            return d.strftime('%Y-%m-%d') + 'T00:00:00Z'

        # 3 windows, same safety-net pattern as ticket_ingestion.py:
        # deals updated in [yesterday, today), deals created in [yesterday, today),
        # and deals created in [day_before_yesterday, yesterday) as a late-arrival catch-up.
        windows = [
            ('updated_since', 'updated_to', fmt(yesterday), fmt(today)),
            ('created_since', 'created_to', fmt(yesterday), fmt(today)),
            ('created_since', 'created_to', fmt(day_before_yesterday), fmt(yesterday)),
        ]

        deal_ids = set()
        for since_key, to_key, date_since, date_to in windows:
            deal_ids |= self._fetch_deal_ids_page(since_key, to_key, date_since, date_to)

        logger.success(f"✅ Found {len(deal_ids)} distinct deal_ids to enrich via /deals list API")
        return list(deal_ids)

    def run(self):
        # Temporarily mock get_active_brands to only return THV
        original_get_active_brands = deal_detail_enrichment.get_active_brands
        try:
            deal_detail_enrichment.get_active_brands = lambda: [self.brand] if self.brand else []
            super().run()
        finally:
            deal_detail_enrichment.get_active_brands = original_get_active_brands


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Deal Detail Enrichment THV for {snapshot_date}")
    
    try:
        job = DealDetailEnrichmentThv(snapshot_date)
        job.run()
    except Exception as e:
        logger.error(f"Failed Deal Detail Enrichment THV: {e}")


if __name__ == "__main__":
    main()
