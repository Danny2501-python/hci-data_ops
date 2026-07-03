"""
Deal Detail Enrichment THV: Pull full deal details for brand erp_id=8
"""

import os
import sys
from datetime import date, datetime, timedelta
from loguru import logger

# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

import deal_detail_enrichment

# Override globals for THV
deal_detail_enrichment.WEBHOOK_TABLE = 'caresoft.cs_deal_wehook_thv'
deal_detail_enrichment.DEAL_DETAIL_TABLE = 'caresoft.cs_deal_detail_api_thv'
deal_detail_enrichment.DEAL_DETAIL_ERROR_TABLE = 'caresoft.cs_deal_detail_error_api_thv'


class DealDetailEnrichmentThv(deal_detail_enrichment.DealDetailEnrichment):
    """
    Deal detail enrichment class specialized for THV (erp_id=8)
    """
    def run(self):
        # Temporarily mock get_active_brands to only return THV
        from common.brand_config import get_brand_by_erp_id
        
        original_get_active_brands = deal_detail_enrichment.get_active_brands
        try:
            brand = get_brand_by_erp_id(8)
            deal_detail_enrichment.get_active_brands = lambda: [brand] if brand else []
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
