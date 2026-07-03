"""
Ticket Detail Enrichment THV: Pull full ticket details for brand erp_id=8
"""

import os
import sys
from datetime import date, datetime, timedelta
from loguru import logger

# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

from ticket_detail_enrichment import TicketDetailEnrichment
from common.brand_config import get_brand_by_erp_id


class TicketDetailEnrichmentThv(TicketDetailEnrichment):
    """
    Ticket detail enrichment class specialized for THV (erp_id=8)
    Refreshes from caresoft.cs_ticket_api_thv to cs_ticket_source_api_thv
    """
    TICKET_BRONZE_TABLE = 'caresoft.cs_ticket_api_thv'
    TARGET_PG_TABLE = 'cs_ticket_source_api_thv'


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = snapshot_date_str if snapshot_date_str else (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    brand = get_brand_by_erp_id(8)
    if not brand:
        logger.error("Brand with erp_id=8 (THV) not found")
        return
        
    job = TicketDetailEnrichmentThv(snapshot_date)
    
    try:
        job.run(brand)
    except Exception as e:
        logger.error(f"Failure running TicketDetailEnrichmentThv for brand {brand['erp_id']}: {e}")


if __name__ == "__main__":
    main()
