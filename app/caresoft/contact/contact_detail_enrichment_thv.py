"""
Contact Detail Enrichment THV: Pull full contact details for brand erp_id=8
"""

import os
import sys
from datetime import date, datetime, timedelta
from loguru import logger

# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

from contact_detail_enrichment import ContactDetailEnrichment
from common.brand_config import get_brand_by_erp_id


class ContactDetailEnrichmentThv(ContactDetailEnrichment):
    """
    Contact detail enrichment class specialized for THV (erp_id=8)
    Refreshes from caresoft.cs_contacts_api_ttc_thv to cs_contact_api_thv
    """
    CONTACT_BRONZE_TABLE = 'caresoft.cs_contacts_api_ttc_thv'
    CONTACT_DETAIL_ERROR_TABLE = 'caresoft.cs_contact_error_thv'
    TARGET_PG_TABLE = 'cs_contact_api_thv'


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    contact_ids_str = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else None
    brand_id_str = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else None
    from_table_str = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] else None

    snapshot_date = snapshot_date_str if snapshot_date_str else (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    brand = get_brand_by_erp_id(8)
    if not brand:
        logger.error("Brand with erp_id=8 (THV) not found")
        return
        
    job = ContactDetailEnrichmentThv(snapshot_date)

    # Resolve manual_ids from either comma-separated string OR from a table
    manual_ids = None
    if contact_ids_str and contact_ids_str != "":
        manual_ids = [cid.strip() for cid in contact_ids_str.split(',')]
    elif from_table_str and from_table_str != "":
        manual_ids = job.get_contact_ids_from_table(from_table_str)
        
    # Standard run or backfill
    try:
        if manual_ids:
            logger.info(f"🚀 Running BACKFILL mode for {len(manual_ids)} Contact IDs")
            job.run(brand, manual_contact_ids=manual_ids)
        else:
            job.run(brand)
    except Exception as e:
        logger.error(f"Failure running ContactDetailEnrichmentThv for brand {brand['erp_id']}: {e}")


if __name__ == "__main__":
    main()
