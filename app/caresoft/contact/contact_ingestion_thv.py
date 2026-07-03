"""
Contact Ingestion THV: Pull Contact data for brand erp_id=8 to cs_contacts_api_ttc_thv table
"""

import os
import sys
from datetime import date, datetime, timedelta
from loguru import logger

# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

from contact_ingestion import ContactIngestion
from common.brand_config import get_brand_by_erp_id


class ContactIngestionThv(ContactIngestion):
    """
    Contact ingestion class specialized for THV (erp_id=8)
    Writes to caresoft.cs_contacts_api_ttc_thv table
    """
    CONTACT_BRONZE_TABLE = 'caresoft.cs_contacts_api_ttc_thv'


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    # Default to T-1 (yesterday) when no parameter provided
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Contact Ingestion THV for {snapshot_date}")
    
    # Get configuration for brand erp_id = 8
    brand = get_brand_by_erp_id(8)
    if not brand:
        logger.error("Brand with erp_id=8 (THV) not found in core_brand table")
        sys.exit(1)
        
    try:
        job = ContactIngestionThv(snapshot_date)
        job.run(brand, days_ago=3)
    except Exception as e:
        logger.error(f"Failed brand {brand['name']} (erp_id=8): {e}")


if __name__ == "__main__":
    main()
