"""
Ticket Source Metadata Ingestion THV: Pull Ticket Source Metadata for brand erp_id=8 to cs_ticket_source_detail_api_thv table
"""

import os
import sys
from datetime import date, datetime, timedelta
from loguru import logger

# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

from ticket_source_metadata import TicketSourceMetadata
from common.brand_config import get_brand_by_erp_id


class TicketSourceMetadataThv(TicketSourceMetadata):
    """
    Ticket source metadata ingestion class specialized for THV (erp_id=8)
    Writes to caresoft.cs_ticket_source_detail_api_thv table
    """
    TICKET_SOURCE_METADATA_TABLE = 'caresoft.cs_ticket_source_detail_api_thv'


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Ticket Source Metadata Ingestion THV for {snapshot_date}")
    
    brand = get_brand_by_erp_id(8)
    if not brand:
        logger.error("Brand with erp_id=8 (THV) not found")
        sys.exit(1)
        
    try:
        job = TicketSourceMetadataThv(snapshot_date)
        job.run(brand=brand)
    except Exception as e:
        logger.error(f"Failed brand {brand['name']} (erp_id=8): {e}")


if __name__ == "__main__":
    main()
