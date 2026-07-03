"""
Ticket Source Metadata Serving Layer THV: Refresh Ticket Source Metadata serving layer for brand erp_id=8
"""

import os
import sys
from datetime import date, datetime, timedelta
from loguru import logger

# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

from ticket_source_metadata_serving_layer import TicketSourceMetadataServingLayer


class TicketSourceMetadataServingLayerThv(TicketSourceMetadataServingLayer):
    """
    Ticket Source Metadata serving layer specialized for THV (erp_id=8)
    Refreshes from caresoft.cs_ticket_source_detail_api_thv to cs_ticket_source_detail_api_thv
    """
    SOURCE_TABLE = 'caresoft.cs_ticket_source_detail_api_thv'
    TARGET_TABLE = 'cs_ticket_source_detail_api_thv'


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Ticket Source Metadata Serving Layer THV for {snapshot_date}")
    
    try:
        job = TicketSourceMetadataServingLayerThv(snapshot_date)
        job.run()
    except Exception as e:
        logger.error(f"Failed Ticket Source Metadata Serving Layer THV: {e}")


if __name__ == "__main__":
    main()
