"""
Deal Detail Serving Layer THV: Refresh Deal serving layer for brand erp_id=8
"""

import os
import sys
from datetime import date, datetime, timedelta
from loguru import logger

# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

import deal_detail_serving_layer

# Override globals for THV
deal_detail_serving_layer.SOURCE_TABLE = 'caresoft.cs_deal_detail_api_thv'


class ServingLayerRefreshThv(deal_detail_serving_layer.ServingLayerRefresh):
    """
    Serving layer refresh specialized for THV (erp_id=8)
    """
    pass


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Deal Detail Serving Layer THV for {snapshot_date}")
    
    try:
        job = ServingLayerRefreshThv(snapshot_date)
        job.run()
    except Exception as e:
        logger.error(f"Failed Deal Detail Serving Layer THV: {e}")


if __name__ == "__main__":
    main()
