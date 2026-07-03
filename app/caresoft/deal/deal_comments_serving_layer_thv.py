"""
Deal Comments Serving Layer THV: Sync comments from Iceberg Gold to PostgreSQL DWH for THV (erp_id=8)
"""

import os
import sys
from datetime import date, datetime, timedelta
from loguru import logger

# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

import deal_comments_serving_layer

# Override globals for THV
deal_comments_serving_layer.SOURCE_TABLE = 'caresoft.cs_deal_api_trans_comments_thv'


class DealCommentsServingLayerThv(deal_comments_serving_layer.DealCommentsServingLayer):
    """
    Deal comments serving layer specialized for THV (erp_id=8)
    """
    pass


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Deal Comments Serving Layer THV for {snapshot_date}")
    
    try:
        job = DealCommentsServingLayerThv(snapshot_date)
        job.run()
    except Exception as e:
        logger.error(f"Failed Deal Comments Serving Layer THV: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
