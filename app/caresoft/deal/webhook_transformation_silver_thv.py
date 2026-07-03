"""
Webhook Transformation THV: Silver transformation class specialized for THV (erp_id=8)
"""

import os
import sys
from datetime import date, datetime, timedelta
from loguru import logger

# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

import webhook_transformation_silver

# Override globals for THV
webhook_transformation_silver.SOURCE_TABLE = 'caresoft.wehook_api_caresoft_thv'
webhook_transformation_silver.TARGET_TABLE = 'caresoft.cs_deal_wehook_thv'
webhook_transformation_silver.TARGET_TABLE_ERROR = 'caresoft.cs_deal_wehook_error_thv'


class WebhookTransformationThv(webhook_transformation_silver.WebhookTransformation):
    """
    Webhook transformation specialized for THV (erp_id=8)
    """
    pass


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    start = datetime.now()
    job = WebhookTransformationThv(snapshot_date)
    job.run()
    logger.info(f"Total Duration: {(datetime.now() - start).total_seconds()}s")


if __name__ == "__main__":
    main()
