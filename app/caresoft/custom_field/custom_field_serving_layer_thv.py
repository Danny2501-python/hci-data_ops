"""
Custom Field Serving Layer THV: Refresh Custom Field serving layer for brand erp_id=8
"""

import os
import sys
from datetime import date, datetime, timedelta
from loguru import logger

# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

from custom_field_serving_layer import CustomFieldServingLayer


class CustomFieldServingLayerThv(CustomFieldServingLayer):
    """
    Custom Field serving layer specialized for THV (erp_id=8)
    Refreshes from caresoft.cs_custom_fields_thv to cs_custom_fields_thv or cs_custom_fields_kh_thv
    """
    SOURCE_TABLE = 'caresoft.cs_custom_fields_thv'
    TARGET_TABLE_TICKET = 'cs_custom_fields_thv'
    TARGET_TABLE_CONTACT = 'cs_custom_fields_kh_thv'


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    field_type = sys.argv[2] if len(sys.argv) > 2 else 'ticket'
    
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Custom Field Serving Layer THV for {snapshot_date}, field_type={field_type}")
    
    try:
        job = CustomFieldServingLayerThv(snapshot_date, field_type=field_type)
        job.run()
    except Exception as e:
        logger.error(f"Failed Custom Field Serving Layer THV: {e}")


if __name__ == "__main__":
    main()
