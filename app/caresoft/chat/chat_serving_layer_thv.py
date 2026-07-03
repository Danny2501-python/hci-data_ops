"""
Chat Serving Layer THV: Refresh Chat serving layer for brand erp_id=8
"""

import os
import sys
from datetime import date, datetime, timedelta
from loguru import logger

# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

from chat_serving_layer import ChatServingLayer


class ChatServingLayerThv(ChatServingLayer):
    """
    Chat serving layer specialized for THV (erp_id=8)
    Refreshes from caresoft.cs_chat_api_thv to cs_chat_api_thv
    """
    SOURCE_TABLE = 'caresoft.cs_chat_api_thv'
    TARGET_TABLE = 'cs_chat_api_thv'


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Chat Serving Layer THV for {snapshot_date}")
    
    try:
        job = ChatServingLayerThv(snapshot_date)
        job.run()
    except Exception as e:
        logger.error(f"Failed Chat Serving Layer THV: {e}")


if __name__ == "__main__":
    main()
