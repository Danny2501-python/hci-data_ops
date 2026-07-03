"""
ERP Direct Ingestion THV: Ingest ERP api_log data for brand erp_id=8
"""

import os
import sys
from datetime import date, datetime, timedelta
from loguru import logger

# Add parent directories to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

import erp_webhook_ingestion

# Override globals for THV
erp_webhook_ingestion.TABLE_NAME = 'caresoft.wehook_api_caresoft_thv'
erp_webhook_ingestion.ERP_CONFIGS = {
    'thv': {'uri': os.getenv('ERP_THV_URI'), 'brand_etl': 8}
}


class ERPDirectIngestionThv(erp_webhook_ingestion.ERPDirectIngestion):
    """
    Subclass that uses overridden module-level variables
    """
    pass


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting ERP Direct Ingestion THV for {snapshot_date}")
    
    try:
        ingestion = ERPDirectIngestionThv(snapshot_date)
        stats = ingestion.run()
        
        failed_erps = [erp for erp, stat in stats.items() if stat['status'] == 'failed']
        if failed_erps:
            logger.warning(f"⚠️  Some ERPs failed: {failed_erps}")
            sys.exit(0)
        else:
            total_records = sum(stat['records'] for stat in stats.values())
            logger.success(f"✅ ERP direct ingestion THV completed: {total_records} total records")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"❌ ERP direct ingestion THV failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
