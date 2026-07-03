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
    Subclass that overrides the run method to process only THV
    """
    def run(self) -> dict:
        """
        Execute full ERP direct ingestion pipeline for THV
        """
        logger.info("=" * 80)
        logger.info(f"Starting ERP Direct Ingestion (Streaming Mode) for {self.snapshot_date}")
        logger.info("=" * 80)
        
        start_time = datetime.now()
        stats = {}
        total_records = 0
        
        # Ensure table exists
        table = self.ensure_table_exists()
        
        # Drop partition ONCE
        try:
            from pyiceberg.expressions import EqualTo
            import gc
            logger.info(f"Deleting partition snapshot_date={self.snapshot_date}...")
            table.delete(delete_filter=EqualTo("snapshot_date", self.snapshot_date))
            logger.success(f"✅ Deleted partition snapshot_date={self.snapshot_date}")
            gc.collect()
        except Exception as e:
            logger.warning(f"Could not delete partition: {e}")
        
        # Process THV ERP
        for erp_name in ['thv']:
            try:
                logger.info(f"\n{'='*60}")
                logger.info(f"Processing ERP: {erp_name.upper()}")
                
                # Create connection
                engine = self.create_erp_connection(erp_name)
                
                if not engine:
                    stats[erp_name] = {'status': 'skipped', 'records': 0}
                    continue
                
                # STREAM Data -> PyArrow -> Iceberg
                record_count = self.stream_and_ingest(erp_name, engine, table)
                
                total_records += record_count
                stats[erp_name] = {'status': 'success', 'records': record_count}
                
                # Close connection
                engine.dispose()
                gc.collect()
                
            except Exception as e:
                logger.error(f"❌ Failed to process ERP {erp_name.upper()}: {str(e)}")
                stats[erp_name] = {'status': 'failed', 'records': 0, 'error': str(e)}
        
        # Warning if no data
        if total_records == 0:
            logger.warning("No data from any ERP system")
        
        # Calculate metrics
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("=" * 80)
        logger.info(f"Ingestion completed. Total: {total_records} records. Duration: {duration:.2f}s")
        return stats


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
