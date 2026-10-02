"""
Direct Ingestion from Source Postgres to Target Postgres (dwh_product) - Not Truncate ERP Version
Inherits from DirectPostgresToPostgres with truncate_erp=False to avoid code duplication.
"""

import os
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from call_functions.direct_postgres_ingestion import DirectPostgresToPostgres
from loguru import logger

class DirectPostgresToPostgresNotTruncateERP(DirectPostgresToPostgres):
    def __init__(self, snapshot_date=None, erp_only=None):
        super().__init__(snapshot_date=snapshot_date, truncate_erp=False, erp_only=erp_only)

if __name__ == "__main__":
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    s_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else None
    t_id_arg = sys.argv[2] if len(sys.argv) > 2 else None
    t_id = None
    if t_id_arg and t_id_arg.strip() and t_id_arg.lower() != 'none':
        logger.info(f"Raw table_id input from CLI: {t_id_arg}")
        raw_val = str(t_id_arg).strip('[] ')
        if ',' in raw_val:
            parts = raw_val.split(',')
            t_id = []
            for p in parts:
                clean_p = p.strip(' "\'')
                if clean_p.isdigit():
                    t_id.append(int(clean_p))
        else:
            clean_p = raw_val.strip(' "\'')
            if clean_p.isdigit():
                t_id = int(clean_p)
        if isinstance(t_id, list) and not t_id:
            t_id = None

    s_type = sys.argv[3] if len(sys.argv) > 3 else None
    l_type = sys.argv[4] if len(sys.argv) > 4 else 'incremental'
    drop_table_arg = sys.argv[5] if len(sys.argv) > 5 else 'false'
    drop_table = drop_table_arg.lower() in ('true', '1', 'yes')
    
    # Arg 6: erp_only (vd 'sun' = chỉ nạp ERP Philippines). Bỏ trống = tất cả ERP
    erp_only_arg = sys.argv[6] if len(sys.argv) > 6 else None

    DirectPostgresToPostgresNotTruncateERP(s_date, erp_only=erp_only_arg).run(table_id=t_id, source_type=s_type, drop_table=drop_table, load_type=l_type)
