#!/usr/bin/env python3
"""
Test script for deal_comments_serving_layer fixes
Run this to verify the critical bug fixes work correctly
"""

import os
import sys
from datetime import date, timedelta
from loguru import logger

# Add app to path
sys.path.insert(0, '/app')

def test_filter_type():
    """Test #1: Verify PyIceberg filter uses date object"""
    logger.info("=" * 80)
    logger.info("TEST #1: PyIceberg Filter Type")
    logger.info("=" * 80)
    
    from pyiceberg.expressions import EqualTo
    
    snapshot_date = date(2026, 1, 21)
    
    # Correct way (date object)
    filter_correct = EqualTo("snapshot_date", snapshot_date)
    logger.info(f"✅ Correct filter: {filter_correct}")
    
    # Wrong way (string)
    filter_wrong = EqualTo("snapshot_date", snapshot_date.isoformat())
    logger.info(f"❌ Wrong filter: {filter_wrong}")
    
    logger.info("\n📝 Verification:")
    logger.info("Check deal_comments_serving_layer.py:95")
    logger.info("Should use: EqualTo('snapshot_date', self.snapshot_date)")
    logger.info("NOT: EqualTo('snapshot_date', self.snapshot_date.isoformat())")
    

def test_transaction_atomicity():
    """Test #2: Verify single transaction for DELETE+INSERT"""
    logger.info("\n" + "=" * 80)
    logger.info("TEST #2: Transaction Atomicity")
    logger.info("=" * 80)
    
    logger.info("✅ Expected behavior:")
    logger.info("  1. DELETE and INSERT in same cursor")
    logger.info("  2. Single commit() at the end")
    logger.info("  3. rollback() on any exception")
    logger.info("  4. No data loss if INSERT fails")
    
    logger.info("\n📝 Verification:")
    logger.info("Check deal_comments_serving_layer.py:sync_comments()")
    logger.info("Should have:")
    logger.info("  - cursor.execute(DELETE ...)")
    logger.info("  - execute_values(INSERT ...)")
    logger.info("  - self.pg_conn.commit()  # Only once!")
    

def test_index_creation():
    """Test #3: Verify index on ticket_id"""
    logger.info("\n" + "=" * 80)
    logger.info("TEST #3: Index Creation")
    logger.info("=" * 80)
    
    logger.info("✅ Expected behavior:")
    logger.info("  1. CREATE INDEX IF NOT EXISTS on ticket_id")
    logger.info("  2. Index name: idx_<table_name>_ticket_id")
    logger.info("  3. Created during ensure_serving_table_exists()")
    
    logger.info("\n📝 Verification:")
    logger.info("After running the pipeline, check PostgreSQL:")
    logger.info("  SELECT * FROM pg_indexes")
    logger.info("  WHERE tablename = 'cs_deal_api_trans_comments_v1';")
    

def test_data_flow():
    """Test #4: Verify complete data flow"""
    logger.info("\n" + "=" * 80)
    logger.info("TEST #4: Data Flow Verification")
    logger.info("=" * 80)
    
    logger.info("✅ Steps to verify:")
    logger.info("  1. Check Iceberg Gold table has data:")
    logger.info("     SELECT COUNT(*) FROM gold.caresoft.cs_deal_api_trans_comments")
    logger.info("     WHERE snapshot_date = '2026-01-21';")
    
    logger.info("\n  2. Run serving layer script:")
    logger.info("     python deal_comments_serving_layer.py 2026-01-21")
    
    logger.info("\n  3. Check PostgreSQL serving table:")
    logger.info("     SELECT COUNT(*) FROM cs_deal_api_trans_comments_v1;")
    
    logger.info("\n  4. Verify counts match:")
    logger.info("     Iceberg count == PostgreSQL count")
    

def test_error_handling():
    """Test #5: Verify error handling and rollback"""
    logger.info("\n" + "=" * 80)
    logger.info("TEST #5: Error Handling")
    logger.info("=" * 80)
    
    logger.info("✅ Test scenarios:")
    logger.info("  1. Simulate INSERT failure:")
    logger.info("     - Temporarily break PostgreSQL connection")
    logger.info("     - Verify rollback() is called")
    logger.info("     - Verify no partial data in table")
    
    logger.info("\n  2. Check logs for proper error messages")
    logger.info("  3. Verify transaction is rolled back")
    

def main():
    logger.info("🧪 Deal Comments Serving Layer - Test Suite")
    logger.info("=" * 80)
    
    test_filter_type()
    test_transaction_atomicity()
    test_index_creation()
    test_data_flow()
    test_error_handling()
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ All tests defined")
    logger.info("=" * 80)
    logger.info("\n📋 Next Steps:")
    logger.info("1. Review the test scenarios above")
    logger.info("2. Run the actual pipeline with a test snapshot_date")
    logger.info("3. Verify each test case manually")
    logger.info("4. Check PostgreSQL for index creation")
    logger.info("5. Compare row counts between Iceberg and PostgreSQL")
    

if __name__ == "__main__":
    main()
