#!/usr/bin/env python3
"""
Demo script to test monthly processing in dry run mode.
"""

import os
import sys
from datetime import datetime

# Add the parent directory to the Python path to import metering_processor
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metering_processor import MeteringProcessor


def main():
    """Test the monthly processing functionality."""
    
    # Test configuration
    BUCKET_NAME = "omnistrate-usage-metering-export-demo"
    SERVICE_NAME = "Postgres"
    ENVIRONMENT_TYPE = "PROD"
    PLAN_ID = "pt-HJSv20iWX0"
    STATE_FILE_PATH = "metering_state.json"
    
    print("Testing Monthly Metering Processor")
    print("=" * 50)
    
    # Initialize processor in dry run mode
    processor = MeteringProcessor(
        bucket_name=BUCKET_NAME,
        state_file_path=STATE_FILE_PATH,
        dry_run=True,
        access_token="test-token"
    )
    
    # Test loading state from S3
    print("1. Testing state loading from S3...")
    state = processor.load_state()
    print(f"   Loaded state: {state}")
    
    # Test getting next month to process
    print("\n2. Testing next month calculation...")
    next_month = processor.get_next_month_to_process(SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID)
    if next_month:
        year, month = next_month
        print(f"   Next month to process: {year}-{month:02d}")
    else:
        print("   No months to process")
    
    # Test contract tracking
    print("\n3. Testing contract tracking...")
    test_contract = "test-contract-123"
    year, month = 2025, 6
    
    is_processed_before = processor.is_contract_month_processed(
        SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID, test_contract, year, month
    )
    print(f"   Contract {test_contract} for {year}-{month:02d} processed: {is_processed_before}")
    
    # Mark as processed
    processor.mark_contract_month_processed(
        SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID, test_contract, year, month
    )
    
    is_processed_after = processor.is_contract_month_processed(
        SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID, test_contract, year, month
    )
    print(f"   After marking, contract processed: {is_processed_after}")
    
    # Test monthly S3 prefix generation
    print("\n4. Testing S3 prefix generation...")
    prefix = processor.get_monthly_s3_prefix(SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID, year, month)
    print(f"   Monthly S3 prefix: {prefix}")
    
    # Test processing pending months (dry run)
    print("\n5. Testing monthly processing (dry run)...")
    try:
        success = processor.process_pending_months(
            SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID, max_months=1
        )
        print(f"   Processing result: {'Success' if success else 'Failed'}")
    except Exception as e:
        print(f"   Processing error: {e}")
    
    print("\n" + "=" * 50)
    print("Monthly processing test completed!")


if __name__ == "__main__":
    main()
