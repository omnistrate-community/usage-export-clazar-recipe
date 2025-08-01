#!/usr/bin/env python3
"""
Test script to verify the improvements made to the metering processor.
"""

import os
import json
import tempfile
from datetime import datetime, timezone
from src.metering_processor import MeteringProcessor

def test_custom_dimensions():
    """Test custom dimension calculation."""
    
    # Set up environment variables for custom dimensions
    os.environ['DIMENSION_pod_hours'] = 'cpu_core_hours / 2'
    os.environ['DIMENSION_memory_gb_hours'] = 'memory_byte_hours / 1073741824'
    
    # Create a test processor
    processor = MeteringProcessor(
        bucket_name='test-bucket',
        dry_run=True
    )
    
    # Test data
    usage_records = [
        {
            'externalPayerId': 'contract-123',
            'dimension': 'cpu_core_hours',
            'value': 100
        },
        {
            'externalPayerId': 'contract-123',
            'dimension': 'memory_byte_hours',
            'value': 2147483648  # 2 GB in bytes
        },
        {
            'externalPayerId': 'contract-456',
            'dimension': 'cpu_core_hours',
            'value': 50
        }
    ]
    
    # Test aggregation with custom dimensions
    aggregated = processor.aggregate_usage_data(usage_records)
    
    print("Custom Dimensions Test Results:")
    for (contract_id, dimension), value in aggregated.items():
        print(f"  {contract_id} - {dimension}: {value}")
    
    # Verify expected results
    expected = {
        ('contract-123', 'cpu_core_hours'): 100,
        ('contract-123', 'memory_byte_hours'): 2147483648,
        ('contract-123', 'pod_hours'): 50.0,  # 100 / 2
        ('contract-123', 'memory_gb_hours'): 2.0,  # 2147483648 / 1073741824
        ('contract-456', 'cpu_core_hours'): 50,
        ('contract-456', 'pod_hours'): 25.0  # 50 / 2
    }
    
    for key, expected_value in expected.items():
        if key in aggregated:
            actual_value = aggregated[key]
            if abs(actual_value - expected_value) < 0.001:  # Allow small floating point differences
                print(f"  ✓ {key}: {actual_value} (expected {expected_value})")
            else:
                print(f"  ✗ {key}: {actual_value} (expected {expected_value})")
        else:
            print(f"  ✗ Missing: {key}")
    
    # Clean up environment variables
    del os.environ['DIMENSION_pod_hours']
    del os.environ['DIMENSION_memory_gb_hours']


def test_error_handling():
    """Test error contract tracking and payload storage."""
    
    # Create a temporary state file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        temp_state_file = f.name
        json.dump({}, f)
    
    try:
        processor = MeteringProcessor(
            bucket_name='test-bucket',
            state_file_path=temp_state_file,
            dry_run=True
        )
        
        # Test marking contract with error and payload
        test_payload = {
            "request": [
                {
                    "cloud": "aws",
                    "contract_id": "test-contract",
                    "dimension": "cpu_core_hours",
                    "start_time": "2025-01-01T00:00:00Z",
                    "end_time": "2025-01-31T23:59:59Z",
                    "quantity": "100"
                }
            ]
        }
        
        processor.mark_contract_month_error(
            service_name='TestService',
            environment_type='PROD',
            plan_id='test-plan',
            contract_id='test-contract',
            year=2025,
            month=1,
            errors=['Test error message'],
            code='TEST_ERROR',
            message='Test error occurred',
            payload=test_payload,
            retry_count=1
        )
        
        # Verify the error was recorded
        state = processor.load_state()
        service_key = 'TestService:PROD:test-plan'
        month_key = '2025-01'
        
        if service_key in state and 'error_contracts' in state[service_key]:
            if month_key in state[service_key]['error_contracts']:
                error_contracts = state[service_key]['error_contracts'][month_key]
                if error_contracts:
                    error_entry = error_contracts[0]
                    print("Error Handling Test Results:")
                    print(f"  Contract ID: {error_entry.get('contract_id')}")
                    print(f"  Errors: {error_entry.get('errors')}")
                    print(f"  Code: {error_entry.get('code')}")
                    print(f"  Message: {error_entry.get('message')}")
                    print(f"  Retry Count: {error_entry.get('retry_count')}")
                    print(f"  Has Payload: {'payload' in error_entry}")
                    
                    if 'payload' in error_entry:
                        payload = error_entry['payload']
                        print(f"  Payload Contract ID: {payload['request'][0]['contract_id']}")
                        print(f"  Payload Quantity: {payload['request'][0]['quantity']}")
                        print("  ✓ Error contract recorded with payload successfully")
                    else:
                        print("  ✗ Payload not found in error entry")
                else:
                    print("  ✗ No error contracts found")
            else:
                print("  ✗ Month not found in error contracts")
        else:
            print("  ✗ Service key or error_contracts not found in state")
    
    finally:
        # Clean up temporary file
        os.unlink(temp_state_file)


def test_cron_scheduling():
    """Test cron job scheduling logic."""
    
    from src.metering_processor import is_first_day_of_month
    
    print("Cron Scheduling Test:")
    print(f"  Current date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}")
    print(f"  Is first day of month: {is_first_day_of_month()}")
    
    # Note: This is a basic test - full cron functionality would need integration testing


if __name__ == '__main__':
    print("Testing Metering Processor Improvements")
    print("=" * 50)
    
    test_custom_dimensions()
    print()
    
    test_error_handling()
    print()
    
    test_cron_scheduling()
    print()
    
    print("All tests completed!")
