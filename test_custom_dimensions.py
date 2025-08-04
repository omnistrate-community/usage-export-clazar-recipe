#!/usr/bin/env python3
"""
Test script for custom dimensions functionality
"""

import sys
import os

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from metering_processor import MeteringProcessor


def test_custom_dimensions():
    """Test custom dimension transformation"""
    
    # Mock custom dimensions
    custom_dimensions = {
        'pod_hours': 'cpu_core_hours / 2',
        'compute_units': 'cpu_core_hours + memory_byte_hours / 1000000000',
        'total_gb_hours': 'memory_byte_hours / 1000000000 + storage_allocated_byte_hours / 1000000000'
    }
    
    # Create processor with custom dimensions
    processor = MeteringProcessor(
        bucket_name='test-bucket',
        custom_dimensions=custom_dimensions
    )
    
    # Test data - aggregated usage data
    test_data = {
        ('contract1', 'cpu_core_hours'): 720.0,
        ('contract1', 'memory_byte_hours'): 2000000000.0,  # 2GB hours
        ('contract1', 'storage_allocated_byte_hours'): 5000000000.0,  # 5GB hours
        ('contract2', 'cpu_core_hours'): 1440.0,
        ('contract2', 'memory_byte_hours'): 4000000000.0,  # 4GB hours
        ('contract2', 'storage_allocated_byte_hours'): 1000000000.0,  # 1GB hours
    }
    
    # Transform dimensions
    result = processor.transform_dimensions(test_data)
    
    print("Original data:")
    for key, value in test_data.items():
        print(f"  {key}: {value}")
    
    print("\nTransformed data:")
    for key, value in result.items():
        print(f"  {key}: {value}")
    
    # Expected results
    expected = {
        ('contract1', 'pod_hours'): 360.0,  # 720 / 2
        ('contract1', 'compute_units'): 722.0,  # 720 + 2000000000/1000000000
        ('contract1', 'total_gb_hours'): 7.0,  # 2 + 5
        ('contract2', 'pod_hours'): 720.0,  # 1440 / 2
        ('contract2', 'compute_units'): 1444.0,  # 1440 + 4000000000/1000000000
        ('contract2', 'total_gb_hours'): 5.0,  # 4 + 1
    }
    
    print("\nExpected results:")
    for key, value in expected.items():
        print(f"  {key}: {value}")
    
    # Verify results
    assert len(result) == len(expected), f"Expected {len(expected)} results, got {len(result)}"
    
    for key, expected_value in expected.items():
        assert key in result, f"Missing key: {key}"
        assert abs(result[key] - expected_value) < 0.001, f"Wrong value for {key}: expected {expected_value}, got {result[key]}"
    
    print("\n✅ All tests passed!")


def test_error_handling():
    """Test error handling in dimension formulas"""
    
    # Custom dimensions with error
    custom_dimensions = {
        'good_dimension': 'cpu_core_hours / 2',
        'bad_dimension': 'cpu_core_hours / 0',  # Division by zero
        'another_good': 'memory_byte_hours'
    }
    
    processor = MeteringProcessor(
        bucket_name='test-bucket',
        custom_dimensions=custom_dimensions
    )
    
    test_data = {
        ('contract1', 'cpu_core_hours'): 720.0,
        ('contract1', 'memory_byte_hours'): 2000000000.0,
    }
    
    result = processor.transform_dimensions(test_data)
    
    print("Error handling test:")
    print(f"Original data had {len(test_data)} entries")
    print(f"Result has {len(result)} entries")
    
    # Since one formula failed, the entire contract should be skipped
    assert len(result) == 0, f"Expected 0 results due to error, got {len(result)}"
    
    print("✅ Error handling test passed!")


if __name__ == "__main__":
    test_custom_dimensions()
    test_error_handling()
    print("\n🎉 All tests completed successfully!")
