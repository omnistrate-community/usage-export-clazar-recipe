#!/usr/bin/env python3
"""
Demo script to show dry run functionality with mock data.
"""

import json
import os
import sys
import tempfile
from datetime import datetime
from unittest.mock import patch

# Add the parent directory to the Python path so we can import metering_processor
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metering_processor import MeteringProcessor


def demo_dry_run():
    """Demonstrate dry run functionality with mock data."""
    
    # Create a temporary state file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        state_file = f.name
    
    # Initialize processor in dry run mode
    processor = MeteringProcessor(
        bucket_name="demo-bucket",
        state_file_path=state_file,
        clazar_api_url="https://api.clazar.io/metering/",
        dry_run=True,
        # access_token="demo-token"
        access_token="eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlNPTWZpcTlPaDc5RGtKeGdMRjNLNiJ9.eyJpc3MiOiJodHRwczovL2NsYXphci51cy5hdXRoMC5jb20vIiwic3ViIjoiME5IOXpiTEFGenY2TDg4VEF6OTMzcWM5RzBEc282SHRAY2xpZW50cyIsImF1ZCI6Imh0dHBzOi8vbTJtLmNsYXphci5pbyIsImlhdCI6MTc1MzgzMzEzMiwiZXhwIjoxNzUzODU0NzMyLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiIwTkg5emJMQUZ6djZMODhUQXo5MzNxYzlHMERzbzZIdCJ9.lSRVZ5B9PqVpTn0sk3Gt9icv5wdSt8eYcnLP39r6SNkIPZvmW_e4FzOXnSvQvclKqp6LonnfC-bXdKCL5jYuH7eb-IJAsYqsX1q2fiSMwAq1noXaBF2g3KplSKYjRuHtx0jcQQRESpIRTn7OlOij7A5xU0MulZwVd7TVK5CMKr3cS1kzApF9WXVLlPzfILO0BoZvsKIYbn3FXGOAePM5L2cRffCeL8ef-8qLZj1sl6a2dpXx2Zma83v-ZDFFt9ipQ-x6fzsftP5Z3JcPDOTGelHTCiXhXfWcRfwWmffqfDilpzlrdNz7dzt-24fly9xp5aiInZGS3WVTOrZumycFlQ"
    )
    
    print("=== Monthly Processing Dry Run Demo ===")
    print("This demo shows how the dry run mode works with mock monthly data.")
    print()
    
    # Mock S3 operations to return sample data
    sample_usage_data = [
        {"externalPayerId": "customer-123", "dimension": "cpu_core_hours", "value": 1000},
        {"externalPayerId": "customer-123", "dimension": "cpu_core_hours", "value": 500},
        {"externalPayerId": "customer-456", "dimension": "memory_byte_hours", "value": 10000},
        {"externalPayerId": "customer-123", "dimension": "memory_byte_hours", "value": 5000},
        {"externalPayerId": "customer-123", "dimension": "storage_allocated_byte_hours", "value": 20000},
        {"externalPayerId": "customer-123", "dimension": "storage_allocated_byte_hours", "value": 10000}
    ]
    
    with patch.object(processor, 'list_monthly_subscription_files') as mock_list_files, \
         patch.object(processor, 'read_s3_json_file') as mock_read_file:
        
        # Mock S3 file listing
        mock_list_files.return_value = ['subscription1.json', 'subscription2.json']
        
        # Mock S3 file reading
        mock_read_file.side_effect = [
            sample_usage_data[:2],  # First file
            sample_usage_data[2:]   # Second file
        ]
        
        # Process one month
        year, month = 2025, 1
        print(f"Processing month: {year}-{month:02d}")
        print(f"Sample input data: {json.dumps(sample_usage_data, indent=2)}")
        print()
        
        success = processor.process_month("DemoService", "PROD", "demo-plan", year, month)
        
        print()
        print(f"Processing successful: {success}")
        print()
        print("Notice how in dry run mode:")
        print("- All data processing happens normally")
        print("- Usage data is aggregated by contract and dimension")
        print("- The payload is logged with full details")
        print("- No actual HTTP request is made")
        print("- Contract processing state is still tracked")
        print("- The function still returns success")


if __name__ == "__main__":
    demo_dry_run()
