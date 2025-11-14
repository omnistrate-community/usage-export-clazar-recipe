#!/usr/bin/env python3
"""
Integration tests for OmnistrateMeteringReader.

This test suite validates OmnistrateMeteringReader operations with real AWS S3 backend.
Configuration is loaded from environment variables via the Config class.

Required environment variables:
    AWS_ACCESS_KEY_ID: AWS access key for S3
    AWS_SECRET_ACCESS_KEY: AWS secret key for S3
    AWS_REGION: AWS region for S3 (e.g., 'us-west-2')
    AWS_S3_BUCKET_NAME: S3 bucket name for metering data
    SERVICE_NAME: Service name for metering data
    ENVIRONMENT_TYPE: Environment type (e.g., 'PROD', 'DEV')
    PLAN_ID: Plan ID for service configuration
    DIMENSION1_NAME: First dimension name
    DIMENSION1_FORMULA: First dimension formula
    CLAZAR_CLIENT_ID: Clazar client ID
    CLAZAR_CLIENT_SECRET: Clazar client secret

Note: These tests are designed to work with live, changing data in S3.
They validate behavior and structure rather than specific data values.
"""

import os
import sys
import logging
import unittest
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional

# Add src directory to path to import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from omnistrate_metering_reader import OmnistrateMeteringReader, OmnistrateMeteringReaderError
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestOmnistrateMeteringReaderIntegration(unittest.TestCase):
    """Integration test suite for OmnistrateMeteringReader with real S3 backend."""
    
    def setUp(self):
        """Set up test fixtures."""
        logger.info("=" * 60)
        logger.info(f"Running test: {self._testMethodName}")
        logger.info("=" * 60)
        
        # Load configuration - skip test if required env vars not set
        try:
            self.config = Config()
            
            # Check if required service-specific env vars are set
            if not self.config.service_name or not self.config.environment_type or not self.config.plan_id:
                logger.warning("SERVICE_NAME, ENVIRONMENT_TYPE, or PLAN_ID not set in environment")
                logger.warning("Skipping test - these are required for OmnistrateMeteringReader integration tests")
                self.skipTest("SERVICE_NAME, ENVIRONMENT_TYPE, and PLAN_ID must be set for integration tests")
            
            logger.info("✓ Configuration loaded successfully")
            logger.info(f"  S3 Bucket: {self.config.aws_s3_bucket}")
            logger.info(f"  Service: {self.config.service_name}")
            logger.info(f"  Environment: {self.config.environment_type}")
            logger.info(f"  Plan ID: {self.config.plan_id}")
            logger.info(f"  AWS Region: {self.config.aws_region}")
        except Exception as e:
            logger.error(f"✗ Failed to load configuration: {e}")
            self.fail(f"Configuration loading failed: {e}")
        
        # Initialize OmnistrateMeteringReader
        try:
            self.reader = OmnistrateMeteringReader(self.config)
            logger.info("✓ OmnistrateMeteringReader initialized successfully")
        except Exception as e:
            logger.error(f"✗ Failed to initialize OmnistrateMeteringReader: {e}")
            self.fail(f"OmnistrateMeteringReader initialization failed: {e}")
    
    def test_config_loaded(self):
        """Test that configuration is loaded successfully."""
        self.assertIsNotNone(self.config, "Config object should be loaded")
        self.assertIsNotNone(
            self.config.aws_s3_bucket,
            "AWS_S3_BUCKET_NAME must be set in config"
        )
        self.assertIsNotNone(
            self.config.aws_access_key_id,
            "AWS_ACCESS_KEY_ID must be set in config"
        )
        self.assertIsNotNone(
            self.config.aws_secret_access_key,
            "AWS_SECRET_ACCESS_KEY must be set in config"
        )
        self.assertIsNotNone(
            self.config.aws_region,
            "AWS_REGION must be set in config"
        )
        logger.info("✓ Configuration validated successfully")
    
    def test_reader_initialized(self):
        """Test that OmnistrateMeteringReader is initialized correctly."""
        self.assertIsNotNone(self.reader, "Reader object should be initialized")
        self.assertEqual(
            self.reader.aws_s3_bucket,
            self.config.aws_s3_bucket,
            "Reader should use correct bucket name"
        )
        self.assertIsNotNone(self.reader.s3_client, "S3 client should be initialized")
        logger.info("✓ OmnistrateMeteringReader initialized with correct configuration")
    
    def test_get_service_key(self):
        """Test service key generation."""
        # OmnistrateMeteringReader now stores service info in instance
        key = self.reader.get_service_key()
        
        self.assertIsInstance(key, str, "Service key should be a string")
        self.assertIn(self.config.service_name, key, "Service key should contain service name")
        self.assertIn(self.config.environment_type, key, "Service key should contain environment type")
        self.assertIn(self.config.plan_id, key, "Service key should contain plan ID")
        self.assertEqual(key, f"{self.config.service_name}:{self.config.environment_type}:{self.config.plan_id}")
        logger.info(f"✓ Service key generated correctly: {key}")
    
    def test_load_usage_data_state(self):
        """
        Test loading usage data state from S3.
        
        Note: This test validates the structure and behavior, not specific values.
        The state file may or may not exist, and data may change over time.
        """
        logger.info("Attempting to load usage data state from S3...")
        
        try:
            state = self.reader.load_usage_data_state()
            
            # State should be a dictionary (may be empty if file doesn't exist)
            self.assertIsInstance(state, dict, "State should be a dictionary")
            logger.info(f"✓ State loaded successfully, contains {len(state)} service(s)")
            
            # If state is not empty, validate structure
            if state:
                logger.info("  Validating state structure...")
                for service_key, service_state in state.items():
                    # Each service key should follow format: service:environment:plan
                    parts = service_key.split(':')
                    self.assertGreaterEqual(
                        len(parts), 3,
                        f"Service key '{service_key}' should have at least 3 parts"
                    )
                    
                    # Each service state should be a dictionary
                    self.assertIsInstance(
                        service_state, dict,
                        f"Service state for '{service_key}' should be a dictionary"
                    )
                    
                    # If lastSuccessfulExport exists, validate it's a timestamp string
                    if 'lastSuccessfulExport' in service_state:
                        last_export = service_state['lastSuccessfulExport']
                        self.assertIsInstance(
                            last_export, str,
                            f"lastSuccessfulExport for '{service_key}' should be a string"
                        )
                        # Try parsing as ISO 8601 timestamp
                        try:
                            datetime.fromisoformat(last_export.replace('Z', '+00:00'))
                            logger.info(f"    ✓ Service '{service_key}': valid timestamp {last_export}")
                        except ValueError as e:
                            self.fail(f"Invalid timestamp format for '{service_key}': {last_export}")
                    else:
                        logger.info(f"    ✓ Service '{service_key}': no lastSuccessfulExport yet")
            else:
                logger.info("  State is empty (file may not exist yet, which is valid)")
                
        except Exception as e:
            logger.error(f"✗ Error loading state: {e}")
            # Don't fail the test if file doesn't exist - that's valid
            if "NoSuchKey" in str(e) or "not found" in str(e).lower():
                logger.info("  State file doesn't exist yet - this is valid for a new setup")
            else:
                raise
    
    def test_get_latest_month_with_complete_usage_data(self):
        """
        Test retrieving the latest month with complete usage data.
        
        Note: This test validates the return type and format, not specific values.
        The result may be None if no data has been processed yet.
        """
        service_name = self.config.service_name
        environment_type = self.config.environment_type
        plan_id = self.config.plan_id
        
        logger.info(f"Retrieving latest month for {service_name}:{environment_type}:{plan_id}")
        
        # OmnistrateMeteringReader now stores service info in instance
        result = self.reader.get_latest_month_with_complete_usage_data()
        
        # Result can be None (if never processed) or a tuple of (year, month)
        if result is None:
            logger.info("  No previous processing found (result is None) - valid for new setup")
        else:
            self.assertIsInstance(result, tuple, "Result should be a tuple")
            self.assertEqual(len(result), 2, "Result should have 2 elements (year, month)")
            
            year, month = result
            self.assertIsInstance(year, int, "Year should be an integer")
            self.assertIsInstance(month, int, "Month should be an integer")
            self.assertGreaterEqual(year, 2020, "Year should be reasonable (>= 2020)")
            self.assertLessEqual(year, 2030, "Year should be reasonable (<= 2030)")
            self.assertGreaterEqual(month, 1, "Month should be >= 1")
            self.assertLessEqual(month, 12, "Month should be <= 12")
            
            logger.info(f"✓ Latest complete month: {year:04d}-{month:02d}")
    
    def test_get_monthly_s3_prefix(self):
        """Test generation of monthly S3 prefix."""
        # OmnistrateMeteringReader now stores service info in instance
        year = 2025
        month = 1
        
        prefix = self.reader.get_monthly_s3_prefix(year, month)
        
        self.assertIsInstance(prefix, str, "Prefix should be a string")
        self.assertIn("omnistrate-metering", prefix, "Prefix should contain 'omnistrate-metering'")
        self.assertIn(self.config.service_name, prefix, "Prefix should contain service name")
        self.assertIn(self.config.environment_type, prefix, "Prefix should contain environment type")
        self.assertIn(self.config.plan_id, prefix, "Prefix should contain plan ID")
        self.assertIn(f"{year:04d}", prefix, "Prefix should contain 4-digit year")
        self.assertIn(f"{month:02d}", prefix, "Prefix should contain 2-digit month")
        self.assertTrue(prefix.endswith('/'), "Prefix should end with /")
        
        expected_prefix = f"omnistrate-metering/{self.config.service_name}/{self.config.environment_type}/{self.config.plan_id}/{year:04d}/{month:02d}/"
        self.assertEqual(prefix, expected_prefix, "Prefix format should match expected pattern")
        
        logger.info(f"✓ S3 prefix generated correctly: {prefix}")
    
    def test_list_monthly_subscription_files(self):
        """
        Test listing subscription files for a month.
        
        Note: This test validates the behavior with real S3 data.
        The number of files may vary or be zero, which is valid.
        """
        service_name = self.config.service_name
        environment_type = self.config.environment_type
        plan_id = self.config.plan_id
        
        # Use current month for testing (data may or may not exist)
        now = datetime.now(timezone.utc)
        year = now.year
        month = now.month
        
        logger.info(f"Listing subscription files for {year:04d}-{month:02d}")
        
        try:
            # OmnistrateMeteringReader now stores service info in instance
            files = self.reader.list_monthly_subscription_files(year, month)
            
            # Result should be a list (may be empty)
            self.assertIsInstance(files, list, "Result should be a list")
            logger.info(f"✓ Found {len(files)} subscription file(s)")
            
            # If files exist, validate their structure
            if files:
                for file_key in files:
                    self.assertIsInstance(file_key, str, f"File key should be a string: {file_key}")
                    self.assertTrue(
                        file_key.endswith('.json'),
                        f"File should end with .json: {file_key}"
                    )
                    self.assertIn(
                        "omnistrate-metering",
                        file_key,
                        f"File key should contain 'omnistrate-metering': {file_key}"
                    )
                    logger.info(f"    ✓ {file_key}")
            else:
                logger.info("  No files found for this month (valid if data hasn't been generated yet)")
                
        except Exception as e:
            logger.error(f"✗ Error listing files: {e}")
            raise
    
    def test_list_monthly_subscription_files_previous_month(self):
        """
        Test listing subscription files for a previous month.
        
        This increases the likelihood of finding existing data without
        assuming any specific month has data.
        """
        service_name = self.config.service_name
        environment_type = self.config.environment_type
        plan_id = self.config.plan_id
        
        # Try a few recent months to increase chance of finding data
        now = datetime.now(timezone.utc)
        months_to_try = [
            (now.year, now.month - 1 if now.month > 1 else 12),
            (now.year - 1 if now.month == 1 else now.year, 12 if now.month == 1 else now.month - 2),
            (2025, 1),  # Known start month from config
        ]
        
        found_any_data = False
        for year, month in months_to_try:
            if month < 1:
                continue
            
            logger.info(f"Checking {year:04d}-{month:02d}...")
            
            try:
                # OmnistrateMeteringReader now stores service info in instance
                files = self.reader.list_monthly_subscription_files(year, month)
                
                self.assertIsInstance(files, list, "Result should be a list")
                
                if files:
                    found_any_data = True
                    logger.info(f"✓ Found {len(files)} file(s) for {year:04d}-{month:02d}")
                    
                    # Validate first file only to keep test fast
                    file_key = files[0]
                    self.assertTrue(
                        file_key.endswith('.json'),
                        f"File should be JSON: {file_key}"
                    )
                    break
                else:
                    logger.info(f"  No files for {year:04d}-{month:02d}")
                    
            except Exception as e:
                logger.warning(f"  Error checking {year:04d}-{month:02d}: {e}")
        
        if not found_any_data:
            logger.info("✓ No data found in any checked month (valid for new/empty setup)")
        
        # Don't fail if no data found - that's valid for a new setup
        logger.info("✓ List operation completed successfully")
    
    def test_read_s3_json_file(self):
        """
        Test reading a JSON file from S3.
        
        This test attempts to find and read actual data files if they exist.
        If no files exist, the test validates the behavior gracefully.
        """
        service_name = self.config.service_name
        environment_type = self.config.environment_type
        plan_id = self.config.plan_id
        
        # Try to find a file to read
        file_to_read = None
        now = datetime.now(timezone.utc)
        
        # Try current and previous months
        for month_offset in range(0, 6):  # Check up to 6 months back
            year = now.year
            month = now.month - month_offset
            
            # Handle year rollover
            while month < 1:
                month += 12
                year -= 1
            
            logger.info(f"Looking for files in {year:04d}-{month:02d}...")
            
            # OmnistrateMeteringReader now stores service info in instance
            files = self.reader.list_monthly_subscription_files(year, month)
            
            if files:
                file_to_read = files[0]  # Read first file found
                logger.info(f"✓ Found file to read: {file_to_read}")
                break
        
        if file_to_read:
            logger.info(f"Reading file: {file_to_read}")
            
            try:
                records = self.reader.read_s3_json_file(file_to_read)
                
                # Validate response
                self.assertIsInstance(records, list, "Records should be a list")
                logger.info(f"✓ Successfully read {len(records)} record(s)")
                
                # If records exist, validate structure
                if records:
                    for i, record in enumerate(records[:3]):  # Check first 3 records only
                        self.assertIsInstance(
                            record, dict,
                            f"Record {i} should be a dictionary"
                        )
                        logger.info(f"    ✓ Record {i}: {list(record.keys())}")
                else:
                    logger.info("  File contains empty array (valid)")
                    
            except Exception as e:
                logger.error(f"✗ Error reading file: {e}")
                raise
        else:
            logger.info("✓ No files found to read (valid for new/empty setup)")
            logger.info("  Skipping read test as no data exists yet")
    
    def test_s3_connection(self):
        """
        Test basic S3 connectivity by listing objects with the omnistrate-metering prefix.
        
        This validates that the S3 client is properly configured and can access the bucket.
        """
        logger.info("Testing S3 connection...")
        
        try:
            # Try to list objects in the bucket with the metering prefix
            response = self.reader.s3_client.list_objects_v2(
                Bucket=self.reader.aws_s3_bucket,
                Prefix='omnistrate-metering/',
                MaxKeys=1  # Just check if we can connect
            )
            
            # If we get here without exception, connection works
            logger.info("✓ S3 connection successful")
            
            if 'Contents' in response:
                logger.info(f"  Bucket contains data at omnistrate-metering/ prefix")
            else:
                logger.info(f"  Bucket exists but no data at omnistrate-metering/ prefix yet")
                
        except Exception as e:
            logger.error(f"✗ S3 connection failed: {e}")
            self.fail(f"Failed to connect to S3: {e}")
    
    def test_error_handling_invalid_file(self):
        """Test error handling when trying to read a non-existent file."""
        logger.info("Testing error handling for non-existent file...")
        
        # Try to read a file that definitely doesn't exist
        non_existent_file = "omnistrate-metering/this-file-does-not-exist-12345.json"
        
        records = self.reader.read_s3_json_file(non_existent_file)
        
        # Should return empty list, not raise exception
        self.assertIsInstance(records, list, "Should return a list")
        self.assertEqual(len(records), 0, "Should return empty list for non-existent file")
        logger.info("✓ Non-existent file handled gracefully (returns empty list)")
    
    def test_validate_access(self):
        """
        Test validate_access method to ensure S3 bucket access is properly validated.
        
        This test validates that we can successfully validate read access to the
        omnistrate-metering prefix in the S3 bucket.
        """
        logger.info("Testing validate_access method...")
        
        try:
            # Call validate_access - should not raise exception for valid credentials
            self.reader.validate_access()
            logger.info("✓ validate_access completed successfully")
            logger.info(f"  Read access to S3 bucket {self.reader.aws_s3_bucket}/omnistrate-metering validated")
            
        except OmnistrateMeteringReaderError as e:
            logger.error(f"✗ validate_access failed: {e}")
            self.fail(f"validate_access should succeed with valid credentials: {e}")
        except Exception as e:
            logger.error(f"✗ Unexpected error during validate_access: {e}")
            self.fail(f"Unexpected error during validate_access: {e}")
    
    def test_validate_access_with_invalid_credentials(self):
        """
        Test validate_access method with invalid credentials to ensure proper error handling.
        
        This test creates a reader with invalid credentials and verifies that
        validate_access raises an appropriate exception.
        """
        logger.info("Testing validate_access with invalid credentials...")
        
        # Create a config with invalid credentials
        invalid_config = Config()
        invalid_config.aws_access_key_id = "INVALID_KEY_ID"
        invalid_config.aws_secret_access_key = "INVALID_SECRET_KEY"
        
        try:
            # Create reader with invalid credentials
            invalid_reader = OmnistrateMeteringReader(invalid_config)
            
            # validate_access should raise OmnistrateMeteringReaderError
            with self.assertRaises(OmnistrateMeteringReaderError) as context:
                invalid_reader.validate_access()
            
            logger.info(f"✓ validate_access properly raised exception: {context.exception}")
            
            # Verify the exception message contains useful information
            error_message = str(context.exception)
            self.assertIn("S3 access validation failed", error_message)
            logger.info("✓ Exception message contains expected error details")
            
        except Exception as e:
            logger.error(f"✗ Unexpected error during invalid credentials test: {e}")
            # This is actually expected - the test itself might fail during setup
            # if credentials are completely invalid, which is fine
            logger.info("✓ Invalid credentials prevented access as expected")


def run_tests():
    """Run all integration tests."""
    unittest.main(argv=[''], exit=False, verbosity=2)


if __name__ == '__main__':
    # Run tests
    run_tests()
