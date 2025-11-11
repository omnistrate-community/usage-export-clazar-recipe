#!/usr/bin/env python3
"""
Unit tests for the Config class.
"""

import os
import sys
import unittest
from unittest.mock import patch

# Add src directory to path to import config module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import Config, ConfigurationError


class TestConfig(unittest.TestCase):
    """Unit tests for the Config class."""

    def setUp(self):
        """Set up test fixtures."""
        # Clear all relevant environment variables before each test
        self.env_vars = [
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION',
            'AWS_S3_BUCKET_NAME', 'CLAZAR_CLIENT_ID', 'CLAZAR_CLIENT_SECRET',
            'CLAZAR_CLOUD', 'SERVICE_NAME', 'ENVIRONMENT_TYPE', 'PLAN_ID',
            'START_MONTH', 'DRY_RUN',
            'DIMENSION1_NAME', 'DIMENSION1_FORMULA',
            'DIMENSION2_NAME', 'DIMENSION2_FORMULA',
            'DIMENSION3_NAME', 'DIMENSION3_FORMULA',
        ]
        for var in self.env_vars:
            if var in os.environ:
                del os.environ[var]
        
        # Set a default custom dimension for tests that don't specifically test dimensions
        os.environ['DIMENSION1_NAME'] = 'default_dimension'
        os.environ['DIMENSION1_FORMULA'] = 'value * 1'

    def tearDown(self):
        """Clean up after each test."""
        for var in self.env_vars:
            if var in os.environ:
                del os.environ[var]

    def test_config_with_defaults(self):
        """Test that Config loads with default values when env vars are not set."""
        config = Config()
        
        # Check defaults
        self.assertEqual(config.aws_s3_bucket, 'omnistrate-usage-metering-export-demo')
        self.assertEqual(config.clazar_client_id, '')
        self.assertEqual(config.clazar_client_secret, '')
        self.assertEqual(config.clazar_cloud, 'aws')
        self.assertEqual(config.service_name, '')
        self.assertEqual(config.environment_type, '')
        self.assertEqual(config.plan_id, '')
        self.assertEqual(config.start_month, '2025-01')
        self.assertFalse(config.dry_run)
        # Should have the default dimension from setUp
        self.assertEqual(len(config.custom_dimensions), 1)
        self.assertIn('default_dimension', config.custom_dimensions)

    def test_config_with_environment_variables(self):
        """Test that Config loads values from environment variables."""
        os.environ['AWS_ACCESS_KEY_ID'] = 'test_key'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'test_secret'
        os.environ['AWS_REGION'] = 'us-west-2'
        os.environ['AWS_S3_BUCKET_NAME'] = 'test-bucket'
        os.environ['CLAZAR_CLIENT_ID'] = 'client123'
        os.environ['CLAZAR_CLIENT_SECRET'] = 'secret456'
        os.environ['CLAZAR_CLOUD'] = 'azure'
        os.environ['SERVICE_NAME'] = 'MySQL'
        os.environ['ENVIRONMENT_TYPE'] = 'DEV'
        os.environ['PLAN_ID'] = 'plan-xyz'
        os.environ['START_MONTH'] = '2024-06'
        os.environ['DRY_RUN'] = 'true'
        
        config = Config()
        
        self.assertEqual(config.aws_access_key_id, 'test_key')
        self.assertEqual(config.aws_secret_access_key, 'test_secret')
        self.assertEqual(config.aws_region, 'us-west-2')
        self.assertEqual(config.aws_s3_bucket, 'test-bucket')
        self.assertEqual(config.clazar_client_id, 'client123')
        self.assertEqual(config.clazar_client_secret, 'secret456')
        self.assertEqual(config.clazar_cloud, 'azure')
        self.assertEqual(config.service_name, 'MySQL')
        self.assertEqual(config.environment_type, 'DEV')
        self.assertEqual(config.plan_id, 'plan-xyz')
        self.assertEqual(config.start_month, '2024-06')
        self.assertTrue(config.dry_run)

    def test_dry_run_boolean_parsing(self):
        """Test that DRY_RUN is parsed correctly for various values."""
        test_cases = {
            'true': True,
            'True': True,
            'TRUE': True,
            '1': True,
            'yes': True,
            'YES': True,
            'false': False,
            'False': False,
            '0': False,
            'no': False,
            '': False,
            'anything': False,
        }
        
        for value, expected in test_cases.items():
            os.environ['DRY_RUN'] = value
            config = Config()
            self.assertEqual(config.dry_run, expected, 
                           f"DRY_RUN='{value}' should result in {expected}")
            del os.environ['DRY_RUN']

    def test_custom_dimensions_single(self):
        """Test loading a single custom dimension."""
        os.environ['DIMENSION1_NAME'] = 'cpu_hours'
        os.environ['DIMENSION1_FORMULA'] = 'cpu * hours'
        
        config = Config()
        
        self.assertEqual(len(config.custom_dimensions), 1)
        self.assertEqual(config.custom_dimensions['cpu_hours'], 'cpu * hours')

    def test_custom_dimensions_multiple(self):
        """Test loading multiple custom dimensions."""
        os.environ['DIMENSION1_NAME'] = 'cpu_hours'
        os.environ['DIMENSION1_FORMULA'] = 'cpu * hours'
        os.environ['DIMENSION2_NAME'] = 'memory_gb'
        os.environ['DIMENSION2_FORMULA'] = 'memory / 1024'
        os.environ['DIMENSION3_NAME'] = 'storage_tb'
        os.environ['DIMENSION3_FORMULA'] = 'storage / 1024'
        
        config = Config()
        
        self.assertEqual(len(config.custom_dimensions), 3)
        self.assertEqual(config.custom_dimensions['cpu_hours'], 'cpu * hours')
        self.assertEqual(config.custom_dimensions['memory_gb'], 'memory / 1024')
        self.assertEqual(config.custom_dimensions['storage_tb'], 'storage / 1024')

    def test_custom_dimensions_partial_raises_error(self):
        """Test that providing only name or only formula raises an error."""
        # Clear the default dimension from setUp
        del os.environ['DIMENSION1_NAME']
        del os.environ['DIMENSION1_FORMULA']
        
        # Only name, no formula
        os.environ['DIMENSION1_NAME'] = 'cpu_hours'
        
        with self.assertRaises(ConfigurationError) as context:
            Config()
        self.assertIn('DIMENSION1_NAME', str(context.exception))
        self.assertIn('DIMENSION1_FORMULA', str(context.exception))
        
        del os.environ['DIMENSION1_NAME']
        
        # Only formula, no name
        os.environ['DIMENSION2_FORMULA'] = 'memory / 1024'
        
        with self.assertRaises(ConfigurationError) as context:
            Config()
        self.assertIn('DIMENSION2_NAME', str(context.exception))
        self.assertIn('DIMENSION2_FORMULA', str(context.exception))

    def test_custom_dimensions_empty_raises_error(self):
        """Test that Config raises an error when no custom dimensions are provided."""
        # Clear the default dimension from setUp
        del os.environ['DIMENSION1_NAME']
        del os.environ['DIMENSION1_FORMULA']
        
        # Don't set any DIMENSION*_NAME or DIMENSION*_FORMULA environment variables
        # This should trigger the check: if len(self.custom_dimensions) == 0
        
        with self.assertRaises(ConfigurationError) as context:
            Config()
        self.assertIn('At least one custom dimension must be provided', str(context.exception))

    def test_validate_aws_credentials_success(self):
        """Test that validate_aws_credentials passes with valid credentials."""
        os.environ['AWS_ACCESS_KEY_ID'] = 'test_key'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'test_secret'
        os.environ['AWS_REGION'] = 'us-west-2'
        
        config = Config()
        # Should not raise an exception
        config.validate_aws_credentials()

    def test_validate_aws_credentials_missing_secret(self):
        """Test that validate_aws_credentials fails when secret is missing."""
        os.environ['AWS_ACCESS_KEY_ID'] = 'test_key'
        
        config = Config()
        with self.assertRaises(ConfigurationError) as context:
            config.validate_aws_credentials()
        self.assertIn('AWS_SECRET_ACCESS_KEY', str(context.exception))

    def test_validate_aws_credentials_missing_key(self):
        """Test that validate_aws_credentials fails when key is missing."""
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'test_secret'
        
        config = Config()
        with self.assertRaises(ConfigurationError) as context:
            config.validate_aws_credentials()
        self.assertIn('AWS_ACCESS_KEY_ID', str(context.exception))

    def test_validate_required_config_success(self):
        """Test that validate_required_config passes with all required values."""
        os.environ['AWS_S3_BUCKET_NAME'] = 'test-bucket'
        os.environ['SERVICE_NAME'] = 'MySQL'
        os.environ['ENVIRONMENT_TYPE'] = 'PROD'
        os.environ['PLAN_ID'] = 'plan-123'
        
        config = Config()
        # Should not raise an exception
        config.validate_required_config()

    def test_validate_required_config_missing_values(self):
        """Test that validate_required_config fails when required values are missing."""
        # All defaults are set, so bucket_name will be present
        # But let's set them to empty strings
        os.environ['AWS_S3_BUCKET_NAME'] = ''
        
        config = Config()
        with self.assertRaises(ConfigurationError) as context:
            config.validate_required_config()
        self.assertIn('Missing required configuration', str(context.exception))

    def test_validate_custom_dimensions_no_duplicates(self):
        """Test that validate_custom_dimensions passes when no duplicates exist."""
        os.environ['DIMENSION1_NAME'] = 'cpu_hours'
        os.environ['DIMENSION1_FORMULA'] = 'cpu * hours'
        os.environ['DIMENSION2_NAME'] = 'memory_gb'
        os.environ['DIMENSION2_FORMULA'] = 'memory / 1024'
        
        config = Config()
        # Should not raise an exception
        config.validate_custom_dimensions()

    def test_validate_custom_dimensions_with_duplicates(self):
        """Test that Config initialization fails when duplicate dimension names exist."""
        os.environ['DIMENSION1_NAME'] = 'cpu_hours'
        os.environ['DIMENSION1_FORMULA'] = 'cpu * hours'
        os.environ['DIMENSION2_NAME'] = 'cpu_hours'  # Duplicate name
        os.environ['DIMENSION2_FORMULA'] = 'different formula'
        
        # Error should be raised during initialization
        with self.assertRaises(ConfigurationError) as context:
            Config()
        self.assertIn('Duplicate dimension name', str(context.exception))

    def test_validate_start_month_success(self):
        """Test that validate_start_month parses valid month formats."""
        test_cases = [
            ('2024-01', (2024, 1)),
            ('2024-12', (2024, 12)),
            ('2025-06', (2025, 6)),
            ('1999-03', (1999, 3)),
        ]
        
        for start_month, expected in test_cases:
            os.environ['START_MONTH'] = start_month
            config = Config()
            result = config.validate_start_month()
            self.assertEqual(result, expected, 
                           f"START_MONTH='{start_month}' should parse to {expected}")
            del os.environ['START_MONTH']

    def test_validate_start_month_default(self):
        """Test that validate_start_month returns default when not set."""
        config = Config()
        result = config.validate_start_month()
        self.assertEqual(result, (2025, 1))

    def test_validate_start_month_invalid_format(self):
        """Test that validate_start_month fails with invalid formats."""
        invalid_formats = [
            '2024/01',
            '202401',
            '2024-1',
            '24-01',
            'invalid',
            '2024-00',  # Month 0 is invalid
            '2024-13',  # Month 13 is invalid
            '1899-01',  # Year too old
            '10000-01', # Year too large
        ]
        
        for invalid_format in invalid_formats:
            os.environ['START_MONTH'] = invalid_format
            config = Config()
            with self.assertRaises(ConfigurationError):
                config.validate_start_month()
            del os.environ['START_MONTH']

    def test_validate_all_success(self):
        """Test that validate_all passes when all validations succeed."""
        os.environ['AWS_ACCESS_KEY_ID'] = 'test_key'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'test_secret'
        os.environ['AWS_REGION'] = 'us-west-2'
        os.environ['AWS_S3_BUCKET_NAME'] = 'test-bucket'
        os.environ['SERVICE_NAME'] = 'MySQL'
        os.environ['ENVIRONMENT_TYPE'] = 'PROD'
        os.environ['PLAN_ID'] = 'plan-123'
        os.environ['START_MONTH'] = '2024-06'
        
        config = Config()
        # Should not raise an exception
        config.validate_all()

    def test_validate_all_fails_on_any_error(self):
        """Test that validate_all fails if any validation fails."""
        # Missing AWS credentials
        os.environ['AWS_S3_BUCKET_NAME'] = 'test-bucket'
        os.environ['SERVICE_NAME'] = 'MySQL'
        os.environ['ENVIRONMENT_TYPE'] = 'PROD'
        os.environ['PLAN_ID'] = 'plan-123'
        
        config = Config()
        with self.assertRaises(ConfigurationError):
            config.validate_all()

    def test_print_summary(self):
        """Test that print_summary runs without errors."""
        os.environ['AWS_REGION'] = 'us-west-2'
        os.environ['AWS_S3_BUCKET_NAME'] = 'test-bucket'
        os.environ['DIMENSION1_NAME'] = 'cpu_hours'
        os.environ['DIMENSION1_FORMULA'] = 'cpu * hours'
        
        config = Config()
        
        # Should not raise an exception
        # We can't easily test the output without capturing stdout,
        # but we can at least verify it doesn't crash
        config.print_summary()


if __name__ == '__main__':
    unittest.main()
