#!/usr/bin/env python3
"""
Unit tests for the OmnistrateMeteringReader class.
"""

import json
import os
import sys
import unittest
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

from botocore.exceptions import ClientError

# Add src directory to path to import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import Config
from omnistrate_metering_reader import OmnistrateMeteringReader, OmnistrateMeteringReaderError


class TestOmnistrateMeteringReader(unittest.TestCase):
    """Unit tests for the OmnistrateMeteringReader class."""

    def setUp(self):
        """Set up test fixtures."""
        # Set up required environment variables
        os.environ['AWS_ACCESS_KEY_ID'] = 'test_access_key'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'test_secret_key'
        os.environ['AWS_REGION'] = 'us-east-1'
        os.environ['AWS_S3_BUCKET_NAME'] = 'test-bucket'
        os.environ['CLAZAR_CLIENT_ID'] = 'test_client_id'
        os.environ['CLAZAR_CLIENT_SECRET'] = 'test_client_secret'
        os.environ['CLAZAR_CLOUD'] = 'aws'
        os.environ['SERVICE_NAME'] = 'test-service'
        os.environ['ENVIRONMENT_TYPE'] = 'PROD'
        os.environ['PLAN_ID'] = 'test-plan'
        os.environ['DIMENSION1_NAME'] = 'test_dimension'
        os.environ['DIMENSION1_FORMULA'] = 'value * 1'

        self.config = Config()
        
    def tearDown(self):
        """Clean up after each test."""
        env_vars = [
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION',
            'AWS_S3_BUCKET_NAME', 'CLAZAR_CLIENT_ID', 'CLAZAR_CLIENT_SECRET',
            'CLAZAR_CLOUD', 'SERVICE_NAME', 'ENVIRONMENT_TYPE', 'PLAN_ID',
            'DIMENSION1_NAME', 'DIMENSION1_FORMULA',
        ]
        for var in env_vars:
            if var in os.environ:
                del os.environ[var]

    @patch('omnistrate_metering_reader.boto3.client')
    def test_init_with_valid_config(self, mock_boto_client):
        """Test that OmnistrateMeteringReader initializes correctly with valid config."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        reader = OmnistrateMeteringReader(self.config)
        
        self.assertEqual(reader.aws_s3_bucket, 'test-bucket')
        self.assertIsNotNone(reader.s3_client)
        self.assertIsNotNone(reader.logger)
        
        # Verify boto3 client was called with correct parameters
        mock_boto_client.assert_called_once_with(
            's3',
            aws_access_key_id='test_access_key',
            aws_secret_access_key='test_secret_key',
            region_name='us-east-1'
        )

    def test_init_without_config(self):
        """Test that OmnistrateMeteringReader raises error when config is None."""
        with self.assertRaises(OmnistrateMeteringReaderError) as context:
            OmnistrateMeteringReader(None)
        
        self.assertIn("Configuration object is required", str(context.exception))

    @patch('omnistrate_metering_reader.boto3.client')
    def test_init_without_bucket_name(self, mock_boto_client):
        """Test that OmnistrateMeteringReader raises error when bucket name is not configured."""
        # Config has a default value for bucket, so we need to set it to None manually
        config = Config()
        config.aws_s3_bucket = None
        
        with self.assertRaises(OmnistrateMeteringReaderError) as context:
            OmnistrateMeteringReader(config)
        
        self.assertIn("AWS S3 bucket name is not configured", str(context.exception))

    @patch('omnistrate_metering_reader.boto3.client')
    def test_init_without_access_key(self, mock_boto_client):
        """Test that OmnistrateMeteringReader raises error when access key is not configured."""
        del os.environ['AWS_ACCESS_KEY_ID']
        config = Config()
        
        with self.assertRaises(OmnistrateMeteringReaderError) as context:
            OmnistrateMeteringReader(config)
        
        self.assertIn("AWS Access Key ID is not configured", str(context.exception))

    @patch('omnistrate_metering_reader.boto3.client')
    def test_init_without_secret_key(self, mock_boto_client):
        """Test that OmnistrateMeteringReader raises error when secret key is not configured."""
        del os.environ['AWS_SECRET_ACCESS_KEY']
        config = Config()
        
        with self.assertRaises(OmnistrateMeteringReaderError) as context:
            OmnistrateMeteringReader(config)
        
        self.assertIn("AWS Secret Access Key is not configured", str(context.exception))

    @patch('omnistrate_metering_reader.boto3.client')
    def test_init_without_region(self, mock_boto_client):
        """Test that OmnistrateMeteringReader raises error when region is not configured."""
        del os.environ['AWS_REGION']
        config = Config()
        
        with self.assertRaises(OmnistrateMeteringReaderError) as context:
            OmnistrateMeteringReader(config)
        
        self.assertIn("AWS region is not configured", str(context.exception))

    @patch('omnistrate_metering_reader.boto3.client')
    def test_get_service_key(self, mock_boto_client):
        """Test that get_service_key generates correct service key."""
        reader = OmnistrateMeteringReader(self.config)
        
        key = reader.get_service_key()
        
        self.assertEqual(key, 'test-service:PROD:test-plan')

    @patch('omnistrate_metering_reader.boto3.client')
    def test_load_usage_data_state_success(self, mock_boto_client):
        """Test that load_usage_data_state successfully loads state from S3."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock S3 response
        state_data = {
            'service1:PROD:plan1': {
                'lastSuccessfulExport': '2025-01-31T23:59:59Z'
            }
        }
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = json.dumps(state_data).encode('utf-8')
        mock_s3.get_object.return_value = mock_response
        
        reader = OmnistrateMeteringReader(self.config)
        state = reader.load_usage_data_state()
        
        self.assertEqual(state, state_data)
        mock_s3.get_object.assert_called_once_with(
            Bucket='test-bucket',
            Key='omnistrate-metering/last_success_export.json'
        )

    @patch('omnistrate_metering_reader.boto3.client')
    def test_load_usage_data_state_file_not_found(self, mock_boto_client):
        """Test that load_usage_data_state returns empty dict when file not found."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock NoSuchKey error
        error_response = {'Error': {'Code': 'NoSuchKey', 'Message': 'Not found'}}
        mock_s3.get_object.side_effect = ClientError(error_response, 'GetObject')
        
        reader = OmnistrateMeteringReader(self.config)
        state = reader.load_usage_data_state()
        
        self.assertEqual(state, {})

    @patch('omnistrate_metering_reader.boto3.client')
    def test_load_usage_data_state_other_error(self, mock_boto_client):
        """Test that load_usage_data_state returns empty dict on other errors."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock other error
        error_response = {'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}}
        mock_s3.get_object.side_effect = ClientError(error_response, 'GetObject')
        
        reader = OmnistrateMeteringReader(self.config)
        state = reader.load_usage_data_state()
        
        self.assertEqual(state, {})

    @patch('omnistrate_metering_reader.boto3.client')
    def test_load_usage_data_state_json_decode_error(self, mock_boto_client):
        """Test that load_usage_data_state returns empty dict on JSON decode error."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock invalid JSON response
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = b'invalid json content'
        mock_s3.get_object.return_value = mock_response
        
        reader = OmnistrateMeteringReader(self.config)
        state = reader.load_usage_data_state()
        
        self.assertEqual(state, {})

    @patch('omnistrate_metering_reader.boto3.client')
    def test_get_latest_month_with_complete_usage_data_success(self, mock_boto_client):
        """Test that get_latest_month_with_complete_usage_data returns correct month."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock S3 response
        state_data = {
            'test-service:PROD:test-plan': {
                'lastSuccessfulExport': '2025-01-31T23:59:59Z'
            }
        }
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = json.dumps(state_data).encode('utf-8')
        mock_s3.get_object.return_value = mock_response
        
        reader = OmnistrateMeteringReader(self.config)
        result = reader.get_latest_month_with_complete_usage_data()
        
        self.assertEqual(result, (2025, 1))

    @patch('omnistrate_metering_reader.boto3.client')
    def test_get_latest_month_with_no_state(self, mock_boto_client):
        """Test that get_latest_month_with_complete_usage_data returns None when no state."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock S3 response with empty state
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = b'{}'
        mock_s3.get_object.return_value = mock_response
        
        reader = OmnistrateMeteringReader(self.config)
        result = reader.get_latest_month_with_complete_usage_data()
        
        self.assertIsNone(result)

    @patch('omnistrate_metering_reader.boto3.client')
    def test_get_latest_month_service_not_in_state(self, mock_boto_client):
        """Test that get_latest_month_with_complete_usage_data returns None when service not in state."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock S3 response with different service
        state_data = {
            'other-service:PROD:plan-456': {
                'lastSuccessfulExport': '2025-01-31T23:59:59Z'
            }
        }
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = json.dumps(state_data).encode('utf-8')
        mock_s3.get_object.return_value = mock_response
        
        reader = OmnistrateMeteringReader(self.config)
        result = reader.get_latest_month_with_complete_usage_data()
        
        self.assertIsNone(result)

    @patch('omnistrate_metering_reader.boto3.client')
    def test_get_latest_month_no_last_export(self, mock_boto_client):
        """Test that get_latest_month_with_complete_usage_data returns None when no lastSuccessfulExport."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock S3 response without lastSuccessfulExport
        state_data = {
            'test-service:PROD:test-plan': {}
        }
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = json.dumps(state_data).encode('utf-8')
        mock_s3.get_object.return_value = mock_response
        
        reader = OmnistrateMeteringReader(self.config)
        result = reader.get_latest_month_with_complete_usage_data()
        
        self.assertIsNone(result)

    @patch('omnistrate_metering_reader.boto3.client')
    def test_get_latest_month_invalid_date_format(self, mock_boto_client):
        """Test that get_latest_month_with_complete_usage_data returns None on invalid date format."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock S3 response with invalid date format
        state_data = {
            'test-service:PROD:test-plan': {
                'lastSuccessfulExport': 'invalid-date'
            }
        }
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = json.dumps(state_data).encode('utf-8')
        mock_s3.get_object.return_value = mock_response
        
        reader = OmnistrateMeteringReader(self.config)
        result = reader.get_latest_month_with_complete_usage_data()
        
        self.assertIsNone(result)

    @patch('omnistrate_metering_reader.boto3.client')
    def test_get_monthly_s3_prefix(self, mock_boto_client):
        """Test that get_monthly_s3_prefix generates correct S3 prefix."""
        reader = OmnistrateMeteringReader(self.config)
        
        prefix = reader.get_monthly_s3_prefix(2025, 1)
        
        self.assertEqual(prefix, 'omnistrate-metering/test-service/PROD/test-plan/2025/01/')

    @patch('omnistrate_metering_reader.boto3.client')
    def test_get_monthly_s3_prefix_double_digit_month(self, mock_boto_client):
        """Test that get_monthly_s3_prefix formats month correctly."""
        reader = OmnistrateMeteringReader(self.config)
        
        prefix = reader.get_monthly_s3_prefix(2025, 12)
        
        self.assertEqual(prefix, 'omnistrate-metering/test-service/PROD/test-plan/2025/12/')

    @patch('omnistrate_metering_reader.boto3.client')
    def test_list_monthly_subscription_files_success(self, mock_boto_client):
        """Test that list_monthly_subscription_files returns correct file list."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock paginator
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'omnistrate-metering/my-service/PROD/plan-123/2025/01/file1.json'},
                    {'Key': 'omnistrate-metering/my-service/PROD/plan-123/2025/01/file2.json'},
                    {'Key': 'omnistrate-metering/my-service/PROD/plan-123/2025/01/file3.txt'},  # Should be filtered out
                ]
            }
        ]
        
        reader = OmnistrateMeteringReader(self.config)
        files = reader.list_monthly_subscription_files(2025, 1)
        
        self.assertEqual(len(files), 2)
        self.assertIn('omnistrate-metering/my-service/PROD/plan-123/2025/01/file1.json', files)
        self.assertIn('omnistrate-metering/my-service/PROD/plan-123/2025/01/file2.json', files)
        self.assertNotIn('omnistrate-metering/my-service/PROD/plan-123/2025/01/file3.txt', files)
        
        mock_s3.get_paginator.assert_called_once_with('list_objects_v2')
        mock_paginator.paginate.assert_called_once_with(
            Bucket='test-bucket',
            Prefix='omnistrate-metering/test-service/PROD/test-plan/2025/01/'
        )

    @patch('omnistrate_metering_reader.boto3.client')
    def test_list_monthly_subscription_files_no_contents(self, mock_boto_client):
        """Test that list_monthly_subscription_files returns empty list when no contents."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock paginator with no contents
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{}]  # No 'Contents' key
        
        reader = OmnistrateMeteringReader(self.config)
        files = reader.list_monthly_subscription_files(2025, 1)
        
        self.assertEqual(files, [])

    @patch('omnistrate_metering_reader.boto3.client')
    def test_list_monthly_subscription_files_client_error(self, mock_boto_client):
        """Test that list_monthly_subscription_files returns empty list on client error."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock client error
        error_response = {'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}}
        mock_s3.get_paginator.side_effect = ClientError(error_response, 'ListObjectsV2')
        
        reader = OmnistrateMeteringReader(self.config)
        files = reader.list_monthly_subscription_files(2025, 1)
        
        self.assertEqual(files, [])

    @patch('omnistrate_metering_reader.boto3.client')
    def test_read_s3_json_file_success(self, mock_boto_client):
        """Test that read_s3_json_file successfully reads and parses JSON."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock S3 response
        json_data = [
            {'id': 1, 'value': 100},
            {'id': 2, 'value': 200}
        ]
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = json.dumps(json_data).encode('utf-8')
        mock_s3.get_object.return_value = mock_response
        
        reader = OmnistrateMeteringReader(self.config)
        data = reader.read_s3_json_file('test-key.json')
        
        self.assertEqual(data, json_data)
        mock_s3.get_object.assert_called_once_with(
            Bucket='test-bucket',
            Key='test-key.json'
        )

    @patch('omnistrate_metering_reader.boto3.client')
    def test_read_s3_json_file_client_error(self, mock_boto_client):
        """Test that read_s3_json_file returns empty list on client error."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock client error
        error_response = {'Error': {'Code': 'NoSuchKey', 'Message': 'Not found'}}
        mock_s3.get_object.side_effect = ClientError(error_response, 'GetObject')
        
        reader = OmnistrateMeteringReader(self.config)
        data = reader.read_s3_json_file('test-key.json')
        
        self.assertEqual(data, [])

    @patch('omnistrate_metering_reader.boto3.client')
    def test_read_s3_json_file_json_decode_error(self, mock_boto_client):
        """Test that read_s3_json_file returns empty list on JSON decode error."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock invalid JSON response
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = b'invalid json'
        mock_s3.get_object.return_value = mock_response
        
        reader = OmnistrateMeteringReader(self.config)
        data = reader.read_s3_json_file('test-key.json')
        
        self.assertEqual(data, [])

    @patch('omnistrate_metering_reader.boto3.client')
    def test_list_monthly_subscription_files_multiple_pages(self, mock_boto_client):
        """Test that list_monthly_subscription_files handles multiple pages correctly."""
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        
        # Mock paginator with multiple pages
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        
        mock_paginator.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'omnistrate-metering/my-service/PROD/plan-123/2025/01/file1.json'},
                    {'Key': 'omnistrate-metering/my-service/PROD/plan-123/2025/01/file2.json'},
                ]
            },
            {
                'Contents': [
                    {'Key': 'omnistrate-metering/my-service/PROD/plan-123/2025/01/file3.json'},
                    {'Key': 'omnistrate-metering/my-service/PROD/plan-123/2025/01/file4.json'},
                ]
            }
        ]
        
        reader = OmnistrateMeteringReader(self.config)
        files = reader.list_monthly_subscription_files(2025, 1)
        
        self.assertEqual(len(files), 4)
        self.assertIn('omnistrate-metering/my-service/PROD/plan-123/2025/01/file1.json', files)
        self.assertIn('omnistrate-metering/my-service/PROD/plan-123/2025/01/file2.json', files)
        self.assertIn('omnistrate-metering/my-service/PROD/plan-123/2025/01/file3.json', files)
        self.assertIn('omnistrate-metering/my-service/PROD/plan-123/2025/01/file4.json', files)


if __name__ == '__main__':
    unittest.main()
