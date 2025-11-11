import unittest
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime, timezone
from botocore.exceptions import ClientError

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from state_manager import StateManager, StateManagerError
from config import Config


class TestStateManager(unittest.TestCase):
    """Unit tests for StateManager class."""

    def setUp(self):
        """Set up test fixtures."""
        # Set required environment variables for Config
        os.environ['AWS_ACCESS_KEY_ID'] = 'test_access_key'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'test_secret_key'
        os.environ['AWS_REGION'] = 'us-west-2'
        os.environ['S3_BUCKET_NAME'] = 'test-bucket'
        os.environ['SERVICE_NAME'] = 'Postgres'
        os.environ['ENVIRONMENT_TYPE'] = 'PROD'
        os.environ['PLAN_ID'] = 'test-plan-123'
        os.environ['DIMENSION1_NAME'] = 'test_dimension'
        os.environ['DIMENSION1_FORMULA'] = 'test_formula'
        os.environ['CLAZAR_CLIENT_ID'] = 'test_client_id'
        os.environ['CLAZAR_CLIENT_SECRET'] = 'test_client_secret'
        
        self.aws_s3_bucket = "test-bucket"
        self.aws_s3_bucket = "test-bucket"
        self.state_file_path = "clazar/Postgres-PROD-test-plan-123-export_state.json"
        self.aws_access_key_id = "test_access_key"
        self.aws_secret_access_key = "test_secret_key"
        self.aws_region = "us-west-2"
        
        # Create mock S3 client
        self.mock_s3_client = Mock()
    
    def tearDown(self):
        """Clean up test fixtures."""
        # Clean up environment variables
        env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION',
                    'S3_BUCKET_NAME', 'SERVICE_NAME', 'ENVIRONMENT_TYPE', 'PLAN_ID',
                    'DIMENSION1_NAME', 'DIMENSION1_FORMULA', 'CLAZAR_CLIENT_ID',
                    'CLAZAR_CLIENT_SECRET']
        for var in env_vars:
            if var in os.environ:
                del os.environ[var]
        
    @patch('omnistrate_metering_reader.boto3.client')
    @patch('state_manager.boto3.client')
    def test_init(self, mock_boto_client, mock_boto_client_metering):
        """Test StateManager initialization."""
        mock_boto_client.return_value = self.mock_s3_client
        mock_boto_client_metering.return_value = self.mock_s3_client
        
        config = Config()
        state_manager = StateManager(config=config)
        
        self.assertEqual(state_manager.aws_s3_bucket, self.aws_s3_bucket)
        self.assertEqual(state_manager.file_path, self.state_file_path)
        # Verify StateManager has a metering_reader instance
        self.assertIsNotNone(state_manager.metering_reader)
        self.assertIsNotNone(state_manager.s3_client)
        
    @patch('state_manager.boto3.client')
    def test_validate_access_success(self, mock_boto_client):
        """Test successful state access validation."""
        mock_boto_client.return_value = self.mock_s3_client
        
        # Mock successful S3 operations
        self.mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: b'{}')
        }
        self.mock_s3_client.put_object.return_value = {}
        
        config = Config()
        state_manager = StateManager(config=config)
        
        # Should not raise exception
        state_manager.validate_access()
        
    @patch('state_manager.boto3.client')
    def test_validate_access_failure(self, mock_boto_client):
        """Test state access validation failure."""
        mock_boto_client.return_value = self.mock_s3_client
        
        # Mock S3 client error
        self.mock_s3_client.get_object.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
            'GetObject'
        )
        
        config = Config()
        state_manager = StateManager(config=config)
        
        # Should raise StateManagerError
        with self.assertRaises(StateManagerError):
            state_manager.validate_access()
            
    @patch('state_manager.boto3.client')
    def test_load_state_existing(self, mock_boto_client):
        """Test loading existing state."""
        mock_boto_client.return_value = self.mock_s3_client
        
        test_state = {"service1": {"last_processed_month": "2025-01"}}
        self.mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(test_state).encode('utf-8'))
        }
        
        config = Config()
        state_manager = StateManager(config=config)
        
        state = state_manager.load_state()
        self.assertEqual(state, test_state)
        
    @patch('state_manager.boto3.client')
    def test_load_state_not_found(self, mock_boto_client):
        """Test loading state when file doesn't exist."""
        mock_boto_client.return_value = self.mock_s3_client
        
        self.mock_s3_client.get_object.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey', 'Message': 'Not Found'}},
            'GetObject'
        )
        
        config = Config()
        state_manager = StateManager(config=config)
        
        state = state_manager.load_state()
        self.assertEqual(state, {})
        
    @patch('state_manager.boto3.client')
    def test_save_state(self, mock_boto_client):
        """Test saving state."""
        mock_boto_client.return_value = self.mock_s3_client
        
        config = Config()
        state_manager = StateManager(config=config)
        
        test_state = {"service1": {"last_processed_month": "2025-01"}}
        state_manager.save_state(test_state)
        
        self.mock_s3_client.put_object.assert_called_once()
        call_args = self.mock_s3_client.put_object.call_args
        self.assertEqual(call_args[1]['Bucket'], self.aws_s3_bucket)
        self.assertEqual(call_args[1]['Key'], self.state_file_path)
        
    @patch('state_manager.boto3.client')
    def test_get_service_key(self, mock_boto_client):
        """Test service key generation."""
        mock_boto_client.return_value = self.mock_s3_client
        
        config = Config()
        state_manager = StateManager(config=config)
        
        key = state_manager.get_service_key("Postgres", "PROD", "plan-123")
        self.assertEqual(key, "Postgres:PROD:plan-123")
        
    @patch('state_manager.boto3.client')
    def test_get_month_key(self, mock_boto_client):
        """Test month key generation."""
        mock_boto_client.return_value = self.mock_s3_client
        
        config = Config()
        state_manager = StateManager(config=config)
        
        key = state_manager.get_month_key(2025, 1)
        self.assertEqual(key, "2025-01")
        
        key = state_manager.get_month_key(2025, 12)
        self.assertEqual(key, "2025-12")


if __name__ == '__main__':
    unittest.main()
