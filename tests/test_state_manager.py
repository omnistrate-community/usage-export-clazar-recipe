import unittest
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime, timezone
from botocore.exceptions import ClientError

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from state_manager import StateManager, StateManagerError


class TestStateManager(unittest.TestCase):
    """Unit tests for StateManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.bucket_name = "test-bucket"
        self.state_file_path = "test_state.json"
        self.aws_access_key_id = "test_access_key"
        self.aws_secret_access_key = "test_secret_key"
        self.aws_region = "us-west-2"
        
        # Create mock S3 client
        self.mock_s3_client = Mock()
        
    @patch('state_manager.boto3.client')
    def test_init(self, mock_boto_client):
        """Test StateManager initialization."""
        mock_boto_client.return_value = self.mock_s3_client
        
        state_manager = StateManager(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file_path,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_region=self.aws_region
        )
        
        self.assertEqual(state_manager.bucket_name, self.bucket_name)
        self.assertEqual(state_manager.state_file_path, self.state_file_path)
        mock_boto_client.assert_called_once()
        
    @patch('state_manager.boto3.client')
    def test_validate_access_success(self, mock_boto_client):
        """Test successful state access validation."""
        mock_boto_client.return_value = self.mock_s3_client
        
        # Mock successful S3 operations
        self.mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=lambda: b'{}')
        }
        self.mock_s3_client.put_object.return_value = {}
        
        state_manager = StateManager(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file_path,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_region=self.aws_region
        )
        
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
        
        state_manager = StateManager(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file_path,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_region=self.aws_region
        )
        
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
        
        state_manager = StateManager(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file_path
        )
        
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
        
        state_manager = StateManager(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file_path
        )
        
        state = state_manager.load_state()
        self.assertEqual(state, {})
        
    @patch('state_manager.boto3.client')
    def test_save_state(self, mock_boto_client):
        """Test saving state."""
        mock_boto_client.return_value = self.mock_s3_client
        
        state_manager = StateManager(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file_path
        )
        
        test_state = {"service1": {"last_processed_month": "2025-01"}}
        state_manager.save_state(test_state)
        
        self.mock_s3_client.put_object.assert_called_once()
        call_args = self.mock_s3_client.put_object.call_args
        self.assertEqual(call_args[1]['Bucket'], self.bucket_name)
        self.assertEqual(call_args[1]['Key'], self.state_file_path)
        
    @patch('state_manager.boto3.client')
    def test_get_service_key(self, mock_boto_client):
        """Test service key generation."""
        mock_boto_client.return_value = self.mock_s3_client
        
        state_manager = StateManager(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file_path
        )
        
        key = state_manager.get_service_key("Postgres", "PROD", "plan-123")
        self.assertEqual(key, "Postgres:PROD:plan-123")
        
    @patch('state_manager.boto3.client')
    def test_get_month_key(self, mock_boto_client):
        """Test month key generation."""
        mock_boto_client.return_value = self.mock_s3_client
        
        state_manager = StateManager(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file_path
        )
        
        key = state_manager.get_month_key(2025, 1)
        self.assertEqual(key, "2025-01")
        
        key = state_manager.get_month_key(2025, 12)
        self.assertEqual(key, "2025-12")


if __name__ == '__main__':
    unittest.main()
