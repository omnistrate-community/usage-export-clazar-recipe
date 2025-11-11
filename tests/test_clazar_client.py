#!/usr/bin/env python3
"""
Unit tests for ClazarClient
"""

import json
import unittest
from unittest.mock import Mock, patch, MagicMock
import requests

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from clazar_client import ClazarClient, ClazarAPIError


class TestClazarClient(unittest.TestCase):
    """Test cases for ClazarClient"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.client_id = "test_client_id"
        self.client_secret = "test_client_secret"
        self.access_token = "test_access_token"
    
    def test_init(self):
        """Test ClazarClient initialization"""
        client = ClazarClient(
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        
        self.assertEqual(client.client_id, self.client_id)
        self.assertEqual(client.client_secret, self.client_secret)
        self.assertIsNone(client.access_token)
        self.assertFalse(client.dry_run)
    
    def test_init_with_access_token(self):
        """Test ClazarClient initialization with access token"""
        client = ClazarClient(access_token=self.access_token)
        
        self.assertEqual(client.access_token, self.access_token)
        self.assertIsNone(client.client_id)
        self.assertIsNone(client.client_secret)
    
    def test_init_dry_run(self):
        """Test ClazarClient initialization in dry run mode"""
        client = ClazarClient(dry_run=True)
        
        self.assertTrue(client.dry_run)
    
    @patch('clazar_client.requests.post')
    def test_authenticate_success(self, mock_post):
        """Test successful authentication"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": self.access_token}
        mock_post.return_value = mock_response
        
        client = ClazarClient(
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        
        token = client.authenticate()
        
        self.assertEqual(token, self.access_token)
        self.assertEqual(client.access_token, self.access_token)
        
        # Verify the API call
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        self.assertIn("https://api.clazar.io/authenticate/", call_args[0])
        self.assertEqual(call_args[1]['json']['client_id'], self.client_id)
        self.assertEqual(call_args[1]['json']['client_secret'], self.client_secret)
    
    @patch('clazar_client.requests.post')
    def test_authenticate_failure_status_code(self, mock_post):
        """Test authentication failure with non-200 status code"""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_response.json.return_value = {"error": "Invalid credentials"}
        mock_post.return_value = mock_response
        
        client = ClazarClient(
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        
        with self.assertRaises(ClazarAPIError) as context:
            client.authenticate()
        
        self.assertIn("Authentication failed", str(context.exception))
        self.assertEqual(context.exception.status_code, 401)
    
    @patch('clazar_client.requests.post')
    def test_authenticate_no_token_in_response(self, mock_post):
        """Test authentication failure when no token in response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "Success"}
        mock_post.return_value = mock_response
        
        client = ClazarClient(
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        
        with self.assertRaises(ClazarAPIError) as context:
            client.authenticate()
        
        self.assertIn("No access token received", str(context.exception))
    
    @patch('clazar_client.requests.post')
    def test_authenticate_network_error(self, mock_post):
        """Test authentication failure due to network error"""
        mock_post.side_effect = requests.RequestException("Connection timeout")
        
        client = ClazarClient(
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        
        with self.assertRaises(ClazarAPIError) as context:
            client.authenticate()
        
        self.assertIn("Network error during authentication", str(context.exception))
    
    def test_authenticate_missing_credentials(self):
        """Test authentication failure when credentials are missing"""
        client = ClazarClient()
        
        with self.assertRaises(ClazarAPIError) as context:
            client.authenticate()
        
        self.assertIn("Client ID and secret are required", str(context.exception))
    
    def test_authenticate_dry_run(self):
        """Test authentication in dry run mode"""
        client = ClazarClient(
            client_id=self.client_id,
            client_secret=self.client_secret,
            dry_run=True
        )
        
        token = client.authenticate()
        
        self.assertEqual(token, "dry_run_token")
        self.assertEqual(client.access_token, "dry_run_token")
    
    @patch('clazar_client.requests.post')
    def test_send_metering_data_success(self, mock_post):
        """Test successful metering data submission"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {"status": "success", "message": "Data received"}
            ]
        }
        mock_post.return_value = mock_response
        
        client = ClazarClient(access_token=self.access_token)
        
        records = [
            {
                "cloud": "aws",
                "contract_id": "contract-123",
                "dimension": "cpu_hours",
                "start_time": "2025-01-01T00:00:00Z",
                "end_time": "2025-01-31T23:59:59Z",
                "quantity": "100"
            }
        ]
        
        result = client.send_metering_data(records)
        
        self.assertIn("results", result)
        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(result["results"][0]["status"], "success")
        
        # Verify the API call
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        self.assertIn("https://api.clazar.io/metering/", call_args[0])
        self.assertEqual(call_args[1]['json']['request'], records)
        self.assertIn("Authorization", call_args[1]['headers'])
        self.assertEqual(call_args[1]['headers']['Authorization'], f"Bearer {self.access_token}")
    
    def test_send_metering_data_empty_records(self):
        """Test sending empty metering data"""
        client = ClazarClient(access_token=self.access_token)
        
        result = client.send_metering_data([])
        
        self.assertEqual(result, {"results": []})
    
    def test_send_metering_data_no_token(self):
        """Test sending metering data without access token"""
        client = ClazarClient()
        
        records = [{"cloud": "aws", "contract_id": "123"}]
        
        with self.assertRaises(ClazarAPIError) as context:
            client.send_metering_data(records)
        
        self.assertIn("Access token is required", str(context.exception))
    
    def test_send_metering_data_dry_run(self):
        """Test sending metering data in dry run mode"""
        client = ClazarClient(
            access_token=self.access_token,
            dry_run=True
        )
        
        records = [
            {
                "cloud": "aws",
                "contract_id": "contract-123",
                "dimension": "cpu_hours",
                "quantity": "100"
            }
        ]
        
        result = client.send_metering_data(records)
        
        self.assertIn("results", result)
        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(result["results"][0]["status"], "success")
        self.assertEqual(result["results"][0]["message"], "Dry run mode")
    
    @patch('clazar_client.requests.post')
    def test_send_metering_data_http_error(self, mock_post):
        """Test metering data submission with HTTP error"""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad request"
        mock_response.json.return_value = {"error": "Invalid data"}
        mock_post.return_value = mock_response
        
        client = ClazarClient(access_token=self.access_token)
        
        records = [{"cloud": "aws", "contract_id": "123"}]
        
        with self.assertRaises(ClazarAPIError) as context:
            client.send_metering_data(records, max_retries=0)
        
        self.assertIn("HTTP 400", str(context.exception))
        self.assertEqual(context.exception.status_code, 400)
    
    @patch('clazar_client.requests.post')
    def test_send_metering_data_unexpected_response_format(self, mock_post):
        """Test metering data submission with unexpected response format"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "Success"}  # Missing 'results'
        mock_post.return_value = mock_response
        
        client = ClazarClient(access_token=self.access_token)
        
        records = [{"cloud": "aws", "contract_id": "123"}]
        
        with self.assertRaises(ClazarAPIError) as context:
            client.send_metering_data(records, max_retries=0)
        
        self.assertIn("Unexpected response format", str(context.exception))
    
    @patch('clazar_client.requests.post')
    @patch('clazar_client.time.sleep')
    def test_send_metering_data_retry_on_network_error(self, mock_sleep, mock_post):
        """Test retrying metering data submission on network error"""
        # First two calls fail, third succeeds
        mock_post.side_effect = [
            requests.RequestException("Connection timeout"),
            requests.RequestException("Connection timeout"),
            Mock(
                status_code=200,
                json=lambda: {"results": [{"status": "success"}]}
            )
        ]
        
        client = ClazarClient(access_token=self.access_token)
        
        records = [{"cloud": "aws", "contract_id": "123"}]
        
        result = client.send_metering_data(records, max_retries=2)
        
        self.assertIn("results", result)
        self.assertEqual(mock_post.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)  # Sleep called before retry
    
    @patch('clazar_client.requests.post')
    @patch('clazar_client.time.sleep')
    def test_send_metering_data_max_retries_exceeded(self, mock_sleep, mock_post):
        """Test metering data submission when max retries exceeded"""
        mock_post.side_effect = requests.RequestException("Connection timeout")
        
        client = ClazarClient(access_token=self.access_token)
        
        records = [{"cloud": "aws", "contract_id": "123"}]
        
        with self.assertRaises(ClazarAPIError) as context:
            client.send_metering_data(records, max_retries=2)
        
        self.assertIn("Network error", str(context.exception))
        self.assertEqual(mock_post.call_count, 3)  # Initial attempt + 2 retries
    
    def test_check_response_for_errors_no_errors(self):
        """Test checking response with no errors"""
        client = ClazarClient()
        
        response_data = {
            "results": [
                {"status": "success", "message": "Data received"}
            ]
        }
        
        has_errors, errors, error_code, error_message, warnings = client.check_response_for_errors(response_data)
        
        self.assertFalse(has_errors)
        self.assertEqual(errors, [])
        self.assertEqual(warnings, [])
    
    def test_check_response_for_errors_with_errors(self):
        """Test checking response with errors"""
        client = ClazarClient()
        
        response_data = {
            "results": [
                {
                    "status": "error",
                    "errors": ["Dimension not found", "Invalid quantity"],
                    "code": "VALIDATION_ERROR",
                    "message": "Validation failed"
                }
            ]
        }
        
        has_errors, errors, error_code, error_message, warnings = client.check_response_for_errors(response_data)
        
        self.assertTrue(has_errors)
        self.assertEqual(len(errors), 2)
        self.assertIn("Dimension not found", errors)
        self.assertIn("Invalid quantity", errors)
        self.assertEqual(error_code, "VALIDATION_ERROR")
        self.assertEqual(error_message, "Validation failed")
        self.assertEqual(warnings, [])
    
    def test_check_response_for_errors_with_string_error(self):
        """Test checking response with string error"""
        client = ClazarClient()
        
        response_data = {
            "results": [
                {
                    "status": "error",
                    "errors": "Single error message",
                    "code": "API_ERROR",
                    "message": "Error occurred"
                }
            ]
        }
        
        has_errors, errors, error_code, error_message, warnings = client.check_response_for_errors(response_data)
        
        self.assertTrue(has_errors)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0], "Single error message")
        self.assertEqual(warnings, [])
    
    def test_check_response_for_errors_with_warnings(self):
        """Test checking response with warnings (non-success status but no errors)"""
        client = ClazarClient()
        
        response_data = {
            "results": [
                {
                    "status": "warning",
                    "message": "Dimension not registered"
                }
            ]
        }
        
        has_errors, errors, error_code, error_message, warnings = client.check_response_for_errors(response_data)
        
        # Warnings should not be treated as errors
        self.assertFalse(has_errors)
        self.assertEqual(errors, [])
        self.assertEqual(len(warnings), 1)
        self.assertEqual(warnings[0]["status"], "warning")
    
    def test_check_response_for_errors_multiple_results(self):
        """Test checking response with multiple results, some with errors"""
        client = ClazarClient()
        
        response_data = {
            "results": [
                {"status": "success", "message": "Data received"},
                {
                    "status": "error",
                    "errors": ["Error in record 2"],
                    "code": "DATA_ERROR",
                    "message": "Invalid data"
                }
            ]
        }
        
        has_errors, errors, error_code, error_message, warnings = client.check_response_for_errors(response_data)
        
        self.assertTrue(has_errors)
        self.assertIn("Error in record 2", errors)
        self.assertEqual(error_code, "DATA_ERROR")
        self.assertEqual(warnings, [])


class TestClazarAPIError(unittest.TestCase):
    """Test cases for ClazarAPIError"""
    
    def test_error_with_message_only(self):
        """Test ClazarAPIError with message only"""
        error = ClazarAPIError("Test error message")
        
        self.assertEqual(error.message, "Test error message")
        self.assertIsNone(error.status_code)
        self.assertIsNone(error.response_data)
        self.assertEqual(str(error), "Test error message")
    
    def test_error_with_all_fields(self):
        """Test ClazarAPIError with all fields"""
        response_data = {"error": "details"}
        error = ClazarAPIError(
            "Test error",
            status_code=400,
            response_data=response_data
        )
        
        self.assertEqual(error.message, "Test error")
        self.assertEqual(error.status_code, 400)
        self.assertEqual(error.response_data, response_data)


if __name__ == '__main__':
    unittest.main()
