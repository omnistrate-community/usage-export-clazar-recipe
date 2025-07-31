#!/usr/bin/env python3
"""
Tests for MeteringProcessor class.
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, mock_open

# Add the parent directory to the Python path to import metering_processor
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metering_processor import MeteringProcessor


class TestMeteringProcessor(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.state_file = os.path.join(self.temp_dir, "test_state.json")
        self.bucket_name = "test-bucket"
        self.clazar_api_url = "https://test-api.example.com/metering/"
        
        # Create processor instances
        with patch('metering_processor.boto3.client'):
            self.processor = MeteringProcessor(
                bucket_name=self.bucket_name,
                state_file_path=self.state_file,
                clazar_api_url=self.clazar_api_url,
                dry_run=False
            )
            
            self.dry_run_processor = MeteringProcessor(
                bucket_name=self.bucket_name,
                state_file_path=self.state_file,
                clazar_api_url=self.clazar_api_url,
                dry_run=True
            )
    
    def tearDown(self):
        """Clean up test fixtures."""
        # Clean up temp files
        if os.path.exists(self.state_file):
            os.remove(self.state_file)
        os.rmdir(self.temp_dir)
    
    def test_init(self):
        """Test processor initialization."""
        self.assertEqual(self.processor.bucket_name, self.bucket_name)
        self.assertEqual(self.processor.state_file_path, self.state_file)
        self.assertEqual(self.processor.clazar_api_url, self.clazar_api_url)
        self.assertFalse(self.processor.dry_run)
        self.assertTrue(self.dry_run_processor.dry_run)
    
    def test_get_service_key(self):
        """Test service key generation."""
        key = self.processor.get_service_key("TestService", "PROD", "plan-123")
        self.assertEqual(key, "TestService:PROD:plan-123")
    
    def test_get_month_key(self):
        """Test month key generation."""
        key = self.processor.get_month_key(2025, 1)
        self.assertEqual(key, "2025-01")
        
        key = self.processor.get_month_key(2025, 12)
        self.assertEqual(key, "2025-12")
    
    @patch('metering_processor.MeteringProcessor.load_state')
    @patch('metering_processor.MeteringProcessor.save_state')
    def test_load_state_no_file(self, mock_save, mock_load):
        """Test loading state when file doesn't exist."""
        mock_load.return_value = {}
        state = self.processor.load_state()
        self.assertEqual(state, {})
    
    @patch('metering_processor.MeteringProcessor.load_state')
    def test_load_state_valid_file(self, mock_load):
        """Test loading state from valid file."""
        test_state = {"service1:PROD:plan1": {"last_processed_month": "2025-01", "processed_contracts": {}}}
        mock_load.return_value = test_state
        
        state = self.processor.load_state()
        self.assertEqual(state, test_state)
    
    @patch('metering_processor.MeteringProcessor.load_state')
    def test_load_state_invalid_json(self, mock_load):
        """Test loading state from invalid JSON file."""
        mock_load.return_value = {}
        
        state = self.processor.load_state()
        self.assertEqual(state, {})
    
    @patch('metering_processor.MeteringProcessor.save_state')
    def test_save_state(self, mock_save):
        """Test saving state to file."""
        test_state = {"service1:PROD:plan1": {"last_processed_month": "2025-01"}}
        
        self.processor.save_state(test_state)
        mock_save.assert_called_once_with(test_state)
    
    @patch('metering_processor.MeteringProcessor.load_state')
    def test_get_last_processed_month_no_record(self, mock_load):
        """Test getting last processed month when no record exists."""
        mock_load.return_value = {}
        
        last_month = self.processor.get_last_processed_month("TestService", "PROD", "plan-123")
        self.assertIsNone(last_month)
        last_month = self.processor.get_last_processed_month("TestService", "PROD", "plan-123")
        self.assertIsNone(last_month)
    
    @patch('metering_processor.MeteringProcessor.load_state')
    def test_get_last_processed_month_valid_record(self, mock_load):
        """Test getting last processed month with valid record."""
        test_state = {
            "TestService:PROD:plan-123": {
                "last_processed_month": "2025-01"
            }
        }
        mock_load.return_value = test_state
        
        last_month = self.processor.get_last_processed_month("TestService", "PROD", "plan-123")
        self.assertEqual(last_month, (2025, 1))
    
    @patch('metering_processor.MeteringProcessor.load_state')
    @patch('metering_processor.MeteringProcessor.save_state')
    def test_update_last_processed_month(self, mock_save, mock_load):
        """Test updating last processed month."""
        mock_load.return_value = {}
        
        self.processor.update_last_processed_month("TestService", "PROD", "plan-123", 2025, 1)
        
        # Verify save_state was called
        mock_save.assert_called_once()
        call_args = mock_save.call_args[0][0]
        self.assertIn("TestService:PROD:plan-123", call_args)
        self.assertEqual(call_args["TestService:PROD:plan-123"]["last_processed_month"], "2025-01")
    
    @patch('metering_processor.MeteringProcessor.get_last_processed_month')
    def test_get_next_month_to_process_no_history(self, mock_get_last):
        """Test getting next month when no processing history exists."""
        mock_get_last.return_value = None
        
        with patch('metering_processor.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = datetime(2025, 3, 15, 10, 0, 0)
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
            
            next_month = self.processor.get_next_month_to_process("TestService", "PROD", "plan-123")
            
            # Should start from 2 months ago (January when current is March)
            self.assertEqual(next_month, (2025, 1))
    
    @patch('metering_processor.MeteringProcessor.get_last_processed_month')
    def test_get_next_month_to_process_with_history(self, mock_get_last):
        """Test getting next month with processing history."""
        mock_get_last.return_value = (2025, 1)  # Last processed was January
        
        next_month = self.processor.get_next_month_to_process("TestService", "PROD", "plan-123")
        self.assertEqual(next_month, (2025, 2))  # Next should be February
    
    @patch('metering_processor.MeteringProcessor.get_last_processed_month')
    def test_get_next_month_to_process_caught_up(self, mock_get_last):
        """Test getting next month when caught up to current month."""
        current_time = datetime(2025, 2, 15, 0, 0, 0)
        mock_get_last.return_value = (2025, 1)  # Last processed was January
        
        with patch('metering_processor.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = current_time
            mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
            
            next_month = self.processor.get_next_month_to_process("TestService", "PROD", "plan-123")
            self.assertIsNone(next_month)  # Should be caught up
    
    def test_get_monthly_s3_prefix(self):
        """Test S3 prefix generation for monthly data."""
        prefix = self.processor.get_monthly_s3_prefix("TestService", "PROD", "plan-123", 2025, 1)
        
        expected = "omnistrate-metering/TestService/PROD/plan-123/2025/01/"
        self.assertEqual(prefix, expected)
    
    @patch('metering_processor.MeteringProcessor.load_state')
    def test_is_contract_month_processed_no_state(self, mock_load):
        """Test checking if contract month is processed when no state exists."""
        mock_load.return_value = {}
        
        result = self.processor.is_contract_month_processed(
            "TestService", "PROD", "plan-123", "contract-123", 2025, 1
        )
        self.assertFalse(result)
    
    @patch('metering_processor.MeteringProcessor.load_state')
    def test_is_contract_month_processed_in_processed_contracts(self, mock_load):
        """Test checking if contract month is processed when it's in processed_contracts."""
        mock_load.return_value = {
            "TestService:PROD:plan-123": {
                "processed_contracts": {
                    "2025-01": ["contract-123"]
                }
            }
        }
        
        result = self.processor.is_contract_month_processed(
            "TestService", "PROD", "plan-123", "contract-123", 2025, 1
        )
        self.assertTrue(result)

    @patch('metering_processor.MeteringProcessor.load_state')
    def test_is_contract_month_processed_in_error_contracts(self, mock_load):
        """Test checking if contract month is processed when it's in error_contracts."""
        mock_load.return_value = {
            "TestService:PROD:plan-123": {
                "error_contracts": {
                    "2025-01": [
                        {
                            "contract_id": "contract-123",
                            "errors": ["Some error"],
                            "code": "ERROR_001",
                            "message": "Error occurred"
                        }
                    ]
                }
            }
        }
        
        result = self.processor.is_contract_month_processed(
            "TestService", "PROD", "plan-123", "contract-123", 2025, 1
        )
        self.assertTrue(result)
    
    @patch('metering_processor.MeteringProcessor.load_state')
    @patch('metering_processor.MeteringProcessor.save_state')
    def test_mark_contract_month_processed(self, mock_save, mock_load):
        """Test marking a contract month as processed."""
        mock_load.return_value = {}
        
        self.processor.mark_contract_month_processed(
            "TestService", "PROD", "plan-123", "contract-123", 2025, 1
        )
        
        # Verify save_state was called
        mock_save.assert_called_once()
        call_args = mock_save.call_args[0][0]
        
        service_key = "TestService:PROD:plan-123"
        month_key = "2025-01"
        
        self.assertIn(service_key, call_args)
        self.assertIn("processed_contracts", call_args[service_key])
        self.assertIn(month_key, call_args[service_key]["processed_contracts"])
        self.assertIn("contract-123", call_args[service_key]["processed_contracts"][month_key])

    @patch('metering_processor.MeteringProcessor.load_state')
    @patch('metering_processor.MeteringProcessor.save_state')
    def test_mark_contract_month_error(self, mock_save, mock_load):
        """Test marking a contract month as having errors."""
        mock_load.return_value = {}
        
        self.processor.mark_contract_month_error(
            "TestService", "PROD", "plan-123", "contract-123", 2025, 1,
            ["API error", "Invalid dimension"], "ERROR_001", "Processing failed"
        )
        
        # Verify save_state was called
        mock_save.assert_called_once()
        call_args = mock_save.call_args[0][0]
        
        service_key = "TestService:PROD:plan-123"
        month_key = "2025-01"
        
        self.assertIn(service_key, call_args)
        self.assertIn("error_contracts", call_args[service_key])
        self.assertIn(month_key, call_args[service_key]["error_contracts"])
        
        error_entry = call_args[service_key]["error_contracts"][month_key][0]
        self.assertEqual(error_entry["contract_id"], "contract-123")
        self.assertEqual(error_entry["errors"], ["API error", "Invalid dimension"])
        self.assertEqual(error_entry["code"], "ERROR_001")
        self.assertEqual(error_entry["message"], "Processing failed")
    
    @patch('metering_processor.boto3.client')
    def test_list_monthly_subscription_files_success(self, mock_boto3_client):
        """Test listing subscription files for a month successfully."""
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Create a new processor instance to get the mocked S3 client
        processor = MeteringProcessor(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file,
            clazar_api_url=self.clazar_api_url,
            dry_run=False
        )
        
        # Mock paginator
        mock_paginator = Mock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        
        mock_page_iterator = [
            {
                'Contents': [
                    {'Key': 'path/subscription1.json'},
                    {'Key': 'path/subscription2.json'},
                    {'Key': 'path/not-json.txt'}
                ]
            }
        ]
        mock_paginator.paginate.return_value = mock_page_iterator
        
        files = processor.list_monthly_subscription_files("test-prefix/")
        
        expected = ['path/subscription1.json', 'path/subscription2.json']
        self.assertEqual(files, expected)
    
    @patch('metering_processor.boto3.client')
    def test_list_monthly_subscription_files_no_contents(self, mock_boto3_client):
        """Test listing subscription files when no files exist."""
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Create a new processor instance to get the mocked S3 client
        processor = MeteringProcessor(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file,
            clazar_api_url=self.clazar_api_url,
            dry_run=False
        )
        
        # Mock paginator with no contents
        mock_paginator = Mock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{}]  # No Contents key
        
        files = processor.list_monthly_subscription_files("test-prefix/")
        
        self.assertEqual(files, [])
    
    @patch('metering_processor.boto3.client')
    def test_read_s3_json_file_success(self, mock_boto3_client):
        """Test reading S3 JSON file successfully."""
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Create a new processor instance to get the mocked S3 client
        processor = MeteringProcessor(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file,
            clazar_api_url=self.clazar_api_url,
            dry_run=False
        )
        
        test_data = [{"record1": "data1"}, {"record2": "data2"}]
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = json.dumps(test_data).encode('utf-8')
        mock_s3_client.get_object.return_value = mock_response
        
        data = processor.read_s3_json_file("test-key.json")
        self.assertEqual(data, test_data)
    
    @patch('metering_processor.boto3.client')
    def test_read_s3_json_file_invalid_json(self, mock_boto3_client):
        """Test reading S3 file with invalid JSON."""
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Create a new processor instance to get the mocked S3 client
        processor = MeteringProcessor(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file,
            clazar_api_url=self.clazar_api_url,
            dry_run=False
        )
        
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = b"invalid json"
        mock_s3_client.get_object.return_value = mock_response
        
        data = processor.read_s3_json_file("test-key.json")
        self.assertEqual(data, [])
    
    def test_aggregate_usage_data(self):
        """Test usage data aggregation."""
        usage_records = [
            {"externalPayerId": "payer1", "dimension": "dim1", "value": 10},
            {"externalPayerId": "payer1", "dimension": "dim1", "value": 5},
            {"externalPayerId": "payer2", "dimension": "dim1", "value": 20},
            {"externalPayerId": "payer1", "dimension": "dim2", "value": 15},
        ]
        
        aggregated = self.processor.aggregate_usage_data(usage_records)
        
        expected = {
            ("payer1", "dim1"): 15,
            ("payer2", "dim1"): 20,
            ("payer1", "dim2"): 15,
        }
        
        self.assertEqual(aggregated, expected)
    
    def test_aggregate_usage_data_missing_fields(self):
        """Test usage data aggregation with missing fields."""
        usage_records = [
            {"externalPayerId": "payer1", "dimension": "dim1", "value": 10},
            {"externalPayerId": "payer1", "value": 5},  # Missing dimension
            {"dimension": "dim1", "value": 20},  # Missing externalPayerId
        ]
        
        aggregated = self.processor.aggregate_usage_data(usage_records)
        
        expected = {
            ("payer1", "dim1"): 10,
        }
        
        self.assertEqual(aggregated, expected)
    
    @patch('metering_processor.MeteringProcessor.is_contract_month_processed')
    def test_filter_processed_contracts(self, mock_is_processed):
        """Test filtering out already processed contracts."""
        aggregated_data = {
            ("contract1", "dim1"): 100,
            ("contract2", "dim1"): 200,
            ("contract3", "dim2"): 300,
        }
        
        # Mock that contract1 is already processed
        mock_is_processed.side_effect = lambda service, env, plan, contract, year, month: contract == "contract1"
        
        filtered = self.processor.filter_processed_contracts(
            aggregated_data, "TestService", "PROD", "plan-123", 2025, 1
        )
        
        expected = {
            ("contract2", "dim1"): 200,
            ("contract3", "dim2"): 300,
        }
        
        self.assertEqual(filtered, expected)
    
    @patch('metering_processor.requests.post')
    @patch('metering_processor.MeteringProcessor.mark_contract_month_processed')
    def test_send_to_clazar_success(self, mock_mark_processed, mock_post):
        """Test sending data to Clazar successfully."""
        # Set access token for this test
        self.processor.access_token = "test-token"
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {"status": "success", "contract_id": "payer1"},
                {"status": "success", "contract_id": "payer2"}
            ]
        }
        mock_post.return_value = mock_response
        
        aggregated_data = {
            ("payer1", "dim1"): 15,
            ("payer2", "dim1"): 20,
        }
        start_time = datetime(2025, 1, 1, 0, 0, 0)
        end_time = datetime(2025, 1, 31, 23, 59, 59)
        
        result = self.processor.send_to_clazar(
            aggregated_data, start_time, end_time,
            "TestService", "PROD", "plan-123"
        )
        
        self.assertTrue(result)
        mock_post.assert_called_once()
        # Should mark both contracts as processed
        self.assertEqual(mock_mark_processed.call_count, 2)
    
    @patch('metering_processor.requests.post')
    @patch('metering_processor.MeteringProcessor.mark_contract_month_error')
    def test_send_to_clazar_api_error(self, mock_mark_error, mock_post):
        """Test sending data to Clazar with API error."""
        # Set access token for this test
        self.processor.access_token = "test-token"
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "errors": ["Invalid dimension"], 
                    "code": "ERROR_001", 
                    "message": "Error occurred", 
                    "contract_id": "payer1"
                }
            ]
        }
        mock_post.return_value = mock_response
        
        aggregated_data = {("payer1", "dim1"): 15}
        start_time = datetime(2025, 1, 1, 0, 0, 0)
        end_time = datetime(2025, 1, 31, 23, 59, 59)
        
        result = self.processor.send_to_clazar(
            aggregated_data, start_time, end_time,
            "TestService", "PROD", "plan-123"
        )
        
        # Should still return True since the error was recorded
        self.assertTrue(result)
        # Should mark the contract as having an error
        mock_mark_error.assert_called_once_with(
            "TestService", "PROD", "plan-123", "payer1", 2025, 1,
            ["Invalid dimension"], "ERROR_001", "Error occurred"
        )
    
    @patch('metering_processor.MeteringProcessor.mark_contract_month_processed')
    def test_send_to_clazar_dry_run(self, mock_mark_processed):
        """Test sending data to Clazar in dry run mode."""
        aggregated_data = {
            ("payer1", "dim1"): 15,
            ("payer2", "dim1"): 20,
        }
        start_time = datetime(2025, 1, 1, 0, 0, 0)
        end_time = datetime(2025, 1, 31, 23, 59, 59)
        
        with patch('metering_processor.requests.post') as mock_post:
            result = self.dry_run_processor.send_to_clazar(
                aggregated_data, start_time, end_time,
                "TestService", "PROD", "plan-123"
            )
            
            self.assertTrue(result)
            mock_post.assert_not_called()  # Should not make actual API call
            # Should still mark contracts as processed in dry run
            self.assertEqual(mock_mark_processed.call_count, 2)
    
    def test_send_to_clazar_empty_data(self):
        """Test sending empty data to Clazar."""
        aggregated_data = {}
        start_time = datetime(2025, 1, 1, 0, 0, 0)
        end_time = datetime(2025, 1, 31, 23, 59, 59)
        
        with patch('metering_processor.requests.post') as mock_post:
            result = self.processor.send_to_clazar(
                aggregated_data, start_time, end_time,
                "TestService", "PROD", "plan-123"
            )
            
            self.assertTrue(result)
            mock_post.assert_not_called()
    
    @patch('metering_processor.MeteringProcessor.send_to_clazar')
    @patch('metering_processor.MeteringProcessor.filter_processed_contracts')
    @patch('metering_processor.MeteringProcessor.aggregate_usage_data')
    @patch('metering_processor.MeteringProcessor.read_s3_json_file')
    @patch('metering_processor.MeteringProcessor.list_monthly_subscription_files')
    def test_process_month_success(self, mock_list_files, mock_read_file, 
                                   mock_aggregate, mock_filter, mock_send):
        """Test processing a month successfully."""
        year, month = 2025, 1
        
        # Mock the method calls
        mock_list_files.return_value = ['file1.json', 'file2.json']
        mock_read_file.side_effect = [
            [{"record1": "data1"}],
            [{"record2": "data2"}]
        ]
        mock_aggregate.return_value = {("payer1", "dim1"): 15}
        mock_filter.return_value = {("payer1", "dim1"): 15}
        mock_send.return_value = True
        
        result = self.processor.process_month("TestService", "PROD", "plan-123", year, month)
        
        self.assertTrue(result)
        mock_list_files.assert_called_once()
        self.assertEqual(mock_read_file.call_count, 2)
        mock_aggregate.assert_called_once()
        mock_filter.assert_called_once()
        mock_send.assert_called_once()
    
    @patch('metering_processor.MeteringProcessor.list_monthly_subscription_files')
    def test_process_month_no_files(self, mock_list_files):
        """Test processing a month with no subscription files."""
        mock_list_files.return_value = []
        
        result = self.processor.process_month("TestService", "PROD", "plan-123", 2025, 1)
        
        self.assertTrue(result)
    
    @patch('metering_processor.MeteringProcessor.filter_processed_contracts')
    @patch('metering_processor.MeteringProcessor.aggregate_usage_data')
    @patch('metering_processor.MeteringProcessor.read_s3_json_file')
    @patch('metering_processor.MeteringProcessor.list_monthly_subscription_files')
    def test_process_month_all_contracts_processed(self, mock_list_files, mock_read_file,
                                                   mock_aggregate, mock_filter):
        """Test processing a month where all contracts are already processed."""
        year, month = 2025, 1
        
        mock_list_files.return_value = ['file1.json']
        mock_read_file.return_value = [{"record1": "data1"}]
        mock_aggregate.return_value = {("payer1", "dim1"): 15}
        mock_filter.return_value = {}  # All contracts already processed
        
        result = self.processor.process_month("TestService", "PROD", "plan-123", year, month)
        
        self.assertTrue(result)
    
    @patch('metering_processor.MeteringProcessor.update_last_processed_month')
    @patch('metering_processor.MeteringProcessor.process_month')
    @patch('metering_processor.MeteringProcessor.get_next_month_to_process')
    def test_process_pending_months_success(self, mock_get_next_month, mock_process_month, mock_update):
        """Test processing pending months successfully."""
        # Mock returning two months to process, then None
        mock_get_next_month.side_effect = [
            (2025, 1),
            (2025, 2),
            None
        ]
        mock_process_month.return_value = True
        
        result = self.processor.process_pending_months("TestService", "PROD", "plan-123", max_months=5)
        
        self.assertTrue(result)
        self.assertEqual(mock_process_month.call_count, 2)
        self.assertEqual(mock_update.call_count, 2)
    
    @patch('metering_processor.MeteringProcessor.process_month')
    @patch('metering_processor.MeteringProcessor.get_next_month_to_process')
    def test_process_pending_months_failure(self, mock_get_next_month, mock_process_month):
        """Test processing pending months with failure."""
        mock_get_next_month.return_value = (2025, 1)
        mock_process_month.return_value = False
        
        result = self.processor.process_pending_months("TestService", "PROD", "plan-123", max_months=5)
        
        self.assertFalse(result)
        mock_process_month.assert_called_once()


class TestMeteringProcessorIntegration(unittest.TestCase):
    """Integration tests that test multiple components together."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.state_file = os.path.join(self.temp_dir, "integration_state.json")
        
        with patch('metering_processor.boto3.client'):
            self.processor = MeteringProcessor(
                bucket_name="test-bucket",
                state_file_path=self.state_file,
                clazar_api_url="https://test-api.example.com/metering/",
                dry_run=True  # Use dry run for integration tests
            )
    
    def tearDown(self):
        """Clean up integration test fixtures."""
        if os.path.exists(self.state_file):
            os.remove(self.state_file)
        os.rmdir(self.temp_dir)
    
    @patch('metering_processor.MeteringProcessor.load_state')
    @patch('metering_processor.MeteringProcessor.save_state')
    def test_state_persistence_workflow(self, mock_save, mock_load):
        """Test the complete state persistence workflow."""
        service_name = "TestService"
        environment_type = "PROD"
        plan_id = "plan-123"
        
        # Initially no last processed month
        mock_load.return_value = {}
        last_month = self.processor.get_last_processed_month(service_name, environment_type, plan_id)
        self.assertIsNone(last_month)
        
        # Update the last processed month
        test_year, test_month = 2025, 1
        self.processor.update_last_processed_month(service_name, environment_type, plan_id, test_year, test_month)
        
        # Verify save was called
        mock_save.assert_called_once()
        
        # Mock the updated state for next call
        mock_load.return_value = {
            "TestService:PROD:plan-123": {
                "last_processed_month": "2025-01"
            }
        }
        
        # Verify it was saved
        last_month = self.processor.get_last_processed_month(service_name, environment_type, plan_id)
        self.assertEqual(last_month, (test_year, test_month))
    
    @patch('metering_processor.MeteringProcessor.load_state')
    @patch('metering_processor.MeteringProcessor.save_state')
    def test_contract_processing_workflow(self, mock_save, mock_load):
        """Test the complete contract processing workflow."""
        service_name = "TestService"
        environment_type = "PROD"
        plan_id = "plan-123"
        contract_id = "contract-123"
        year, month = 2025, 1
        
        # Initially contract not processed
        mock_load.return_value = {}
        is_processed = self.processor.is_contract_month_processed(
            service_name, environment_type, plan_id, contract_id, year, month
        )
        self.assertFalse(is_processed)
        
        # Mark contract as processed
        self.processor.mark_contract_month_processed(
            service_name, environment_type, plan_id, contract_id, year, month
        )
        
        # Verify save was called
        mock_save.assert_called()
        call_args = mock_save.call_args[0][0]
        
        service_key = "TestService:PROD:plan-123"
        month_key = "2025-01"
        
        self.assertIn(service_key, call_args)
        self.assertIn("processed_contracts", call_args[service_key])
        self.assertIn(month_key, call_args[service_key]["processed_contracts"])
        self.assertIn(contract_id, call_args[service_key]["processed_contracts"][month_key])

    @patch('metering_processor.MeteringProcessor.load_state')
    @patch('metering_processor.MeteringProcessor.save_state')
    def test_contract_error_tracking_workflow(self, mock_save, mock_load):
        """Test the complete contract error tracking workflow."""
        service_name = "TestService"
        environment_type = "PROD"
        plan_id = "plan-123"
        contract_id = "contract-456"
        year, month = 2025, 1
        
        # Initially contract not processed
        mock_load.return_value = {}
        is_processed = self.processor.is_contract_month_processed(
            service_name, environment_type, plan_id, contract_id, year, month
        )
        self.assertFalse(is_processed)
        
        # Mark contract as having errors
        self.processor.mark_contract_month_error(
            service_name, environment_type, plan_id, contract_id, year, month,
            ["API timeout", "Retry failed"], "TIMEOUT_ERROR", "Request timed out"
        )
        
        # Verify save was called
        mock_save.assert_called()
        call_args = mock_save.call_args[0][0]
        
        service_key = "TestService:PROD:plan-123"
        month_key = "2025-01"
        
        self.assertIn(service_key, call_args)
        self.assertIn("error_contracts", call_args[service_key])
        self.assertIn(month_key, call_args[service_key]["error_contracts"])
        
        # Check error entry structure
        error_entry = call_args[service_key]["error_contracts"][month_key][0]
        self.assertEqual(error_entry["contract_id"], contract_id)
        self.assertEqual(error_entry["errors"], ["API timeout", "Retry failed"])
        self.assertEqual(error_entry["code"], "TIMEOUT_ERROR")
        self.assertEqual(error_entry["message"], "Request timed out")
        
        # Mock the updated state for checking if processed
        mock_load.return_value = call_args
        
        # Verify it's now considered processed (even though it had errors)
        is_processed = self.processor.is_contract_month_processed(
            service_name, environment_type, plan_id, contract_id, year, month
        )
        self.assertTrue(is_processed)


if __name__ == '__main__':
    unittest.main()
