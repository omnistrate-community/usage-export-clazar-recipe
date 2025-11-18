#!/usr/bin/env python3
"""
Unit tests for MeteringProcessor
"""

import unittest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime, timezone
from collections import defaultdict
import calendar

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from metering_processor import MeteringProcessor
from config import Config
from clazar_client import ClazarClient, ClazarAPIError
from state_manager import StateManager, StateManagerError
from omnistrate_metering_reader import OmnistrateMeteringReader


class TestMeteringProcessor(unittest.TestCase):
    """Unit tests for MeteringProcessor class."""

    def setUp(self):
        """Set up test fixtures."""
        # Set required environment variables for Config
        os.environ['AWS_ACCESS_KEY_ID'] = 'test_access_key'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'test_secret_key'
        os.environ['AWS_REGION'] = 'us-west-2'
        os.environ['AWS_S3_BUCKET_NAME'] = 'test-bucket'
        os.environ['SERVICE_NAME'] = 'Postgres'
        os.environ['ENVIRONMENT_TYPE'] = 'PROD'
        os.environ['PLAN_ID'] = 'test-plan-123'
        os.environ['CLAZAR_CLIENT_ID'] = 'test_client_id'
        os.environ['CLAZAR_CLIENT_SECRET'] = 'test_client_secret'
        os.environ['CLAZAR_CLOUD'] = 'aws'
        os.environ['DIMENSION1_NAME'] = 'custom_dimension'
        os.environ['DIMENSION1_FORMULA'] = 'memory_byte_hours + storage_allocated_byte_hours'
        
        self.config = Config()
        self.mock_state_manager = Mock(spec=StateManager)
        self.mock_clazar_client = Mock(spec=ClazarClient)
        self.mock_metering_reader = Mock(spec=OmnistrateMeteringReader)
        
        self.processor = MeteringProcessor(
            config=self.config,
            metering_reader=self.mock_metering_reader,
            state_manager=self.mock_state_manager,
            clazar_client=self.mock_clazar_client
        )

    def tearDown(self):
        """Clean up test fixtures."""
        env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION',
                    'AWS_S3_BUCKET_NAME', 'SERVICE_NAME', 'ENVIRONMENT_TYPE', 'PLAN_ID',
                    'CLAZAR_CLIENT_ID', 'CLAZAR_CLIENT_SECRET', 'CLAZAR_CLOUD',
                    'DIMENSION1_NAME', 'DIMENSION1_FORMULA']
        for var in env_vars:
            if var in os.environ:
                del os.environ[var]

    def test_init(self):
        """Test MeteringProcessor initialization."""
        self.assertEqual(self.processor.aws_s3_bucket, 'test-bucket')
        self.assertIsNotNone(self.processor.state_manager)
        self.assertIsNotNone(self.processor.clazar_client)
        self.assertEqual(self.processor.clazar_cloud, 'aws')
        self.assertIsNotNone(self.processor.custom_dimensions)

    def test_get_next_month_to_process_first_time(self):
        """Test getting next month when never processed before."""
        self.mock_state_manager.get_last_processed_month.return_value = None
        self.mock_metering_reader.get_latest_month_with_complete_usage_data.return_value = (2025, 3)
        
        result = self.processor.get_next_month_to_process(
            default_start_month=(2025, 1)
        )
        
        self.assertEqual(result, (2025, 1))
        self.mock_state_manager.get_last_processed_month.assert_called_once_with()

    def test_get_next_month_to_process_next_month(self):
        """Test getting next month when already processed some months."""
        self.mock_state_manager.get_last_processed_month.return_value = (2025, 1)
        self.mock_metering_reader.get_latest_month_with_complete_usage_data.return_value = (2025, 3)
        
        result = self.processor.get_next_month_to_process(
            default_start_month=(2025, 1)
        )
        
        self.assertEqual(result, (2025, 2))

    def test_get_next_month_to_process_year_rollover(self):
        """Test getting next month across year boundary."""
        self.mock_state_manager.get_last_processed_month.return_value = (2024, 12)
        self.mock_metering_reader.get_latest_month_with_complete_usage_data.return_value = (2025, 3)
        
        result = self.processor.get_next_month_to_process(
            default_start_month=(2024, 1)
        )
        
        self.assertEqual(result, (2025, 1))

    def test_get_next_month_to_process_caught_up(self):
        """Test getting next month when caught up."""
        self.mock_state_manager.get_last_processed_month.return_value = (2025, 3)
        self.mock_metering_reader.get_latest_month_with_complete_usage_data.return_value = (2025, 3)
        
        result = self.processor.get_next_month_to_process(
            default_start_month=(2025, 1)
        )
        
        self.assertIsNone(result)

    def test_get_next_month_to_process_no_complete_data(self):
        """Test getting next month when no complete data available."""
        self.mock_state_manager.get_last_processed_month.return_value = None
        self.mock_metering_reader.get_latest_month_with_complete_usage_data.return_value = None
        
        result = self.processor.get_next_month_to_process(
            default_start_month=(2025, 1)
        )
        
        self.assertIsNone(result)

    def test_aggregate_usage_data(self):
        """Test aggregating usage data."""
        usage_records = [
            {'externalPayerId': 'contract-1', 'dimension': 'cpu_core_hours', 'value': 100, 'pricePerUnit': 0.05},
            {'externalPayerId': 'contract-1', 'dimension': 'cpu_core_hours', 'value': 50, 'pricePerUnit': 0.05},
            {'externalPayerId': 'contract-1', 'dimension': 'memory_byte_hours', 'value': 200, 'pricePerUnit': 0.1},
            {'externalPayerId': 'contract-2', 'dimension': 'cpu_core_hours', 'value': 75, 'pricePerUnit': 0.05},
        ]
        
        result = self.processor.aggregate_usage_data(usage_records)
        
        expected = {
            ('contract-1', 'cpu_core_hours'): (150, 7.5),
            ('contract-1', 'memory_byte_hours'): (200, 20.0),
            ('contract-2', 'cpu_core_hours'): (75, 3.75),
        }
        self.assertEqual(result, expected)

    def test_aggregate_usage_data_missing_fields(self):
        """Test aggregating usage data with missing fields."""
        usage_records = [
            {'externalPayerId': 'contract-1', 'dimension': 'cpu_core_hours', 'value': 100, 'pricePerUnit': 0.05},
            {'externalPayerId': 'contract-1', 'value': 50, 'pricePerUnit': 0.05},  # Missing dimension
            {'dimension': 'memory_byte_hours', 'value': 200, 'pricePerUnit': 0.1},  # Missing externalPayerId
            {'externalPayerId': 'contract-2', 'dimension': 'cpu_core_hours', 'value': 75, 'pricePerUnit': 0.05},
        ]
        
        result = self.processor.aggregate_usage_data(usage_records)
        
        expected = {
            ('contract-1', 'cpu_core_hours'): (100, 5),
            ('contract-2', 'cpu_core_hours'): (75, 3.75),
        }
        self.assertEqual(result, expected)

    def test_transform_dimensions_no_custom_dimensions(self):
        """Test transform dimensions when no custom dimensions defined."""
        # Create processor without custom dimensions by setting empty custom_dimensions
        processor = MeteringProcessor(
            config=self.config,
            metering_reader=self.mock_metering_reader,
            state_manager=self.mock_state_manager,
            clazar_client=self.mock_clazar_client
        )
        # Override custom_dimensions to be empty for this test
        processor.custom_dimensions = {}
        
        aggregated_data = {
            ('contract-1', 'cpu_core_hours'): 100,
            ('contract-1', 'memory_byte_hours'): 200,
        }
        
        result = processor.transform_dimensions(aggregated_data)
        
        # Should return original data when no custom dimensions
        self.assertEqual(result, aggregated_data)

    def test_transform_dimensions_with_custom_dimensions(self):
        """Test transform dimensions with custom dimension formulas."""
        aggregated_data = {
            ('contract-1', 'memory_byte_hours'): (1000, 0),
            ('contract-1', 'storage_allocated_byte_hours'): (2000, 0),
            ('contract-2', 'memory_byte_hours'): (500, 0),
            ('contract-2', 'storage_allocated_byte_hours'): (1500, 0),
        }
        
        result = self.processor.transform_dimensions(aggregated_data)
        
        expected = {
            ('contract-1', 'custom_dimension'): 3000,
            ('contract-2', 'custom_dimension'): 2000,
        }
        self.assertEqual(result, expected)

    def test_transform_dimensions_formula_error(self):
        """Test transform dimensions with invalid formula."""
        os.environ['DIMENSION1_FORMULA'] = 'invalid_dimension + other_dimension'
        config = Config()
        processor = MeteringProcessor(
            config=config,
            metering_reader=self.mock_metering_reader,
            state_manager=self.mock_state_manager,
            clazar_client=self.mock_clazar_client
        )
        
        aggregated_data = {
            ('contract-1', 'memory_byte_hours'): 1000,
            ('contract-1', 'storage_allocated_byte_hours'): 2000,
        }
        
        result = processor.transform_dimensions(aggregated_data)
        
        # Should return empty dict when formula fails
        self.assertEqual(result, {})

    def test_transform_dimensions_with_price_per_unit_formula(self):
        """Test transform dimensions with cpu_core_hours_total / 0.05 formula."""
        # Create processor with custom dimension using price per unit
        os.environ['DIMENSION1_NAME'] = 'cost_dimension'
        os.environ['DIMENSION1_FORMULA'] = 'cpu_core_hours_total / 0.05'
        config = Config()
        processor = MeteringProcessor(
            config=config,
            metering_reader=self.mock_metering_reader,
            state_manager=self.mock_state_manager,
            clazar_client=self.mock_clazar_client
        )
        
        aggregated_data = {
            ('contract-1', 'cpu_core_hours'): (10000, 10000 * 50),
            ('contract-2', 'cpu_core_hours'): (20000, 20000 * 100),
        }
        
        result = processor.transform_dimensions(aggregated_data)
        
        # contract-1: 10000 * 50 / 0.05 = 10000000.0
        # contract-2: 20000 * 100 / 0.05 = 40000000.0
        expected = {
            ('contract-1', 'cost_dimension'): 10000000.0,
            ('contract-2', 'cost_dimension'): 40000000.0,
        }
        self.assertEqual(result, expected)
        
        # Clean up environment variable
        del os.environ['DIMENSION1_NAME']
        del os.environ['DIMENSION1_FORMULA']

    def test_transform_dimensions_with_real_world_usage_data(self):
        """Test transform dimensions with real-world usage data from Omnistrate."""
        # Create processor with custom dimension using price per unit formula
        os.environ['DIMENSION1_NAME'] = 'cost_dimension'
        os.environ['DIMENSION1_FORMULA'] = 'cpu_core_hours_total / 0.05'
        config = Config()
        processor = MeteringProcessor(
            config=config,
            metering_reader=self.mock_metering_reader,
            state_manager=self.mock_state_manager,
            clazar_client=self.mock_clazar_client
        )
        
        # Real-world usage records from Omnistrate
        usage_records = [
            {
                "timestamp": "2025-11-14T03:01:59Z",
                "organizationId": "org-ng3178atx4",
                "organizationName": "Omnistrate",
                "customerId": "user-wqo8MXGOWw",
                "customerEmail": "xzhang+billing+canary+org-mm1ll2x0oy@omnistrate.com",
                "subscriptionId": "sub-3YH5N1M4zz",
                "externalPayerId": "ae641bd1-edf8-4038-bfed-d2ff556c729e",
                "serviceId": "s-P6UJ5XUunY",
                "serviceName": "pg",
                "serviceEnvironmentId": "se-6dkTBqXrUu",
                "serviceEnvironmentType": "PROD",
                "productTierId": "pt-HJSv20iWX0",
                "productTierName": "pg",
                "hostClusterId": "hc-pelsk80ph",
                "instanceId": "instance-8qtgw2dx7",
                "podName": "postgres-0",
                "instanceType": "t4g.small",
                "hostName": "ip-172-0-63-226.us-east-2.compute.internal",
                "dimension": "storage_allocated_byte_hours",
                "value": 10737418240,
                "pricePerUnit": 0.05
            },
            {
                "timestamp": "2025-11-14T03:01:59Z",
                "organizationId": "org-ng3178atx4",
                "organizationName": "Omnistrate",
                "customerId": "user-wqo8MXGOWw",
                "customerEmail": "xzhang+billing+canary+org-mm1ll2x0oy@omnistrate.com",
                "subscriptionId": "sub-3YH5N1M4zz",
                "externalPayerId": "ae641bd1-edf8-4038-bfed-d2ff556c729e",
                "serviceId": "s-P6UJ5XUunY",
                "serviceName": "pg",
                "serviceEnvironmentId": "se-6dkTBqXrUu",
                "serviceEnvironmentType": "PROD",
                "productTierId": "pt-HJSv20iWX0",
                "productTierName": "pg",
                "hostClusterId": "hc-pelsk80ph",
                "instanceId": "instance-8qtgw2dx7",
                "podName": "postgres-0",
                "instanceType": "t4g.small",
                "hostName": "ip-172-0-63-226.us-east-2.compute.internal",
                "dimension": "memory_byte_hours",
                "value": 2147483648,
                "pricePerUnit": 0.1
            },
            {
                "timestamp": "2025-11-14T03:01:59Z",
                "organizationId": "org-ng3178atx4",
                "organizationName": "Omnistrate",
                "customerId": "user-wqo8MXGOWw",
                "customerEmail": "xzhang+billing+canary+org-mm1ll2x0oy@omnistrate.com",
                "subscriptionId": "sub-3YH5N1M4zz",
                "externalPayerId": "ae641bd1-edf8-4038-bfed-d2ff556c729e",
                "serviceId": "s-P6UJ5XUunY",
                "serviceName": "pg",
                "serviceEnvironmentId": "se-6dkTBqXrUu",
                "serviceEnvironmentType": "PROD",
                "productTierId": "pt-HJSv20iWX0",
                "productTierName": "pg",
                "hostClusterId": "hc-pelsk80ph",
                "instanceId": "instance-8qtgw2dx7",
                "podName": "postgres-0",
                "instanceType": "t4g.small",
                "hostName": "ip-172-0-63-226.us-east-2.compute.internal",
                "dimension": "cpu_core_hours",
                "value": 2,
                "pricePerUnit": 0.2
            },
            {
                "timestamp": "2025-11-14T03:01:59Z",
                "organizationId": "org-ng3178atx4",
                "organizationName": "Omnistrate",
                "customerId": "user-wqo8MXGOWw",
                "customerEmail": "xzhang+billing+canary+org-mm1ll2x0oy@omnistrate.com",
                "subscriptionId": "sub-3YH5N1M4zz",
                "externalPayerId": "ae641bd1-edf8-4038-bfed-d2ff556c729e",
                "serviceId": "s-P6UJ5XUunY",
                "serviceName": "pg",
                "serviceEnvironmentId": "se-6dkTBqXrUu",
                "serviceEnvironmentType": "PROD",
                "productTierId": "pt-HJSv20iWX0",
                "productTierName": "pg",
                "hostClusterId": "hc-pelsk80ph",
                "instanceId": "instance-8qtgw2dx7",
                "podName": "postgres-0",
                "instanceType": "t4g.small",
                "hostName": "ip-172-0-63-226.us-east-2.compute.internal",
                "dimension": "replica_hours",
                "value": 1
            }
        ]
        
        # First aggregate the usage data
        aggregated_data = processor.aggregate_usage_data(usage_records)
        
        # Verify aggregation worked correctly
        expected_aggregated = {
            ('ae641bd1-edf8-4038-bfed-d2ff556c729e', 'storage_allocated_byte_hours'): (10737418240, 536870912),
            ('ae641bd1-edf8-4038-bfed-d2ff556c729e', 'memory_byte_hours'): (2147483648, 214748364.8),
            ('ae641bd1-edf8-4038-bfed-d2ff556c729e', 'cpu_core_hours'): (2, 0.4),
            ('ae641bd1-edf8-4038-bfed-d2ff556c729e', 'replica_hours'): (1, 0),
        }
        self.assertEqual(aggregated_data, expected_aggregated)
        
        # Transform dimensions using the formula
        result = processor.transform_dimensions(aggregated_data)
        
        # Expected calculation: cpu_core_hours * cpu_core_hours_price_per_unit / 0.05
        # = 2 * 0.2 / 0.05 = 0.4 / 0.05 = 8.0
        expected = {
            ('ae641bd1-edf8-4038-bfed-d2ff556c729e', 'cost_dimension'): 8.0,
        }
        self.assertEqual(result, expected)
        
        # Clean up environment variable
        del os.environ['DIMENSION1_NAME']
        del os.environ['DIMENSION1_FORMULA']

    def test_filter_success_contracts(self):
        """Test filtering already processed contracts."""
        aggregated_data = {
            ('contract-1', 'dimension-1'): 100,
            ('contract-2', 'dimension-1'): 200,
            ('contract-3', 'dimension-1'): 300,
        }
        
        # Mock contract-2 as already processed
        def is_processed(contract, year, month):
            return contract == 'contract-2'
        
        self.mock_state_manager.is_contract_month_processed.side_effect = is_processed
        
        result = self.processor.filter_success_contracts(
            aggregated_data, 2025, 1
        )
        
        expected = {
            ('contract-1', 'dimension-1'): 100,
            ('contract-3', 'dimension-1'): 300,
        }
        self.assertEqual(result, expected)

    def test_send_to_clazar_success(self):
        """Test sending data to Clazar successfully."""
        aggregated_data = {
            ('contract-1', 'dimension-1'): 100,
            ('contract-1', 'dimension-2'): 200,
        }
        
        start_time = datetime(2025, 1, 1)
        end_time = datetime(2025, 1, 31, 23, 59, 59)
        
        self.mock_clazar_client.authenticate.return_value = None
        self.mock_clazar_client.send_metering_data.return_value = {'status': 'success'}
        self.mock_clazar_client.check_response_for_errors.return_value = (
            False, [], None, None, []
        )
        
        result = self.processor.send_to_clazar(
            aggregated_data, start_time, end_time
        )
        
        self.assertTrue(result)
        self.mock_clazar_client.authenticate.assert_called_once()
        self.mock_clazar_client.send_metering_data.assert_called_once()
        self.mock_state_manager.mark_contract_month_processed.assert_called_once()

    def test_send_to_clazar_empty_data(self):
        """Test sending empty data to Clazar."""
        aggregated_data = {}
        start_time = datetime(2025, 1, 1)
        end_time = datetime(2025, 1, 31, 23, 59, 59)
        
        result = self.processor.send_to_clazar(
            aggregated_data, start_time, end_time
        )
        
        self.assertTrue(result)
        self.mock_clazar_client.send_metering_data.assert_not_called()

    def test_send_to_clazar_api_error(self):
        """Test sending data to Clazar with API errors."""
        aggregated_data = {
            ('contract-1', 'dimension-1'): 100,
        }
        
        start_time = datetime(2025, 1, 1)
        end_time = datetime(2025, 1, 31, 23, 59, 59)
        
        self.mock_clazar_client.authenticate.return_value = None
        self.mock_clazar_client.send_metering_data.return_value = {'status': 'error'}
        self.mock_clazar_client.check_response_for_errors.return_value = (
            True, ['Error message'], 'ERROR_CODE', 'Error occurred', []
        )
        
        result = self.processor.send_to_clazar(
            aggregated_data, start_time, end_time
        )
        
        self.assertFalse(result)
        self.mock_state_manager.mark_contract_month_error.assert_called_once()

    def test_send_to_clazar_no_client(self):
        """Test sending data to Clazar without client."""
        with self.assertRaises(ValueError) as context:
            processor = MeteringProcessor(
                config=self.config,
                metering_reader=self.mock_metering_reader,
                state_manager=self.mock_state_manager,
                clazar_client=None
            )
        
        self.assertIn("ClazarClient object is required", str(context.exception))

    def test_send_to_clazar_authentication_failure(self):
        """Test sending data to Clazar when authentication fails."""
        aggregated_data = {
            ('contract-1', 'dimension-1'): 100,
        }
        
        start_time = datetime(2025, 1, 1)
        end_time = datetime(2025, 1, 31, 23, 59, 59)
        
        self.mock_clazar_client.authenticate.side_effect = ClazarAPIError('Auth failed')
        
        result = self.processor.send_to_clazar(
            aggregated_data, start_time, end_time
        )
        
        self.assertFalse(result)

    def test_retry_error_contracts_success(self):
        """Test retrying failed contracts successfully."""
        error_contracts = [
            {
                'contract_id': 'contract-1',
                'payload': {
                    'request': [
                        {
                            'cloud': 'aws',
                            'contract_id': 'contract-1',
                            'dimension': 'dimension-1',
                            'quantity': '100'
                        }
                    ]
                }
            }
        ]
        
        self.mock_state_manager.get_error_contracts_for_retry.return_value = error_contracts
        self.mock_clazar_client.send_metering_data.return_value = {'status': 'success'}
        self.mock_clazar_client.check_response_for_errors.return_value = (
            False, [], None, None, []
        )
        
        result = self.processor.retry_error_contracts(2025, 1)
        
        self.assertTrue(result)
        self.mock_state_manager.remove_error_contract.assert_called_once()
        self.mock_state_manager.mark_contract_month_processed.assert_called_once()

    def test_retry_error_contracts_no_errors(self):
        """Test retrying when no error contracts exist."""
        self.mock_state_manager.get_error_contracts_for_retry.return_value = []
        
        result = self.processor.retry_error_contracts(2025, 1)
        
        self.assertTrue(result)
        self.mock_clazar_client.send_metering_data.assert_not_called()

    def test_retry_error_contracts_failure(self):
        """Test retrying failed contracts with continued failure."""
        error_contracts = [
            {
                'contract_id': 'contract-1',
                'payload': {
                    'request': [
                        {
                            'cloud': 'aws',
                            'contract_id': 'contract-1',
                            'dimension': 'dimension-1',
                            'quantity': '100'
                        }
                    ]
                }
            }
        ]
        
        self.mock_state_manager.get_error_contracts_for_retry.return_value = error_contracts
        self.mock_clazar_client.send_metering_data.return_value = {'status': 'error'}
        self.mock_clazar_client.check_response_for_errors.return_value = (
            True, ['Error message'], 'ERROR_CODE', 'Error occurred', []
        )
        
        result = self.processor.retry_error_contracts(2025, 1)
        
        self.assertFalse(result)
        self.mock_state_manager.mark_contract_month_error.assert_called()

    def test_process_month_success(self):
        """Test processing a month successfully."""
        self.mock_state_manager.get_error_contracts_for_retry.return_value = []
        self.mock_metering_reader.list_monthly_subscription_files.return_value = [
            'path/to/file1.json'
        ]
        
        self.mock_metering_reader.read_s3_json_file.return_value = [
            {'externalPayerId': 'contract-1', 'dimension': 'cpu_core_hours', 'value': 100, 'pricePerUnit': 0.05}
        ]
        
        # Add read_s3_json_file method to processor to delegate to metering_reader
        self.processor.read_s3_json_file = self.mock_metering_reader.read_s3_json_file
        
        self.mock_state_manager.is_contract_month_processed.return_value = False
        self.mock_clazar_client.authenticate.return_value = None
        self.mock_clazar_client.send_metering_data.return_value = {'status': 'success'}
        self.mock_clazar_client.check_response_for_errors.return_value = (
            False, [], None, None, []
        )
        
        result = self.processor.process_month(2025, 1)
        
        self.assertTrue(result)

    def test_process_month_no_files(self):
        """Test processing a month with no subscription files."""
        self.mock_state_manager.get_error_contracts_for_retry.return_value = []
        self.mock_metering_reader.list_monthly_subscription_files.return_value = []
        
        result = self.processor.process_month(2025, 1)
        
        self.assertTrue(result)

    def test_process_next_month_success(self):
        """Test processing next month successfully."""
        self.mock_state_manager.get_last_processed_month.return_value = None
        self.mock_metering_reader.get_latest_month_with_complete_usage_data.return_value = (2025, 3)
        
        with patch.object(self.processor, 'process_month', return_value=True):
            result = self.processor.process_next_month(
                start_month=(2025, 1)
            )
        
        self.assertTrue(result)
        self.mock_state_manager.update_last_processed_month.assert_called_once_with(
            2025, 1
        )

    def test_process_next_month_caught_up(self):
        """Test processing next month when caught up."""
        self.mock_state_manager.get_last_processed_month.return_value = (2025, 3)
        self.mock_metering_reader.get_latest_month_with_complete_usage_data.return_value = (2025, 3)
        
        result = self.processor.process_next_month(
            start_month=(2025, 1)
        )
        
        self.assertTrue(result)
        self.mock_state_manager.update_last_processed_month.assert_not_called()

    def test_process_next_month_failure(self):
        """Test processing next month with failure."""
        self.mock_state_manager.get_last_processed_month.return_value = None
        self.mock_metering_reader.get_latest_month_with_complete_usage_data.return_value = (2025, 3)
        
        with patch.object(self.processor, 'process_month', return_value=False):
            result = self.processor.process_next_month(
                start_month=(2025, 1)
            )
        
        self.assertFalse(result)
        self.mock_state_manager.update_last_processed_month.assert_not_called()


if __name__ == '__main__':
    unittest.main()
