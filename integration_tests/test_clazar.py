#!/usr/bin/env python3
"""
Integration tests for Clazar API client.

This test suite validates Clazar authentication and metering data submission.
Configuration is loaded from environment variables via the Config class.

Required environment variables:
    CLAZAR_CLIENT_ID: Clazar client ID
    CLAZAR_CLIENT_SECRET: Clazar client secret
    CLAZAR_CONTRACT_ID: Clazar contract ID (optional, will generate UUID if not set)
    CLAZAR_CLOUD: Cloud provider (default: 'aws')
    DIMENSION1_NAME: First dimension name
    DIMENSION1_FORMULA: First dimension formula
"""

import os
import sys
import logging
import unittest
import uuid
from datetime import datetime, timedelta, timezone

# Add src directory to path to import clazar_client module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from clazar_client import ClazarClient, ClazarAPIError
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestClazarIntegration(unittest.TestCase):
    """Test suite for Clazar integration and API operations."""
    
    def setUp(self):
        """Set up test fixtures."""
        logger.info("=" * 60)
        logger.info(f"Running test: {self._testMethodName}")
        logger.info("=" * 60)
        
        # Load configuration - fail test if config cannot be loaded
        try:
            self.config = Config()
            self.client_id = self.config.clazar_client_id
            self.client_secret = self.config.clazar_client_secret
            logger.info("✓ Configuration loaded successfully")
        except Exception as e:
            logger.error(f"✗ Failed to load configuration: {e}")
            self.fail(f"Configuration loading failed: {e}")
    
    def test_config_loaded(self):
        """Test that configuration is loaded successfully."""
        self.assertIsNotNone(self.config, "Config object should be loaded")
        self.assertIsNotNone(
            self.client_id,
            "CLAZAR_CLIENT_ID must be set in config"
        )
        self.assertIsNotNone(
            self.client_secret,
            "CLAZAR_CLIENT_SECRET must be set in config"
        )
        self.assertIsNotNone(
            self.config.clazar_cloud,
            "CLAZAR_CLOUD must be set in config"
        )
        logger.info("✓ Configuration validated successfully")
        logger.info(f"  Clazar Cloud: {self.config.clazar_cloud}")
        logger.info(f"  Custom Dimensions: {list(self.config.custom_dimensions.keys())}")
    
    def test_clazar_authentication(self):
        """Test Clazar authentication with real API credentials from Config."""
        logger.info(f"Client ID: {self.client_id[:8]}..." if len(self.client_id) > 8 else "Client ID: [set]")
        logger.info(f"Clazar Cloud: {self.config.clazar_cloud}")
        
        # Create client and authenticate
        logger.info("Initializing Clazar client...")
        client = ClazarClient(config=self.config)
        
        logger.info("Attempting to authenticate...")
        token = client.authenticate()
        
        # Validate token
        self.assertIsNotNone(token, "Authentication failed: No token received")
        self.assertIsInstance(token, str, f"Invalid token type: {type(token)}")
        self.assertGreater(len(token), 0, "Empty token received")
        
        # Log success (mask the token for security)
        token_preview = token[:10] + "..." if len(token) > 10 else "[token]"
        logger.info(f"✓ Authentication successful! Token received: {token_preview}")
        logger.info(f"✓ Token length: {len(token)} characters")
    
    def test_authentication_with_invalid_credentials(self):
        """Test that authentication fails with invalid credentials."""
        logger.info("Testing authentication with invalid credentials...")
        
        # Create a config with invalid credentials
        invalid_config = Config()
        invalid_config.clazar_client_id = "invalid_id"
        invalid_config.clazar_client_secret = "invalid_secret"
        
        # Create client with invalid credentials
        client = ClazarClient(config=invalid_config)
        
        # Expect authentication to raise ClazarAPIError
        with self.assertRaises(ClazarAPIError) as context:
            client.authenticate()
        
        logger.info(f"✓ Authentication correctly failed with invalid credentials")
        logger.info(f"  Error message: {context.exception.message}")
    
    def test_send_metering_data_to_clazar(self):
        """Test sending a single metering record to Clazar using Config parameters."""
        logger.info("Testing sending metering data to Clazar...")
        
        # Create client and authenticate
        logger.info("Initializing Clazar client...")
        client = ClazarClient(config=self.config)
        
        logger.info("Authenticating...")
        client.authenticate()
        logger.info("✓ Authentication successful")
        
        # Get the first custom dimension name from config
        dimension_name = list(self.config.custom_dimensions.keys())[0]
        logger.info(f"Using dimension: {dimension_name}")
        
        # Calculate start_time and end_time for the last second
        now = datetime.now(timezone.utc)
        end_time = now.replace(microsecond=0)  # Current second
        start_time = end_time - timedelta(seconds=1)  # One second ago
        
        # Create metering record using parameters from config
        # Format matches Clazar API specification (simplified structure)
        metering_record = {
            "cloud": self.config.clazar_cloud,
            "contract_id": os.environ.get('CLAZAR_CONTRACT_ID', str(uuid.uuid4())),
            "dimension": dimension_name,
            "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "quantity": "1",
            "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        logger.info(f"Sending metering record:")
        logger.info(f"  Cloud: {metering_record['cloud']}")
        logger.info(f"  Contract ID: {metering_record['contract_id']}")
        logger.info(f"  Dimension: {metering_record['dimension']}")
        logger.info(f"  Quantity: {metering_record['quantity']}")
        logger.info(f"  Start time: {metering_record['start_time']}")
        logger.info(f"  End time: {metering_record['end_time']}")
        
        # Send the metering data
        response = client.send_metering_data([metering_record])
        
        # Validate response
        self.assertIsNotNone(response, "No response received from Clazar")
        self.assertIn("results", response, "Response missing 'results' field")
        
        # Check for errors
        has_errors, errors, error_code, error_message, warnings = client.check_response_for_errors(response)
        duplicate_violation = False

        if warnings:
            logger.warning(f"Clazar returned warnings: {warnings}")

        if has_errors:
            error_messages = [
                err.get("message") if isinstance(err, dict) else str(err)
                for err in errors
            ]
            duplicate_violation = any(
                msg and "same contract_id" in msg and "same hour" in msg
                for msg in error_messages
            )
            if duplicate_violation:
                logger.warning(
                    "Clazar rejected duplicate metering record for same contract/dimension within an hour; treating as expected during reruns."
                )
            else:
                logger.error("✗ Clazar API returned errors:")
                logger.error(f"  Error code: {error_code}")
                logger.error(f"  Error message: {error_message}")
                logger.error(f"  Errors: {errors}")
                self.fail(f"Clazar API returned errors: {error_message}")
        else:
            logger.info("✓ Metering data sent successfully")
            logger.info(f"  Response: {response}")

        if duplicate_violation:
            logger.info("✓ Duplicate metering record gracefully ignored by Clazar")
            logger.info(f"  Error message: {error_message}")
    
    def test_send_metering_data_with_invalid_dimension(self):
        """Test that sending metering data with invalid dimension returns an error."""
        logger.info("Testing metering data with invalid dimension name...")
        
        # Create client and authenticate
        logger.info("Initializing Clazar client...")
        client = ClazarClient(config=self.config)
        
        logger.info("Authenticating...")
        client.authenticate()
        logger.info("✓ Authentication successful")
        
        # Calculate start_time and end_time for the last second
        now = datetime.now(timezone.utc)
        end_time = now.replace(microsecond=0)  # Current second
        start_time = end_time - timedelta(seconds=1)  # One second ago
        
        # Create metering record with invalid dimension name
        invalid_dimension = "InvalidDimensionThatDoesNotExist_" + str(uuid.uuid4())[:8]
        metering_record = {
            "cloud": self.config.clazar_cloud,
            "contract_id": os.environ.get('CLAZAR_CONTRACT_ID', str(uuid.uuid4())),
            "dimension": invalid_dimension,
            "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "quantity": "1",
            "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        logger.info(f"Sending metering record with invalid dimension:")
        logger.info(f"  Cloud: {metering_record['cloud']}")
        logger.info(f"  Contract ID: {metering_record['contract_id']}")
        logger.info(f"  Invalid Dimension: {metering_record['dimension']}")
        logger.info(f"  Quantity: {metering_record['quantity']}")
        logger.info(f"  Start time: {metering_record['start_time']}")
        logger.info(f"  End time: {metering_record['end_time']}")
        
        # Send the metering data
        response = client.send_metering_data([metering_record])
        
        # Validate response exists
        self.assertIsNotNone(response, "No response received from Clazar")
        self.assertIn("results", response, "Response missing 'results' field")
        
        # Check for errors - we expect warnings for invalid dimension
        has_errors, errors, error_code, error_message, warnings = client.check_response_for_errors(response)
        
        self.assertFalse(has_errors, "Invalid dimension response should produce warnings, not errors")
        self.assertEqual(len(errors), 0, "Did not expect errors for invalid dimension test")
        self.assertGreater(len(warnings), 0, "Expected warning entries for invalid dimension")
        non_success_warnings = [
            warning for warning in warnings
            if warning.get("status", "success").lower() != "success"
        ]
        self.assertGreater(len(non_success_warnings), 0, "Expected at least one non-success warning entry")
        for warning in non_success_warnings:
            self.assertEqual(
                warning.get("dimension"),
                invalid_dimension,
                "Warning response did not reference the invalid dimension"
            )
        
        logger.info("✓ Invalid dimension correctly rejected by Clazar with warnings")
        logger.info(f"  Warnings: {warnings}")
    
    def test_send_metering_data_with_invalid_contract_id(self):
        """Test that sending metering data with invalid contract ID returns an error."""
        logger.info("Testing metering data with invalid contract ID...")
        
        # Create client and authenticate
        logger.info("Initializing Clazar client...")
        client = ClazarClient(config=self.config)
        
        logger.info("Authenticating...")
        client.authenticate()
        logger.info("✓ Authentication successful")
        
        # Get the first custom dimension name from config
        dimension_name = list(self.config.custom_dimensions.keys())[0]
        logger.info(f"Using dimension: {dimension_name}")
        
        # Calculate start_time and end_time for the last second
        now = datetime.now(timezone.utc)
        end_time = now.replace(microsecond=0)  # Current second
        start_time = end_time - timedelta(seconds=1)  # One second ago
        
        # Create metering record with invalid contract ID
        invalid_contract_id = "invalid-contract-" + str(uuid.uuid4())
        metering_record = {
            "cloud": self.config.clazar_cloud,
            "contract_id": invalid_contract_id,
            "dimension": dimension_name,
            "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "quantity": "1",
            "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        logger.info(f"Sending metering record with invalid contract ID:")
        logger.info(f"  Cloud: {metering_record['cloud']}")
        logger.info(f"  Invalid Contract ID: {metering_record['contract_id']}")
        logger.info(f"  Dimension: {metering_record['dimension']}")
        logger.info(f"  Quantity: {metering_record['quantity']}")
        logger.info(f"  Start time: {metering_record['start_time']}")
        logger.info(f"  End time: {metering_record['end_time']}")
        
        # Send the metering data
        response = client.send_metering_data([metering_record])
        
        # Validate response exists
        self.assertIsNotNone(response, "No response received from Clazar")
        self.assertIn("results", response, "Response missing 'results' field")
        
        # Check for errors - we expect errors for invalid contract ID
        has_errors, errors, error_code, error_message, warnings = client.check_response_for_errors(response)
        
        # Assert that we got an error as expected
        self.assertTrue(has_errors, "Expected error for invalid contract ID but none was returned")
        self.assertGreater(len(errors), 0, "Expected error list to be non-empty")
        
        logger.info("✓ Invalid contract ID correctly rejected by Clazar")
        logger.info(f"  Error code: {error_code}")
        logger.info(f"  Error message: {error_message}")
        logger.info(f"  Errors: {errors}")
        if warnings:
            logger.warning(f"  Unexpected warnings: {warnings}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
