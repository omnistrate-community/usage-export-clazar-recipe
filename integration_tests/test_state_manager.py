#!/usr/bin/env python3
"""
Integration tests for StateManager.

This test suite validates StateManager operations with real AWS S3 backend.
Configuration is loaded from environment variables via the Config class.

Required environment variables:
    AWS_ACCESS_KEY_ID: AWS access key for S3
    AWS_SECRET_ACCESS_KEY: AWS secret key for S3
    AWS_REGION: AWS region for S3 (e.g., 'us-west-2')
    AWS_S3_BUCKET_NAME: S3 bucket name for state storage
    SERVICE_NAME: Service name for state tracking
    ENVIRONMENT_TYPE: Environment type (e.g., 'PROD', 'DEV')
    PLAN_ID: Plan ID for service configuration
    DIMENSION1_NAME: First dimension name
    DIMENSION1_FORMULA: First dimension formula
    CLAZAR_CLIENT_ID: Clazar client ID
    CLAZAR_CLIENT_SECRET: Clazar client secret
"""

import os
import sys
import logging
import unittest
import uuid
from datetime import datetime, timezone

# Add src directory to path to import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from state_manager import StateManager, StateManagerError
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestStateManagerIntegration(unittest.TestCase):
    """Integration test suite for StateManager with real S3 backend."""
    
    def setUp(self):
        """Set up test fixtures."""
        logger.info("=" * 60)
        logger.info(f"Running test: {self._testMethodName}")
        logger.info("=" * 60)
        
        # Load configuration - fail test if config cannot be loaded
        try:
            self.config = Config()
            
            # Set test-specific values if not already configured
            if not self.config.service_name:
                self.config.service_name = "Test service"
            if not self.config.environment_type:
                self.config.environment_type = "TEST"
            if not self.config.plan_id:
                self.config.plan_id = "pt-xxxxxxxx"
            
            logger.info("✓ Configuration loaded successfully")
            logger.info(f"  S3 Bucket: {self.config.aws_s3_bucket}")
            logger.info(f"  Service: {self.config.service_name}")
            logger.info(f"  Environment: {self.config.environment_type}")
            logger.info(f"  Plan ID: {self.config.plan_id}")
        except Exception as e:
            logger.error(f"✗ Failed to load configuration: {e}")
            self.fail(f"Configuration loading failed: {e}")
        
        # Initialize StateManager
        try:
            self.state_manager = StateManager(self.config)
            logger.info("✓ StateManager initialized successfully")
        except Exception as e:
            logger.error(f"✗ Failed to initialize StateManager: {e}")
            self.fail(f"StateManager initialization failed: {e}")
        
        # Generate unique test identifiers to avoid conflicts
        self.test_service = f"test-service-{uuid.uuid4().hex[:8]}"
        self.test_env = "TEST"
        self.test_plan = f"plan-{uuid.uuid4().hex[:8]}"
        self.test_contract = f"contract-{uuid.uuid4().hex[:8]}"
        
        logger.info(f"Test identifiers:")
        logger.info(f"  Service: {self.test_service}")
        logger.info(f"  Environment: {self.test_env}")
        logger.info(f"  Plan: {self.test_plan}")
        logger.info(f"  Contract: {self.test_contract}")
    
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
    
    def test_state_manager_initialization(self):
        """Test StateManager can be initialized with valid config."""
        self.assertIsNotNone(self.state_manager, "StateManager should be initialized")
        self.assertEqual(
            self.state_manager.aws_s3_bucket,
            self.config.aws_s3_bucket,
            "StateManager should use config S3 bucket"
        )
        self.assertIsNotNone(
            self.state_manager.s3_client,
            "StateManager should have S3 client"
        )
        self.assertIsNotNone(
            self.state_manager.metering_reader,
            "StateManager should have metering reader"
        )
        logger.info("✓ StateManager initialization validated")
    
    def test_validate_access(self):
        """Test that StateManager can validate S3 access."""
        logger.info("Testing S3 access validation...")
        
        try:
            self.state_manager.validate_access()
            logger.info("✓ S3 access validated successfully")
        except StateManagerError as e:
            self.fail(f"S3 access validation failed: {e}")
    
    def test_load_and_save_state(self):
        """Test loading and saving state to S3."""
        logger.info("Testing state load and save operations...")
        
        # Load initial state
        initial_state = self.state_manager.load_state()
        self.assertIsInstance(initial_state, dict, "State should be a dictionary")
        logger.info(f"✓ Loaded initial state with {len(initial_state)} services")
        
        # Add test data to state
        test_key = f"test-{uuid.uuid4().hex[:8]}"
        initial_state[test_key] = {
            "test_field": "test_value",
            "timestamp": datetime.now(timezone.utc).isoformat() + 'Z'
        }
        
        # Save modified state
        try:
            self.state_manager.save_state(initial_state)
            logger.info("✓ State saved successfully")
        except Exception as e:
            self.fail(f"Failed to save state: {e}")
        
        # Reload state and verify changes persisted
        reloaded_state = self.state_manager.load_state()
        self.assertIn(test_key, reloaded_state, "Test key should be in reloaded state")
        self.assertEqual(
            reloaded_state[test_key]["test_field"],
            "test_value",
            "Test value should match"
        )
        logger.info("✓ State persistence verified")
        
        # Clean up test data
        del reloaded_state[test_key]
        self.state_manager.save_state(reloaded_state)
        logger.info("✓ Test data cleaned up")
    
    def test_service_key_generation(self):
        """Test generating unique service keys."""
        service_key = self.state_manager.get_service_key(
            self.test_service,
            self.test_env,
            self.test_plan
        )
        
        expected_key = f"{self.test_service}:{self.test_env}:{self.test_plan}"
        self.assertEqual(service_key, expected_key, "Service key format should match")
        logger.info(f"✓ Service key generated: {service_key}")
    
    def test_month_key_generation(self):
        """Test generating month keys."""
        month_key = self.state_manager.get_month_key(2024, 11)
        self.assertEqual(month_key, "2024-11", "Month key format should be YYYY-MM")
        
        month_key_padded = self.state_manager.get_month_key(2024, 3)
        self.assertEqual(month_key_padded, "2024-03", "Month should be zero-padded")
        logger.info("✓ Month key generation validated")
    
    def test_mark_and_check_contract_processed(self):
        """Test marking a contract as processed and checking its status."""
        logger.info("Testing contract processing status tracking...")
        
        year = 2024
        month = 11
        
        # Check initial status (should be False)
        is_processed = self.state_manager.is_contract_month_processed(
            self.test_service,
            self.test_env,
            self.test_plan,
            self.test_contract,
            year,
            month
        )
        self.assertFalse(is_processed, "Contract should not be processed initially")
        logger.info("✓ Initial status verified (not processed)")
        
        # Mark contract as processed
        self.state_manager.mark_contract_month_processed(
            self.test_service,
            self.test_env,
            self.test_plan,
            self.test_contract,
            year,
            month
        )
        logger.info("✓ Contract marked as processed")
        
        # Check status again (should be True)
        is_processed = self.state_manager.is_contract_month_processed(
            self.test_service,
            self.test_env,
            self.test_plan,
            self.test_contract,
            year,
            month
        )
        self.assertTrue(is_processed, "Contract should be marked as processed")
        logger.info("✓ Processed status verified")
        
        # Clean up
        self._cleanup_test_state()
    
    def test_mark_and_retrieve_error_contracts(self):
        """Test marking contracts with errors and retrieving them for retry."""
        logger.info("Testing error contract tracking...")
        
        year = 2024
        month = 11
        error_messages = ["API error", "Connection timeout"]
        error_code = "500"
        error_message = "Internal Server Error"
        test_payload = {"test": "data"}
        
        # Mark contract with error
        self.state_manager.mark_contract_month_error(
            self.test_service,
            self.test_env,
            self.test_plan,
            self.test_contract,
            year,
            month,
            error_messages,
            code=error_code,
            message=error_message,
            payload=test_payload,
            retry_count=0
        )
        logger.info("✓ Contract marked with error")
        
        # Verify contract is marked as processed (with error)
        is_processed = self.state_manager.is_contract_month_processed(
            self.test_service,
            self.test_env,
            self.test_plan,
            self.test_contract,
            year,
            month
        )
        self.assertTrue(is_processed, "Contract with error should be marked as processed")
        logger.info("✓ Error contract shows as processed")
        
        # Retrieve error contracts for retry
        retry_contracts = self.state_manager.get_error_contracts_for_retry(
            self.test_service,
            self.test_env,
            self.test_plan,
            year,
            month,
            max_retries=5
        )
        
        self.assertEqual(len(retry_contracts), 1, "Should have one error contract")
        self.assertEqual(
            retry_contracts[0]["contract_id"],
            self.test_contract,
            "Contract ID should match"
        )
        self.assertEqual(
            retry_contracts[0]["code"],
            error_code,
            "Error code should match"
        )
        self.assertIn("API error", retry_contracts[0]["errors"], "Error message should be present")
        logger.info(f"✓ Retrieved error contract for retry: {retry_contracts[0]['contract_id']}")
        
        # Clean up
        self._cleanup_test_state()
    
    def test_remove_error_contract(self):
        """Test removing a contract from error list after successful retry."""
        logger.info("Testing error contract removal...")
        
        year = 2024
        month = 11
        
        # Mark contract with error
        self.state_manager.mark_contract_month_error(
            self.test_service,
            self.test_env,
            self.test_plan,
            self.test_contract,
            year,
            month,
            ["Test error"],
            retry_count=0
        )
        logger.info("✓ Contract marked with error")
        
        # Verify error exists
        retry_contracts = self.state_manager.get_error_contracts_for_retry(
            self.test_service,
            self.test_env,
            self.test_plan,
            year,
            month
        )
        self.assertEqual(len(retry_contracts), 1, "Should have one error contract")
        
        # Remove error contract
        self.state_manager.remove_error_contract(
            self.test_service,
            self.test_env,
            self.test_plan,
            self.test_contract,
            year,
            month
        )
        logger.info("✓ Error contract removed")
        
        # Verify error is removed
        retry_contracts = self.state_manager.get_error_contracts_for_retry(
            self.test_service,
            self.test_env,
            self.test_plan,
            year,
            month
        )
        self.assertEqual(len(retry_contracts), 0, "Should have no error contracts")
        logger.info("✓ Error contract removal verified")
        
        # Clean up
        self._cleanup_test_state()
    
    def test_last_processed_month_tracking(self):
        """Test tracking the last processed month."""
        logger.info("Testing last processed month tracking...")
        
        # Check initial last processed month (should be None)
        last_month = self.state_manager.get_last_processed_month(
            self.test_service,
            self.test_env,
            self.test_plan
        )
        self.assertIsNone(last_month, "Last processed month should be None initially")
        logger.info("✓ Initial state verified (no last processed month)")
        
        # Update last processed month
        year = 2024
        month = 10
        self.state_manager.update_last_processed_month(
            self.test_service,
            self.test_env,
            self.test_plan,
            year,
            month
        )
        logger.info(f"✓ Updated last processed month to {year}-{month:02d}")
        
        # Verify last processed month
        last_month = self.state_manager.get_last_processed_month(
            self.test_service,
            self.test_env,
            self.test_plan
        )
        self.assertIsNotNone(last_month, "Last processed month should be set")
        self.assertEqual(last_month, (year, month), "Last processed month should match")
        logger.info(f"✓ Last processed month verified: {last_month}")
        
        # Update to a different month
        new_year = 2024
        new_month = 11
        self.state_manager.update_last_processed_month(
            self.test_service,
            self.test_env,
            self.test_plan,
            new_year,
            new_month
        )
        logger.info(f"✓ Updated last processed month to {new_year}-{new_month:02d}")
        
        # Verify update
        last_month = self.state_manager.get_last_processed_month(
            self.test_service,
            self.test_env,
            self.test_plan
        )
        self.assertEqual(last_month, (new_year, new_month), "Last processed month should be updated")
        logger.info(f"✓ Last processed month update verified: {last_month}")
        
        # Clean up
        self._cleanup_test_state()
    
    def test_max_retries_limit(self):
        """Test that error contracts exceeding max retries are not returned."""
        logger.info("Testing max retries limit...")
        
        year = 2024
        month = 11
        max_retries = 3
        
        # Mark contract with error at max retries
        self.state_manager.mark_contract_month_error(
            self.test_service,
            self.test_env,
            self.test_plan,
            self.test_contract,
            year,
            month,
            ["Max retries reached"],
            retry_count=max_retries
        )
        logger.info(f"✓ Contract marked with error at retry count {max_retries}")
        
        # Try to retrieve for retry
        retry_contracts = self.state_manager.get_error_contracts_for_retry(
            self.test_service,
            self.test_env,
            self.test_plan,
            year,
            month,
            max_retries=max_retries
        )
        
        self.assertEqual(
            len(retry_contracts),
            0,
            "Contract at max retries should not be returned"
        )
        logger.info("✓ Max retries limit enforced correctly")
        
        # Clean up
        self._cleanup_test_state()
    
    def test_multiple_contracts_same_month(self):
        """Test tracking multiple contracts for the same month."""
        logger.info("Testing multiple contracts for same month...")
        
        year = 2024
        month = 11
        contract1 = f"contract-{uuid.uuid4().hex[:8]}"
        contract2 = f"contract-{uuid.uuid4().hex[:8]}"
        contract3 = f"contract-{uuid.uuid4().hex[:8]}"
        
        # Mark first contract as processed
        self.state_manager.mark_contract_month_processed(
            self.test_service,
            self.test_env,
            self.test_plan,
            contract1,
            year,
            month
        )
        
        # Mark second contract with error
        self.state_manager.mark_contract_month_error(
            self.test_service,
            self.test_env,
            self.test_plan,
            contract2,
            year,
            month,
            ["Error for contract 2"],
            retry_count=0
        )
        
        # Mark third contract as processed
        self.state_manager.mark_contract_month_processed(
            self.test_service,
            self.test_env,
            self.test_plan,
            contract3,
            year,
            month
        )
        
        logger.info("✓ Marked 3 contracts (2 success, 1 error)")
        
        # Verify all contracts are tracked
        self.assertTrue(
            self.state_manager.is_contract_month_processed(
                self.test_service, self.test_env, self.test_plan, contract1, year, month
            ),
            "Contract 1 should be processed"
        )
        self.assertTrue(
            self.state_manager.is_contract_month_processed(
                self.test_service, self.test_env, self.test_plan, contract2, year, month
            ),
            "Contract 2 should be processed (with error)"
        )
        self.assertTrue(
            self.state_manager.is_contract_month_processed(
                self.test_service, self.test_env, self.test_plan, contract3, year, month
            ),
            "Contract 3 should be processed"
        )
        
        # Verify error contract can be retrieved
        retry_contracts = self.state_manager.get_error_contracts_for_retry(
            self.test_service,
            self.test_env,
            self.test_plan,
            year,
            month
        )
        self.assertEqual(len(retry_contracts), 1, "Should have one error contract")
        self.assertEqual(
            retry_contracts[0]["contract_id"],
            contract2,
            "Error contract should be contract2"
        )
        
        logger.info("✓ Multiple contracts tracked correctly")
        
        # Clean up
        self._cleanup_test_state()
    
    def _cleanup_test_state(self):
        """Clean up test data from state file."""
        try:
            state = self.state_manager.load_state()
            service_key = self.state_manager.get_service_key(
                self.test_service,
                self.test_env,
                self.test_plan
            )
            
            if service_key in state:
                del state[service_key]
                self.state_manager.save_state(state)
                logger.info("✓ Test state cleaned up")
        except Exception as e:
            logger.warning(f"Failed to clean up test state: {e}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
