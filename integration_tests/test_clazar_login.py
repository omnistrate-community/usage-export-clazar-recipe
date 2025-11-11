#!/usr/bin/env python3
"""
Smoke test for Clazar authentication.

This test reads Clazar API credentials from environment variables and
validates that we can successfully authenticate and receive a valid token.

Required environment variables:
    CLAZAR_CLIENT_ID: Clazar client ID
    CLAZAR_CLIENT_SECRET: Clazar client secret
"""

import os
import sys
import logging
import unittest

# Add src directory to path to import clazar_client module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from clazar_client import ClazarClient, ClazarAPIError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestClazarAuthentication(unittest.TestCase):
    """Test suite for Clazar authentication."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Read credentials from environment variables
        self.client_id = os.environ.get('CLAZAR_CLIENT_ID')
        self.client_secret = os.environ.get('CLAZAR_CLIENT_SECRET')
        
        logger.info("=" * 60)
        logger.info(f"Running test: {self._testMethodName}")
        logger.info("=" * 60)
    
    def test_environment_variables_set(self):
        """Test that required environment variables are set."""
        self.assertIsNotNone(
            self.client_id,
            "CLAZAR_CLIENT_ID environment variable is not set"
        )
        self.assertIsNotNone(
            self.client_secret,
            "CLAZAR_CLIENT_SECRET environment variable is not set"
        )
        logger.info("✓ Environment variables are set")
    
    def test_clazar_authentication(self):
        """Test Clazar authentication with real API credentials."""
        # Skip if credentials not available
        if not self.client_id or not self.client_secret:
            self.skipTest("CLAZAR_CLIENT_ID or CLAZAR_CLIENT_SECRET not set")
        
        logger.info("Environment variables loaded successfully")
        logger.info(f"Client ID: {self.client_id[:8]}..." if len(self.client_id) > 8 else "Client ID: [set]")
        
        # Create client and authenticate
        logger.info("Initializing Clazar client...")
        client = ClazarClient(client_id=self.client_id, client_secret=self.client_secret)
        
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
        
        # Create client with invalid credentials
        client = ClazarClient(client_id="invalid_id", client_secret="invalid_secret")
        
        # Expect authentication to raise ClazarAPIError
        with self.assertRaises(ClazarAPIError) as context:
            client.authenticate()
        
        logger.info(f"✓ Authentication correctly failed with invalid credentials")
        logger.info(f"  Error message: {context.exception.message}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
