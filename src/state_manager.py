#!/usr/bin/env python3
"""
State management for the metering processor.

This module handles reading, writing, and validating state stored in S3.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from config import Config
from omnistrate_metering_reader import OmnistrateMeteringReader

class StateManagerError(Exception):
    """Exception raised for state management errors."""
    pass


class StateManager:
    """Manages state persistence in S3 for metering processing."""
    
    def __init__(self, config : Config):
        """
        Initialize the state manager.
        
        Args:
            config: Configuration object containing AWS credentials
        """
        if not config:
            raise StateManagerError("Configuration object is required to initialize StateManager.")
        if not config.aws_s3_bucket:
            raise StateManagerError("AWS S3 bucket name is not configured.")
        if not config.environment_type:
            raise StateManagerError("Environment type is not configured.")
        if not config.plan_id:
            raise StateManagerError("Plan ID is not configured.")
        if not config.aws_access_key_id:
            raise StateManagerError("AWS Access Key ID is not configured.")
        if not config.aws_secret_access_key:
            raise StateManagerError("AWS Secret Access Key is not configured.")
        if not config.aws_region:
            raise StateManagerError("AWS region is not configured.")

        self.aws_s3_bucket = config.aws_s3_bucket
        self.file_path = f"clazar/{config.service_name}-{config.environment_type}-{config.plan_id}-export_state.json"
        
        # Configure AWS credentials and create S3 client
        s3_kwargs = {}
        if config.aws_access_key_id:
            s3_kwargs['aws_access_key_id'] = config.aws_access_key_id
        if config.aws_secret_access_key:
            s3_kwargs['aws_secret_access_key'] = config.aws_secret_access_key
        if config.aws_region:
            s3_kwargs['region_name'] = config.aws_region
        
        self.s3_client = boto3.client('s3', **s3_kwargs)
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        
        # Initialize OmnistrateMeteringReader for reading usage data
        self.metering_reader = OmnistrateMeteringReader(config)

        # Set maximum retries for error handling
        self.max_retries = 5
        
        self.logger.info(f"StateManager initialized for s3://{self.aws_s3_bucket}/{self.file_path}")
    
    def validate_access(self):
        """
        Validate that we can read and write to the state file in S3.
        
        Raises:
            StateManagerError: If validation fails
        """
        try:
            # Try to read existing state or initialize new one
            try:
                self.load_state()
                self.logger.info("Successfully validated read access to state file")
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    self.logger.info("State file does not exist, will create on first write")
                else:
                    raise StateManagerError(f"Failed to read state file: {e}")
            
            # Try to write a test state
            test_state = self.load_state()
            test_state['_validation_test'] = {
                'timestamp': datetime.now(timezone.utc).isoformat() + 'Z',
                'status': 'validated'
            }
            self.save_state(test_state)
            self.logger.info("Successfully validated write access to state file")
            
            # Clean up test entry
            del test_state['_validation_test']
            self.save_state(test_state)
            
        except Exception as e:
            raise StateManagerError(f"State validation failed: {e}")
    
    def load_state(self) -> Dict:
        """
        Load the processing state from the S3 state file.
        
        Returns:
            Dictionary containing the state information
        """
        try:
            response = self.s3_client.get_object(Bucket=self.aws_s3_bucket, Key=self.file_path)
            content = response['Body'].read().decode('utf-8')
            state = json.loads(content)
            self.logger.debug(f"Loaded state from S3: s3://{self.aws_s3_bucket}/{self.file_path}")
            return state
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                self.logger.info(f"State file not found in S3 at s3://{self.aws_s3_bucket}/{self.file_path}, initializing with default state")
                return {}
            else:
                self.logger.error(f"Error loading state file from S3: {e}")
                raise
        except (json.JSONDecodeError, IOError) as e:
            self.logger.error(f"Error parsing state file: {e}")
            return {}

    def save_state(self, state: Dict):
        """
        Save the processing state to the S3 state file.
        
        Args:
            state: Dictionary containing the state information
        """
        try:
            state_content = json.dumps(state, indent=2)
            self.s3_client.put_object(
                Bucket=self.aws_s3_bucket,
                Key=self.file_path,
                Body=state_content,
                ContentType='application/json'
            )
            self.logger.debug(f"Saved state to S3: s3://{self.aws_s3_bucket}/{self.file_path}")
        except ClientError as e:
            self.logger.error(f"Error saving state file to S3: {e}")
            raise

    def get_month_key(self, year: int, month: int) -> str:
        """
        Generate a unique key for a month.
        
        Args:
            year: Year
            month: Month
            
        Returns:
            Month key in format YYYY-MM
        """
        return f"{year:04d}-{month:02d}"

    def is_contract_month_processed(self, contract_id: str, year: int, month: int) -> bool:
        """
        Check if a specific contract for a month has been processed (either successfully or with errors).
        
        Args:
            contract_id: Contract ID (external payer ID)
            year: Year
            month: Month
            
        Returns:
            True if contract-month has been processed (successfully or with errors), False otherwise
        """
        state = self.load_state()
        month_key = self.get_month_key(year, month)
        
        # Check if in processed contracts (successful)
        if 'success_contracts' in state:
            if month_key in state['success_contracts']:
                if contract_id in state['success_contracts'][month_key]:
                    return True
        
        # Check if in error contracts (failed but recorded)
        if 'error_contracts' in state:
            if month_key in state['error_contracts']:
                for error_entry in state['error_contracts'][month_key]:
                    if error_entry.get('contract_id') == contract_id:
                        return True
        
        return False

    def mark_contract_month_processed(self, contract_id: str, year: int, month: int):
        """
        Mark a specific contract for a month as processed (successfully).
        
        Args:
            contract_id: Contract ID (external payer ID)
            year: Year
            month: Month
        """
        state = self.load_state()
        month_key = self.get_month_key(year, month)
        
        if 'success_contracts' not in state:
            state['success_contracts'] = {}
        
        if month_key not in state['success_contracts']:
            state['success_contracts'][month_key] = []
        
        if contract_id not in state['success_contracts'][month_key]:
            state['success_contracts'][month_key].append(contract_id)
        
        state['last_updated'] = datetime.now(timezone.utc).isoformat() + 'Z'
        self.save_state(state)

    def mark_contract_month_error(self, contract_id: str, year: int, month: int,
                                 errors: List[str], code: str = None, message: str = None,
                                 payload: Dict = None):
        """
        Mark a specific contract for a month as having errors.
        
        Args:
            contract_id: Contract ID (external payer ID)
            year: Year
            month: Month
            errors: List of error messages
            code: Error code
            message: Error message
            payload: The payload that failed to be sent
        """
        state = self.load_state()
        month_key = self.get_month_key(year, month)
    
        if 'error_contracts' not in state:
            state['error_contracts'] = {}
        
        if month_key not in state['error_contracts']:
            state['error_contracts'][month_key] = []
        
        # Check if this contract already has an error entry for this month
        existing_error = None
        for error_entry in state['error_contracts'][month_key]:
            if error_entry.get('contract_id') == contract_id:
                existing_error = error_entry
                break
        
        if existing_error:
            # Update existing error entry
            existing_error['errors'].extend(errors)
            if code:
                existing_error['code'] = code
            if message:
                existing_error['message'] = message
            if payload:
                existing_error['payload'] = payload
            existing_error['retry_count'] = existing_error.get('retry_count', 0) + 1
            existing_error['last_retry_time'] = datetime.now(timezone.utc).isoformat() + 'Z'
        else:
            # Create new error entry
            error_entry = {
                "contract_id": contract_id,
                "errors": errors,
                "retry_count": 1,
                "last_retry_time": datetime.now(timezone.utc).isoformat() + 'Z'
            }
            if code:
                error_entry["code"] = code
            if message:
                error_entry["message"] = message
            if payload:
                error_entry["payload"] = payload
            
            state['error_contracts'][month_key].append(error_entry)
        
        state['last_updated'] = datetime.now(timezone.utc).isoformat() + 'Z'
        self.save_state(state)

    def get_error_contracts_for_retry(self, year: int, month: int) -> List[Dict]:
        """
        Get error contracts that can be retried for a specific month.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            year: Year
            month: Month
            
        Returns:
            List of error contract entries that can be retried
        """
        state = self.load_state()
        month_key = self.get_month_key(year, month)
        
        if ('error_contracts' not in state or 
            month_key not in state['error_contracts']):
            return []
        
        retry_contracts = []
        for error_entry in state['error_contracts'][month_key]:
            retry_count = error_entry.get('retry_count', 0)
            if retry_count < self.max_retries:
                retry_contracts.append(error_entry)
        
        return retry_contracts

    def remove_error_contract(self, contract_id: str, year: int, month: int):
        """
        Remove a contract from error contracts (when it succeeds on retry).
        
        Args:
            contract_id: Contract ID
            year: Year
            month: Month
        """
        state = self.load_state()
        month_key = self.get_month_key(year, month)
        
        if ('error_contracts' in state and 
            month_key in state['error_contracts']):
            
            # Remove the error entry for this contract
            state['error_contracts'][month_key] = [
                entry for entry in state['error_contracts'][month_key]
                if entry.get('contract_id') != contract_id
            ]
            
            # Clean up empty month entry
            if not state['error_contracts'][month_key]:
                del state['error_contracts'][month_key]
            
            state['last_updated'] = datetime.now(timezone.utc).isoformat() + 'Z'
            self.save_state(state)

    def get_last_processed_month(self) -> Optional[Tuple[int, int]]:
        """
        Get the last processed month for a specific service configuration.
            
        Returns:
            Tuple of (year, month) for last processed month, or None if never processed
        """
        state = self.load_state()
        
        try:
            last_processed_str = state.get('last_processed_month')
            if not last_processed_str:
                return None
            
            # Parse YYYY-MM format
            year, month = map(int, last_processed_str.split('-'))
            return (year, month)
        except (KeyError, ValueError) as e:
            self.logger.error(f"Error parsing last processed month for state in {self.file_path}: {e}")
            return None

    def update_last_processed_month(self, year: int, month: int):
        """
        Update the last processed month for a specific service configuration.
        
        Args:
            year: Year of the month that was processed
            month: Month that was processed
        """
        state = self.load_state()
        
        month_key = self.get_month_key(year, month)
        state['last_processed_month'] = month_key
        state['last_updated'] = datetime.now(timezone.utc).isoformat() + 'Z'

        self.save_state(state)
