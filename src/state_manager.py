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
        if not config.bucket_name:
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

        self.bucket_name = config.bucket_name
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
        
        self.logger.info(f"StateManager initialized for s3://{self.bucket_name}/{self.file_path}")
    
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
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.file_path)
            content = response['Body'].read().decode('utf-8')
            state = json.loads(content)
            self.logger.debug(f"Loaded state from S3: s3://{self.bucket_name}/{self.file_path}")
            return state
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                self.logger.info(f"State file not found in S3 at s3://{self.bucket_name}/{self.file_path}, initializing with default state")
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
                Bucket=self.bucket_name,
                Key=self.file_path,
                Body=state_content,
                ContentType='application/json'
            )
            self.logger.debug(f"Saved state to S3: s3://{self.bucket_name}/{self.file_path}")
        except ClientError as e:
            self.logger.error(f"Error saving state file to S3: {e}")
            raise

    def get_service_key(self, service_name: str, environment_type: str, plan_id: str) -> str:
        """
        Generate a unique key for a service configuration.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            
        Returns:
            Unique service key
        """
        return f"{service_name}:{environment_type}:{plan_id}"

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

    def is_contract_month_processed(self, service_name: str, environment_type: str, 
                                   plan_id: str, contract_id: str, year: int, month: int) -> bool:
        """
        Check if a specific contract for a month has been processed (either successfully or with errors).
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            contract_id: Contract ID (external payer ID)
            year: Year
            month: Month
            
        Returns:
            True if contract-month has been processed (successfully or with errors), False otherwise
        """
        state = self.load_state()
        service_key = self.get_service_key(service_name, environment_type, plan_id)
        month_key = self.get_month_key(year, month)
        
        if service_key not in state:
            return False
        
        # Check if in processed contracts (successful)
        if 'success_contracts' in state[service_key]:
            if month_key in state[service_key]['success_contracts']:
                if contract_id in state[service_key]['success_contracts'][month_key]:
                    return True
        
        # Check if in error contracts (failed but recorded)
        if 'error_contracts' in state[service_key]:
            if month_key in state[service_key]['error_contracts']:
                for error_entry in state[service_key]['error_contracts'][month_key]:
                    if error_entry.get('contract_id') == contract_id:
                        return True
        
        return False

    def mark_contract_month_processed(self, service_name: str, environment_type: str, 
                                     plan_id: str, contract_id: str, year: int, month: int):
        """
        Mark a specific contract for a month as processed (successfully).
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            contract_id: Contract ID (external payer ID)
            year: Year
            month: Month
        """
        state = self.load_state()
        service_key = self.get_service_key(service_name, environment_type, plan_id)
        month_key = self.get_month_key(year, month)
        
        if service_key not in state:
            state[service_key] = {}
        
        if 'success_contracts' not in state[service_key]:
            state[service_key]['success_contracts'] = {}
        
        if month_key not in state[service_key]['success_contracts']:
            state[service_key]['success_contracts'][month_key] = []
        
        if contract_id not in state[service_key]['success_contracts'][month_key]:
            state[service_key]['success_contracts'][month_key].append(contract_id)
        
        state[service_key]['last_updated'] = datetime.now(timezone.utc).isoformat() + 'Z'
        self.save_state(state)

    def mark_contract_month_error(self, service_name: str, environment_type: str, 
                                 plan_id: str, contract_id: str, year: int, month: int,
                                 errors: List[str], code: str = None, message: str = None,
                                 payload: Dict = None, retry_count: int = 5):
        """
        Mark a specific contract for a month as having errors.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            contract_id: Contract ID (external payer ID)
            year: Year
            month: Month
            errors: List of error messages
            code: Error code
            message: Error message
            payload: The payload that failed to be sent
            retry_count: Number of retries attempted
        """
        state = self.load_state()
        service_key = self.get_service_key(service_name, environment_type, plan_id)
        month_key = self.get_month_key(year, month)
        
        if service_key not in state:
            state[service_key] = {}
        
        if 'error_contracts' not in state[service_key]:
            state[service_key]['error_contracts'] = {}
        
        if month_key not in state[service_key]['error_contracts']:
            state[service_key]['error_contracts'][month_key] = []
        
        # Check if this contract already has an error entry for this month
        existing_error = None
        for error_entry in state[service_key]['error_contracts'][month_key]:
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
            existing_error['retry_count'] = retry_count
            existing_error['last_retry_time'] = datetime.now(timezone.utc).isoformat() + 'Z'
        else:
            # Create new error entry
            error_entry = {
                "contract_id": contract_id,
                "errors": errors,
                "retry_count": retry_count,
                "last_retry_time": datetime.now(timezone.utc).isoformat() + 'Z'
            }
            if code:
                error_entry["code"] = code
            if message:
                error_entry["message"] = message
            if payload:
                error_entry["payload"] = payload
            
            state[service_key]['error_contracts'][month_key].append(error_entry)
        
        state[service_key]['last_updated'] = datetime.now(timezone.utc).isoformat() + 'Z'
        self.save_state(state)

    def get_error_contracts_for_retry(self, service_name: str, environment_type: str, 
                                     plan_id: str, year: int, month: int, max_retries: int = 5) -> List[Dict]:
        """
        Get error contracts that can be retried for a specific month.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            year: Year
            month: Month
            max_retries: Maximum number of retry attempts
            
        Returns:
            List of error contract entries that can be retried
        """
        state = self.load_state()
        service_key = self.get_service_key(service_name, environment_type, plan_id)
        month_key = self.get_month_key(year, month)
        
        if (service_key not in state or 
            'error_contracts' not in state[service_key] or 
            month_key not in state[service_key]['error_contracts']):
            return []
        
        retry_contracts = []
        for error_entry in state[service_key]['error_contracts'][month_key]:
            retry_count = error_entry.get('retry_count', 0)
            if retry_count < max_retries:
                retry_contracts.append(error_entry)
        
        return retry_contracts

    def remove_error_contract(self, service_name: str, environment_type: str, 
                             plan_id: str, contract_id: str, year: int, month: int):
        """
        Remove a contract from error contracts (when it succeeds on retry).
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            contract_id: Contract ID
            year: Year
            month: Month
        """
        state = self.load_state()
        service_key = self.get_service_key(service_name, environment_type, plan_id)
        month_key = self.get_month_key(year, month)
        
        if (service_key in state and 
            'error_contracts' in state[service_key] and 
            month_key in state[service_key]['error_contracts']):
            
            # Remove the error entry for this contract
            state[service_key]['error_contracts'][month_key] = [
                entry for entry in state[service_key]['error_contracts'][month_key]
                if entry.get('contract_id') != contract_id
            ]
            
            # Clean up empty month entry
            if not state[service_key]['error_contracts'][month_key]:
                del state[service_key]['error_contracts'][month_key]
            
            state[service_key]['last_updated'] = datetime.now(timezone.utc).isoformat() + 'Z'
            self.save_state(state)

    def get_last_processed_month(self, service_name: str, environment_type: str, 
                                plan_id: str) -> Optional[Tuple[int, int]]:
        """
        Get the last processed month for a specific service configuration.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            
        Returns:
            Tuple of (year, month) for last processed month, or None if never processed
        """
        state = self.load_state()
        service_key = self.get_service_key(service_name, environment_type, plan_id)
        
        if service_key not in state:
            return None
        
        try:
            last_processed_str = state[service_key].get('last_processed_month')
            if not last_processed_str:
                return None
            
            # Parse YYYY-MM format
            year, month = map(int, last_processed_str.split('-'))
            return (year, month)
        except (KeyError, ValueError) as e:
            self.logger.error(f"Error parsing last processed month for {service_key}: {e}")
            return None

    def update_last_processed_month(self, service_name: str, environment_type: str, 
                                   plan_id: str, year: int, month: int):
        """
        Update the last processed month for a specific service configuration.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            year: Year of the month that was processed
            month: Month that was processed
        """
        state = self.load_state()
        service_key = self.get_service_key(service_name, environment_type, plan_id)
        
        if service_key not in state:
            state[service_key] = {}
        
        month_key = self.get_month_key(year, month)
        state[service_key]['last_processed_month'] = month_key
        state[service_key]['last_updated'] = datetime.now(timezone.utc).isoformat() + 'Z'

        self.save_state(state)

    def load_usage_data_state(self) -> Dict:
        """
        Load the usage data state from the S3 state file.

        Returns:
            Dictionary containing the state information
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key="omnistrate-metering/last_success_export.json")
            content = response['Body'].read().decode('utf-8')
            state = json.loads(content)
            self.logger.debug(f"Loaded usage data state from S3: s3://{self.bucket_name}/omnistrate-metering/last_success_export.json")
            return state
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                self.logger.error("omnistrate-metering/last_success_export.json file not found in S3")
                return {}
            else:
                self.logger.error(f"Error loading omnistrate-metering/last_success_export.json file from S3: {e}")
                return {}
        except (json.JSONDecodeError, IOError) as e:
            self.logger.error(f"Error parsing omnistrate-metering/last_success_export.json file: {e}")
            return {}

    def get_latest_month_with_complete_usage_data(self, service_name: str, environment_type: str, 
                                plan_id: str) -> Optional[Tuple[int, int]]:
        """
        Get the latest month for which complete usage data is available.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            
        Returns:
            Tuple of (year, month) for last processed month, or None if never processed
        """
        state = self.load_usage_data_state()
    
        if not state:
            return None

        service_key = self.get_service_key(service_name, environment_type, plan_id)
        
        if service_key not in state:
            return None
        
        try:
            last_processed_str = state[service_key].get('lastSuccessfulExport')
            if not last_processed_str:
                return None

            # Parse ISO 8601 timestamp (e.g., "2025-01-31T23:59:59Z")
            # We need to extract year and month from this
            dt = datetime.fromisoformat(last_processed_str.replace('Z', '+00:00'))
            
            # Return the year and month of the last successful export
            return (dt.year, dt.month)
        except (KeyError, ValueError) as e:
            self.logger.error(f"Error parsing last successful export for {service_key}: {e}")
            return None
