#!/usr/bin/env python3
"""
S3 to Clazar Usage Metering Script

This script pulls usage metering data from S3 and uploads aggregated data to Clazar.
It processes data monthly and ensures only one metering record per month per buyer-dimension combo.
"""

import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import calendar

import boto3
import requests
from botocore.exceptions import ClientError, NoCredentialsError


class MeteringProcessor:
    def __init__(self, bucket_name: str, state_file_path: str = "metering_state.json", 
                 dry_run: bool = False, access_token: str = None, cloud: str = "aws", 
                 aws_access_key_id: str = None, aws_secret_access_key: str = None, aws_region: str = None,
                 custom_dimensions: Dict[str, str] = None):
        """
        Initialize the metering processor.
        
        Args:
            bucket_name: S3 bucket name containing metering data
            state_file_path: Path to the state file in S3 that tracks last processed months
            dry_run: If True, skip actual API calls and only log payloads
            access_token: Clazar access token for authentication
            cloud: Cloud name (e.g., 'aws', 'azure', 'gcp')
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_region: AWS region
            custom_dimensions: Dict mapping custom dimension names to their formulas
        """
        self.bucket_name = bucket_name
        self.state_file_path = state_file_path
        self.dry_run = dry_run
        self.access_token = access_token
        self.cloud = cloud
        self.custom_dimensions = custom_dimensions or {}
        
        # Configure AWS credentials and create S3 client
        s3_kwargs = {}
        s3_kwargs['aws_access_key_id'] = aws_access_key_id
        s3_kwargs['aws_secret_access_key'] = aws_secret_access_key
        if aws_region:
            s3_kwargs['region_name'] = aws_region
        
        self.s3_client = boto3.client('s3', **s3_kwargs)
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Log AWS configuration (without exposing sensitive data)
        self.logger.info(f"Using provided AWS credentials for region: {aws_region}")
        
    def load_state(self) -> Dict:
        """
        Load the processing state from the S3 state file.
        
        Returns:
            Dictionary containing the state information
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.state_file_path)
            content = response['Body'].read().decode('utf-8')
            state = json.loads(content)
            self.logger.info(f"Loaded state from S3: s3://{self.bucket_name}/{self.state_file_path}")
            return state
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                self.logger.info("State file not found in S3, initializing with default state")
                return {}
            else:
                self.logger.error(f"Error loading state file from S3: {e}")
                return {}
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
                Key=self.state_file_path,
                Body=state_content,
                ContentType='application/json'
            )
            self.logger.info(f"Saved state to S3: s3://{self.bucket_name}/{self.state_file_path}")
        except ClientError as e:
            self.logger.error(f"Error saving state file to S3: {e}")

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
                                 payload: Dict = None, retry_count: int = 0):
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
            self.logger.info(f"Loaded state from S3: s3://{self.bucket_name}/omnistrate-metering/last_success_export.json")
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
            last_processed_str = state[service_key].get('last_processed_to')
            if not last_processed_str:
                return None

            # Parse the YYYY-MM-DDTHH:MM:SSZ format
            last_processed_to = datetime.strptime(last_processed_str, '%Y-%m-%dT%H:%M:%SZ')
            
            # Get last day of the month
            year = last_processed_to.year
            month = last_processed_to.month
            last_day_of_the_month = calendar.monthrange(year, month)[1]

            # Get the last complete month
            if last_processed_to.date().day != last_day_of_the_month or last_processed_to.minute != 59:
                # If not at the end of the month, adjust to the last complete month
                if last_processed_to.month == 1:
                    year = last_processed_to.year - 1
                    month = 12
                else:
                    year = last_processed_to.year
                    month = last_processed_to.month - 1
            else:
                year = last_processed_to.year
                month = last_processed_to.month

        except (KeyError, ValueError) as e:
            self.logger.error(f"Error parsing last processed month for {service_key}: {e}")
            return None

    def get_next_month_to_process(self, service_name: str, environment_type: str, 
                                 plan_id: str, default_start_month: Optional[Tuple[int, int]] = None) -> Optional[Tuple[int, int]]:
        """
        Get the next month that needs to be processed.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            default_start_month: Optional default start month (as tuple of (year, month))
            
        Returns:
            Tuple of (year, month) for next month to process, or None if caught up
        """
        last_processed = self.get_last_processed_month(service_name, environment_type, plan_id)
        latest_month_with_complete_usage_data = self.get_latest_month_with_complete_usage_data(service_name, environment_type, plan_id)
        if latest_month_with_complete_usage_data is None:
            self.logger.error(f"Failed to retrieve latest month with complete usage data")
            return None
        
        if last_processed is None:
            # If never processed, start from the default start month
            next_year, next_month = default_start_month
        
        # Calculate next month
        year, month = last_processed
        if month == 12:
            next_year, next_month = year + 1, 1
        else:
            next_year, next_month = year, month + 1
        
        # Check if next month is beyond the latest month with complete usage data
        latest_year, latest_month = latest_month_with_complete_usage_data
        if (next_year > latest_year) or (next_year == latest_year and next_month > latest_month):
            self.logger.info(f"Already caught up to the latest month with complete usage data: {latest_year}-{latest_month:02d}")
            return None
        
        return (next_year, next_month)

    def get_monthly_s3_prefix(self, service_name: str, environment_type: str, 
                             plan_id: str, year: int, month: int) -> str:
        """
        Generate S3 prefix for a specific month.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type (e.g., PROD, DEV)
            plan_id: Plan ID
            year: Year
            month: Month
            
        Returns:
            S3 prefix string for the entire month
        """
        return (f"omnistrate-metering/{service_name}/{environment_type}/"
                f"{plan_id}/{year:04d}/{month:02d}/")

    def list_monthly_subscription_files(self, prefix: str) -> List[str]:
        """
        List all subscription JSON files in the given S3 prefix (for entire month).
        
        Args:
            prefix: S3 prefix to search (should cover entire month)
            
        Returns:
            List of S3 object keys
        """
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            json_files = []
            for page in page_iterator:
                if 'Contents' in page:
                    # Filter for JSON files
                    json_files.extend([
                        obj['Key'] for obj in page['Contents'] 
                        if obj['Key'].endswith('.json')
                    ])
            
            self.logger.info(f"Found {len(json_files)} subscription files in {prefix}")
            return json_files
            
        except ClientError as e:
            self.logger.error(f"Error listing S3 objects: {e}")
            return []

    def read_s3_json_file(self, key: str) -> List[Dict]:
        """
        Read and parse a JSON file from S3.
        
        Args:
            key: S3 object key
            
        Returns:
            List of usage records
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            
            self.logger.debug(f"Read {len(data)} records from {key}")
            return data
            
        except ClientError as e:
            self.logger.error(f"Error reading S3 file {key}: {e}")
            return []
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing JSON from {key}: {e}")
            return []

    def aggregate_usage_data(self, usage_records: List[Dict]) -> Dict[Tuple[str, str], float]:
        """
        Aggregate usage data by externalPayerId (contract_id) and dimension for monthly data.
        
        Args:
            usage_records: List of usage records
            
        Returns:
            Dictionary with (externalPayerId, dimension) as key and total usage as value
        """
        aggregated_data = defaultdict(int)
        
        for record in usage_records:
            external_payer_id = record.get('externalPayerId')
            dimension = record.get('dimension')
            value = record.get('value', 0)
            
            if not external_payer_id or not dimension:
                self.logger.warning(f"Skipping record with missing data: {record}")
                continue
            
            key = (external_payer_id, dimension)
            aggregated_data[key] += int(value)
        
        self.logger.info(f"Aggregated {len(usage_records)} records into {len(aggregated_data)} entries")
        return dict(aggregated_data)

    def transform_dimensions(self, aggregated_data: Dict[Tuple[str, str], float]) -> Dict[Tuple[str, str], float]:
        """
        Transform dimensions according to custom dimension formulas.
        
        Args:
            aggregated_data: Original aggregated data with (contract_id, dimension) as key
            
        Returns:
            Transformed aggregated data with custom dimensions
        """
        if not self.custom_dimensions:
            # No custom dimensions defined, return original data
            return aggregated_data
        
        # Group data by contract for easier processing
        contract_data = defaultdict(dict)
        for (contract_id, dimension), value in aggregated_data.items():
            contract_data[contract_id][dimension] = value
        
        transformed_data = {}
        
        for contract_id, dimensions in contract_data.items():
            # Apply custom dimension transformations
            for custom_name, formula in self.custom_dimensions.items():
                try:
                    # Create a safe evaluation context with available dimensions
                    eval_context = {
                        'memory_byte_hours': dimensions.get('memory_byte_hours', 0),
                        'storage_allocated_byte_hours': dimensions.get('storage_allocated_byte_hours', 0),
                        'cpu_core_hours': dimensions.get('cpu_core_hours', 0),
                        # Add mathematical functions for safety
                        '__builtins__': {
                            'abs': abs, 'min': min, 'max': max, 'round': round,
                            'int': int, 'float': float
                        }
                    }
                    
                    # Evaluate the formula
                    result = eval(formula, eval_context)
                    if not isinstance(result, (int, float)) or result < 0:
                        raise ValueError(f"Formula must evaluate to a non-negative number, got: {result}")
                    
                    transformed_data[(contract_id, custom_name)] = float(result)
                    self.logger.debug(f"Contract {contract_id}: {custom_name} = {result} (formula: {formula})")
                    
                except Exception as e:
                    error_msg = f"Error evaluating formula for dimension '{custom_name}' and contract '{contract_id}': {e}"
                    self.logger.error(error_msg)
                    # Don't add this dimension to the result, effectively skipping this contract's data
                    # This ensures we don't send invalid/incomplete usage data to Clazar
                    if contract_id in [key[0] for key in transformed_data.keys()]:
                        # Remove any previous dimensions for this contract if we had an error
                        transformed_data = {k: v for k, v in transformed_data.items() if k[0] != contract_id}
                    break  # Skip this contract entirely if any dimension fails
        
        if transformed_data:
            self.logger.info(f"Transformed {len(aggregated_data)} original dimension entries into {len(transformed_data)} custom dimension entries")
        else:
            self.logger.warning("No valid custom dimension data was generated. Check your dimension formulas.")
        
        return transformed_data

    def filter_success_contracts(self, aggregated_data: Dict[Tuple[str, str], float],
                                  service_name: str, environment_type: str, plan_id: str,
                                  year: int, month: int) -> Dict[Tuple[str, str], float]:
        """
        Filter out contracts that have already been processed for this month.
        
        Args:
            aggregated_data: Aggregated usage data
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            year: Year
            month: Month
            
        Returns:
            Filtered aggregated data with only unprocessed contracts
        """
        filtered_data = {}
        
        for (contract_id, dimension), quantity in aggregated_data.items():
            if not self.is_contract_month_processed(service_name, environment_type, plan_id, 
                                                   contract_id, year, month):
                filtered_data[(contract_id, dimension)] = quantity
            else:
                self.logger.info(f"Skipping already processed contract {contract_id} for {year}-{month:02d}")
        
        self.logger.info(f"Filtered from {len(aggregated_data)} to {len(filtered_data)} unprocessed contract records")
        return filtered_data

    def send_to_clazar(self, aggregated_data: Dict[Tuple[str, str], float], 
                      start_time: datetime, end_time: datetime,
                      service_name: str, environment_type: str, plan_id: str,
                      max_retries: int = 5) -> bool:
        """
        Send aggregated usage data to Clazar and track processed contracts.
        Includes retry logic with exponential backoff for failed contracts.
        
        Args:
            aggregated_data: Aggregated usage data
            start_time: Start time for the metering period
            end_time: End time for the metering period
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            max_retries: Maximum retry attempts for failed contracts
            
        Returns:
            True if successful, False otherwise
        """
        if not aggregated_data:
            self.logger.info("No data to send to Clazar")
            return True
        
        # Prepare the payload grouped by contract
        contract_records = defaultdict(list)
        
        for (external_payer_id, dimension), quantity in aggregated_data.items():
            record = {
                "cloud": self.cloud,
                "contract_id": external_payer_id,
                "dimension": dimension,
                "start_time": start_time.isoformat() + "Z",
                "end_time": end_time.isoformat() + "Z",
                "quantity": str(int(quantity))  # Ensure it's a string of positive integer
            }
            contract_records[external_payer_id].append(record)
        
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "Authorization": f"Bearer {self.access_token}" if self.access_token else ""
        }
        
        if not self.access_token and not self.dry_run:
            self.logger.error("Access token is required for sending data to Clazar")
            return False
        
        year, month = start_time.year, start_time.month
        all_success = True
        
        # Process each contract separately for better error handling and retry logic
        for contract_id, records in contract_records.items():
            payload = {"request": records}
            success = False
            
            for attempt in range(max_retries + 1):
                try:
                    if attempt > 0:
                        # Exponential backoff: 2^attempt seconds
                        wait_time = 2 ** attempt
                        self.logger.info(f"Retrying contract {contract_id} (attempt {attempt + 1}/{max_retries + 1}) after {wait_time}s delay")
                        time.sleep(wait_time)
                    
                    self.logger.info(f"Sending {len(records)} metering records to Clazar for contract {contract_id}")
                    
                    if self.dry_run:
                        self.logger.info("DRY RUN MODE: Would send the following payload to Clazar:")
                        self.logger.info(f"URL: https://api.clazar.io/metering/")
                        self.logger.info(f"Payload: {json.dumps(payload, indent=2)}")
                        self.logger.info("DRY RUN MODE: Skipping actual API call")
                        
                        # In dry run, mark all contracts as processed
                        self.mark_contract_month_processed(service_name, environment_type, plan_id, 
                                                         contract_id, year, month)
                        success = True
                        break
                    
                    response = requests.post("https://api.clazar.io/metering/", json=payload, headers=headers, timeout=30)
                    
                    if response.status_code != 200:
                        raise requests.RequestException(f"HTTP {response.status_code}: {response.text}")
                    
                    response_data = response.json()
                    if "results" not in response_data:
                        raise ValueError("Unexpected response format from Clazar API")

                    # Check for errors in the response
                    has_errors = False
                    for result in response_data.get("results", []):
                        if "errors" in result and result["errors"]:
                            has_errors = True
                            break
                        elif "status" in result and result["status"] != "success":
                            # Log warning but don't treat as error
                            self.logger.warning(f"Sent data to Clazar with warnings: status={result['status']}. Please check if the dimensions are registered in Clazar.")
                    
                    if has_errors:
                        # Extract error details
                        errors = []
                        error_code = "API_ERROR"
                        error_message = "Unknown error"
                        
                        for result in response_data.get("results", []):
                            if "errors" in result and result["errors"]:
                                if isinstance(result["errors"], list):
                                    errors.extend(result["errors"])
                                else:
                                    errors.append(str(result["errors"]))
                                
                                error_code = result.get('code', 'API_ERROR')
                                error_message = result.get('message', 'Unknown error')
                        
                        if attempt == max_retries:
                            # Final attempt failed, record as error
                            self.logger.error(f"Final attempt failed for contract {contract_id}: {error_code} - {error_message}")
                            self.logger.error(f"Errors: {errors}")
                            self.mark_contract_month_error(service_name, environment_type, plan_id, 
                                                         contract_id, year, month, errors, error_code, 
                                                         error_message, payload, attempt)
                            all_success = False
                            break
                        else:
                            # Retry on next iteration
                            self.logger.warning(f"Attempt {attempt + 1} failed for contract {contract_id}: {error_code} - {error_message}")
                            continue
                    else:
                        # Success
                        self.logger.info(f"Successfully sent data to Clazar for contract {contract_id}")
                        self.logger.info(f"Response: {response_data}")
                        
                        # Remove from error contracts if it was previously failed
                        self.remove_error_contract(service_name, environment_type, plan_id, 
                                                 contract_id, year, month)
                        
                        # Mark as successfully processed
                        self.mark_contract_month_processed(service_name, environment_type, plan_id, 
                                                         contract_id, year, month)
                        success = True
                        break
                        
                except requests.RequestException as e:
                    if attempt == max_retries:
                        self.logger.error(f"Final network error for contract {contract_id}: {e}")
                        self.mark_contract_month_error(service_name, environment_type, plan_id, 
                                                     contract_id, year, month, [str(e)], "NETWORK_ERROR", 
                                                     str(e), payload, attempt)
                        all_success = False
                        break
                    else:
                        self.logger.warning(f"Network error on attempt {attempt + 1} for contract {contract_id}: {e}")
                        continue
                
                except Exception as e:
                    if attempt == max_retries:
                        self.logger.error(f"Final unexpected error for contract {contract_id}: {e}")
                        self.mark_contract_month_error(service_name, environment_type, plan_id, 
                                                     contract_id, year, month, [str(e)], "UNEXPECTED_ERROR", 
                                                     str(e), payload, attempt)
                        all_success = False
                        break
                    else:
                        self.logger.warning(f"Unexpected error on attempt {attempt + 1} for contract {contract_id}: {e}")
                        continue
            
            if not success:
                all_success = False
        
        return all_success

    def retry_error_contracts(self, service_name: str, environment_type: str, 
                             plan_id: str, year: int, month: int, max_retries: int = 5) -> bool:
        """
        Retry sending failed contracts for a specific month.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            year: Year
            month: Month
            max_retries: Maximum retry attempts
            
        Returns:
            True if all retries were successful, False otherwise
        """
        error_contracts = self.get_error_contracts_for_retry(service_name, environment_type, 
                                                           plan_id, year, month, max_retries)
        
        if not error_contracts:
            self.logger.info(f"No error contracts to retry for {year}-{month:02d}")
            return True
        
        self.logger.info(f"Retrying {len(error_contracts)} error contracts for {year}-{month:02d}")
        
        # Define the time window (month boundary)
        start_time = datetime(year, month, 1)
        last_day = calendar.monthrange(year, month)[1]
        end_time = datetime(year, month, last_day, 23, 59, 59)
        
        all_success = True
        
        for error_entry in error_contracts:
            contract_id = error_entry.get('contract_id')
            payload = error_entry.get('payload')
            retry_count = error_entry.get('retry_count', 0)
            
            if not contract_id or not payload:
                self.logger.warning(f"Skipping error contract with missing data: {error_entry}")
                continue
            
            # Retry this specific contract
            headers = {
                "accept": "application/json",
                "content-type": "application/json",
                "Authorization": f"Bearer {self.access_token}" if self.access_token else ""
            }
            
            success = False
            current_retry = retry_count
            
            while current_retry < max_retries:
                current_retry += 1
                
                try:
                    # Exponential backoff
                    wait_time = 2 ** current_retry
                    self.logger.info(f"Retrying contract {contract_id} (retry {current_retry}/{max_retries}) after {wait_time}s delay")
                    time.sleep(wait_time)
                    
                    if self.dry_run:
                        self.logger.info("DRY RUN MODE: Would retry sending the following payload to Clazar:")
                        self.logger.info(f"URL: https://api.clazar.io/metering/")
                        self.logger.info(f"Payload: {json.dumps(payload, indent=2)}")
                        
                        # In dry run, remove from error and mark as processed
                        self.remove_error_contract(service_name, environment_type, plan_id, 
                                                 contract_id, year, month)
                        self.mark_contract_month_processed(service_name, environment_type, plan_id, 
                                                         contract_id, year, month)
                        success = True
                        break

                    response = requests.post("https://api.clazar.io/metering/", json=payload, headers=headers, timeout=30)

                    if response.status_code != 200:
                        raise requests.RequestException(f"HTTP {response.status_code}: {response.text}")
                    
                    response_data = response.json()
                    if "results" not in response_data:
                        raise ValueError("Unexpected response format from Clazar API")

                    # Check for errors in the response
                    has_errors = False
                    for result in response_data.get("results", []):
                        if "errors" in result and result["errors"]:
                            has_errors = True
                            break
                    
                    if has_errors:
                        # Update retry count but continue trying
                        errors = []
                        error_code = "API_ERROR"
                        error_message = "Unknown error"
                        
                        for result in response_data.get("results", []):
                            if "errors" in result and result["errors"]:
                                if isinstance(result["errors"], list):
                                    errors.extend(result["errors"])
                                else:
                                    errors.append(str(result["errors"]))
                                
                                error_code = result.get('code', 'API_ERROR')
                                error_message = result.get('message', 'Unknown error')
                        
                        self.logger.warning(f"Retry {current_retry} failed for contract {contract_id}: {error_code} - {error_message}")
                        
                        # Update error entry with new retry count
                        self.mark_contract_month_error(service_name, environment_type, plan_id, 
                                                     contract_id, year, month, errors, error_code, 
                                                     error_message, payload, current_retry)
                        
                        if current_retry >= max_retries:
                            self.logger.error(f"Max retries reached for contract {contract_id}")
                            all_success = False
                            break
                        continue
                    else:
                        # Success - remove from error contracts and mark as processed
                        self.logger.info(f"Successfully retried contract {contract_id} on attempt {current_retry}")
                        self.logger.info(f"Response: {response_data}")
                        
                        self.remove_error_contract(service_name, environment_type, plan_id, 
                                                 contract_id, year, month)
                        self.mark_contract_month_processed(service_name, environment_type, plan_id, 
                                                         contract_id, year, month)
                        success = True
                        break
                        
                except Exception as e:
                    self.logger.warning(f"Retry {current_retry} error for contract {contract_id}: {e}")
                    
                    # Update error entry with new retry count
                    self.mark_contract_month_error(service_name, environment_type, plan_id, 
                                                 contract_id, year, month, [str(e)], "RETRY_ERROR", 
                                                 str(e), payload, current_retry)
                    
                    if current_retry >= max_retries:
                        self.logger.error(f"Max retries reached for contract {contract_id} due to error: {e}")
                        all_success = False
                        break
            
            if not success:
                all_success = False
        
        return all_success

    def process_month(self, service_name: str, environment_type: str, 
                     plan_id: str, year: int, month: int, max_retries: int = 5) -> bool:
        """
        Process usage data for a specific month.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            year: Year to process
            month: Month to process
            max_retries: Maximum retry attempts for failed contracts
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Processing month: {year}-{month:02d} for {service_name}/{environment_type}/{plan_id}")
        
        # First, retry any existing error contracts
        retry_success = self.retry_error_contracts(service_name, environment_type, plan_id, year, month, max_retries)
        
        # Get S3 prefix for the month
        prefix = self.get_monthly_s3_prefix(service_name, environment_type, plan_id, year, month)
        
        # List all subscription files for the month
        subscription_files = self.list_monthly_subscription_files(prefix)
        
        if not subscription_files:
            self.logger.info(f"No subscription files found for {year}-{month:02d}")
            # Return success of retry attempts if there were any
            return retry_success
        
        # Read and aggregate all usage data
        all_usage_records = []
        for file_key in subscription_files:
            usage_records = self.read_s3_json_file(file_key)
            all_usage_records.extend(usage_records)
        
        if not all_usage_records:
            self.logger.info(f"No usage records found for {year}-{month:02d}")
            # Return success of retry attempts if there were any
            return retry_success
        
        # Aggregate the data
        aggregated_data = self.aggregate_usage_data(all_usage_records)
        
        # Transform dimensions according to custom dimension formulas
        if self.custom_dimensions:
            aggregated_data = self.transform_dimensions(aggregated_data)
            if not aggregated_data:
                self.logger.error(f"All dimension transformations failed for {year}-{month:02d}. Skipping this month.")
                return False
        
        # Filter out already processed contracts
        filtered_data = self.filter_success_contracts(aggregated_data, service_name, 
                                                       environment_type, plan_id, year, month)
        
        if not filtered_data:
            self.logger.info(f"All contracts for {year}-{month:02d} have already been processed")
            # Return success of retry attempts if there were any
            return retry_success
        
        # Define the time window (month boundary)
        start_time = datetime(year, month, 1)
        # Last day of the month
        last_day = calendar.monthrange(year, month)[1]
        end_time = datetime(year, month, last_day, 23, 59, 59)
        
        # Send to Clazar
        send_success = self.send_to_clazar(filtered_data, start_time, end_time, 
                                         service_name, environment_type, plan_id, max_retries)
        
        # Return True only if both retry and send operations were successful
        return retry_success and send_success

    def process_next_month(self, service_name: str, environment_type: str, 
                          plan_id: str, max_retries: int = 5, start_month: tuple = (2025, 1)) -> bool:
        """
        Process the next pending month for a specific service configuration.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            max_retries: Maximum retry attempts for failed contracts
            start_month: Default start month if no previous processing history
            
        Returns:
            True if processing was successful, False otherwise
        """
        self.logger.info(f"Starting processing for {service_name}/{environment_type}/{plan_id}")
        
        next_month = self.get_next_month_to_process(service_name, environment_type, plan_id, 
                                                    default_start_month=start_month)
        
        if next_month is None:
            self.logger.info("No more months to process, caught up!")
            return True
        
        year, month = next_month
        self.logger.info(f"Processing month: {year}-{month:02d}")
        
        success = self.process_month(service_name, environment_type, plan_id, year, month, max_retries)
        
        if success:
            # Update state only if processing was successful
            self.update_last_processed_month(service_name, environment_type, plan_id, year, month)
            self.logger.info(f"Successfully processed month {year}-{month:02d}")
        else:
            self.logger.error(f"Failed to process month {year}-{month:02d}")
        
        return success

def main_processing():
    """Main processing function to run the metering processor."""
    
    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION')
    BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'omnistrate-usage-metering-export-demo')

    # Clazar Configuration
    CLAZAR_CLIENT_ID = os.getenv('CLAZAR_CLIENT_ID', '')
    CLAZAR_CLIENT_SECRET = os.getenv('CLAZAR_CLIENT_SECRET', '')
    CLAZAR_CLOUD = os.getenv('CLAZAR_CLOUD', 'aws')

    # Metering Processor Configuration
    SERVICE_NAME = os.getenv('SERVICE_NAME', 'Postgres')
    ENVIRONMENT_TYPE = os.getenv('ENVIRONMENT_TYPE', 'PROD')
    PLAN_ID = os.getenv('PLAN_ID', 'pt-HJSv20iWX0')
    STATE_FILE_PATH = os.getenv('STATE_FILE_PATH', 'metering_state.json')
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '5'))
    START_MONTH = os.getenv('START_MONTH', '2025-01')
    DRY_RUN = os.getenv('DRY_RUN', 'false').lower() in ('true', '1', 'yes')
    
    # Parse custom dimensions from environment variables
    custom_dimensions = {}
    for i in range(1, 4):  # Support up to 3 custom dimensions
        name_key = f'DIMENSION{i}_NAME'
        formula_key = f'DIMENSION{i}_FORMULA'
        
        dimension_name = os.getenv(name_key)
        dimension_formula = os.getenv(formula_key)
        
        if dimension_name and dimension_formula:
            custom_dimensions[dimension_name] = dimension_formula
        elif dimension_name or dimension_formula:
            print(f"Error: Both {name_key} and {formula_key} must be provided together")
            sys.exit(1)
    
    # Validate no duplicate dimension names
    if len(custom_dimensions) != len(set(custom_dimensions.keys())):
        print("Error: Duplicate dimension names found in custom dimensions")
        sys.exit(1)
    
    if custom_dimensions:
        print(f"Custom dimensions configured: {list(custom_dimensions.keys())}")
        for name, formula in custom_dimensions.items():
            print(f"  {name}: {formula}")
    
    # Validate required environment variables
    if not all([BUCKET_NAME, SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID]):
        print("Error: Missing required configuration. Please set environment variables:")
        print("S3_BUCKET_NAME, SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID")
        sys.exit(1)
    
    # Validate AWS credentials
    if not AWS_SECRET_ACCESS_KEY:
        print("Error: AWS_SECRET_ACCESS_KEY is missing")
        sys.exit(1)
    if not AWS_ACCESS_KEY_ID:
        print("Error: AWS_ACCESS_KEY_ID is missing")
        sys.exit(1)
    
    try:
        # Authenticate with Clazar
        url = "https://api.clazar.io/authenticate/"

        payload = {
            "client_id": CLAZAR_CLIENT_ID,
            "client_secret": CLAZAR_CLIENT_SECRET
        }
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers)
        if response.status_code != 200:
            print(f"Error authenticating with Clazar: {response.status_code} - {response.text}")
            sys.exit(1)

        access_token = response.json().get("access_token")
        if not access_token:
            print("Error: No access token received from Clazar")
            sys.exit(1)

        # Initialize the processor
        processor = MeteringProcessor(
            bucket_name=BUCKET_NAME, 
            state_file_path=STATE_FILE_PATH,
            dry_run=DRY_RUN, 
            access_token=access_token, 
            cloud=CLAZAR_CLOUD,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_region=AWS_REGION,
            custom_dimensions=custom_dimensions
        )
        
        # Process next month
        if START_MONTH:
            try:
                start_year, start_month = map(int, START_MONTH.split('-'))
            except ValueError:
                print(f"Invalid START_MONTH format: {START_MONTH}. Expected format: YYYY-MM")
                sys.exit(1)
        else:
            start_year, start_month = 2025, 1  # Default start month if not set

        success = processor.process_next_month(
            SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID, MAX_RETRIES, (start_year, start_month)
        )
        
        if success:
            print("Metering processing completed successfully")
            return True
        else:
            print("Metering processing failed")
            return False
            
    except NoCredentialsError:
        print("Error: AWS credentials not found.")
        print("Please configure AWS credentials by setting AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


def main():
    """Main function to run the metering processor."""
    
    success = main_processing()
    if not success:
        sys.exit(1)  # Exit with error code if processing failed


if __name__ == "__main__":
    main()