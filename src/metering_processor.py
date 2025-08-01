#!/usr/bin/env python3
"""
S3 to Clazar Usage Metering Script

This script pulls usage metering data from S3 and uploads aggregated data to Clazar.
It processes data monthly and ensures only one metering record per month per buyer-dimension combo.
Can run as a cron job on the first day of every month at 00:10 UTC.
"""

import json
import logging
import os
import sys
import time
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import calendar

import boto3
import requests
from botocore.exceptions import ClientError, NoCredentialsError


class MeteringProcessor:
    def __init__(self, bucket_name: str, state_file_path: str = "metering_state.json", 
                 dry_run: bool = False, access_token: str = None, cloud: str = "aws", 
                 aws_access_key_id: str = None, aws_secret_access_key: str = None, aws_region: str = None):
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
        """
        self.bucket_name = bucket_name
        self.state_file_path = state_file_path
        self.dry_run = dry_run
        self.access_token = access_token
        self.cloud = cloud
        
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
        
        # Load custom dimensions configuration
        self.custom_dimensions = self._load_custom_dimensions()
        
    def _load_custom_dimensions(self) -> Dict[str, str]:
        """
        Load custom dimension configurations from environment variables.
        
        Environment variables should be in format:
        DIMENSION_<name>=<formula>
        
        For example:
        DIMENSION_pod_hours=cpu_core_hours / 2
        DIMENSION_memory_gb_hours=memory_byte_hours / 1073741824
        
        Returns:
            Dictionary mapping new dimension names to their formulas
        """
        custom_dimensions = {}
        
        for key, value in os.environ.items():
            if key.startswith('DIMENSION_'):
                dimension_name = key[10:].lower()  # Remove 'DIMENSION_' prefix
                formula = value.strip()
                custom_dimensions[dimension_name] = formula
                self.logger.info(f"Loaded custom dimension: {dimension_name} = {formula}")
        
        return custom_dimensions
    
    def _evaluate_dimension_formula(self, formula: str, base_dimensions: Dict[str, float]) -> float:
        """
        Evaluate a dimension formula using base dimension values.
        
        Args:
            formula: The formula string (e.g., "cpu_core_hours / 2")
            base_dimensions: Dictionary of base dimension values
            
        Returns:
            Calculated value for the custom dimension
        """
        try:
            # Create a safe evaluation context with only math operations and base dimensions
            safe_dict = {
                '__builtins__': {},
                'abs': abs, 'round': round, 'min': min, 'max': max,
                'int': int, 'float': float,
            }
            safe_dict.update(base_dimensions)
            
            # Basic validation to prevent code injection
            if any(dangerous in formula for dangerous in ['import', '__', 'exec', 'eval', 'open', 'file']):
                raise ValueError(f"Unsafe formula detected: {formula}")
            
            result = eval(formula, safe_dict)
            return float(result)
        except Exception as e:
            self.logger.error(f"Error evaluating formula '{formula}' with dimensions {base_dimensions}: {e}")
            return 0.0
        
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

    def get_next_month_to_process(self, service_name: str, environment_type: str, 
                                 plan_id: str) -> Optional[Tuple[int, int]]:
        """
        Get the next month that needs to be processed.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            
        Returns:
            Tuple of (year, month) for next month to process, or None if caught up
        """
        last_processed = self.get_last_processed_month(service_name, environment_type, plan_id)
        current_date = datetime.now(timezone.utc)
        current_month = (current_date.year, current_date.month)
        
        if last_processed is None:
            # If never processed, start from 2 months ago to avoid processing incomplete current month
            target_date = current_date.replace(day=1) - timedelta(days=32)  # Go back at least one month
            target_date = target_date.replace(day=1)  # First day of that month
            start_month = (target_date.year, target_date.month)
            self.logger.info(f"No previous processing found, starting from {start_month[0]}-{start_month[1]:02d}")
            return start_month
        
        # Calculate next month
        year, month = last_processed
        if month == 12:
            next_year, next_month = year + 1, 1
        else:
            next_year, next_month = year, month + 1
        
        # Don't process the current month as it might be incomplete
        if (next_year, next_month) >= current_month:
            self.logger.info("Caught up with current month, no processing needed")
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
        Also calculates custom dimensions based on environment variable configurations.
        
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
        
        # Calculate custom dimensions
        if self.custom_dimensions:
            self.logger.info(f"Calculating {len(self.custom_dimensions)} custom dimensions")
            
            # Group by external_payer_id to calculate custom dimensions per contract
            contracts = {}
            for (external_payer_id, dimension), value in aggregated_data.items():
                if external_payer_id not in contracts:
                    contracts[external_payer_id] = {}
                contracts[external_payer_id][dimension] = value
            
            # Calculate custom dimensions for each contract
            for external_payer_id, base_dimensions in contracts.items():
                for custom_dim_name, formula in self.custom_dimensions.items():
                    try:
                        # Check if all required dimensions are available for this contract
                        # Parse the formula to find required dimension names
                        import re
                        dimension_names = re.findall(r'\b[a-z_]+\b', formula)
                        required_dims = [dim for dim in dimension_names if dim not in ['abs', 'round', 'min', 'max', 'int', 'float']]
                        
                        # Only calculate if all required dimensions are available
                        if all(dim in base_dimensions for dim in required_dims):
                            custom_value = self._evaluate_dimension_formula(formula, base_dimensions)
                            if custom_value > 0:  # Only add positive values
                                key = (external_payer_id, custom_dim_name)
                                aggregated_data[key] = custom_value
                                self.logger.debug(f"Calculated {custom_dim_name}={custom_value} for contract {external_payer_id}")
                        else:
                            missing_dims = [dim for dim in required_dims if dim not in base_dimensions]
                            self.logger.debug(f"Skipping {custom_dim_name} for contract {external_payer_id}: missing dimensions {missing_dims}")
                    except Exception as e:
                        self.logger.error(f"Failed to calculate custom dimension {custom_dim_name} for contract {external_payer_id}: {e}")
        
        self.logger.info(f"Aggregated {len(usage_records)} records into {len(aggregated_data)} entries")
        return dict(aggregated_data)

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

    def process_pending_months(self, service_name: str, environment_type: str, 
                              plan_id: str, max_months: int = 12, max_retries: int = 5) -> bool:
        """
        Process all pending months for a specific service configuration.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            max_months: Maximum number of months to process in one run
            max_retries: Maximum retry attempts for failed contracts
            
        Returns:
            True if all processing was successful, False otherwise
        """
        self.logger.info(f"Starting processing for {service_name}/{environment_type}/{plan_id}")
        
        processed_count = 0
        all_successful = True
        
        while processed_count < max_months:
            next_month = self.get_next_month_to_process(service_name, environment_type, plan_id)
            
            if next_month is None:
                self.logger.info("No more months to process, caught up!")
                break
            
            year, month = next_month
            self.logger.info(f"Processing month {processed_count + 1}/{max_months}: {year}-{month:02d}")
            
            success = self.process_month(service_name, environment_type, plan_id, year, month, max_retries)
            
            if success:
                # Update state only if processing was successful
                self.update_last_processed_month(service_name, environment_type, plan_id, year, month)
                processed_count += 1
            else:
                self.logger.error(f"Failed to process month {year}-{month:02d}, stopping")
                all_successful = False
                break
        
        self.logger.info(f"Processed {processed_count} months. Success: {all_successful}")
        return all_successful


def is_first_day_of_month() -> bool:
    """
    Check if today is the first day of the month.
    
    Returns:
        True if today is the first day of the month, False otherwise
    """
    now = datetime.now(timezone.utc)
    return now.day == 1


def wait_for_scheduled_time():
    """
    Wait until 00:10 UTC if running as a cron job and it's the first day of the month.
    """
    now = datetime.now(timezone.utc)
    target_time = now.replace(hour=0, minute=10, second=0, microsecond=0)
    
    # If it's past 00:10, target the next day's 00:10
    if now >= target_time:
        target_time += timedelta(days=1)
    
    wait_seconds = (target_time - now).total_seconds()
    
    if wait_seconds > 0 and wait_seconds < 86400:  # Less than 24 hours
        print(f"Waiting until {target_time.strftime('%Y-%m-%d %H:%M:%S UTC')} to run (waiting {wait_seconds:.0f} seconds)")
        time.sleep(wait_seconds)


def run_as_cron_job():
    """
    Run the script continuously as a cron job, executing on the first day of every month at 00:10 UTC.
    """
    print("Starting cron job mode - will run on the first day of every month at 00:10 UTC")
    
    while True:
        try:
            now = datetime.now(timezone.utc)
            
            # Check if it's the first day of the month and around 00:10 UTC
            if is_first_day_of_month():
                current_time = now.time()
                target_time = now.replace(hour=0, minute=10, second=0, microsecond=0).time()
                
                # Run if it's between 00:10 and 00:20 UTC (10-minute window)
                if target_time <= current_time <= now.replace(hour=0, minute=20, second=0, microsecond=0).time():
                    print(f"Executing scheduled job at {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                    main_processing()
                    
                    # Sleep until the next day to avoid running multiple times
                    sleep_until_next_day = (86400 - (now.hour * 3600 + now.minute * 60 + now.second))
                    print(f"Job completed. Sleeping for {sleep_until_next_day} seconds until next day")
                    time.sleep(sleep_until_next_day)
                else:
                    # Wait until 00:10 UTC
                    wait_for_scheduled_time()
            else:
                # Sleep until the next day
                next_day = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                sleep_seconds = (next_day - now).total_seconds()
                print(f"Not the first day of the month. Sleeping for {sleep_seconds:.0f} seconds until next day")
                time.sleep(sleep_seconds)
                
        except KeyboardInterrupt:
            print("Cron job interrupted by user")
            break
        except Exception as e:
            print(f"Error in cron job: {e}")
            # Sleep for an hour before retrying
            time.sleep(3600)


def main_processing():
    """Main processing function that can be called from cron job or directly."""
    
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
    MAX_MONTHS_PER_RUN = int(os.getenv('MAX_MONTHS_PER_RUN', '12'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '5'))
    DRY_RUN = os.getenv('DRY_RUN', 'false').lower() in ('true', '1', 'yes')
    
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
            aws_region=AWS_REGION
        )
        
        # Process all pending months
        success = processor.process_pending_months(
            SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID, MAX_MONTHS_PER_RUN, MAX_RETRIES
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
    
    # Check if running as cron job
    CRON_MODE = os.getenv('CRON_MODE', 'false').lower() in ('true', '1', 'yes')
    
    if CRON_MODE:
        run_as_cron_job()
    else:
        # Run once
        success = main_processing()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()