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
from collections import defaultdict
import datetime
from typing import Dict, List, Optional, Tuple
import calendar

import boto3
import requests
from botocore.exceptions import ClientError, NoCredentialsError


class MeteringProcessor:
    def __init__(self, bucket_name: str, state_file_path: str = "metering_state.json", 
                 clazar_api_url: str = "https://api.clazar.io/metering/", dry_run: bool = False,
                 access_token: str = None, cloud: str = "aws", aws_access_key_id: str = None,
                 aws_secret_access_key: str = None, aws_region: str = None):
        """
        Initialize the metering processor.
        
        Args:
            bucket_name: S3 bucket name containing metering data
            state_file_path: Path to the state file in S3 that tracks last processed months
            clazar_api_url: Clazar API endpoint URL
            dry_run: If True, skip actual API calls and only log payloads
            access_token: Clazar access token for authentication
            cloud: Cloud name (e.g., 'aws', 'azure', 'gcp')
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            aws_region: AWS region
        """
        self.bucket_name = bucket_name
        self.state_file_path = state_file_path
        self.clazar_api_url = clazar_api_url
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
        
        state[service_key]['last_updated'] = datetime.datetime.now(datetime.UTC).isoformat() + 'Z'
        self.save_state(state)

    def mark_contract_month_error(self, service_name: str, environment_type: str, 
                                 plan_id: str, contract_id: str, year: int, month: int,
                                 errors: List[str], code: str = None, message: str = None):
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
        else:
            # Create new error entry
            error_entry = {
                "contract_id": contract_id,
                "errors": errors,
            }
            if code:
                error_entry["code"] = code
            if message:
                error_entry["message"] = message
            
            state[service_key]['error_contracts'][month_key].append(error_entry)
        
        state[service_key]['last_updated'] = datetime.datetime.now(datetime.UTC).isoformat() + 'Z'
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
        state[service_key]['last_updated'] = datetime.datetime.now(datetime.UTC).isoformat() + 'Z'

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
        current_date = datetime.datetime.now(datetime.UTC)
        current_month = (current_date.year, current_date.month)
        
        if last_processed is None:
            # If never processed, start from 2 months ago to avoid processing incomplete current month
            target_date = current_date.replace(day=1) - datetime.timedelta(days=32)  # Go back at least one month
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
                      service_name: str, environment_type: str, plan_id: str) -> bool:
        """
        Send aggregated usage data to Clazar and track processed contracts.
        
        Args:
            aggregated_data: Aggregated usage data
            start_time: Start time for the metering period
            end_time: End time for the metering period
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            
        Returns:
            True if successful, False otherwise
        """
        if not aggregated_data:
            self.logger.info("No data to send to Clazar")
            return True
        
        # Prepare the payload
        metering_records = []
        contract_ids = set()
        
        for (external_payer_id, dimension), quantity in aggregated_data.items():
            record = {
                "cloud": self.cloud,
                "contract_id": external_payer_id,
                "dimension": dimension,
                "start_time": start_time.isoformat() + "Z",
                "end_time": end_time.isoformat() + "Z",
                "quantity": str(quantity)
            }
            metering_records.append(record)
            contract_ids.add(external_payer_id)
        
        payload = {"request": metering_records}
        
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "Authorization": f"Bearer {self.access_token}" if self.access_token else ""
        }
        
        if not self.access_token and not self.dry_run:
            self.logger.error("Access token is required for sending data to Clazar")
            return False
        
        try:
            self.logger.info(f"Sending {len(metering_records)} metering records to Clazar for {len(contract_ids)} contracts")
            
            if self.dry_run:
                self.logger.info("DRY RUN MODE: Would send the following payload to Clazar:")
                self.logger.info(f"URL: {self.clazar_api_url}")
                self.logger.info(f"Payload: {json.dumps(payload, indent=2)}")
                self.logger.info("DRY RUN MODE: Skipping actual API call")
                
                # In dry run, mark all contracts as processed
                year, month = start_time.year, start_time.month
                for contract_id in contract_ids:
                    self.mark_contract_month_processed(service_name, environment_type, plan_id, 
                                                     contract_id, year, month)
                return True
            
            response = requests.post(self.clazar_api_url, json=payload, headers=headers)
            
            if "results" not in response.json():
                self.logger.error("Unexpected response format from Clazar API")
                self.logger.info(f"Response: {response.text}")
                return False

            # Track successful submissions and errors per contract
            successful_contracts = set()
            error_contracts = {}
            year, month = start_time.year, start_time.month
            
            for result in response.json().get("results", []):
                contract_id = result.get("contract_id")
                
                if "errors" in result and result["errors"]:
                    # Record the error
                    error_msg = result.get('message', 'Unknown error')
                    error_code = result.get('code', 'API_ERROR')
                    errors = result["errors"] if isinstance(result["errors"], list) else [str(result["errors"])]
                    
                    self.logger.error(f"Clazar API error for contract {contract_id}: {error_code} - {error_msg}")
                    self.logger.error(f"Errors: {errors}")
                    
                    if contract_id:
                        self.mark_contract_month_error(service_name, environment_type, plan_id, 
                                                     contract_id, year, month, errors, error_code, error_msg)
                    
                    # Continue processing other contracts instead of returning False immediately
                    continue
                    
                elif "status" in result and result["status"] != "success":
                    self.logger.warning(f"Sent data to Clazar with warnings: status={result['status']}. Please check if the dimensions are registered in Clazar.")
                    self.logger.info(f"Response: {response.json()}")
                    # Still mark as successful if we got a response
                    if contract_id:
                        successful_contracts.add(contract_id)
                else:
                    self.logger.info("Successfully sent data to Clazar")
                    self.logger.info(f"Response: {response.json()}")
                    if contract_id:
                        successful_contracts.add(contract_id)
            
            # Mark successfully processed contracts
            for contract_id in successful_contracts:
                self.mark_contract_month_processed(service_name, environment_type, plan_id, 
                                                 contract_id, year, month)
            
            # Return True if we had any successful contracts, or if all contracts were processed (even with errors)
            total_contracts_handled = len(successful_contracts) + len([r for r in response.json().get("results", []) if "errors" in r and r["errors"]])
            
            return total_contracts_handled > 0
                
        except requests.RequestException as e:
            self.logger.error(f"Error sending data to Clazar: {e}")
            return False

    def process_month(self, service_name: str, environment_type: str, 
                     plan_id: str, year: int, month: int) -> bool:
        """
        Process usage data for a specific month.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            year: Year to process
            month: Month to process
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Processing month: {year}-{month:02d} for {service_name}/{environment_type}/{plan_id}")
        
        # Get S3 prefix for the month
        prefix = self.get_monthly_s3_prefix(service_name, environment_type, plan_id, year, month)
        
        # List all subscription files for the month
        subscription_files = self.list_monthly_subscription_files(prefix)
        
        if not subscription_files:
            self.logger.info(f"No subscription files found for {year}-{month:02d}")
            return True
        
        # Read and aggregate all usage data
        all_usage_records = []
        for file_key in subscription_files:
            usage_records = self.read_s3_json_file(file_key)
            all_usage_records.extend(usage_records)
        
        if not all_usage_records:
            self.logger.info(f"No usage records found for {year}-{month:02d}")
            return True
        
        # Aggregate the data
        aggregated_data = self.aggregate_usage_data(all_usage_records)
        
        # Filter out already processed contracts
        filtered_data = self.filter_success_contracts(aggregated_data, service_name, 
                                                       environment_type, plan_id, year, month)
        
        if not filtered_data:
            self.logger.info(f"All contracts for {year}-{month:02d} have already been processed")
            return True
        
        # Define the time window (month boundary)
        start_time = datetime.datetime(year, month, 1)
        # Last day of the month
        last_day = calendar.monthrange(year, month)[1]
        end_time = datetime.datetime(year, month, last_day, 23, 59, 59)
        
        # Send to Clazar
        return self.send_to_clazar(filtered_data, start_time, end_time, 
                                 service_name, environment_type, plan_id)

    def process_pending_months(self, service_name: str, environment_type: str, 
                              plan_id: str, max_months: int = 12) -> bool:
        """
        Process all pending months for a specific service configuration.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            max_months: Maximum number of months to process in one run
            
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
            
            success = self.process_month(service_name, environment_type, plan_id, year, month)
            
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


def main():
    """Main function to run the metering processor."""
    
    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION')
    BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'omnistrate-usage-metering-export-demo')

    # Clazar Configuration
    CLAZAR_CLIENT_ID = os.getenv('CLAZAR_CLIENT_ID', '')
    CLAZAR_CLIENT_SECRET = os.getenv('CLAZAR_CLIENT_SECRET', '')
    CLAZAR_API_URL = os.getenv('CLAZAR_API_URL', 'https://api.clazar.io/metering/')
    CLAZAR_CLOUD = os.getenv('CLAZAR_CLOUD', 'aws')

    # Metering Processor Configuration
    SERVICE_NAME = os.getenv('SERVICE_NAME', 'Postgres')
    ENVIRONMENT_TYPE = os.getenv('ENVIRONMENT_TYPE', 'PROD')
    PLAN_ID = os.getenv('PLAN_ID', 'pt-HJSv20iWX0')
    STATE_FILE_PATH = os.getenv('STATE_FILE_PATH', 'metering_state.json')
    MAX_MONTHS_PER_RUN = int(os.getenv('MAX_MONTHS_PER_RUN', '12'))
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
            clazar_api_url=CLAZAR_API_URL, 
            dry_run=DRY_RUN, 
            access_token=access_token, 
            cloud=CLAZAR_CLOUD,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_region=AWS_REGION
        )
        
        # Process all pending months
        success = processor.process_pending_months(
            SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID, MAX_MONTHS_PER_RUN
        )
        
        if success:
            print("Metering processing completed successfully")
            sys.exit(0)
        else:
            print("Metering processing failed")
            sys.exit(1)
            
    except NoCredentialsError:
        print("Error: AWS credentials not found.")
        print("Please configure AWS credentials by setting AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()