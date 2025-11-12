#!/usr/bin/env python3
"""
Omnistrate Metering Reader

This module handles reading usage metering data from the omnistrate-metering S3 path.
It provides methods to:
- Read the last successful export state
- Get the latest month with complete usage data
- Generate S3 prefixes for monthly data
- List and read subscription files
- Read usage records from S3
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from config import Config


class OmnistrateMeteringReaderError(Exception):
    """Exception raised for Omnistrate Metering Reader errors."""
    pass


class OmnistrateMeteringReader:
    """Handles reading usage metering data from S3 omnistrate-metering path."""
    
    def __init__(self, config: Config):
        """
        Initialize the Omnistrate Metering Reader.
        
        Args:
            config: Configuration object containing AWS credentials and bucket info
        """
        if not config:
            raise OmnistrateMeteringReaderError("Configuration object is required to initialize OmnistrateMeteringReader.")
        if not config.aws_s3_bucket:
            raise OmnistrateMeteringReaderError("AWS S3 bucket name is not configured.")
        if not config.aws_access_key_id:
            raise OmnistrateMeteringReaderError("AWS Access Key ID is not configured.")
        if not config.aws_secret_access_key:
            raise OmnistrateMeteringReaderError("AWS Secret Access Key is not configured.")
        if not config.aws_region:
            raise OmnistrateMeteringReaderError("AWS region is not configured.")
        if not config.service_name:
            raise OmnistrateMeteringReaderError("Service name is not configured.")
        if not config.environment_type:
            raise OmnistrateMeteringReaderError("Environment type is not configured.")
        if not config.plan_id:
            raise OmnistrateMeteringReaderError("Plan ID is not configured.")

        self.aws_s3_bucket = config.aws_s3_bucket
        self.service_name = config.service_name
        self.environment_type = config.environment_type
        self.plan_id = config.plan_id
        
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
        
        self.logger.info(f"OmnistrateMeteringReader initialized for bucket: {self.aws_s3_bucket}")

    def get_service_key(self) -> str:
        """
        Generate a unique key for a service configuration.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            
        Returns:
            Unique service key
        """
        return f"{self.service_name}:{self.environment_type}:{self.plan_id}"

    def load_usage_data_state(self) -> Dict:
        """
        Load the usage data state from the S3 state file.

        Returns:
            Dictionary containing the state information
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.aws_s3_bucket, 
                Key="omnistrate-metering/last_success_export.json"
            )
            content = response['Body'].read().decode('utf-8')
            state = json.loads(content)
            self.logger.debug(
                f"Loaded usage data state from S3: s3://{self.aws_s3_bucket}/omnistrate-metering/last_success_export.json"
            )
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

    def get_latest_month_with_complete_usage_data(self) -> Optional[Tuple[int, int]]:
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

        service_key = self.get_service_key()
        
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

    def list_monthly_subscription_files(self, service_name: str, environment_type: str, plan_id: str, year: int, month: int) -> List[str]:
        """
        List all subscription JSON files in the given S3 prefix (for entire month).
        
        Args:
            prefix: S3 prefix to search (should cover entire month)
            
        Returns:
            List of S3 object keys
        """
        # Get S3 prefix for the month
        prefix = self.get_monthly_s3_prefix(service_name, environment_type, plan_id, year, month)
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=self.aws_s3_bucket,
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
            response = self.s3_client.get_object(Bucket=self.aws_s3_bucket, Key=key)
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
