#!/usr/bin/env python3
"""
Configuration management for the metering processor.

This module handles reading and validating environment variables.
"""

import os
import sys
from typing import Dict, Optional, Tuple


class ConfigurationError(Exception):
    """Exception raised for configuration errors."""
    pass


class Config:
    """Configuration class that reads and validates environment variables."""
    
    def __init__(self):
        """Initialize configuration by reading environment variables."""
        self._load_aws_config()
        self._load_clazar_config()
        self._load_processor_config()
        self._load_custom_dimensions()
        
    def _load_aws_config(self):
        """Load and validate AWS configuration."""
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION')
        self.aws_s3_bucket = os.getenv('S3_BUCKET_NAME', 'omnistrate-usage-metering-export-demo')
        
    def _load_clazar_config(self):
        """Load and validate Clazar configuration."""
        self.clazar_client_id = os.getenv('CLAZAR_CLIENT_ID', '')
        self.clazar_client_secret = os.getenv('CLAZAR_CLIENT_SECRET', '')
        self.clazar_cloud = os.getenv('CLAZAR_CLOUD', 'aws')
        
    def _load_processor_config(self):
        """Load and validate processor configuration."""
        self.service_name = os.getenv('SERVICE_NAME', 'Postgres')
        self.environment_type = os.getenv('ENVIRONMENT_TYPE', 'PROD')
        self.plan_id = os.getenv('PLAN_ID', 'pt-HJSv20iWX0')
        self.start_month = os.getenv('START_MONTH', '2025-01')
        self.dry_run = os.getenv('DRY_RUN', 'false').lower() in ('true', '1', 'yes')
        
    def _load_custom_dimensions(self):
        """Load and validate custom dimensions configuration."""
        self.custom_dimensions = {}
        for i in range(1, 4):  # Support up to 3 custom dimensions
            name_key = f'DIMENSION{i}_NAME'
            formula_key = f'DIMENSION{i}_FORMULA'
            
            dimension_name = os.getenv(name_key)
            dimension_formula = os.getenv(formula_key)

            # Check if both name and formula are provided
            if dimension_name and dimension_formula:
                # Check for duplicate dimension names
                if dimension_name in self.custom_dimensions:
                    raise ConfigurationError(
                        f"Duplicate dimension name '{dimension_name}' found. "
                        f"Dimension names must be unique."
                    )
                self.custom_dimensions[dimension_name] = dimension_formula
            elif dimension_name or dimension_formula:
                # One is provided but not the other - this is an error
                raise ConfigurationError(
                    f"Both {name_key} and {formula_key} must be provided together"
                )
        
        if len(self.custom_dimensions) == 0:
            raise ConfigurationError("At least one custom dimension must be provided")
        
    def setup_logging(self):
        """Set up logging configuration."""
        import logging
        logging.basicConfig(
            level=os.getenv('LOG_LEVEL', 'INFO').upper(),
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logging.info("Logging is configured to level: %s", os.getenv('LOG_LEVEL', 'INFO').upper())

    def validate_aws_credentials(self):
        """
        Validate that required AWS credentials are present.
        
        Raises:
            ConfigurationError: If AWS credentials are missing
        """
        if not self.aws_secret_access_key:
            raise ConfigurationError("AWS_SECRET_ACCESS_KEY is missing")
        if not self.aws_access_key_id:
            raise ConfigurationError("AWS_ACCESS_KEY_ID is missing")
        if not self.aws_region:
            raise ConfigurationError("AWS_REGION is missing")
    
    def validate_required_config(self):
        """
        Validate that all required configuration is present.
        
        Raises:
            ConfigurationError: If required configuration is missing
        """
        if not all([self.aws_s3_bucket, self.service_name, self.environment_type, self.plan_id]):
            raise ConfigurationError(
                "Missing required configuration. Please set environment variables: "
                "S3_BUCKET_NAME, SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID"
            )
    
    def validate_custom_dimensions(self):
        """
        Validate custom dimensions configuration.
        
        Note: Duplicate checking is performed during loading in _load_custom_dimensions.
        This method is kept for API consistency and potential future validations.
        """
        # Duplicates are already checked during loading
        pass
    
    def validate_start_month(self) -> Tuple[int, int]:
        """
        Validate and parse the START_MONTH configuration.
        
        Returns:
            Tuple of (year, month)
            
        Raises:
            ConfigurationError: If START_MONTH format is invalid
        """
        if self.start_month:
            try:
                parts = self.start_month.split('-')
                if len(parts) != 2:
                    raise ValueError("Must be in YYYY-MM format")
                
                year_str, month_str = parts
                
                # Check for proper formatting (YYYY and MM)
                if len(year_str) != 4 or len(month_str) != 2:
                    raise ValueError("Year must be 4 digits and month must be 2 digits")
                
                year = int(year_str)
                month = int(month_str)
                
                if not (1 <= month <= 12):
                    raise ValueError("Month must be between 1 and 12")
                if year < 1900 or year > 9999:
                    raise ValueError("Year must be between 1900 and 9999")
                return year, month
            except (ValueError, AttributeError) as e:
                raise ConfigurationError(
                    f"Invalid START_MONTH format: {self.start_month}. "
                    f"Expected format: YYYY-MM. Error: {str(e)}"
                )
        return 2025, 1  # Default start month
    
    def validate_all(self):
        """
        Run all validation checks.
        
        Raises:
            ConfigurationError: If any validation fails
        """
        self.validate_required_config()
        self.validate_aws_credentials()
        self.validate_custom_dimensions()
        self.validate_start_month()
    
    def print_summary(self):
        """Print a summary of the configuration (without sensitive data)."""
        print(f"Configuration loaded:")
        print(f"  Log Level: {os.getenv('LOG_LEVEL', 'INFO').upper()}")
        print(f"  AWS S3 Bucket: {self.aws_s3_bucket}")
        print(f"  AWS Region: {self.aws_region}")
        print(f"  Service: {self.service_name}")
        print(f"  Environment: {self.environment_type}")
        print(f"  Plan ID: {self.plan_id}")
        print(f"  Start Month: {self.start_month}")
        print(f"  Dry Run: {self.dry_run}")
        print(f"  Clazar Cloud: {self.clazar_cloud}")
        
        if self.custom_dimensions:
            print(f"  Custom dimensions configured: {list(self.custom_dimensions.keys())}")
            for name, formula in self.custom_dimensions.items():
                print(f"    {name}: {formula}")
                
