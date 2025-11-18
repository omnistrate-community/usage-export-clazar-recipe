#!/usr/bin/env python3
"""
S3 to Clazar Usage Metering Script

This script pulls usage metering data from S3 and uploads aggregated data to Clazar.
It processes data monthly and ensures only one metering record per month per buyer-dimension combo.
"""

import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import calendar

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from config import Config, ConfigurationError
from clazar_client import ClazarClient, ClazarAPIError
from state_manager import StateManager, StateManagerError
from omnistrate_metering_reader import OmnistrateMeteringReader

class MeteringProcessor:
    def __init__(self, config: Config, metering_reader: OmnistrateMeteringReader, state_manager: StateManager, clazar_client: ClazarClient = None):
        """
        Initialize the metering processor.
        
        Args:
            config: Config instance containing all configuration
            state_manager: StateManager instance for state persistence
            clazar_client: ClazarClient instance for API interactions
        """
        if not config:
            raise ValueError("Configuration object is required to initialize MeteringProcessor.")
        if not config.service_name:
            raise ValueError("Service name is not configured.")
        if not config.environment_type:
            raise ValueError("Environment type is not configured.")
        if not config.plan_id:
            raise ValueError("Plan ID is not configured.")  
        if not metering_reader:
            raise ValueError("OmnistrateMeteringReader object is required to initialize MeteringProcessor.")
        if not state_manager:
            raise ValueError("StateManager object is required to initialize MeteringProcessor.")
        if not clazar_client:
            raise ValueError("ClazarClient object is required to initialize MeteringProcessor.")
        if not config.aws_s3_bucket:
            raise ValueError("AWS S3 bucket name is not configured.")
        if not config.clazar_cloud:
            raise ValueError("Clazar cloud is not configured.")

        self.aws_s3_bucket = config.aws_s3_bucket
        self.state_manager = state_manager
        self.clazar_client = clazar_client
        self.clazar_cloud = config.clazar_cloud
        self.custom_dimensions = config.custom_dimensions or {}
        self.metering_reader = metering_reader
        self.service_name = config.service_name
        self.environment_type = config.environment_type
        self.plan_id = config.plan_id

        self.logger = logging.getLogger(__name__)

    def get_next_month_to_process(self, default_start_month: Optional[Tuple[int, int]] = None) -> Optional[Tuple[int, int]]:
        """
        Get the next month that needs to be processed.
        
        Args:
            default_start_month: Optional default start month (as tuple of (year, month))
            
        Returns:
            Tuple of (year, month) for next month to process, or None if caught up
        """
        last_processed = self.state_manager.get_last_processed_month()
        latest_month_with_complete_usage_data = self.metering_reader.get_latest_month_with_complete_usage_data()
        if latest_month_with_complete_usage_data is None:
            self.logger.error(f"Failed to retrieve latest month with complete usage data")
            return None
        
        if last_processed is None:
            # If never processed, start from the default start month
            next_year, next_month = default_start_month
        else:
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

    def aggregate_usage_data(self, usage_records: List[Dict]) -> Dict[Tuple[str, str], Tuple[int, float]]:
        """
        Aggregate usage data by externalPayerId (contract_id) and dimension for monthly data.
        
        Args:
            usage_records: List of usage records
            
        Returns:
            Dictionary with (externalPayerId, dimension) as key and total usage as value
        """
        aggregated_data = defaultdict(lambda: (0, 0))
        
        for record in usage_records:
            external_payer_id = record.get('externalPayerId')
            dimension = record.get('dimension')
            value = record.get('value', 0)
            pricePerUnit = record.get('pricePerUnit', 0)
            
            if not external_payer_id:
                self.logger.warning(f"Skipping record with missing data: no external payer ID, timestamp {record.get('timestamp')}")
                continue
            if not dimension:
                self.logger.warning(f"Skipping record with missing data: no dimension, timestamp {record.get('timestamp')}, externalPayerId {external_payer_id}")
                continue
            if not pricePerUnit or float(pricePerUnit) <= 0:
                self.logger.warning(f"Skipping record with invalid pricePerUnit: {pricePerUnit}, timestamp {record.get('timestamp')}, externalPayerId {external_payer_id}, dimension {dimension}")
                continue
            
            key = (external_payer_id, dimension)
            current_value, current_price = aggregated_data[key]
            aggregated_data[key] = (current_value + int(value), max(float(pricePerUnit), current_price))
        
        self.logger.info(f"Aggregated {len(usage_records)} records into {len(aggregated_data)} entries")
        return dict(aggregated_data)

    def transform_dimensions(self, aggregated_data: Dict[Tuple[str, str], Tuple[int, float]]) -> Dict[Tuple[str, str], float]:
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
                    # Handle both tuple (value, price) and plain number formats
                    def get_value(dim_data):
                        if isinstance(dim_data, tuple):
                            return dim_data[0]
                        return dim_data
                    
                    def get_price(dim_data):
                        if isinstance(dim_data, tuple):
                            return dim_data[1]
                        return 0.0
                    
                    eval_context = {
                        'memory_byte_hours': get_value(dimensions.get('memory_byte_hours', (0, 0))),
                        "memory_byte_hours_price_per_unit": get_price(dimensions.get("memory_byte_hours", (0, 0))),
                        'storage_allocated_byte_hours': get_value(dimensions.get('storage_allocated_byte_hours', (0, 0))),
                        "storage_allocated_byte_hours_price_per_unit": get_price(dimensions.get("storage_allocated_byte_hours", (0, 0))),
                        'cpu_core_hours': get_value(dimensions.get('cpu_core_hours', (0, 0))),
                        "cpu_core_hours_price_per_unit": get_price(dimensions.get("cpu_core_hours", (0, 0))),
                        'pricePerUnit': get_value(dimensions.get('pricePerUnit', (0, 0))),
                        'replica_hours': get_value(dimensions.get('replica_hours', (0, 0))),
                        "replica_hours_price_per_unit": get_price(dimensions.get("replica_hours", (0, 0))),
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
                                  year: int, month: int) -> Dict[Tuple[str, str], float]:
        """
        Filter out contracts that have already been processed for this month.
        
        Args:
            aggregated_data: Aggregated usage data
            year: Year
            month: Month
            
        Returns:
            Filtered aggregated data with only unprocessed contracts
        """
        filtered_data = {}
        
        for (contract_id, dimension), quantity in aggregated_data.items():
            if not self.state_manager.is_contract_month_processed(contract_id, year, month):
                filtered_data[(contract_id, dimension)] = quantity
            else:
                self.logger.info(f"Skipping already processed contract {contract_id} for {year}-{month:02d}")
        
        self.logger.info(f"Filtered from {len(aggregated_data)} to {len(filtered_data)} unprocessed contract records")
        return filtered_data

    def send_to_clazar(self, aggregated_data: Dict[Tuple[str, str], float], 
                      start_time: datetime, end_time: datetime) -> bool:
        """
        Send aggregated usage data to Clazar and track processed contracts.
        Includes retry logic with exponential backoff for failed contracts.
        
        Args:
            aggregated_data: Aggregated usage data
            start_time: Start time for the metering period
            end_time: End time for the metering period
            
        Returns:
            True if successful, False otherwise
        """
        if not aggregated_data:
            self.logger.info("No data to send to Clazar")
            return True
        
        # Prepare the payload grouped by contract
        contract_records = defaultdict(list)
        
        for (external_payer_id, dimension), quantity in aggregated_data.items():
            # Extract value from tuple if needed (value, price) format
            value = quantity[0] if isinstance(quantity, tuple) else quantity
            record = {
                "cloud": self.clazar_cloud,
                "contract_id": external_payer_id,
                "dimension": dimension,
                "start_time": start_time.isoformat() + "Z",
                "end_time": end_time.isoformat() + "Z",
                "quantity": str(int(value))  # Ensure it's a string of positive integer
            }
            contract_records[external_payer_id].append(record)
        
        if not self.clazar_client:
            self.logger.error("Clazar client is required for sending data")
            return False
        
        year, month = start_time.year, start_time.month
        all_success = True

        # Login to Clazar once before sending data to consider expiry of tokens
        try:
            self.clazar_client.authenticate()
        except ClazarAPIError as e:
            self.logger.error(f"Failed to login to Clazar: {e.message}")
            return False
        
        # Process each contract separately for better error handling and retry logic
        for contract_id, records in contract_records.items():
            success = False
            
            try:
                self.logger.info(f"Sending {len(records)} metering records to Clazar for contract {contract_id}")
                
                # Use the Clazar client to send data
                response_data = self.clazar_client.send_metering_data(records)
                
                # Check for errors in the response
                has_errors, errors, error_code, error_message, warnings = self.clazar_client.check_response_for_errors(response_data)
                if warnings:
                    self.logger.warning(f"Clazar returned warnings for contract {contract_id}: {warnings}")
                
                if has_errors:
                    self.logger.error(f"Failed to send data for contract {contract_id}: {error_code} - {error_message}")
                    self.logger.error(f"Errors: {errors}")
                    self.state_manager.mark_contract_month_error(contract_id, year, month, errors, error_code, 
                                                                 error_message, {"request": records})
                    all_success = False
                else:
                    # Success
                    self.logger.info(f"Successfully sent data to Clazar for contract {contract_id}")
                    self.logger.info(f"Response: {response_data}")
                    
                    # Remove from error contracts if it was previously failed
                    self.state_manager.remove_error_contract(contract_id, year, month)
                    
                    # Mark as successfully processed
                    self.state_manager.mark_contract_month_processed(contract_id, year, month)
                    success = True
                    
            except ClazarAPIError as e:
                self.logger.error(f"Clazar API error for contract {contract_id}: {e.message}")
                self.state_manager.mark_contract_month_error(contract_id, year, month, [e.message], 
                                                            "API_ERROR", e.message, {"request": records})
                all_success = False
                
            except Exception as e:
                self.logger.error(f"Unexpected error for contract {contract_id}: {e}")
                self.state_manager.mark_contract_month_error(contract_id, year, month, [str(e)], 
                                             "UNEXPECTED_ERROR", str(e), {"request": records})
                all_success = False
            
            if not success:
                all_success = False
        
        return all_success

    def retry_error_contracts(self, year: int, month: int) -> bool:
        """
        Retry sending failed contracts for a specific month.
        
        Args:
            year: Year
            month: Month
            
        Returns:
            True if all retries were successful, False otherwise
        """
        error_contracts = self.state_manager.get_error_contracts_for_retry(year, month)
        
        if not error_contracts:
            self.logger.info(f"No previously submitted contracts with error to retry for {year}-{month:02d}")
            return True
        
        self.logger.info(f"Retrying {len(error_contracts)} error contracts for {year}-{month:02d}")
        
        all_success = True
        
        for error_entry in error_contracts:
            contract_id = error_entry.get('contract_id')
            payload = error_entry.get('payload')
            
            if not contract_id or not payload:
                self.logger.warning(f"Skipping error contract with missing data: {error_entry}")
                continue
            
            # Retry this specific contract
            success = False
            
            try:
                # Extract records from payload
                records = payload.get('request', [])
                
                self.logger.info(f"Retrying contract {contract_id} for {year}-{month:02d}")
                
                # Use the Clazar client to send data (it handles retries internally)
                response_data = self.clazar_client.send_metering_data(records)
                
                # Check for errors in the response
                has_errors, errors, error_code, error_message, warnings = self.clazar_client.check_response_for_errors(response_data)
                if warnings:
                    self.logger.warning(f"Clazar returned warnings for contract {contract_id}: {warnings}")
                
                if has_errors:
                    self.logger.error(f"Retry failed for contract {contract_id}: {error_code} - {error_message}")
                    self.logger.error(f"Errors: {errors}")
                    
                    # Update error entry with new retry count
                    self.state_manager.mark_contract_month_error(
                                                 contract_id, year, month, errors, error_code, 
                                                 error_message, payload,)
                    all_success = False
                else:
                    # Success - remove from error contracts and mark as processed
                    self.logger.info(f"Successfully retried contract {contract_id}")
                    self.logger.info(f"Response: {response_data}")
                    
                    self.state_manager.remove_error_contract(contract_id, year, month)
                    self.state_manager.mark_contract_month_processed(contract_id, year, month)
                    success = True
                    
            except ClazarAPIError as e:
                self.logger.error(f"Clazar API error retrying contract {contract_id}: {e.message}")
                self.state_manager.mark_contract_month_error(contract_id, year, month, 
                                                             [e.message], "RETRY_ERROR", 
                                                             e.message, payload)
                all_success = False
                
            except Exception as e:
                self.logger.error(f"Unexpected error retrying contract {contract_id}: {e}")
                self.state_manager.mark_contract_month_error(contract_id, year, month, 
                                                             [str(e)], "RETRY_ERROR", 
                                                             str(e), payload)
                all_success = False
            
            if not success:
                all_success = False
        
        return all_success

    def process_month(self, year: int, month: int) -> bool:
        """
        Process usage data for a specific month.
        
        Args:
            year: Year to process
            month: Month to process
            max_retries: Maximum retry attempts for failed contracts
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Processing month: {year}-{month:02d} for {self.service_name}/{self.environment_type}/{self.plan_id}")
        
        # First, retry any existing error contracts
        retry_success = self.retry_error_contracts(year, month)
        
        # List all subscription files for the month
        subscription_files = self.metering_reader.list_monthly_subscription_files(year, month)
        
        if not subscription_files:
            self.logger.info(f"No subscription files found for {year}-{month:02d}")
            # Return success of retry attempts if there were any
            return retry_success
        
        # Read and aggregate all usage data
        all_usage_records = []
        for file_key in subscription_files:
            usage_records = self.metering_reader.read_s3_json_file(file_key)
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
                self.logger.error(f"No data transformations succeeded for {year}-{month:02d}. Skipping this month.")
                return False
        
        # Filter out already processed contracts
        filtered_data = self.filter_success_contracts(aggregated_data, year, month)
        
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
        send_success = self.send_to_clazar(filtered_data, start_time, end_time)
        
        # Return True only if both retry and send operations were successful
        return retry_success and send_success

    def process_next_month(self, start_month: tuple = (2025, 1)) -> bool:
        """
        Process the next pending month for a specific service configuration.
        
        Args:
            start_month: Default start month if no previous processing history
            
        Returns:
            True if processing was successful, False otherwise
        """
        self.logger.info(f"Starting processing for {self.service_name}/{self.environment_type}/{self.plan_id}")
        
        next_month = self.get_next_month_to_process(default_start_month=start_month)
        
        if next_month is None:
            self.logger.info("No more months to process, caught up!")
            return True
        
        year, month = next_month
        self.logger.info(f"Processing month: {year}-{month:02d}")
        
        success = self.process_month(year, month)
        
        if success:
            # Update state only if processing was successful
            self.state_manager.update_last_processed_month(year, month)
            self.logger.info(f"Successfully processed month {year}-{month:02d}")
        else:
            self.logger.error(f"Failed to process month {year}-{month:02d}")
        
        return success

def main_processing(processor : MeteringProcessor, default_start_year: int, default_start_month: int) -> bool:
    """Main processing function to run the metering processor."""       
    try:
        success = processor.process_next_month(
             (default_start_year, default_start_month)
        )
        
        if success:
            logging.info("Metering processing completed successfully")
            return True
        else:
            logging.error("Metering processing failed")
            return False
    except ConfigurationError as e:
        logging.error(f"Configuration error: {e}")
        return False
    except ClazarAPIError as e:
        logging.error(f"Clazar API error: {e.message}")
        return False
    except StateManagerError as e:
        logging.error(f"State manager error: {e}")
        return False
    except NoCredentialsError:
        logging.error("AWS credentials not found.")
        logging.error("Please configure AWS credentials by setting AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.")
        return False
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return False


def main():
    """Main function to run the metering processor in a continuous loop."""
    import time

    # Load and validate configuration
    try:
        config = Config()
        config.setup_logging()
        config.validate_all()
        config.print_summary()
    except ConfigurationError as e:
        logging.error(f"Error: {e}")
        sys.exit(1)

    # Initialize Omnistrate metering reader
    logging.info("Initializing Omnistrate metering reader...")
    metering_reader = OmnistrateMeteringReader(config)
    logging.info("Omnistrate metering reader initialized successfully")
    try :
        metering_reader.validate_access()
        logging.info("Omnistrate metering reader validated successfully")
    except Exception as e:
        logging.error(f"Error validating Omnistrate metering reader: {e}")
        sys.exit(1)

    # Initialize StateManager and validate access
    logging.info("Initializing state manager...")
    state_manager = StateManager(config)
    try:
        state_manager.validate_access()
        logging.info("State manager validated successfully")
    except StateManagerError as e:
        logging.error(f"Error validating state manager: {e}")
        sys.exit(1)
    
    # Initialize Clazar client and authenticate
    clazar_client = ClazarClient(config)
    try:
        clazar_client.authenticate()
        logging.info("Clazar client authenticated successfully")
    except ClazarAPIError as e:
        logging.error(f"Error authenticating with Clazar: {e.message}")
        sys.exit(1)

    # Initialize the processor
    processor = MeteringProcessor(
        config=config,
        metering_reader=metering_reader,
        state_manager=state_manager,
        clazar_client=clazar_client,
    )
    
    # Run once initially
    logging.info(f"Starting metering processor in continuous mode ({config.processing_interval_seconds}-second interval)")

    # Process next month
    default_start_year, default_start_month = config.validate_start_month()
    
    while True:
        try:
            logging.info("=" * 80)
            logging.info("Starting processing cycle at %s", time.strftime('%Y-%m-%d %H:%M:%S'))
            logging.info("=" * 80)
            
            success = main_processing(processor, default_start_year, default_start_month)
            
            if success:
                logging.info("Processing cycle completed successfully")
            else:
                logging.warning("Processing cycle completed with errors")
            
            logging.info("=" * 80)
            logging.info(f"Waiting {config.processing_interval_seconds} seconds until next cycle...")
            logging.info("=" * 80)
            
            # Sleep for configured interval
            time.sleep(config.processing_interval_seconds)
            
        except KeyboardInterrupt:
            logging.info("\nReceived interrupt signal. Shutting down gracefully...")
            sys.exit(0)
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}")
            logging.info(f"Waiting {config.processing_interval_seconds} seconds before retry...")
            time.sleep(config.processing_interval_seconds)

