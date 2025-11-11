#!/usr/bin/env python3
"""
Clazar API Client

This module provides a client for interacting with the Clazar API,
including authentication and sending metering data.
"""

import json
import logging
import time
from typing import Any, Dict, List, Tuple

import requests

from config import Config


class ClazarAPIError(Exception):
    """Exception raised for Clazar API errors."""
    
    def __init__(self, message: str, status_code: int = None, response_data: Dict = None):
        self.message = message
        self.status_code = status_code
        self.response_data = response_data
        super().__init__(self.message)


class ClazarClient:
    """Client for interacting with the Clazar API."""
    
    BASE_URL = "https://api.clazar.io"
    AUTH_ENDPOINT = "/authenticate/"
    METERING_ENDPOINT = "/metering/"
    
    def __init__(self, config: Config):
        """
        Initialize the Clazar client.
        
        Args:
            config: Configuration object containing Clazar credentials
        """
        self.client_id = config.clazar_client_id
        self.client_secret = config.clazar_client_secret
        self.access_token = None
        self.dry_run = config.dry_run
        
        self.logger = logging.getLogger(__name__)
    
    def authenticate(self) -> str:
        """
        Authenticate with Clazar and get an access token.
        
        Returns:
            Access token string
            
        Raises:
            ClazarAPIError: If authentication fails
        """
        if self.dry_run:
            self.logger.info("DRY RUN MODE: Skipping authentication")
            self.access_token = "dry_run_token"
            return "dry_run_token"
        
        if not self.client_id or not self.client_secret:
            raise ClazarAPIError("Client ID and secret are required for authentication")
        
        url = f"{self.BASE_URL}{self.AUTH_ENDPOINT}"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json"
        }
        
        try:
            self.logger.info("Authenticating with Clazar...")
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            
            if response.status_code != 200:
                raise ClazarAPIError(
                    f"Authentication failed: {response.text}",
                    status_code=response.status_code,
                    response_data=response.json() if response.text else None
                )
            
            response_data = response.json()
            access_token = response_data.get("access_token")
            
            if not access_token:
                raise ClazarAPIError("No access token received from Clazar")
            
            self.access_token = access_token
            self.logger.info("Successfully authenticated with Clazar")
            return access_token
            
        except requests.RequestException as e:
            raise ClazarAPIError(f"Network error during authentication: {e}")
    
    def send_metering_data(self, records: List[Dict], max_retries: int = 5) -> Dict:
        """
        Send metering data to Clazar.
        
        Args:
            records: List of metering records to send
            max_retries: Maximum number of retry attempts
            
        Returns:
            Response data from Clazar API
            
        Raises:
            ClazarAPIError: If the API call fails after all retries
        """
        if not records:
            self.logger.warning("No records to send")
            return {"results": []}
        
        if not self.access_token and not self.dry_run:
            raise ClazarAPIError("Access token is required for sending metering data")
        
        payload = {"request": records}
        
        if self.dry_run:
            self.logger.info("DRY RUN MODE: Would send the following payload to Clazar:")
            self.logger.info(f"URL: {self.BASE_URL}{self.METERING_ENDPOINT}")
            self.logger.info(f"Payload: {json.dumps(payload, indent=2)}")
            self.logger.info("DRY RUN MODE: Skipping actual API call")
            
            # Return a mock successful response
            return {
                "results": [
                    {"status": "success", "message": "Dry run mode"}
                    for _ in records
                ]
            }
        
        url = f"{self.BASE_URL}{self.METERING_ENDPOINT}"
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }
        
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    # Exponential backoff: 2^attempt seconds
                    wait_time = 2 ** attempt
                    self.logger.info(f"Retrying after {wait_time}s delay (attempt {attempt + 1}/{max_retries + 1})...")
                    time.sleep(wait_time)
                
                self.logger.info(f"Sending {len(records)} metering records to Clazar (attempt {attempt + 1}/{max_retries + 1})")
                response = requests.post(url, json=payload, headers=headers, timeout=30)
                
                if response.status_code != 200:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    self.logger.warning(error_msg)
                    
                    # Only retry on server errors (5xx) or rate limiting (429)
                    if response.status_code >= 500 or response.status_code == 429:
                        if attempt < max_retries:
                            continue
                    
                    # Don't retry on client errors (4xx except 429)
                    raise ClazarAPIError(
                        error_msg,
                        status_code=response.status_code,
                        response_data=response.json() if response.text else None
                    )
                
                response_data = response.json()
                
                if "results" not in response_data:
                    raise ClazarAPIError("Unexpected response format from Clazar API")
                
                self.logger.info(f"Successfully sent {len(records)} metering records to Clazar")
                self.logger.debug(f"Response: {response_data}")
                
                return response_data
                
            except requests.Timeout as e:
                self.logger.warning(f"Request timeout on attempt {attempt + 1}/{max_retries + 1}: {e}")
                if attempt < max_retries:
                    continue
                raise ClazarAPIError(f"Request timed out after {max_retries + 1} attempts: {e}")
            
            except requests.ConnectionError as e:
                self.logger.warning(f"Connection error on attempt {attempt + 1}/{max_retries + 1}: {e}")
                if attempt < max_retries:
                    continue
                raise ClazarAPIError(f"Connection failed after {max_retries + 1} attempts: {e}")
            
            except requests.RequestException as e:
                self.logger.warning(f"Network error on attempt {attempt + 1}/{max_retries + 1}: {e}")
                if attempt < max_retries:
                    continue
                raise ClazarAPIError(f"Network error after {max_retries + 1} attempts: {e}")
            
            except ClazarAPIError:
                # Re-raise ClazarAPIError as-is (already has proper error info)
                raise
            
            except Exception as e:
                self.logger.error(f"Unexpected error on attempt {attempt + 1}/{max_retries + 1}: {e}")
                if attempt < max_retries:
                    continue
                raise ClazarAPIError(f"Unexpected error after {max_retries + 1} attempts: {e}")
        
        # Should not reach here, but just in case
        raise ClazarAPIError(f"Failed to send metering data after {max_retries + 1} attempts")
    
    def check_response_for_errors(self, response_data: Dict) -> Tuple[bool, List[Any], str, str, List[Dict[str, Any]]]:
        """
        Check Clazar API response for errors.
        
        Args:
            response_data: Response data from Clazar API
            
        Returns:
            Tuple of (has_errors, error_list, error_code, error_message, warnings)
        """
        has_errors = False
        errors: List[Any] = []
        error_code = "API_ERROR"
        error_message = "Unknown error"
        warnings: List[Dict[str, Any]] = []
        
        for result in response_data.get("results", []):
            if "errors" in result and result["errors"]:
                has_errors = True
                if isinstance(result["errors"], list):
                    errors.extend(result["errors"])
                else:
                    errors.append(str(result["errors"]))
                
                error_code = result.get('code', 'API_ERROR')
                error_message = result.get('message', 'Unknown error')
            elif "status" in result and result["status"] != "success":
                warnings.append(result)
                # Log warning but don't treat as error
                self.logger.warning(
                    f"Sent data to Clazar with warnings: status={result['status']}. "
                    "Please check if the dimensions are registered in Clazar."
                )
        
        return has_errors, errors, error_code, error_message, warnings
