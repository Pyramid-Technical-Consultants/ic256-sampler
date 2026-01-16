"""Utility functions for device validation and network operations.

This module provides helper functions for IP address validation and
device type checking.
"""
import ipaddress
import requests
from typing import Optional
from .device_paths import ADMIN_PATHS


def is_valid_ipv4(ip: str) -> bool:
    """Check if a string is a valid IPv4 address.
    
    Args:
        ip: IP address string to validate
        
    Returns:
        True if valid IPv4 address, False otherwise
    """
    try:
        ipaddress.IPv4Address(ip)
        return True
    except ipaddress.AddressValueError:
        return False


def is_valid_device(ip: str, device_name: str) -> bool:
    """Validate if a device at the given IP address matches the expected device name.
    
    For IC256 devices, checks if device name starts with 'IC256'.
    For other devices, checks for exact match.
    
    Args:
        ip: IP address of the device to validate
        device_name: Expected device name (e.g., "IC256", "TX2")
        
    Returns:
        True if device matches expected name, False otherwise
    """
    if not is_valid_ipv4(ip):
        return False
    try:
        url = f"http://{ip}{ADMIN_PATHS['device_type']}"
        get_device_name = ""
        response = requests.get(url, timeout=2)  # Timeout set to 2 seconds
        if response.status_code == 200:
            get_device_name = response.text.replace('"', "").upper()
        
        # For IC256 devices, check if device name starts with "IC256"
        if device_name.upper().startswith("IC256"):
            return get_device_name.startswith("IC256")
        else:
            # For other devices (like TX2), check for exact match
            return device_name.upper() in get_device_name
    except (requests.exceptions.RequestException, requests.exceptions.Timeout, 
            requests.exceptions.ConnectionError, ValueError, AttributeError):
        return False
