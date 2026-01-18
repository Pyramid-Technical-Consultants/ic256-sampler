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
        response = requests.get(url, timeout=2)
        
        if response.status_code != 200:
            return False
        
        actual_device_name = response.text.replace('"', "").upper()
        expected_name = device_name.upper()
        
        # For IC256 devices, check if device name starts with "IC256"
        if expected_name.startswith("IC256"):
            return actual_device_name.startswith("IC256")
        # For other devices (like TX2), check for exact match
        return expected_name in actual_device_name
        
    except (requests.exceptions.RequestException, ValueError, AttributeError):
        return False
