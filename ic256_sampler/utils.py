"""Utility functions for device validation and network operations."""

import ipaddress
import requests
from .device_paths import ADMIN_PATHS


def is_valid_ipv4(ip: str) -> bool:
    """Check if a string is a valid IPv4 address."""
    try:
        ipaddress.IPv4Address(ip)
        return True
    except ipaddress.AddressValueError:
        return False


def is_valid_device(ip: str, device_name: str) -> bool:
    """Validate if a device at the given IP address matches the expected device name."""
    if not is_valid_ipv4(ip):
        return False

    try:
        url = f"http://{ip}{ADMIN_PATHS['device_type']}"
        response = requests.get(url, timeout=2)

        if response.status_code != 200:
            return False

        actual_device_name = response.text.replace('"', "").upper()
        expected_name = device_name.upper()

        if expected_name.startswith("IC256"):
            return actual_device_name.startswith("IC256")
        return expected_name in actual_device_name

    except (requests.exceptions.RequestException, ValueError, AttributeError):
        return False
