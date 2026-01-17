"""File path generation utilities for data collection."""

from typing import Tuple
from .device_manager import get_timestamp_strings, IC256_CONFIG, TX2_CONFIG, DeviceConfig


def generate_file_path(
    save_folder: str,
    device_config: DeviceConfig,
    date: str,
    time_str: str,
) -> str:
    """Generate a file path for data collection output.
    
    Args:
        save_folder: Directory to save the file
        device_config: Device configuration (determines filename prefix)
        date: Date string in YYYYMMDD format
        time_str: Time string in HHMMSS format
        
    Returns:
        Full file path string
    """
    file_name = f"{device_config.filename_prefix}-{date}-{time_str}.csv"
    return f"{save_folder}/{file_name}"


def get_file_path_for_primary_device(
    save_folder: str,
    devices_added: list[str],
) -> Tuple[str, DeviceConfig]:
    """Get file path for the primary device.
    
    Uses IC256 if available, otherwise TX2.
    
    Args:
        save_folder: Directory to save the file
        devices_added: List of device names that were added
        
    Returns:
        Tuple of (file_path, device_config)
    """
    date, time_str = get_timestamp_strings()
    
    # Use IC256 if available, otherwise TX2
    primary_device_config = IC256_CONFIG if IC256_CONFIG.device_name in devices_added else TX2_CONFIG
    
    file_path = generate_file_path(save_folder, primary_device_config, date, time_str)
    
    return file_path, primary_device_config
