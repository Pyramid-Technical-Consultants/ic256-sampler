"""Device management and setup utilities.

This module provides reusable functions for device setup, channel creation,
and thread management to reduce code duplication.
"""
from typing import Dict, Optional, Any, Callable, Tuple
from datetime import datetime
import threading
from .igx_client import IGXWebsocketClient
from .data_collection import collect_data
from .device_paths import IC256_45_PATHS, TX2_PATHS
from .utils import is_valid_device


class DeviceConfig:
    """Configuration for a device type."""
    
    def __init__(
        self,
        device_name: str,
        device_type: str,
        channel_creator: Callable[[IGXWebsocketClient], Dict[str, Any]],
        env_channel_creator: Optional[Callable[[IGXWebsocketClient], Dict[str, Any]]] = None,
        filename_prefix: str = "",
    ):
        self.device_name = device_name
        self.device_type = device_type
        self.channel_creator = channel_creator
        self.env_channel_creator = env_channel_creator
        self.filename_prefix = filename_prefix or device_name


def create_ic256_channels(client: IGXWebsocketClient) -> Dict[str, Any]:
    """Create channel dictionary for IC256 device."""
    return {
        "mean_channel_a": client.field(IC256_45_PATHS["adc"]["gaussian_fit_a_mean"]),
        "sigma_channel_a": client.field(IC256_45_PATHS["adc"]["gaussian_fit_a_sigma"]),
        "mean_channel_b": client.field(IC256_45_PATHS["adc"]["gaussian_fit_b_mean"]),
        "sigma_channel_b": client.field(IC256_45_PATHS["adc"]["gaussian_fit_b_sigma"]),
        "primary_channel": client.field(IC256_45_PATHS["adc"]["primary_dose"]),
        "channel_sum": client.field(IC256_45_PATHS["adc"]["channel_sum"]),
        "external_trigger": client.field(IC256_45_PATHS["adc"]["gate_signal"]),
    }


def create_ic256_env_channels(client: IGXWebsocketClient) -> Dict[str, Any]:
    """Create environment channel dictionary for IC256 device."""
    return {
        "temperature": client.field(IC256_45_PATHS["environmental_sensor"]["temperature"]),
        "humidity": client.field(IC256_45_PATHS["environmental_sensor"]["humidity"]),
        "pressure": client.field(IC256_45_PATHS["environmental_sensor"]["pressure"]),
        "connected": client.field(IC256_45_PATHS["environmental_sensor"]["state"]),
    }


def create_tx2_channels(client: IGXWebsocketClient) -> Dict[str, Any]:
    """Create channel dictionary for TX2 device."""
    return {
        "probe_a": client.field(TX2_PATHS["adc"]["channel_5"]),
        "probe_b": client.field(TX2_PATHS["adc"]["channel_1"]),
        "fr2": client.field(TX2_PATHS["adc"]["fr2"]),
    }


# Device configurations
IC256_CONFIG = DeviceConfig(
    device_name="IC256-42/35",
    device_type="IC256",
    channel_creator=create_ic256_channels,
    env_channel_creator=create_ic256_env_channels,
    filename_prefix="IC256_42x35",
)

TX2_CONFIG = DeviceConfig(
    device_name="TX2",
    device_type="TX2",
    channel_creator=create_tx2_channels,
    env_channel_creator=None,
    filename_prefix="TX2",
)


def setup_device_thread(
    config: DeviceConfig,
    ip_address: str,
    sampling_rate: int,
    date: str,
    time_str: str,
    note: str,
    save_folder: str,
    stop_event: threading.Event,
    log_callback: Optional[Callable[[str, str], None]] = None,
) -> Optional[threading.Thread]:
    """Set up a device and create data collection thread.
    
    Args:
        config: Device configuration
        ip_address: IP address of the device
        sampling_rate: Sampling rate in Hz
        date: Date string for filename (YYYYMMDD)
        time_str: Time string for filename (HHMMSS)
        note: Note string for data collection
        save_folder: Folder to save data files
        stop_event: Threading event to signal stop
        log_callback: Optional callback for logging (message, level)
        
    Returns:
        Thread object for data collection, or None if setup failed
    """
    if not ip_address:
        return None
    
    # Validate device
    if not is_valid_device(ip_address, config.device_type):
        if log_callback:
            log_callback(
                f"{config.device_name} device at {ip_address} validation failed (skipping)",
                "WARNING"
            )
        return None
    
    try:
        if log_callback:
            log_callback(f"Connecting to {config.device_name} device at {ip_address}", "INFO")
        
        client = IGXWebsocketClient(ip_address)
        channels = config.channel_creator(client)
        env_channels = config.env_channel_creator(client) if config.env_channel_creator else None
        
        file_name = f"{config.filename_prefix}-{date}-{time_str}.csv"
        
        thread = threading.Thread(
            target=collect_data,
            name=f"{config.device_type.lower()}_device",
            daemon=True,
            args=(
                client,
                channels,
                env_channels,
                file_name,
                config.device_type.lower(),
                note,
                save_folder,
                stop_event,
                sampling_rate,
            ),
        )
        
        if log_callback:
            log_callback(f"{config.device_name} device thread created: {file_name}", "INFO")
        
        return thread
    except Exception as e:
        error_msg = f"Failed to set up {config.device_name} at {ip_address}: {str(e)}"
        if log_callback:
            log_callback(error_msg, "ERROR")
        print(error_msg)
        return None


def get_timestamp_strings() -> Tuple[str, str]:
    """Get current date and time strings for filenames.
    
    Returns:
        Tuple of (date_string, time_string) in format (YYYYMMDD, HHMMSS)
    """
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%H%M%S")
