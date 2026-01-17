"""
Device IO Path Configuration

This module contains all device IO paths used throughout the application.
Update these paths when the device API changes.

All paths are relative to the device root and should be used with the device client's field() method
or constructed into full HTTP URLs as needed.

To change device names (e.g., from "ic256" to "ic256_42"), update the constants below.
Note: The API changed from "ic256_45" to "ic256" - all API paths now use "ic256".
"""

# Device Name Constants - Update these if device names change in the API
IC256_45_DEVICE_NAME = "ic256"
TX2_DEVICE_NAME = "tx2"

# IC256-45 Device Paths (built dynamically using IC256_45_DEVICE_NAME)
IC256_45_PATHS = {
    # ADC Channels - Gaussian Fit
    "adc": {
        "primary_dose": f"/{IC256_45_DEVICE_NAME}/dose_adc/channel/value",
        "channel_sum": f"/{IC256_45_DEVICE_NAME}/adc/channel_sum/value",
        "gaussian_fit_a_mean": f"/{IC256_45_DEVICE_NAME}/adc/gaussian_fit_a/mean/value",
        "gaussian_fit_a_sigma": f"/{IC256_45_DEVICE_NAME}/adc/gaussian_fit_a/standard_deviation/value",
        "gaussian_fit_b_mean": f"/{IC256_45_DEVICE_NAME}/adc/gaussian_fit_b/mean/value",
        "gaussian_fit_b_sigma": f"/{IC256_45_DEVICE_NAME}/adc/gaussian_fit_b/standard_deviation/value",
        "integration_frequency": f"/{IC256_45_DEVICE_NAME}/adc/integration_frequency/value",
        "gate_signal": f"/{IC256_45_DEVICE_NAME}/gate_signal/value",
        "sample_frequency": f"/{IC256_45_DEVICE_NAME}/adc/sample_frequency/value",
    },
    # Single Dose Module ADC
    "single_dose_module": {
        "sample_frequency": f"/{IC256_45_DEVICE_NAME}/dose_adc/sample_frequency/value",
        "user_units": f"/{IC256_45_DEVICE_NAME}/dose_adc/user_units/value",
    },
    # High Voltage
    "high_voltage": {
        "monitor_voltage_internal": f"/{IC256_45_DEVICE_NAME}/high_voltage/monitor_voltage_internal/value",
    },
    # Environmental Sensor (I2C2)
    "environmental_sensor": {
        "temperature": f"/{IC256_45_DEVICE_NAME}/i2c2/environmental_sensor/temperature/value",
        "humidity": f"/{IC256_45_DEVICE_NAME}/i2c2/environmental_sensor/humidity/value",
        "pressure": f"/{IC256_45_DEVICE_NAME}/i2c2/environmental_sensor/pressure/value",
        "state": f"/{IC256_45_DEVICE_NAME}/i2c2/environmental_sensor/state/value",
    },
    # HTTP IO Endpoints (used with /io/ prefix)
    "io": {
        "fan_out": f"/io/{IC256_45_DEVICE_NAME}/fan_out/value.json",
        "gate_in_signal_conversion_mode": f"/io/{IC256_45_DEVICE_NAME}/gate_in_signal_conversion/mode/value.json",
        "gate_in_signal_conversion_process_signal": f"/io/{IC256_45_DEVICE_NAME}/gate_in_signal_conversion/process_signal/value.json",
    },
}

# TX2 Device Paths (built dynamically using TX2_DEVICE_NAME)
TX2_PATHS = {
    # ADC Channels
    "adc": {
        "channel_5": f"/{TX2_DEVICE_NAME}/adc/channel_5/value",
        "channel_1": f"/{TX2_DEVICE_NAME}/adc/channel_1/value",
        "fr2": f"/{TX2_DEVICE_NAME}/adc/fr2/value",
        "channel_5_units": f"/{TX2_DEVICE_NAME}/adc/channel_5/units",
        "conversion_frequency": f"/{TX2_DEVICE_NAME}/adc/conversion_frequency/value",
        "sample_frequency": f"/{TX2_DEVICE_NAME}/adc/sample_frequency/value",
    },
}

# Common/Admin Paths
ADMIN_PATHS = {
    "device_type": "/io/admin/device_type/value.json",
}

# Helper functions to get paths
def _get_path(paths_dict: dict, category: str = None, key: str = None) -> str:
    """Generic helper to get a path from a paths dictionary.
    
    Args:
        paths_dict: Dictionary containing paths (either nested or flat)
        category: Optional category name for nested dictionaries
        key: Key to look up
        
    Returns:
        Path string
        
    Raises:
        KeyError: If category or key doesn't exist
    """
    if category:
        if category not in paths_dict:
            raise KeyError(f"Invalid category '{category}'. Available: {list(paths_dict.keys())}")
        paths_dict = paths_dict[category]
    
    if key not in paths_dict:
        available = list(paths_dict.keys())
        error_msg = f"Invalid key '{key}'" + (f" in category '{category}'" if category else "")
        raise KeyError(f"{error_msg}. Available: {available}")
    return paths_dict[key]


def get_ic256_45_path(category: str, key: str) -> str:
    """Get an IC256-45 path by category and key."""
    return _get_path(IC256_45_PATHS, category, key)


def get_tx2_path(category: str, key: str) -> str:
    """Get a TX2 path by category and key."""
    return _get_path(TX2_PATHS, category, key)


def get_admin_path(key: str) -> str:
    """Get an admin path by key."""
    return _get_path(ADMIN_PATHS, key=key)


def build_http_url(ip_address: str, io_path: str) -> str:
    """Build a full HTTP URL from an IP address and IO path.
    
    Args:
        ip_address: IP address (IPv4)
        io_path: IO path (should start with /)
        
    Returns:
        Full HTTP URL string
        
    Raises:
        ValueError: If ip_address is empty or io_path doesn't start with /
    """
    if not ip_address:
        raise ValueError("IP address cannot be empty")
    if not io_path.startswith("/"):
        raise ValueError(f"IO path must start with '/', got: {io_path}")
    return f"http://{ip_address}{io_path}"
