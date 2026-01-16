"""
Device IO Path Configuration

This module contains all device IO paths used throughout the application.
Update these paths when the device API changes.

All paths are relative to the device root and should be used with the device client's field() method
or constructed into full HTTP URLs as needed.

To change device names (e.g., from "ic256_45" to "ic256_42"), update the constants below.
"""

# Device Name Constants - Update these if device names change in the API
IC256_45_DEVICE_NAME = "ic256_45"
TX2_DEVICE_NAME = "tx2"

# IC256-45 Device Paths (built dynamically using IC256_45_DEVICE_NAME)
IC256_45_PATHS = {
    # ADC Channels - Gaussian Fit
    "adc": {
        "primary_dose": f"/{IC256_45_DEVICE_NAME}/single_dose_module_adc/channel_/value",
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
        "sample_frequency": f"/{IC256_45_DEVICE_NAME}/single_dose_module_adc/sample_frequency/value",
        "user_units": f"/{IC256_45_DEVICE_NAME}/single_dose_module_adc/user_units/value",
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
def get_ic256_45_path(category: str, key: str) -> str:
    """Get an IC256-45 path by category and key."""
    return IC256_45_PATHS[category][key]


def get_tx2_path(category: str, key: str) -> str:
    """Get a TX2 path by category and key."""
    return TX2_PATHS[category][key]


def get_admin_path(key: str) -> str:
    """Get an admin path by key."""
    return ADMIN_PATHS[key]


def build_http_url(ip_address: str, io_path: str) -> str:
    """Build a full HTTP URL from an IP address and IO path."""
    return f"http://{ip_address}{io_path}"
