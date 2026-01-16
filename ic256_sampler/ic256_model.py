"""IC256 Device Model.

This module encapsulates all IC256-specific conversion logic, magic numbers,
device-specific behavior, and device setup. This keeps VirtualDatabase generic and device-agnostic.
"""

from typing import Callable, Any, Dict
from .virtual_database import Converter, ColumnDefinition, ChannelPolicy
from .igx_client import IGXWebsocketClient
from .device_paths import IC256_45_PATHS


# IC256-specific constants
MEAN_OFFSET = 128.5  # Offset for mean value conversion
X_STRIP_OFFSET = 1.65  # mm per count for X axis
Y_STRIP_OFFSET = 1.38  # mm per count for Y axis
ERROR_GAUSS = -10000  # Error value for gaussian fields


def convert_mean_ic256(value: Any, x_axis: bool = True) -> float:
    """Convert IC256 mean value from device units to millimeters.
    
    Args:
        value: Raw mean value from device
        x_axis: True for X axis, False for Y axis
        
    Returns:
        Converted value in millimeters
    """
    if value is None or value == "":
        return ERROR_GAUSS
    
    try:
        numeric_value = float(value)
    except (ValueError, TypeError):
        return ERROR_GAUSS
    
    offset = X_STRIP_OFFSET if x_axis else Y_STRIP_OFFSET
    return (numeric_value - MEAN_OFFSET) * offset


def convert_sigma_ic256(value: Any, x_axis: bool = True) -> float:
    """Convert IC256 sigma value from device units to millimeters.
    
    Args:
        value: Raw sigma value from device
        x_axis: True for X axis, False for Y axis
        
    Returns:
        Converted value in millimeters
    """
    if value is None or value == "":
        return ERROR_GAUSS
    
    try:
        numeric_value = float(value)
    except (ValueError, TypeError):
        return ERROR_GAUSS
    
    offset = X_STRIP_OFFSET if x_axis else Y_STRIP_OFFSET
    return numeric_value * offset


class IC256Model:
    """Model for IC256 device with all device-specific logic.
    
    This class encapsulates:
    - Conversion functions for IC256 channels
    - Column definitions for IC256
    - Device-specific constants and magic numbers
    """
    
    @staticmethod
    def get_gaussian_x_mean_converter() -> Converter:
        """Get converter for X mean to millimeters."""
        return convert_mean_ic256
    
    @staticmethod
    def get_gaussian_x_sigma_converter() -> Converter:
        """Get converter for X sigma to millimeters."""
        return lambda v: convert_sigma_ic256(v, x_axis=True)
    
    @staticmethod
    def get_gaussian_y_mean_converter() -> Converter:
        """Get converter for Y mean to millimeters."""
        return lambda v: convert_mean_ic256(v, x_axis=False)
    
    @staticmethod
    def get_gaussian_y_sigma_converter() -> Converter:
        """Get converter for Y sigma to millimeters."""
        return lambda v: convert_sigma_ic256(v, x_axis=False)
    
    @staticmethod
    def create_columns(reference_channel: str) -> list[ColumnDefinition]:
        """Create column definitions for IC256 device.
        
        Args:
            reference_channel: Channel path to use as reference
            
        Returns:
            List of ColumnDefinition objects in CSV order
        """
        from .device_paths import IC256_45_PATHS
        
        return [
            # Timestamp is computed, not from a channel
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            
            # Gaussian fit channels - synchronized (arrive together) with converters
            ColumnDefinition(
                name="X centroid (mm)",
                channel_path=IC256_45_PATHS["adc"]["gaussian_fit_a_mean"],
                policy=ChannelPolicy.SYNCHRONIZED,
                converter=IC256Model.get_gaussian_x_mean_converter(),
            ),
            ColumnDefinition(
                name="X sigma (mm)",
                channel_path=IC256_45_PATHS["adc"]["gaussian_fit_a_sigma"],
                policy=ChannelPolicy.SYNCHRONIZED,
                converter=IC256Model.get_gaussian_x_sigma_converter(),
            ),
            ColumnDefinition(
                name="Y centroid (mm)",
                channel_path=IC256_45_PATHS["adc"]["gaussian_fit_b_mean"],
                policy=ChannelPolicy.SYNCHRONIZED,
                converter=IC256Model.get_gaussian_y_mean_converter(),
            ),
            ColumnDefinition(
                name="Y sigma (mm)",
                channel_path=IC256_45_PATHS["adc"]["gaussian_fit_b_sigma"],
                policy=ChannelPolicy.SYNCHRONIZED,
                converter=IC256Model.get_gaussian_y_sigma_converter(),
            ),
            
            # Primary dose - interpolated (may have different rate)
            ColumnDefinition(
                name="Dose",
                channel_path=IC256_45_PATHS["adc"]["primary_dose"],
                policy=ChannelPolicy.INTERPOLATED,
            ),
            
            # Channel sum - synchronized (arrives with gaussian)
            ColumnDefinition(
                name="Channel Sum",
                channel_path=IC256_45_PATHS["adc"]["channel_sum"],
                policy=ChannelPolicy.SYNCHRONIZED,
            ),
            
            # External trigger - asynchronous (snap to nearest)
            ColumnDefinition(
                name="External trigger",
                channel_path=IC256_45_PATHS["adc"]["gate_signal"],
                policy=ChannelPolicy.ASYNCHRONOUS,
            ),
            
            # Environment channels - interpolated (slow updates)
            ColumnDefinition(
                name="Temperature (℃)",
                channel_path=IC256_45_PATHS["environmental_sensor"]["temperature"],
                policy=ChannelPolicy.INTERPOLATED,
            ),
            ColumnDefinition(
                name="Humidity (%rH)",
                channel_path=IC256_45_PATHS["environmental_sensor"]["humidity"],
                policy=ChannelPolicy.INTERPOLATED,
            ),
            ColumnDefinition(
                name="Pressure (hPa)",
                channel_path=IC256_45_PATHS["environmental_sensor"]["pressure"],
                policy=ChannelPolicy.INTERPOLATED,
            ),
            
            # Note is computed, not from a channel
            ColumnDefinition(name="Note", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
        ]
    
    @staticmethod
    def get_error_gauss() -> int:
        """Get error value for gaussian fields."""
        return ERROR_GAUSS
    
    @staticmethod
    def setup_device(device_client: IGXWebsocketClient, frequency: int) -> None:
        """Set up IC256 device sampling frequencies.
        
        Args:
            device_client: IGXWebsocketClient connected to IC256 device
            frequency: Sampling frequency in Hz
        """
        ic256_fields = {
            "primary_sample": device_client.field(
                IC256_45_PATHS["single_dose_module"]["sample_frequency"]
            ),
            "integration_freq": device_client.field(
                IC256_45_PATHS["adc"]["integration_frequency"]
            ),
            "sample_freq": device_client.field(
                IC256_45_PATHS["adc"]["sample_frequency"]
            ),
        }
        device_client.sendSubscribeFields({ic256_fields[field]: False for field in ic256_fields})
        ic256_fields["primary_sample"].setValue(frequency)
        ic256_fields["integration_freq"].setValue(frequency)
        ic256_fields["sample_freq"].setValue(frequency)
    
    @staticmethod
    def get_field_to_path_mapping() -> Dict[str, str]:
        """Get the mapping from field names to channel paths for IC256.
        
        Returns:
            Dictionary mapping field names to channel paths
        """
        return {
            "mean_channel_a": IC256_45_PATHS["adc"]["gaussian_fit_a_mean"],
            "sigma_channel_a": IC256_45_PATHS["adc"]["gaussian_fit_a_sigma"],
            "mean_channel_b": IC256_45_PATHS["adc"]["gaussian_fit_b_mean"],
            "sigma_channel_b": IC256_45_PATHS["adc"]["gaussian_fit_b_sigma"],
            "primary_channel": IC256_45_PATHS["adc"]["primary_dose"],
            "channel_sum": IC256_45_PATHS["adc"]["channel_sum"],
            "external_trigger": IC256_45_PATHS["adc"]["gate_signal"],
            # Environmental sensor channels (treated same as any other channel)
            "temperature": IC256_45_PATHS["environmental_sensor"]["temperature"],
            "humidity": IC256_45_PATHS["environmental_sensor"]["humidity"],
            "pressure": IC256_45_PATHS["environmental_sensor"]["pressure"],
            "env_connected": IC256_45_PATHS["environmental_sensor"]["state"],
        }
    
    @staticmethod
    def get_reference_channel() -> str:
        """Get the reference channel path for IC256.
        
        Returns:
            Channel path to use as timing reference
        """
        return IC256_45_PATHS["adc"]["channel_sum"]