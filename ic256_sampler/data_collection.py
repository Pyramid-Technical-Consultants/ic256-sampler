"""Data collection module for IC256 and TX2 devices.

This module handles asynchronous data collection from device channels,
timestamp alignment, and CSV file writing with proper error handling.
"""
import time
import csv
import threading
import bisect
import os
from typing import Dict, List, Tuple, Optional, Any, Union
from collections import deque
from .igx_client import IGXWebsocketClient
from .device_paths import IC256_45_PATHS, TX2_PATHS

# Error value constants
ERROR_VALUE: int = -1
ERROR_GAUSS: int = -10000

# Strip pitch constants (mm)
X_STRIP_OFFSET: float = 1.65
Y_STRIP_OFFSET: float = 1.38
MEAN_OFFSET: float = 128.5  # Offset for mean value conversion

# Data collection timing constants
UPDATE_INTERVAL: float = 0.002  # seconds between data collection updates
TIME_BIN_SIZE: float = 0.005  # seconds - group data within this time window for bundling
ALIGNMENT_WINDOW: float = 0.01  # seconds - maximum time difference for aligning timestamps
ALIGNMENT_WINDOW_MULTIPLIER: int = 2  # Multiplier for lenient alignment matching

# Buffer management constants
MAX_BUFFER_TIME: float = 2.0  # seconds - keep last N seconds of data in buffer
BUFFER_CLEANUP_MULTIPLIER: float = 2.0  # Multiplier for buffer cleanup cutoff
BUFFER_CLEANUP_INTERVAL: int = 50  # Cleanup buffers every N loops (performance optimization)

# CSV writing constants
# At 6000 Hz: 300 rows = 50ms, 3000 rows = 500ms
FLUSH_INTERVAL: int = 6000  # Flush file every N rows for real-time visibility (~50ms at 6000 Hz)
FSYNC_INTERVAL: int = 12000  # Force OS sync every N rows (~500ms at 6000 Hz, fsync is expensive)
WRITE_TIMEOUT: float = 1.0  # Write partial data if no complete rows after N seconds
MIN_FIELDS_RATIO: float = 0.5  # Minimum ratio of fields required to write a row

# Debug logging intervals
DEBUG_LOG_INTERVAL: int = 500  # Log missing channels every N loops
STATUS_LOG_INTERVAL: int = 1000  # Log status every N loops
MISSING_FIELDS_LOG_INTERVAL: int = 100  # Log missing fields every N loops

# Data point type: (value, elapsed_time_seconds, timestamp_nanoseconds)
DataPoint = Tuple[Any, float, int]
ChannelBuffer = deque[DataPoint]  # Use deque for O(1) append and efficient popleft
ChannelBuffers = Dict[str, ChannelBuffer]

# Performance optimization: Cache for finding closest points
class _SortedBufferCache:
    """Cache for sorted buffer data to avoid repeated sorting."""
    def __init__(self):
        self.sorted_data: Optional[List[DataPoint]] = None
        self.last_size: int = 0
        self.channel_name: Optional[str] = None
    
    def get_sorted(self, channel_name: str, buffer: ChannelBuffer) -> List[DataPoint]:
        """Get sorted buffer data, using cache if buffer hasn't changed."""
        if (self.channel_name == channel_name and 
            self.sorted_data is not None and 
            len(buffer) == self.last_size):
            return self.sorted_data
        
        # Cache miss - sort and cache
        self.sorted_data = sorted(buffer, key=lambda x: x[1])
        self.last_size = len(buffer)
        self.channel_name = channel_name
        return self.sorted_data


def convert_mean(value: Union[float, int, str, None], x_axis: bool = True) -> float:
    """Convert mean value to millimeters.
    
    Args:
        value: The mean value to convert (can be None, empty string, or numeric)
        x_axis: If True, use X strip offset; if False, use Y strip offset
        
    Returns:
        Converted value in millimeters, or ERROR_GAUSS if conversion fails
    """
    if value is None or value == ERROR_VALUE or value == "":
        return ERROR_GAUSS
    
    try:
        numeric_value = float(value)
    except (ValueError, TypeError):
        return ERROR_GAUSS
    
    offset = X_STRIP_OFFSET if x_axis else Y_STRIP_OFFSET
    return (numeric_value - MEAN_OFFSET) * offset


def convert_sigma(value: Union[float, int, str, None], x_axis: bool = True) -> float:
    """Convert sigma value to millimeters.
    
    Args:
        value: The sigma value to convert (can be None, empty string, or numeric)
        x_axis: If True, use X strip offset; if False, use Y strip offset
        
    Returns:
        Converted value in millimeters, or ERROR_GAUSS if conversion fails
    """
    if value is None or value == ERROR_VALUE or value == "":
        return ERROR_GAUSS
    
    try:
        numeric_value = float(value)
    except (ValueError, TypeError):
        return ERROR_GAUSS
    
    offset = X_STRIP_OFFSET if x_axis else Y_STRIP_OFFSET
    return numeric_value * offset


def process_gaussian_values(
    x_mean: Union[float, int, str, None],
    x_sigma: Union[float, int, str, None],
    y_mean: Union[float, int, str, None],
    y_sigma: Union[float, int, str, None],
) -> Tuple[float, float, float, float]:
    """Process gaussian fit values, converting all to millimeters.
    
    Args:
        x_mean: X-axis mean value
        x_sigma: X-axis sigma value
        y_mean: Y-axis mean value
        y_sigma: Y-axis sigma value
        
    Returns:
        Tuple of (x_mean_mm, x_sigma_mm, y_mean_mm, y_sigma_mm)
    """
    return (
        convert_mean(x_mean, x_axis=True),
        convert_sigma(x_sigma, x_axis=True),
        convert_mean(y_mean, x_axis=False),
        convert_sigma(y_sigma, x_axis=False),
    )


def get_environment_data(
    device_client: IGXWebsocketClient,
    env_channels: Optional[Dict[str, Any]],
) -> Tuple[List[str], str]:
    """Read environment sensor data and primary channel units.
    
    Args:
        device_client: The websocket client for the device
        env_channels: Dictionary of environment channel fields, or None
        
    Returns:
        Tuple of (environment_data_list, primary_units_string)
        environment_data_list contains [temperature, humidity, pressure]
    """
    environment: List[str] = ["", "", ""]
    primary_units: str = ""
    
    if not env_channels:
        return environment, primary_units

    try:
        device_client.sendSubscribeFields(
            {env_channels[field]: False for field in env_channels}
        )
        device_client.updateSubscribedFields()

        if env_channels["connected"].getValue() == "ready":
            environment = [
                env_channels["temperature"].getValue(),
                env_channels["humidity"].getValue(),
                env_channels["pressure"].getValue(),
            ]

        units_field = device_client.field(
            IC256_45_PATHS["single_dose_module"]["user_units"]
        )
        device_client.sendSubscribeFields({units_field: False})
        device_client.updateSubscribedFields()
        primary_units = units_field.getValue() or ""
    except Exception as e:
        print(f"Error reading environment data: {e}")
        # Return defaults on error
    
    return environment, primary_units


def get_headers(device_name: str, primary_units: str = "", probe_units: str = "") -> List[str]:
    """Get CSV headers for the specified device type.
    
    Args:
        device_name: Name of the device (e.g., "ic256_45", "tx2")
        primary_units: Units for the primary dose channel
        probe_units: Units for probe channels (TX2 only)
        
    Returns:
        List of header strings for the CSV file
    """
    if "ic256" in device_name.lower():
        return [
            "Timestamp (s)",
            "X centroid (mm)",
            "X sigma (mm)",
            "Y centroid (mm)",
            "Y sigma (mm)",
            f"Dose ({primary_units})",
            "Channel Sum",
            "External trigger",
            "Temperature (℃)",
            "Humidity (%rH)",
            "Pressure (hPa)",
            "Note",
        ]
    elif device_name.lower() == "tx2":
        return [
            "Timestamp (s)",
            f"Probe A ({probe_units})",
            f"Probe B ({probe_units})",
            "FR2",
            "Note",
        ]
    return ["Timestamp (s)"]


def set_up_device(
    device_client: IGXWebsocketClient,
    device_name: str,
    frequency: int,
) -> None:
    """Set up device sampling frequencies.
    
    Args:
        device_client: The websocket client for the device
        device_name: Name of the device (e.g., "ic256_45", "tx2")
        frequency: Sampling frequency in Hz
        
    Raises:
        Exception: If device setup fails
    """
    if "ic256" in device_name.lower():
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
    else:  # TX2
        tx2_fields = {
            "conversion_freq": device_client.field(
                TX2_PATHS["adc"]["conversion_frequency"]
            ),
            "sample_freq": device_client.field(TX2_PATHS["adc"]["sample_frequency"]),
        }
        device_client.sendSubscribeFields({tx2_fields[field]: False for field in tx2_fields})
        tx2_fields["conversion_freq"].setValue(40000)
        tx2_fields["sample_freq"].setValue(frequency)


def write_row(
    writer: csv.writer,
    timestamp: float,
    row_data: List[Any],
    device_name: str,
    environment: List[str],
    note: str,
) -> None:
    """Format row data and write to CSV file.
    
    Args:
        writer: CSV writer object
        timestamp: Timestamp in seconds (elapsed time)
        row_data: List of data values for the row
        device_name: Name of the device
        environment: List of environment values [temperature, humidity, pressure]
        note: Note string to append to the row
    """
    if "ic256" in device_name.lower():
        x_mean, x_sigma, y_mean, y_sigma = process_gaussian_values(
            row_data[0], row_data[1], row_data[2], row_data[3]
        )
        row = (
            [f"{timestamp:.12e}", x_mean, x_sigma, y_mean, y_sigma]
            + row_data[4:]
            + environment
            + [note]
        )
    else:  # tx2
        row = [f"{timestamp:.12e}"] + row_data + [note]
    writer.writerow(row)


def _get_tx2_probe_units(device_client: IGXWebsocketClient) -> str:
    """Get probe units for TX2 device.
    
    Args:
        device_client: The websocket client for TX2 device
        
    Returns:
        Probe units string, or empty string on error
    """
    try:
        probe_field = device_client.field(TX2_PATHS["adc"]["channel_5_units"])
        device_client.sendSubscribeFields({probe_field: False})
        device_client.updateSubscribedFields()
        return probe_field.getValue() or ""
    except (AttributeError, ValueError, KeyError, RuntimeError):
        # Device communication error - return empty string
        return ""


def _get_time_bin(elapsed_time: float) -> float:
    """Round elapsed time to nearest bin for bundling async data.
    
    Args:
        elapsed_time: Elapsed time in seconds
        
    Returns:
        Binned timestamp in seconds
    """
    return round(elapsed_time / TIME_BIN_SIZE) * TIME_BIN_SIZE


def _interpolate_primary_dose(
    target_time: float,
    primary_dose_channel: Optional[str],
    channel_data_buffer: ChannelBuffers,
    cache: Optional[_SortedBufferCache] = None,
) -> Optional[DataPoint]:
    """Linearly interpolate primary dose channel value at target_time.
    
    Uses bisect for O(log n) lookup instead of O(n) linear search.
    
    Args:
        target_time: Target timestamp in seconds (elapsed time)
        primary_dose_channel: Name of the primary dose channel, or None
        channel_data_buffer: Dictionary of channel data buffers
        cache: Optional cache for sorted data
        
    Returns:
        Interpolated data point (value, timestamp, original_timestamp_ns) or None
    """
    if not primary_dose_channel or primary_dose_channel not in channel_data_buffer:
        return None
    
    primary_data = channel_data_buffer[primary_dose_channel]
    if not primary_data:
        return None
    
    # Use cache if available, otherwise sort
    if cache:
        sorted_data = cache.get_sorted(primary_dose_channel, primary_data)
    else:
        sorted_data = sorted(primary_data, key=lambda x: x[1])
    
    # Use bisect for efficient binary search
    timestamps = [point[1] for point in sorted_data]
    idx = bisect.bisect_left(timestamps, target_time)
    
    # Find the two points that bracket target_time
    before_point: Optional[DataPoint] = None
    after_point: Optional[DataPoint] = None
    
    if idx > 0:
        before_point = sorted_data[idx - 1]
    if idx < len(sorted_data):
        after_point = sorted_data[idx]
    
    # If we have points on both sides, interpolate
    if before_point and after_point:
        t1, v1 = before_point[1], before_point[0]
        t2, v2 = after_point[1], after_point[0]
        
        # Avoid division by zero
        if abs(t2 - t1) < 1e-9:
            return (v1, target_time, before_point[2])
        
        # Linear interpolation: v = v1 + (v2 - v1) * (t - t1) / (t2 - t1)
        try:
            interpolated_value = v1 + (v2 - v1) * (target_time - t1) / (t2 - t1)
            return (interpolated_value, target_time, before_point[2])
        except (TypeError, ValueError):
            # If values aren't numeric, use the closest point
            if abs(target_time - t1) < abs(target_time - t2):
                return (v1, target_time, before_point[2])
            else:
                return (v2, target_time, after_point[2])
    
    # If we only have one side, use the closest point
    if before_point:
        return (before_point[0], target_time, before_point[2])
    elif after_point:
        return (after_point[0], target_time, after_point[2])
    
    return None


def _find_closest_point(
    buffer: ChannelBuffer,
    target_time: float,
    threshold: float,
    cache: Optional[_SortedBufferCache] = None,
) -> Optional[DataPoint]:
    """Find closest point in buffer to target_time using binary search.
    
    Args:
        buffer: Channel data buffer (deque)
        target_time: Target timestamp
        threshold: Maximum time difference allowed
        cache: Optional cache for sorted data
        
    Returns:
        Closest data point if within threshold, None otherwise
    """
    if not buffer:
        return None
    
    # Use cache if available, otherwise sort
    if cache:
        sorted_data = cache.get_sorted("", buffer)
    else:
        sorted_data = sorted(buffer, key=lambda x: x[1])
    
    # Binary search for insertion point
    timestamps = [point[1] for point in sorted_data]
    idx = bisect.bisect_left(timestamps, target_time)
    
    # Check points around the insertion point
    candidates: List[Tuple[float, DataPoint]] = []
    
    if idx > 0:
        candidates.append((abs(sorted_data[idx - 1][1] - target_time), sorted_data[idx - 1]))
    if idx < len(sorted_data):
        candidates.append((abs(sorted_data[idx][1] - target_time), sorted_data[idx]))
    
    if not candidates:
        return None
    
    # Find closest candidate
    closest_dist, closest_point = min(candidates, key=lambda x: x[0])
    
    if closest_dist <= threshold:
        return closest_point
    return None


def _find_best_aligned_data(
    target_elapsed: float,
    strip_data_channels: List[str],
    primary_dose_channel: Optional[str],
    channel_data_buffer: ChannelBuffers,
    all_channels: List[str],
    cache: Optional[_SortedBufferCache] = None,
) -> Dict[str, DataPoint]:
    """Find the best aligned data across all channels for a target timestamp.
    
    Uses strip data timestamps as reference and interpolates primary dose.
    External trigger channel is aligned to closest timestamp (no interpolation).
    Optimized with binary search for O(log n) lookups.
    
    Args:
        target_elapsed: Target elapsed time in seconds
        strip_data_channels: List of strip data channel names
        primary_dose_channel: Name of primary dose channel, or None
        channel_data_buffer: Dictionary of channel data buffers
        all_channels: List of all channel names
        cache: Optional cache for sorted data
        
    Returns:
        Dictionary mapping channel names to aligned data points
    """
    aligned_data: Dict[str, DataPoint] = {}
    alignment_threshold = ALIGNMENT_WINDOW * ALIGNMENT_WINDOW_MULTIPLIER
    
    # Special channel that should not be interpolated - just find closest timestamp
    external_trigger_channel = "external_trigger"
    
    # First, find strip data channels to use as reference timestamps
    strip_timestamp: Optional[float] = None
    for strip_channel in strip_data_channels:
        if strip_channel in channel_data_buffer:
            closest = _find_closest_point(
                channel_data_buffer[strip_channel],
                target_elapsed,
                alignment_threshold,
                cache,
            )
            if closest:
                strip_timestamp = closest[1]
                aligned_data[strip_channel] = closest
                break
    
    # If no strip data found, use target_elapsed as reference
    reference_time = strip_timestamp if strip_timestamp is not None else target_elapsed
    
    # Get all strip data channels at the reference time
    for strip_channel in strip_data_channels:
        if strip_channel in aligned_data:
            continue  # Already found
        
        if strip_channel in channel_data_buffer:
            closest = _find_closest_point(
                channel_data_buffer[strip_channel],
                reference_time,
                alignment_threshold,
                cache,
            )
            if closest:
                aligned_data[strip_channel] = closest
    
    # Linearly interpolate primary dose channel to match reference_time
    if primary_dose_channel and primary_dose_channel not in aligned_data:
        interpolated = _interpolate_primary_dose(
            reference_time, primary_dose_channel, channel_data_buffer, cache
        )
        if interpolated:
            aligned_data[primary_dose_channel] = interpolated
    
    # Align external trigger to closest timestamp (no interpolation)
    if external_trigger_channel in all_channels and external_trigger_channel not in aligned_data:
        if external_trigger_channel in channel_data_buffer:
            closest = _find_closest_point(
                channel_data_buffer[external_trigger_channel],
                reference_time,
                alignment_threshold,
                cache,
            )
            if closest:
                aligned_data[external_trigger_channel] = closest
    
    # Align other channels to reference time
    for field_name in all_channels:
        if (field_name in aligned_data or 
            field_name in strip_data_channels or 
            field_name == external_trigger_channel):
            continue  # Already handled
        
        if field_name in channel_data_buffer:
            closest = _find_closest_point(
                channel_data_buffer[field_name],
                reference_time,
                alignment_threshold,
                cache,
            )
            if closest:
                aligned_data[field_name] = closest
    
    return aligned_data


def _fill_missing_row_values(
    row_data: List[Optional[Any]], 
    num_gaussian_fields: int = 4,
    channel_names: Optional[List[str]] = None
) -> List[Any]:
    """Fill missing values in row data with appropriate defaults.
    
    Args:
        row_data: List of row data values (may contain None)
        num_gaussian_fields: Number of gaussian fields at the start (use ERROR_VALUE)
        channel_names: Optional list of channel names to identify special channels
        
    Returns:
        List with None values replaced by appropriate defaults
    """
    filled_row: List[Any] = []
    for i, value in enumerate(row_data):
        if value is None:
            # First N fields are gaussian values (numeric), use ERROR_VALUE
            if i < num_gaussian_fields:
                filled_row.append(ERROR_VALUE)
            else:
                # Check if this is the external_trigger channel (should be 0 or 1)
                if channel_names and i < len(channel_names) and channel_names[i] == "external_trigger":
                    filled_row.append(0)  # Default trigger to 0 (off)
                else:
                    filled_row.append("")
        else:
            # Convert boolean trigger values to 0/1
            if channel_names and i < len(channel_names) and channel_names[i] == "external_trigger":
                # Ensure trigger is 0 or 1 (convert True/False to 1/0)
                if isinstance(value, bool):
                    filled_row.append(1 if value else 0)
                elif value in (1, "1", True):
                    filled_row.append(1)
                elif value in (0, "0", False, None):
                    filled_row.append(0)
                else:
                    # Try to convert to int, default to 0 if invalid
                    try:
                        filled_row.append(int(bool(value)))
                    except (ValueError, TypeError):
                        filled_row.append(0)
            else:
                filled_row.append(value)
    return filled_row


def _collect_channel_data(
    channels: Dict[str, Any],
    channel_data_buffer: ChannelBuffers,
    first_timestamp: Optional[int],
    cleanup_buffers: bool = False,
) -> Tuple[Optional[int], int, set]:
    """Collect data from all channels and update buffers.
    
    Optimized to avoid per-point buffer cleanup - cleanup happens periodically.
    
    Args:
        channels: Dictionary of channel field objects
        channel_data_buffer: Dictionary of channel data buffers to update
        first_timestamp: First timestamp seen (nanoseconds), or None
        cleanup_buffers: If True, perform buffer cleanup (expensive operation)
        
    Returns:
        Tuple of (updated_first_timestamp, data_received_count, channels_with_data_set)
    """
    channels_with_data: set = set()
    data_received_count: int = 0
    updated_first_timestamp = first_timestamp
    
    for field_name, channel in channels.items():
        try:
            data = channel.getDatums()
            if not data:
                continue
            
            channels_with_data.add(field_name)
            data_received_count += len(data)

            for value, ts_ns in data:
                if updated_first_timestamp is None:
                    updated_first_timestamp = ts_ns
                
                elapsed_time = (ts_ns - updated_first_timestamp) / 1e9
                
                # Store data point with timestamp (O(1) append with deque)
                channel_data_buffer[field_name].append((value, elapsed_time, ts_ns))
            
            # Periodic cleanup only when requested (not per point)
            if cleanup_buffers and updated_first_timestamp is not None:
                cutoff_time = (time.time() * 1e9 - updated_first_timestamp) / 1e9 - MAX_BUFFER_TIME
                buffer = channel_data_buffer[field_name]
                # Remove old data from left side of deque (efficient)
                while buffer and buffer[0][1] < cutoff_time:
                    buffer.popleft()
        except Exception as e:
            print(f"Error collecting data from {field_name}: {e}")
            continue
    
    return updated_first_timestamp, data_received_count, channels_with_data


def _write_data_row(
    writer: csv.writer,
    target_elapsed: float,
    aligned_data: Dict[str, DataPoint],
    channels: Dict[str, Any],
    device_name: str,
    environment: List[str],
    note: str,
) -> bool:
    """Write a single data row to CSV if sufficient data is available.
    
    Args:
        writer: CSV writer object
        target_elapsed: Target timestamp for the row
        aligned_data: Dictionary of aligned channel data
        channels: Dictionary of all channels
        device_name: Name of the device
        environment: Environment data [temperature, humidity, pressure]
        note: Note string
        
    Returns:
        True if row was written, False otherwise
    """
    # Build row data in correct channel order
    row_data: List[Optional[Any]] = []
    channel_names_list = list(channels.keys())  # Preserve channel order
    for field_name in channel_names_list:
        if field_name in aligned_data:
            row_data.append(aligned_data[field_name][0])  # Get value
        else:
            row_data.append(None)
    
    filled_count = sum(1 for v in row_data if v is not None)
    min_fields_required = max(1, int(len(channels) * MIN_FIELDS_RATIO))
    
    if filled_count >= min_fields_required:
        try:
            filled_row = _fill_missing_row_values(row_data, num_gaussian_fields=4, channel_names=channel_names_list)
            write_row(writer, target_elapsed, filled_row, device_name, environment, note)
            return True
        except Exception as e:
            print(f"Error writing row at timestamp {target_elapsed}: {e}")
            import traceback
            traceback.print_exc()
    
    return False


def _cleanup_old_buffer_data(
    channel_data_buffer: ChannelBuffers,
    current_elapsed: float,
    row_interval: float,
) -> None:
    """Remove old data points from buffers to prevent memory growth.
    
    Optimized to use deque.popleft() for O(1) removal from front.
    
    Args:
        channel_data_buffer: Dictionary of channel data buffers
        current_elapsed: Current elapsed time in seconds
        row_interval: Time interval between rows in seconds
    """
    cutoff_time = current_elapsed - (row_interval * BUFFER_CLEANUP_MULTIPLIER)
    for buffer in channel_data_buffer.values():
        # Efficiently remove from left side of deque
        while buffer and buffer[0][1] < cutoff_time:
            buffer.popleft()


def collect_data(
    device_client: IGXWebsocketClient,
    channels: Dict[str, Any],
    env_channels: Optional[Dict[str, Any]],
    file_name: str,
    device_name: str,
    note: str,
    save_folder: str,
    stop_event: threading.Event,
    sampling_rate: int,
) -> None:
    """Collect data from device and write to CSV file.
    
    This function handles asynchronous data collection from multiple channels,
    aligns timestamps across channels, interpolates primary dose data to match
    strip data timestamps, and writes regularly-spaced rows to CSV.
    
    Args:
        device_client: Websocket client for device communication
        channels: Dictionary of channel field objects to collect from
        env_channels: Dictionary of environment channel fields, or None
        file_name: Name of the CSV file to write
        device_name: Name/type of the device (e.g., "ic256_45", "tx2")
        note: Note string to include in each row
        save_folder: Folder path to save the CSV file
        stop_event: Threading event to signal when to stop collection
        sampling_rate: Sampling rate in Hz (determines row spacing)
        
    Raises:
        IOError: If file cannot be opened
        OSError: If file operations fail
    """
    try:
        # Get environment data and units
        environment, primary_units = get_environment_data(device_client, env_channels)

        # Get probe units for TX2
        probe_units = ""
        if device_name.lower() == "tx2":
            probe_units = _get_tx2_probe_units(device_client)

        headers = get_headers(device_name, primary_units, probe_units)

        # Subscribe all channels
        device_client.sendSubscribeFields({channels[field]: True for field in channels})
        device_client.updateSubscribedFields()

        # Initialize data collection state
        first_timestamp: Optional[int] = None
        # Use deque for efficient append/popleft operations
        channel_data_buffer: ChannelBuffers = {
            field_name: deque() for field_name in channels.keys()
        }
        loop_count: int = 0
        
        # Cache channel keys list to avoid repeated conversions
        channel_keys_list = list(channels.keys())
        
        # Performance optimization: cache for sorted buffer lookups
        buffer_cache = _SortedBufferCache()
        
        # Identify channel types
        primary_dose_channel: Optional[str] = (
            "primary_channel" if "primary_channel" in channels else None
        )
        strip_data_channels: List[str] = [
            "mean_channel_a",
            "sigma_channel_a",
            "mean_channel_b",
            "sigma_channel_b",
        ]

        # Open file with error handling and optimized buffering
        file_path = f"{save_folder}/{file_name}"
        try:
            # Use unbuffered mode (buffering=0) or line buffering (buffering=1) for real-time writes
            # Line buffering ensures each line is flushed immediately, but we'll also flush explicitly
            # Note: buffering=1 is line buffered, which works with text mode
            file = open(file_path, mode="w", newline="", encoding="utf-8-sig", buffering=1)
        except (IOError, OSError) as e:
            print(f"Error opening file {file_path}: {e}")
            device_client.close()
            return

        try:
            writer = csv.writer(file)
            writer.writerow(headers)
            file.flush()  # Ensure headers are written immediately
            try:
                os.fsync(file.fileno())  # Force OS to write headers to disk
            except (OSError, AttributeError):
                pass

            # Initialize timing variables
            rows_written: int = 0
            last_write_time: float = time.time()
            row_interval: float = 1.0 / sampling_rate  # Time between rows in seconds
            next_row_elapsed: float = 0.0  # Next row timestamp in elapsed seconds
            last_written_timestamp: float = -1.0  # Track last written timestamp to avoid duplicates in cleanup

            # Main data collection loop
            while not stop_event.is_set():
                try:
                    device_client.updateSubscribedFields()
                    time.sleep(UPDATE_INTERVAL)
                    loop_count += 1

                    # Collect data from all channels
                    # Only cleanup buffers periodically (every N loops) for performance
                    cleanup_buffers = (loop_count % BUFFER_CLEANUP_INTERVAL == 0)
                    first_timestamp, data_received_count, channels_with_data = _collect_channel_data(
                        channels, channel_data_buffer, first_timestamp, cleanup_buffers
                    )
                    
                    # Initialize timing on first data
                    if first_timestamp is not None and next_row_elapsed == 0.0:
                        next_row_elapsed = 0.0  # First row at t=0
                    
                    # Debug logging (cache current_elapsed calculation)
                    current_elapsed_cached: Optional[float] = None
                    # Removed verbose debug logging - check GUI log tab for details

                    # Check if we should write data based on sampling rate timing
                    # Cache time calculations to avoid repeated calls
                    should_write = False
                    current_time = time.time()
                    
                    if first_timestamp is not None:
                        # Reuse cached value if available, otherwise calculate once
                        if current_elapsed_cached is None:
                            current_elapsed_cached = (time.time() * 1e9 - first_timestamp) / 1e9
                        if current_elapsed_cached >= next_row_elapsed:
                            should_write = True
                    
                    # Fallback: Write if we've been waiting too long
                    if not should_write and current_time - last_write_time > WRITE_TIMEOUT:
                        should_write = True
                        # Removed verbose timeout logging
                        if first_timestamp is not None:
                            current_elapsed_cached = (time.time() * 1e9 - first_timestamp) / 1e9
                            next_row_elapsed = current_elapsed_cached

                    if should_write and first_timestamp is not None:
                        target_elapsed = next_row_elapsed
                        
                        # Find best aligned data across all channels (use cached channel list)
                        aligned_data = _find_best_aligned_data(
                            target_elapsed,
                            strip_data_channels,
                            primary_dose_channel,
                            channel_data_buffer,
                            channel_keys_list,
                            buffer_cache,
                        )
                        
                        # Write row if sufficient data available
                        if _write_data_row(
                            writer, target_elapsed, aligned_data, channels,
                            device_name, environment, note
                        ):
                            rows_written += 1
                            last_written_timestamp = target_elapsed  # Track last written timestamp
                            
                            # Flush frequently for real-time visibility
                            # Flush every few rows to ensure data is written to OS buffer
                            # This ensures the file grows during collection, not just at the end
                            if rows_written % FLUSH_INTERVAL == 0:
                                file.flush()  # Flush Python buffer to OS
                            
                            # Force OS sync less frequently (every N rows) since fsync is very expensive
                            # fsync blocks until data is physically written to disk, which can be slow
                            # This prevents blocking while still ensuring data is written periodically
                            if rows_written % FSYNC_INTERVAL == 0:
                                file.flush()  # Ensure buffer is flushed before fsync
                                try:
                                    os.fsync(file.fileno())  # Force OS to write to disk
                                except (OSError, AttributeError):
                                    # Fallback if fsync fails (e.g., on some systems)
                                    pass
                            
                            # Cleanup old buffer data (reuse cached elapsed time)
                            if current_elapsed_cached is None and first_timestamp is not None:
                                current_elapsed_cached = (time.time() * 1e9 - first_timestamp) / 1e9
                            if current_elapsed_cached is not None:
                                _cleanup_old_buffer_data(channel_data_buffer, current_elapsed_cached, row_interval)
                        
                        # Advance to next row time
                        next_row_elapsed += row_interval
                        last_write_time = current_time
                    
                    # Periodic flush even if no row written (every N loops) to ensure any buffered data is written
                    # This ensures data is flushed even if timing conditions aren't met
                    if loop_count % 100 == 0 and rows_written > 0:
                        file.flush()  # Flush any buffered data
                        # Only fsync periodically to avoid blocking
                        if rows_written % FSYNC_INTERVAL == 0:
                            try:
                                os.fsync(file.fileno())  # Force OS write
                            except (OSError, AttributeError):
                                pass
                    
                    # Removed verbose status logging - check GUI log tab for details

                except Exception as e:
                    print(f"Error in data collection loop: {e}")
                    import traceback
                    traceback.print_exc()
                    continue

            # Write remaining incomplete data (if any)
            # Only write timestamps that are after the last written timestamp to avoid duplicates
            if first_timestamp is not None:
                all_timestamps = set()
                for field_name in channel_keys_list:
                    for data_point in channel_data_buffer[field_name]:
                        all_timestamps.add(data_point[1])
                
                # Filter out timestamps that have already been written
                # Only write timestamps after the last written one (with small tolerance for rounding)
                min_timestamp_threshold = last_written_timestamp - (row_interval * 0.5) if last_written_timestamp >= 0 else -1.0
                timestamps_to_process = sorted([ts for ts in all_timestamps if ts > min_timestamp_threshold])
                
                # Limit cleanup to prevent excessive delays
                max_cleanup_time = 10.0  # Maximum seconds to spend on cleanup
                max_cleanup_rows = 10000  # Maximum number of rows to write during cleanup
                cleanup_start_time = time.time()
                cleanup_rows_written = 0
                
                for ts in timestamps_to_process:
                    # Check if we're taking too long or have written too many rows
                    if time.time() - cleanup_start_time > max_cleanup_time:
                        print(f"Cleanup timeout: processed {cleanup_rows_written} of {len(timestamps_to_process)} timestamps")
                        break
                    if cleanup_rows_written >= max_cleanup_rows:
                        print(f"Cleanup row limit reached: processed {cleanup_rows_written} of {len(timestamps_to_process)} timestamps")
                        break
                    
                    aligned_data = _find_best_aligned_data(
                        ts, strip_data_channels, primary_dose_channel,
                        channel_data_buffer, channel_keys_list, buffer_cache
                    )
                    if aligned_data:
                        if _write_data_row(
                            writer, ts, aligned_data, channels,
                            device_name, environment, note
                        ):
                            rows_written += 1
                            cleanup_rows_written += 1
                            
                            # Periodic flush during cleanup to show progress
                            if cleanup_rows_written % 100 == 0:
                                file.flush()
                                try:
                                    os.fsync(file.fileno())
                                except (OSError, AttributeError):
                                    pass
            
            file.flush()  # Final flush before closing
            # Removed completion message - check GUI log tab

        finally:
            file.close()

    except Exception as e:
        print(f"Critical error in collect_data: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            device_client.close()
        except Exception:
            pass  # Ignore errors during cleanup
