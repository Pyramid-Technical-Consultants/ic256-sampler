"""Data collection module for IC256 and TX2 devices.

This module handles asynchronous data collection from device channels,
timestamp alignment, and CSV file writing with proper error handling.

Built from first principles: collect data losslessly first, then write rows.
"""

import time
import csv
import threading
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
UPDATE_INTERVAL: float = 0.001  # seconds between data collection updates
ROW_INTERVAL_MULTIPLIER: float = 0.5  # Write rows at half the data rate to ensure we have data

# CSV writing constants
FLUSH_INTERVAL: int = 1000  # Flush file every N rows
FSYNC_INTERVAL: int = 5000  # Force OS sync every N rows
MIN_FIELDS_RATIO: float = 0.1  # Minimum ratio of fields required to write a row

# Data point type: (value, elapsed_time_seconds, timestamp_nanoseconds)
DataPoint = Tuple[Any, float, int]
ChannelBuffer = deque[DataPoint]
ChannelBuffers = Dict[str, ChannelBuffer]


def convert_mean(value: Union[float, int, str, None], x_axis: bool = True) -> float:
    """Convert mean value to millimeters."""
    if value is None or value == ERROR_VALUE or value == "":
        return ERROR_GAUSS
    try:
        numeric_value = float(value)
    except (ValueError, TypeError):
        return ERROR_GAUSS
    offset = X_STRIP_OFFSET if x_axis else Y_STRIP_OFFSET
    return (numeric_value - MEAN_OFFSET) * offset


def convert_sigma(value: Union[float, int, str, None], x_axis: bool = True) -> float:
    """Convert sigma value to millimeters."""
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
    """Process gaussian fit values, converting all to millimeters."""
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
    """Read environment sensor data and primary channel units."""
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
    
    return environment, primary_units


def get_headers(device_name: str, primary_units: str = "", probe_units: str = "") -> List[str]:
    """Get CSV headers for the specified device type."""
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
    """Set up device sampling frequencies."""
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
    """Format row data and write to CSV file."""
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
    """Get probe units for TX2 device."""
    try:
        probe_field = device_client.field(TX2_PATHS["adc"]["channel_5_units"])
        device_client.sendSubscribeFields({probe_field: False})
        device_client.updateSubscribedFields()
        return probe_field.getValue() or ""
    except (AttributeError, ValueError, KeyError, RuntimeError):
        return ""


def _collect_all_channel_data(
    channels: Dict[str, Any],
    channel_data_buffer: ChannelBuffers,
    first_timestamp: Optional[int],
) -> Tuple[Optional[int], int]:
    """Collect ALL data from all channels and store in buffers.
    
    This is the core data collection function - it must be lossless.
    Based on simple_capture.py which we know works perfectly.
    
    Returns:
        Tuple of (updated_first_timestamp, total_data_points_collected)
    """
    updated_first_timestamp = first_timestamp
    total_points = 0
    
    # Process each channel - each may have data or not (partial updates are normal)
    for field_name, channel in channels.items():
        try:
            # Get array of arrays: [[value, timestamp], [value, timestamp], ...]
            data = channel.getDatums()
            
            if not data:
                continue  # No data for this channel in this update - normal
            
            # Process EVERY entry in the array
            for data_point in data:
                if not isinstance(data_point, (list, tuple)) or len(data_point) < 2:
                    continue
                
                value = data_point[0]
                ts_raw = data_point[1]
                
                # Convert timestamp to nanoseconds
                try:
                    if isinstance(ts_raw, float):
                        if ts_raw < 1e12:  # Likely seconds
                            ts_ns = int(ts_raw * 1e9)
                        else:  # Already in nanoseconds
                            ts_ns = int(ts_raw)
                    elif isinstance(ts_raw, int):
                        ts_ns = ts_raw
                    else:
                        continue
                except (ValueError, TypeError, OverflowError):
                    continue
                
                # Track first timestamp
                if updated_first_timestamp is None:
                    updated_first_timestamp = ts_ns
                
                # Calculate elapsed time
                elapsed_time = (ts_ns - updated_first_timestamp) / 1e9
                
                # Store in buffer - this is our complete database
                channel_data_buffer[field_name].append((value, elapsed_time, ts_ns))
                total_points += 1
                
        except Exception as e:
            print(f"Error collecting data from {field_name}: {e}")
            continue
    
    return updated_first_timestamp, total_points


def _get_channel_value_at_time(
    buffer: ChannelBuffer,
    target_elapsed: float,
    tolerance: float = 0.001,
) -> Optional[Any]:
    """Get the value from a channel buffer closest to target_elapsed time.
    
    Simple linear search - we'll optimize later if needed.
    """
    if not buffer:
        return None
    
    closest = None
    min_diff = float('inf')
    
    for value, elapsed, ts_ns in buffer:
        diff = abs(elapsed - target_elapsed)
        if diff < min_diff and diff <= tolerance:
            min_diff = diff
            closest = value
    
    return closest


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
    statistics: Optional[Dict[str, Any]] = None,
) -> None:
    """Collect data from device and write to CSV file.
    
    STEP 1: Collect all data losslessly (like simple_capture)
    STEP 2: Write rows at the sampling rate from the collected data
    """
    try:
        # Get environment data and units
        environment, primary_units = get_environment_data(device_client, env_channels)

        # Get probe units for TX2
        probe_units = ""
        if device_name.lower() == "tx2":
            probe_units = _get_tx2_probe_units(device_client)

        headers = get_headers(device_name, primary_units, probe_units)

        # Subscribe all channels with buffered data
        device_client.sendSubscribeFields({channels[field]: True for field in channels})
        device_client.updateSubscribedFields()

        # Initialize buffers - one per channel to store ALL data
        first_timestamp: Optional[int] = None
        channel_data_buffer: ChannelBuffers = {
            field_name: deque() for field_name in channels.keys()
        }
        
        # Open file
        file_path = f"{save_folder}/{file_name}"
        try:
            file = open(file_path, mode="w", newline="", encoding="utf-8-sig", buffering=1)
        except (IOError, OSError) as e:
            print(f"Error opening file {file_path}: {e}")
            device_client.close()
            return
        
        # Initialize statistics
        if statistics is not None:
            statistics["rows"] = 0
            statistics["file_size"] = 0
            statistics["file_path"] = file_path

        try:
            writer = csv.writer(file)
            writer.writerow(headers)
            file.flush()

            # Initialize timing
            rows_written: int = 0
            row_interval: float = 1.0 / sampling_rate
            next_row_elapsed: float = 0.0
            
            # Main loop: collect data continuously, write rows continuously
            # Based on simple_capture pattern - tight loop that runs continuously
            # CRITICAL: Write rows in a tight loop, collect data less frequently
            data_collection_counter = 0
            
            while not stop_event.is_set():
                rows_written_this_iteration = 0
                
                # STEP 1: Write rows continuously (priority - maintain sampling rate)
                if first_timestamp is not None:
                    current_time = time.time()
                    current_elapsed = (current_time * 1e9 - first_timestamp) / 1e9
                    
                    # Write ALL rows that are due - tight loop like simple_capture
                    # CRITICAL: Don't limit - write as many rows as needed to catch up
                    # At 3000 Hz, we need to write 3000 rows/second, so write continuously
                    while next_row_elapsed <= current_elapsed and not stop_event.is_set():
                        # Get values from each channel at this time
                        row_data = []
                        for field_name in channels.keys():
                            value = _get_channel_value_at_time(
                                channel_data_buffer[field_name],
                                next_row_elapsed,
                                tolerance=row_interval * 2.0  # Allow 2x row interval for tolerance
                            )
                            row_data.append(value)
                        
                        # Write row if we have at least some data
                        filled_count = sum(1 for v in row_data if v is not None)
                        if filled_count > 0:
                            # Fill missing values
                            filled_row = []
                            for i, value in enumerate(row_data):
                                if value is None:
                                    # Use defaults based on position
                                    if i < 4:  # Gaussian fields
                                        filled_row.append(ERROR_VALUE)
                                    elif i == len(row_data) - 1:  # External trigger
                                        filled_row.append(0)
                                    else:
                                        filled_row.append("")
                                else:
                                    filled_row.append(value)
                            
                            write_row(writer, next_row_elapsed, filled_row, device_name, environment, note)
                            rows_written += 1
                            rows_written_this_iteration += 1
                            
                            # Update statistics (less frequently to reduce overhead)
                            if statistics is not None and rows_written % 100 == 0:
                                statistics["rows"] = rows_written
                                if rows_written % FLUSH_INTERVAL == 0:
                                    try:
                                        statistics["file_size"] = os.path.getsize(file_path)
                                    except (OSError, AttributeError):
                                        pass
                            
                            # Flush periodically (less frequently)
                            if rows_written % (FLUSH_INTERVAL // 10) == 0:
                                file.flush()
                            
                            if rows_written % FSYNC_INTERVAL == 0:
                                file.flush()
                                try:
                                    os.fsync(file.fileno())
                                except (OSError, AttributeError):
                                    pass
                        
                        # Always advance to next row time
                        next_row_elapsed += row_interval
                
                # STEP 2: Collect data (every iteration - data collection is fast)
                # Don't throttle data collection - we need data to write rows
                device_client.updateSubscribedFields()
                first_timestamp, data_points = _collect_all_channel_data(
                    channels, channel_data_buffer, first_timestamp
                )
                
                # Initialize timing on first data
                if first_timestamp is not None and next_row_elapsed == 0.0:
                    next_row_elapsed = 0.0
                
                # Minimal sleep - only if we didn't write any rows this iteration
                # This keeps the loop tight for row writing
                if rows_written_this_iteration == 0:
                    time.sleep(UPDATE_INTERVAL)
            
            # Final flush
            file.flush()
            if statistics is not None:
                try:
                    statistics["file_size"] = os.path.getsize(file_path)
                except (OSError, AttributeError):
                    pass

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
            pass
