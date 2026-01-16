"""Data collection module for IC256 and TX2 devices.

This module handles asynchronous data collection from device channels using
the IODatabase, VirtualDatabase, and CSVWriter architecture.

Built from first principles: collect data losslessly first, then write rows.
"""

import time
import threading
from typing import Dict, List, Optional, Any
from .igx_client import IGXWebsocketClient
from .io_database import IODatabase
from .virtual_database import VirtualDatabase
from .csv_writer import CSVWriter
from .device_paths import IC256_45_PATHS, TX2_PATHS
from .ic256_model import IC256Model

# Data collection timing constants
UPDATE_INTERVAL: float = 0.001  # seconds between data collection updates

# CSV writing constants
FLUSH_INTERVAL: int = 1000  # Flush file every N rows
FSYNC_INTERVAL: int = 5000  # Force OS sync every N rows


def get_environment_data(
    device_client: IGXWebsocketClient,
    env_channels: Optional[Dict[str, Any]],
) -> List[str]:
    """Read environment sensor data.
    
    Returns:
        List of [temperature, humidity, pressure] as strings
    """
    environment: List[str] = ["", "", ""]
    
    if not env_channels:
        return environment

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
    except Exception as e:
        print(f"Error reading environment data: {e}")
    
    return environment


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


def _collect_all_channel_data(
    channels: Dict[str, Any],
    io_database: IODatabase,
    first_timestamp: Optional[int],
) -> Optional[int]:
    """Collect ALL data from all channels and store in IODatabase.
    
    This is the core data collection function - it must be lossless.
    Based on simple_capture.py which we know works perfectly.
    
    Args:
        channels: Dictionary mapping field names to IGXField objects
        io_database: IODatabase to store data in
        first_timestamp: Current first timestamp (or None)
        field_to_path: Dictionary mapping field names to channel paths
        
    Returns:
        Updated first timestamp (or None if no new data)
    """
    updated_first_timestamp = first_timestamp
    
    # Process each channel - each may have data or not (partial updates are normal)
    for field_name, channel in channels.items():
        try:
            # Get array of arrays: [[value, timestamp], [value, timestamp], ...]
            data = channel.getDatums()
            
            if not data:
                continue  # No data for this channel in this update - normal
            
            # Get channel path from field name mapping
            # The channels dict uses field names, but we need actual device paths
            channel_path = field_to_path.get(field_name)
            if not channel_path:
                # Fallback: try to get path from field object
                try:
                    channel_path = channel.getPath()
                except (AttributeError, TypeError):
                    # Last resort: use field name
                    channel_path = field_name
            
            # Ensure channel exists in database
            if channel_path not in io_database.get_all_channels():
                io_database.add_channel(channel_path)
            
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
                
                # Add to database (database handles elapsed time calculation)
                io_database.add_data_point(channel_path, value, ts_ns)
                
        except Exception as e:
            print(f"Error collecting data from {field_name}: {e}")
            continue
    
    return updated_first_timestamp


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
    """Collect data from device and write to CSV file using new architecture.
    
    Architecture:
    1. IODatabase: Collects all raw data losslessly
    2. VirtualDatabase: Creates synthetic rows at sampling rate with conversions
    3. CSVWriter: Writes rows to disk asynchronously
    """
    try:
        # Get environment data
        environment = get_environment_data(device_client, env_channels)

        # Subscribe all channels with buffered data
        device_client.sendSubscribeFields({channels[field]: True for field in channels})
        device_client.updateSubscribedFields()

        # Initialize IODatabase for lossless data collection
        io_database = IODatabase()
        first_timestamp: Optional[int] = None
        
        # Determine reference channel and create columns based on device type
        # We need to map from channel field names to actual paths
        # The channels dict uses field names like "channel_sum", "primary_channel", etc.
        # But we need the actual device paths
        
        field_to_path: Dict[str, str] = {}
        
        if "ic256" in device_name.lower():
            # For IC256, use channel_sum as reference
            reference_channel = IC256_45_PATHS["adc"]["channel_sum"]
            columns = IC256Model.create_columns(reference_channel)
            
            # Create mapping from field names to channel paths
            field_to_path = {
                "mean_channel_a": IC256_45_PATHS["adc"]["gaussian_fit_a_mean"],
                "sigma_channel_a": IC256_45_PATHS["adc"]["gaussian_fit_a_sigma"],
                "mean_channel_b": IC256_45_PATHS["adc"]["gaussian_fit_b_mean"],
                "sigma_channel_b": IC256_45_PATHS["adc"]["gaussian_fit_b_sigma"],
                "primary_channel": IC256_45_PATHS["adc"]["primary_dose"],
                "channel_sum": IC256_45_PATHS["adc"]["channel_sum"],
                "external_trigger": IC256_45_PATHS["adc"]["gate_signal"],
            }
        else:  # TX2
            # TODO: Create TX2Model when needed
            from .virtual_database import create_tx2_columns
            reference_channel = TX2_PATHS["adc"]["channel_5"]
            columns = create_tx2_columns(io_database, reference_channel)
            
            field_to_path = {
                "probe_a": TX2_PATHS["adc"]["channel_5"],
                "probe_b": TX2_PATHS["adc"]["channel_1"],
                "fr2": TX2_PATHS["adc"]["fr2"],
            }
        
        # Create VirtualDatabase
        virtual_database = VirtualDatabase(
            io_database=io_database,
            reference_channel=reference_channel,
            sampling_rate=sampling_rate,
            columns=columns,
        )
        
        # Create CSVWriter
        file_path = f"{save_folder}/{file_name}"
        csv_writer = CSVWriter(
            virtual_database=virtual_database,
            file_path=file_path,
            device_name=device_name,
            note=note,
        )
        
        # Initialize statistics
        if statistics is not None:
            statistics["rows"] = 0
            statistics["file_size"] = 0
            statistics["file_path"] = file_path

        # Main loop: collect data continuously, build virtual database, write rows
        rows_written_last_check = 0
        
        while not stop_event.is_set():
            # STEP 1: Collect data continuously (lossless collection)
            device_client.updateSubscribedFields()
            first_timestamp = _collect_all_channel_data(
                channels, io_database, first_timestamp, field_to_path
            )
            
            # STEP 2: Rebuild virtual database with new data
            # This creates rows at the sampling rate with all conversions applied
            virtual_database.rebuild()
            
            # STEP 3: Write new rows to CSV
            new_rows = csv_writer.write_all()
            
            # STEP 4: Update statistics
            if statistics is not None:
                total_rows = csv_writer.rows_written
                if total_rows != rows_written_last_check:
                    statistics["rows"] = total_rows
                    if total_rows % FLUSH_INTERVAL == 0:
                        try:
                            statistics["file_size"] = csv_writer.file_size
                        except (OSError, AttributeError):
                            pass
                    rows_written_last_check = total_rows
                
                # Prune old rows from virtual database if safe
                if csv_writer.can_prune_rows(rows_to_keep=1000):
                    prunable = csv_writer.get_prunable_row_count(rows_to_keep=1000)
                    if prunable > 0:
                        virtual_database.prune_rows(keep_last_n=1000)
            
            # STEP 5: Flush/sync periodically
            if csv_writer.rows_written % FLUSH_INTERVAL == 0:
                csv_writer.flush()
            if csv_writer.rows_written % FSYNC_INTERVAL == 0:
                csv_writer.sync()
            
            # Small sleep to prevent tight loop
            time.sleep(UPDATE_INTERVAL)
        
        # Final flush and close
        csv_writer.flush()
        csv_writer.sync()
        csv_writer.close()
        
        if statistics is not None:
            try:
                statistics["file_size"] = csv_writer.file_size
            except (OSError, AttributeError):
                pass

    except Exception as e:
        print(f"Critical error in collect_data: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            device_client.close()
        except Exception:
            pass
