"""Device Manager - OOP class for managing multiple device connections.

This module provides the DeviceManager class which handles multiple IGXWebsocketClient
connections, creates threads for data collection, and maintains a shared IODatabase
that all devices feed into.
"""

from typing import Dict, Optional, Any, Callable, List
import threading
from datetime import datetime
from .igx_client import IGXWebsocketClient
from .io_database import IODatabase
from .utils import is_valid_device
from .ic256_model import IC256Model


class DeviceConfig:
    """Configuration for a device type."""
    
    def __init__(
        self,
        device_name: str,
        device_type: str,
        channel_creator: Callable[[IGXWebsocketClient], Dict[str, Any]],
        env_channel_creator: Optional[Callable[[IGXWebsocketClient], Dict[str, Any]]] = None,
        filename_prefix: str = "",
        model_creator: Optional[Callable[[], Any]] = None,
    ):
        self.device_name = device_name
        self.device_type = device_type
        self.channel_creator = channel_creator
        self.env_channel_creator = env_channel_creator
        self.filename_prefix = filename_prefix or device_name
        self.model_creator = model_creator or (lambda: IC256Model() if "ic256" in device_type.lower() else None)


class DeviceConnection:
    """Represents a single device connection and its data collection thread."""
    
    def __init__(
        self,
        config: DeviceConfig,
        ip_address: str,
        client: IGXWebsocketClient,
        channels: Dict[str, Any],
        env_channels: Optional[Dict[str, Any]],
        model: Any,
        field_to_path: Dict[str, str],
        thread: threading.Thread,
    ):
        self.config = config
        self.ip_address = ip_address
        self.client = client
        self.channels = channels
        self.env_channels = env_channels
        self.model = model
        self.field_to_path = field_to_path
        self.thread = thread
        self.statistics: Dict[str, Any] = {"rows": 0, "file_size": 0}


class DeviceManager:
    """Manages multiple device connections and shared data collection.
    
    This class:
    - Maintains multiple IGXWebsocketClient connections
    - Creates threads for each device to collect data
    - Provides a shared IODatabase that all devices feed into
    - Manages device lifecycle (start, stop, cleanup)
    """
    
    def __init__(self):
        """Initialize the DeviceManager with a shared IODatabase."""
        self.io_database = IODatabase()
        self.connections: Dict[str, DeviceConnection] = {}
        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        self._running = False
    
    def add_device(
        self,
        config: DeviceConfig,
        ip_address: str,
        sampling_rate: int,
        log_callback: Optional[Callable[[str, str], None]] = None,
    ) -> bool:
        """Add a device connection to the manager.
        
        Args:
            config: Device configuration
            ip_address: IP address of the device
            sampling_rate: Sampling rate in Hz
            log_callback: Optional callback for logging (message, level)
            
        Returns:
            True if device was added successfully, False otherwise
        """
        if not ip_address:
            return False
        
        # Validate device
        if not is_valid_device(ip_address, config.device_type):
            if log_callback:
                log_callback(
                    f"{config.device_name} device at {ip_address} validation failed (skipping)",
                    "WARNING"
                )
            return False
        
        try:
            if log_callback:
                log_callback(f"Connecting to {config.device_name} device at {ip_address}", "INFO")
            
            # Create client and channels
            client = IGXWebsocketClient(ip_address)
            channels = config.channel_creator(client)
            env_channels = config.env_channel_creator(client) if config.env_channel_creator else None
            
            # Create model and set up device
            model = config.model_creator()
            if model is None:
                raise ValueError(f"No model creator for device type: {config.device_type}")
            
            # Set up device using model
            model.setup_device(client, sampling_rate)
            
            # Get field mapping from model
            field_to_path = model.get_field_to_path_mapping()
            
            # Create data collection thread for this device
            thread = threading.Thread(
                target=self._collect_from_device,
                name=f"{config.device_type.lower()}_device_{ip_address}",
                daemon=True,
                args=(config, client, channels, env_channels, model, field_to_path, ip_address),
            )
            
            # Create connection object
            connection = DeviceConnection(
                config=config,
                ip_address=ip_address,
                client=client,
                channels=channels,
                env_channels=env_channels,
                model=model,
                field_to_path=field_to_path,
                thread=thread,
            )
            
            # Store connection
            with self._lock:
                self.connections[config.device_name] = connection
            
            if log_callback:
                log_callback(f"{config.device_name} device connection created: {ip_address}", "INFO")
            
            return True
            
        except Exception as e:
            error_msg = f"Failed to add {config.device_name} at {ip_address}: {str(e)}"
            if log_callback:
                log_callback(error_msg, "ERROR")
            print(error_msg)
            return False
    
    def start(self) -> None:
        """Start all device connections and begin data collection."""
        with self._lock:
            if self._running:
                return
            
            self.stop_event.clear()
            self._running = True
            
            # Start all threads
            for connection in self.connections.values():
                connection.thread.start()
    
    def stop(self) -> None:
        """Stop all device connections and data collection."""
        with self._lock:
            if not self._running:
                return
            
            self._running = False
            self.stop_event.set()
            
            # Wait for all threads to finish
            for connection in self.connections.values():
                if connection.thread.is_alive():
                    connection.thread.join(timeout=5.0)
            
            # Close all clients
            for connection in self.connections.values():
                try:
                    connection.client.close()
                except Exception:
                    pass
    
    def get_statistics(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics from all device connections.
        
        Returns:
            Dictionary mapping device names to their statistics
        """
        with self._lock:
            return {
                name: conn.statistics.copy()
                for name, conn in self.connections.items()
            }
    
    def get_io_database(self) -> IODatabase:
        """Get the shared IODatabase.
        
        Returns:
            The shared IODatabase instance
        """
        return self.io_database
    
    def clear_database(self) -> None:
        """Clear the shared IODatabase."""
        with self._lock:
            self.io_database.clear()
    
    def _collect_from_device(
        self,
        config: DeviceConfig,
        client: IGXWebsocketClient,
        channels: Dict[str, Any],
        env_channels: Optional[Dict[str, Any]],
        model: Any,
        field_to_path: Dict[str, str],
        ip_address: str,
    ) -> None:
        """Collect data from a single device into the shared IODatabase.
        
        This is the thread target for each device connection.
        """
        first_timestamp: Optional[int] = None
        
        try:
            # Subscribe all channels with buffered data
            client.sendSubscribeFields({
                field: True for field in channels.values()
            })
            client.updateSubscribedFields()
            
            # Main collection loop
            while not self.stop_event.is_set():
                # Update subscribed fields
                client.updateSubscribedFields()
                
                # Collect all data from this device
                first_timestamp = self._collect_all_channel_data(
                    channels, field_to_path, first_timestamp
                )
                
                # Small sleep to prevent tight loop
                threading.Event().wait(0.001)
                
        except Exception as e:
            print(f"Error collecting data from {config.device_name} at {ip_address}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            try:
                client.close()
            except Exception:
                pass
    
    def _collect_all_channel_data(
        self,
        channels: Dict[str, Any],
        field_to_path: Dict[str, str],
        first_timestamp: Optional[int],
    ) -> Optional[int]:
        """Collect ALL data from all channels and store in shared IODatabase.
        
        Args:
            channels: Dictionary mapping field names to IGXField objects
            field_to_path: Dictionary mapping field names to channel paths
            first_timestamp: Current first timestamp (or None)
            
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
                channel_path = field_to_path.get(field_name)
                if not channel_path:
                    # Fallback: try to get path from field object
                    try:
                        channel_path = channel.getPath()
                    except (AttributeError, TypeError):
                        # Last resort: use field name
                        channel_path = field_name
                
                # Ensure channel exists in database
                with self._lock:
                    if channel_path not in self.io_database.get_all_channels():
                        self.io_database.add_channel(channel_path)
                
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
                    
                    # Add to shared database (thread-safe)
                    with self._lock:
                        self.io_database.add_data_point(channel_path, value, ts_ns)
                        
            except Exception as e:
                print(f"Error collecting data from {field_name}: {e}")
                continue
        
        return updated_first_timestamp


# Device configuration creators (for backward compatibility)
def create_ic256_channels(client: IGXWebsocketClient) -> Dict[str, Any]:
    """Create channel dictionary for IC256 device."""
    from .device_paths import IC256_45_PATHS
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
    from .device_paths import IC256_45_PATHS
    return {
        "temperature": client.field(IC256_45_PATHS["environmental_sensor"]["temperature"]),
        "humidity": client.field(IC256_45_PATHS["environmental_sensor"]["humidity"]),
        "pressure": client.field(IC256_45_PATHS["environmental_sensor"]["pressure"]),
        "connected": client.field(IC256_45_PATHS["environmental_sensor"]["state"]),
    }


def create_tx2_channels(client: IGXWebsocketClient) -> Dict[str, Any]:
    """Create channel dictionary for TX2 device."""
    from .device_paths import TX2_PATHS
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
    model_creator=lambda: IC256Model(),
)

TX2_CONFIG = DeviceConfig(
    device_name="TX2",
    device_type="TX2",
    channel_creator=create_tx2_channels,
    env_channel_creator=None,
    filename_prefix="TX2",
)


def get_timestamp_strings() -> tuple[str, str]:
    """Get current date and time strings for filenames.
    
    Returns:
        Tuple of (date_string, time_string) in format (YYYYMMDD, HHMMSS)
    """
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%H%M%S")
