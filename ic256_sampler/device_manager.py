"""Device Manager - OOP class for managing multiple device connections.

This module provides the DeviceManager class which handles multiple IGXWebsocketClient
connections, creates threads for data collection, and maintains a shared IODatabase
that all devices feed into.
"""

from typing import Dict, Optional, Any, Callable, List
import threading
import time
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
        model: Any,
        field_to_path: Dict[str, str],
        thread: threading.Thread,
        keepalive_thread: threading.Thread,
    ):
        self.config = config
        self.ip_address = ip_address
        self.client = client
        self.channels = channels
        self.model = model
        self.field_to_path = field_to_path
        self.thread = thread
        self.keepalive_thread = keepalive_thread
        self.statistics: Dict[str, Any] = {"rows": 0, "file_size": 0}
        self._connection_status = "disconnected"  # "connected", "disconnected", "error"
        self._status_lock = threading.Lock()  # Lock for thread-safe status updates


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
        self._status_callback: Optional[Callable[[Dict[str, str]], None]] = None  # Callback for status updates
    
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
        
        if not is_valid_device(ip_address, config.device_type):
            if log_callback:
                log_callback(
                    f"{config.device_name} device at {ip_address} validation failed (skipping)",
                    "WARNING"
                )
            return False
        
        try:
            with self._lock:
                if config.device_name in self.connections:
                    existing_conn = self.connections[config.device_name]
                    if existing_conn.ip_address == ip_address:
                        existing_conn.model.setup_device(existing_conn.client, sampling_rate)
                        
                        if log_callback:
                            log_callback(f"Reusing existing connection for {config.device_name} at {ip_address}", "INFO")
                        return True
                    else:
                        try:
                            if existing_conn.thread.is_alive():
                                existing_conn.thread.join(timeout=1.0)
                            existing_conn.client.close()
                        except Exception:
                            pass
                        del self.connections[config.device_name]
            
            if log_callback:
                log_callback(f"Connecting to {config.device_name} device at {ip_address}", "INFO")
            
            client = IGXWebsocketClient(ip_address)
            channels = config.channel_creator(client)
            
            model = config.model_creator()
            if model is None:
                raise ValueError(f"No model creator for device type: {config.device_type}")
            
            model.setup_device(client, sampling_rate)
            field_to_path = model.get_field_to_path_mapping()
            
            client.sendSubscribeFields({
                field: True for field in channels.values()
            })
            
            connection_working = False
            try:
                client.updateSubscribedFields()
                connection_working = True
            except Exception as e:
                connection_working = False
                if log_callback:
                    log_callback(f"Connection test failed for {config.device_name}: {e}", "WARNING")
            
            keepalive_thread = threading.Thread(
                target=self._keepalive_message_loop,
                name=f"{config.device_type.lower()}_keepalive_{ip_address}",
                daemon=True,
                args=(client, channels, field_to_path, config.device_name),
            )
            keepalive_thread.start()
            
            thread = threading.Thread(
                target=self._collect_from_device,
                name=f"{config.device_type.lower()}_device_{ip_address}",
                daemon=True,
                args=(config, client, channels, model, field_to_path, ip_address),
            )
            
            connection = DeviceConnection(
                config=config,
                ip_address=ip_address,
                client=client,
                channels=channels,
                model=model,
                field_to_path=field_to_path,
                thread=thread,
                keepalive_thread=keepalive_thread,
            )
            
            initial_status = "connected" if connection_working else "disconnected"
            with connection._status_lock:
                connection._connection_status = initial_status
            
            with self._lock:
                self.connections[config.device_name] = connection
            
            self._notify_status_change()
            
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
            
            # Start all threads (only if not already started)
            for connection in self.connections.values():
                if not connection.thread.is_alive():
                    connection.thread.start()
    
    def stop(self) -> None:
        """Stop all device connections and data collection.
        
        Note: This does NOT close the websocket connections - they are kept alive
        for reuse between acquisitions. Connections persist for the entire program lifecycle.
        
        Note: This does NOT clear the database - the caller should clear it after
        processing is complete to avoid losing data that's still being processed.
        """
        with self._lock:
            if not self._running:
                return
            
            self._running = False
            self.stop_event.set()
            
            # Wait for all threads to finish
            for connection in self.connections.values():
                if connection.thread.is_alive():
                    connection.thread.join(timeout=5.0)
            
            # NOTE: Database is NOT cleared here - caller should clear it after
            # processing is complete to avoid losing data that's still being processed
    
    def close_all_connections(self) -> None:
        """Close all websocket connections.
        
        This should be called when:
        - IP addresses change
        - Application is shutting down
        
        This method stops all threads (including keep-alive threads) and closes connections.
        """
        with self._lock:
            if self._running:
                self._running = False
                self.stop_event.set()
                
                for connection in self.connections.values():
                    if connection.thread.is_alive():
                        connection.thread.join(timeout=1.0)
            
            for connection in self.connections.values():
                try:
                    connection.client.close()
                except Exception:
                    pass
            
            for connection in list(self.connections.values()):
                if connection.keepalive_thread and connection.keepalive_thread.is_alive():
                    connection.keepalive_thread.join(timeout=0.5)
            
            self.connections.clear()
    
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
    
    def _keepalive_message_loop(
        self,
        client: IGXWebsocketClient,
        channels: Dict[str, Any],
        field_to_path: Dict[str, str],
        device_name: str,
    ) -> None:
        """Keep-alive message loop that runs continuously to keep the connection active.
        
        This thread:
        - Continuously calls updateSubscribedFields() to process incoming messages
        - Keeps the websocket connection alive and exercised
        - Processes messages even when not collecting (data is ignored outside collection)
        - Updates connection status based on websocket health
        
        Args:
            client: The websocket client
            channels: Dictionary of channel fields
            field_to_path: Dictionary mapping field names to channel paths
            device_name: Name of the device for status tracking
        """
        try:
            while True:
                with self._lock:
                    if device_name not in self.connections:
                        break
                    conn = self.connections.get(device_name)
                    if not conn or conn.client.ws == "":
                        break
                
                try:
                    is_connected = False
                    try:
                        if client.ws != "":
                            client.updateSubscribedFields()
                            is_connected = True
                        else:
                            is_connected = False
                    except (ConnectionAbortedError, ConnectionResetError, OSError):
                        is_connected = False
                        break
                    except Exception as e:
                        error_str = str(e).lower()
                        error_type = type(e).__name__.lower()
                        if any(keyword in error_str or keyword in error_type for keyword in ['connection', 'socket', 'network', 'timeout', 'broken', 'closed', 'abort', 'reset']):
                            is_connected = False
                            break
                        else:
                            is_connected = True
                    
                    status_changed = False
                    with self._lock:
                        if device_name in self.connections:
                            conn = self.connections[device_name]
                            with conn._status_lock:
                                old_status = conn._connection_status
                                conn._connection_status = "connected" if is_connected else "disconnected"
                                if old_status != conn._connection_status:
                                    status_changed = True
                    
                    if status_changed:
                        self._notify_status_change()
                    
                    time.sleep(0.001)
                    
                except (ConnectionAbortedError, ConnectionResetError, OSError) as e:
                    status_changed = False
                    with self._lock:
                        if device_name in self.connections:
                            conn = self.connections[device_name]
                            with conn._status_lock:
                                old_status = conn._connection_status
                                conn._connection_status = "error"
                                if old_status != "error":
                                    status_changed = True
                    
                    if status_changed:
                        self._notify_status_change()
                    
                    print(f"Connection error in keep-alive message loop: {e}")
                    break
                except Exception as e:
                    print(f"Error in keep-alive message loop: {e}")
                    time.sleep(0.1)
                    
        except Exception as e:
            print(f"Fatal error in keep-alive message loop: {e}")
            import traceback
            traceback.print_exc()
    
    def _notify_status_change(self) -> None:
        """Notify status callback of connection status changes.
        
        This method is safe to call from any thread and does not hold locks.
        It should NOT be called while holding self._lock to avoid deadlocks.
        """
        if not hasattr(self, '_status_callback'):
            return
        
        if self._status_callback:
            try:
                status_dict = self.get_connection_status()
                self._status_callback(status_dict)
            except Exception as e:
                print(f"Error in status callback: {e}")
    
    def set_status_callback(self, callback: Optional[Callable[[Dict[str, str]], None]]) -> None:
        """Set callback function for connection status updates.
        
        Args:
            callback: Function that takes a Dict[str, str] mapping device names to status
                     ("connected", "disconnected", "error")
        """
        with self._lock:
            self._status_callback = callback
    
    def get_connection_status(self) -> Dict[str, str]:
        """Get connection status for all devices.
        
        Returns:
            Dictionary mapping device names to their connection status
            ("connected", "disconnected", "error")
        """
        with self._lock:
            return {
                name: conn._connection_status
                for name, conn in self.connections.items()
            }
    
    def _collect_from_device(
        self,
        config: DeviceConfig,
        client: IGXWebsocketClient,
        channels: Dict[str, Any],
        model: Any,
        field_to_path: Dict[str, str],
        ip_address: str,
    ) -> None:
        """Collect data from a single device into the shared IODatabase.
        
        This is the thread target for each device connection during active collection.
        Note: The keep-alive thread handles the message loop, so this thread
        primarily focuses on data collection when active.
        """
        first_timestamp: Optional[int] = None
        
        try:
            while not self.stop_event.is_set():
                first_timestamp = self._collect_all_channel_data(
                    channels, field_to_path, first_timestamp
                )
                time.sleep(0.001)
                
        except Exception as e:
            print(f"Error collecting data from {config.device_name} at {ip_address}: {e}")
            import traceback
            traceback.print_exc()
    
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
        
        for field_name, channel in channels.items():
            try:
                data = channel.getDatums()
                
                if not data:
                    continue
                
                channel_path = field_to_path.get(field_name)
                if not channel_path:
                    try:
                        channel_path = channel.getPath()
                    except (AttributeError, TypeError):
                        channel_path = field_name
                
                with self._lock:
                    if channel_path not in self.io_database.get_all_channels():
                        self.io_database.add_channel(channel_path)
                
                points_to_add = []
                
                for data_point in data:
                    if not isinstance(data_point, (list, tuple)) or len(data_point) < 2:
                        continue
                    
                    value = data_point[0]
                    ts_raw = data_point[1]
                    
                    try:
                        if isinstance(ts_raw, float):
                            if ts_raw < 1e12:
                                ts_ns = int(ts_raw * 1e9)
                            else:
                                ts_ns = int(ts_raw)
                        elif isinstance(ts_raw, int):
                            ts_ns = ts_raw
                        else:
                            continue
                    except (ValueError, TypeError, OverflowError):
                        continue
                    
                    if updated_first_timestamp is None:
                        updated_first_timestamp = ts_ns
                    
                    points_to_add.append((channel_path, value, ts_ns))
                
                if points_to_add:
                    with self._lock:
                        for ch_path, val, ts in points_to_add:
                            self.io_database.add_data_point(ch_path, val, ts)
                    
                    # Clear datums after processing to avoid re-reading the same data
                    # This is safe because we've already processed all the data points
                    channel.clearDatums()
                        
            except Exception as e:
                print(f"Error collecting data from {field_name}: {e}")
                continue
        
        return updated_first_timestamp


def create_ic256_channels(client: IGXWebsocketClient) -> Dict[str, Any]:
    """Create channel dictionary for IC256 device (including environmental channels)."""
    from .device_paths import IC256_45_PATHS
    return {
        "mean_channel_a": client.field(IC256_45_PATHS["adc"]["gaussian_fit_a_mean"]),
        "sigma_channel_a": client.field(IC256_45_PATHS["adc"]["gaussian_fit_a_sigma"]),
        "mean_channel_b": client.field(IC256_45_PATHS["adc"]["gaussian_fit_b_mean"]),
        "sigma_channel_b": client.field(IC256_45_PATHS["adc"]["gaussian_fit_b_sigma"]),
        "primary_channel": client.field(IC256_45_PATHS["adc"]["primary_dose"]),
        "channel_sum": client.field(IC256_45_PATHS["adc"]["channel_sum"]),
        "external_trigger": client.field(IC256_45_PATHS["adc"]["gate_signal"]),
        "temperature": client.field(IC256_45_PATHS["environmental_sensor"]["temperature"]),
        "humidity": client.field(IC256_45_PATHS["environmental_sensor"]["humidity"]),
        "pressure": client.field(IC256_45_PATHS["environmental_sensor"]["pressure"]),
        "env_connected": client.field(IC256_45_PATHS["environmental_sensor"]["state"]),
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
    env_channel_creator=None,  # Environmental channels now included in main channels
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
