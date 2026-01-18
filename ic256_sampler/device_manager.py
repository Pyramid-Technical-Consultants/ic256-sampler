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
    
    def _check_existing_connection(
        self,
        config: DeviceConfig,
        ip_address: str,
        sampling_rate: int,
        log_callback: Optional[Callable[[str, str], None]],
    ) -> Optional[bool]:
        """Check if we can reuse an existing connection.
        
        Returns:
            True if connection was reused, False if old connection was removed,
            None if no existing connection
        """
        with self._lock:
            if config.device_name not in self.connections:
                return None
            
            existing_conn = self.connections[config.device_name]
            if existing_conn.ip_address == ip_address:
                existing_conn.model.setup_device(existing_conn.client, sampling_rate)
                if log_callback:
                    log_callback(f"Reusing existing connection for {config.device_name} at {ip_address}", "INFO")
                return True
            else:
                # IP changed - remove old connection
                try:
                    # Stop and join main collection thread
                    if existing_conn.thread.is_alive():
                        existing_conn.thread.join(timeout=1.0)
                    # Stop and join keepalive thread
                    if existing_conn.keepalive_thread and existing_conn.keepalive_thread.is_alive():
                        existing_conn.keepalive_thread.join(timeout=0.5)
                    existing_conn.client.close()
                except Exception:
                    pass
                del self.connections[config.device_name]
                return False
    
    def _create_device_connection(
        self,
        config: DeviceConfig,
        ip_address: str,
        sampling_rate: int,
        log_callback: Optional[Callable[[str, str], None]],
    ) -> Optional[DeviceConnection]:
        """Create a new device connection.
        
        Returns:
            DeviceConnection if successful, None otherwise
        """
        if log_callback:
            log_callback(f"Connecting to {config.device_name} device at {ip_address}", "INFO")
        
        try:
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
            
            # Test connection
            connection_working = False
            try:
                client.updateSubscribedFields()
                connection_working = True
            except Exception as e:
                if log_callback:
                    log_callback(f"Connection test failed for {config.device_name}: {e}", "WARNING")
            
            # Create threads
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
            
            return connection
            
        except Exception as e:
            error_msg = f"Failed to create connection for {config.device_name} at {ip_address}: {str(e)}"
            if log_callback:
                log_callback(error_msg, "ERROR")
            print(error_msg)
            return None
    
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
        
        # Check if we can reuse existing connection
        reuse_result = self._check_existing_connection(config, ip_address, sampling_rate, log_callback)
        if reuse_result is True:
            return True
        
        # Create new connection
        connection = self._create_device_connection(config, ip_address, sampling_rate, log_callback)
        if connection is None:
            return False
        
        # Store connection
        with self._lock:
            self.connections[config.device_name] = connection
        
        self._notify_status_change()
        
        if log_callback:
            log_callback(f"{config.device_name} device connection created: {ip_address}", "INFO")
        
        return True
    
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
        
        Non-blocking method that signals threads to stop. Does not close connections
        or clear the database - those are handled separately.
        """
        with self._lock:
            if not self._running:
                return
            self._running = False
            self.stop_event.set()
    
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
    
    def _update_connection_status(self, device_name: str, new_status: str) -> bool:
        """Update connection status for a device.
        
        Args:
            device_name: Name of the device
            new_status: New status ("connected", "disconnected", "error")
            
        Returns:
            True if status changed, False otherwise
        """
        with self._lock:
            if device_name not in self.connections:
                return False
            conn = self.connections[device_name]
            with conn._status_lock:
                old_status = conn._connection_status
                if old_status != new_status:
                    conn._connection_status = new_status
                    return True
        return False
    
    def _is_connection_valid(self, device_name: str, client: IGXWebsocketClient) -> bool:
        """Check if connection still exists and is valid.
        
        Args:
            device_name: Name of the device
            client: WebSocket client to check
            
        Returns:
            True if connection is valid, False otherwise
        """
        with self._lock:
            if device_name not in self.connections:
                return False
            conn = self.connections.get(device_name)
            if not conn or conn.client is not client:
                return False
            # Safely check websocket connection status
            try:
                return client.ws != "" and client.ws.connected
            except (AttributeError, TypeError):
                return False
    
    @staticmethod
    def _is_connection_error(e: Exception) -> bool:
        """Check if exception is a connection-related error."""
        error_str = str(e).lower()
        error_type = type(e).__name__.lower()
        keywords = ['connection', 'socket', 'network', 'timeout', 'broken', 'closed', 'abort', 'reset']
        return any(kw in error_str or kw in error_type for kw in keywords)
    
    def _keepalive_message_loop(
        self,
        client: IGXWebsocketClient,
        channels: Dict[str, Any],
        field_to_path: Dict[str, str],
        device_name: str,
    ) -> None:
        """Keep-alive message loop that runs continuously to keep the connection active."""
        try:
            while True:
                if not self._is_connection_valid(device_name, client):
                    break
                
                try:
                    if client.ws != "" and client.ws.connected:
                        client.updateSubscribedFields()
                        if self._update_connection_status(device_name, "connected"):
                            self._notify_status_change()
                    else:
                        if self._update_connection_status(device_name, "disconnected"):
                            self._notify_status_change()
                    time.sleep(0.001)
                    
                except (ConnectionAbortedError, ConnectionResetError, OSError) as e:
                    if self._update_connection_status(device_name, "error"):
                        self._notify_status_change()
                    
                    print(f"Connection error in keep-alive loop: {e}. Attempting reconnect...")
                    try:
                        client.reconnect()
                        print(f"Successfully reconnected: {device_name}")
                        if self._update_connection_status(device_name, "connected"):
                            self._notify_status_change()
                        time.sleep(0.1)
                    except Exception as reconnect_error:
                        print(f"Failed to reconnect: {device_name}, error: {reconnect_error}")
                        break
                except Exception as e:
                    if self._is_connection_error(e):
                        if self._update_connection_status(device_name, "disconnected"):
                            self._notify_status_change()
                        break
                    print(f"Error in keep-alive loop: {e}")
                    time.sleep(0.1)
                    
        except Exception as e:
            print(f"Fatal error in keep-alive loop: {e}")
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
            status_dict = {}
            for name, conn in self.connections.items():
                with conn._status_lock:
                    status_dict[name] = conn._connection_status
            return status_dict
    
    def _remove_connection(self, device_name: str) -> bool:
        """Remove a device connection.
        
        Args:
            device_name: Name of the device to remove
            
        Returns:
            True if a connection was removed, False otherwise
        """
        with self._lock:
            if device_name not in self.connections:
                return False
            
            old_conn = self.connections[device_name]
            try:
                # Stop and join main collection thread
                if old_conn.thread.is_alive():
                    old_conn.thread.join(timeout=1.0)
                # Stop and join keepalive thread
                if old_conn.keepalive_thread and old_conn.keepalive_thread.is_alive():
                    old_conn.keepalive_thread.join(timeout=0.5)
                old_conn.client.close()
            except Exception:
                pass
            del self.connections[device_name]
            return True
    
    def _ensure_device_connection(
        self,
        config: DeviceConfig,
        ip_address: Optional[str],
        sampling_rate: int,
        log_callback: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        """Ensure a device connection exists with the given IP address.
        
        Args:
            config: Device configuration
            ip_address: IP address (None or empty to remove connection)
            sampling_rate: Sampling rate in Hz
            log_callback: Optional callback for logging (message, level)
        """
        if ip_address:
            with self._lock:
                needs_connection = (
                    config.device_name not in self.connections or
                    self.connections[config.device_name].ip_address != ip_address
                )
            
            if needs_connection:
                # Remove old connection if it exists
                if self._remove_connection(config.device_name):
                    self._notify_status_change()
                
                # Create new connection
                self.add_device(config, ip_address, sampling_rate, log_callback)
        else:
            # IP is empty - remove connection if it exists
            if self._remove_connection(config.device_name):
                self._notify_status_change()
    
    def ensure_connections(
        self,
        ic256_ip: Optional[str],
        tx2_ip: Optional[str],
        sampling_rate: int,
        log_callback: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        """Ensure websocket connections exist for configured devices.
        
        Creates connections if they don't exist, or updates them if IPs changed.
        Connections persist for the entire program lifecycle.
        
        Args:
            ic256_ip: IC256 IP address (None or empty to remove connection)
            tx2_ip: TX2 IP address (None or empty to remove connection)
            sampling_rate: Sampling rate in Hz
            log_callback: Optional callback for logging (message, level)
        """
        self._ensure_device_connection(IC256_CONFIG, ic256_ip, sampling_rate, log_callback)
        self._ensure_device_connection(TX2_CONFIG, tx2_ip, sampling_rate, log_callback)
    
    def _ensure_connection_open(self, connection: DeviceConnection, device_name: str, log_callback: Optional[Callable[[str, str], None]]) -> bool:
        """Ensure a device connection is open, reconnecting if necessary."""
        if connection.client.ws != "" and connection.client.ws.connected:
            return True
        
        if log_callback:
            log_callback(f"Connection closed for {device_name}. Attempting reconnect...", "WARNING")
        
        try:
            connection.client.reconnect()
            return True
        except Exception as e:
            if log_callback:
                log_callback(f"Failed to reconnect {device_name}: {e}", "ERROR")
            return False
    
    def _setup_device_and_resubscribe(self, connection: DeviceConnection, sampling_rate: int) -> bool:
        """Set up device and re-subscribe to data channels.
        
        Args:
            connection: Device connection
            sampling_rate: Sampling rate in Hz
            
        Returns:
            True if setup succeeded, False otherwise
        """
        try:
            connection.model.setup_device(connection.client, sampling_rate)
            # CRITICAL: setup_device() calls sendSubscribeFields() which REPLACES
            # the subscribedFields dictionary with only frequency fields, losing
            # all data channel subscriptions. We must re-subscribe to data channels.
            connection.client.sendSubscribeFields({
                field: True for field in connection.channels.values()
            })
            return True
        except Exception:
            return False
    
    def setup_device_for_collection(
        self,
        device_name: str,
        sampling_rate: int,
        log_callback: Optional[Callable[[str, str], None]] = None,
    ) -> bool:
        """Set up a device for data collection (setup_device and re-subscribe channels).
        
        Args:
            device_name: Name of the device to set up
            sampling_rate: Sampling rate in Hz
            log_callback: Optional callback for logging (message, level)
            
        Returns:
            True if setup succeeded, False otherwise
        """
        with self._lock:
            if device_name not in self.connections:
                if log_callback:
                    log_callback(f"Device {device_name} not found in connections", "ERROR")
                return False
            connection = self.connections[device_name]
        
        # Ensure connection is open
        if not self._ensure_connection_open(connection, device_name, log_callback):
            return False
        
        # Try to setup device
        if self._setup_device_and_resubscribe(connection, sampling_rate):
            return True
        
        # Setup failed - check if connection is still open
        if connection.client.ws == "" or not connection.client.ws.connected:
            if not self._ensure_connection_open(connection, device_name, log_callback):
                return False
            return self._setup_device_and_resubscribe(connection, sampling_rate)
        
        # Connection still open but setup failed - try to re-subscribe anyway
        try:
            connection.client.sendSubscribeFields({
                field: True for field in connection.channels.values()
            })
            return True
        except Exception:
            return False
    
    def setup_single_device(
        self,
        ip_address: str,
        device_type: str,
        sampling_rate: int,
        log_callback: Optional[Callable[[str, str], None]] = None,
    ) -> bool:
        """Set up a single device for configuration (standalone, not for collection).
        
        This is used for the "Apply" button setup, not for data collection.
        
        Args:
            ip_address: IP address of the device
            device_type: Device type ("IC256" or "TX2")
            sampling_rate: Sampling rate in Hz
            log_callback: Optional callback for logging (message, level)
            
        Returns:
            True if setup succeeded, False otherwise
        """
        from .utils import is_valid_device
        from .ic256_model import IC256Model
        
        if not ip_address or not is_valid_device(ip_address, device_type):
            return False
        
        client = None
        try:
            if log_callback:
                log_callback(f"Setting up {device_type} device at {ip_address}", "INFO")
            client = IGXWebsocketClient(ip_address)
            # Set up device using model
            if device_type == "IC256":
                model = IC256Model()
                model.setup_device(client, sampling_rate)
            if log_callback:
                log_callback(f"{device_type} device setup successful at {ip_address}", "INFO")
            return True
        except Exception as e:
            error_msg = f"Failed to set up {device_type} at {ip_address}: {str(e)}"
            if log_callback:
                log_callback(error_msg, "ERROR")
            print(error_msg)
            return False
        finally:
            if client:
                try:
                    client.close()
                except Exception:
                    pass
    
    def restore_fan_setting(
        self,
        ip_address: str,
        log_callback: Optional[Callable[[str, str], None]] = None,
        timeout: float = 20.0,
    ) -> None:
        """Restore IC256 fan setting in a background thread.
        
        Args:
            ip_address: IP address of the IC256 device
            log_callback: Optional callback for logging (message, level)
            timeout: Timeout for HTTP request in seconds
        """
        import requests
        from .device_paths import IC256_45_PATHS, build_http_url
        import time
        
        try:
            url_fan_ic256 = build_http_url(ip_address, IC256_45_PATHS["io"]["fan_control"])
            time.sleep(2.0)  # Delay to let device finish data collection cleanup
            requests.put(url_fan_ic256, json="on_mode", timeout=timeout)
            if log_callback:
                log_callback(
                    f"Fan setting restored: {IC256_45_PATHS['io']['fan_control']}",
                    "INFO"
                )
        except requests.exceptions.Timeout:
            if log_callback:
                log_callback(
                    "Fan restore timed out (device may be busy) - this is not critical",
                    "WARNING"
                )
        except requests.exceptions.RequestException as e:
            if log_callback:
                log_callback(
                    f"Fan restore failed (non-critical): {str(e)}",
                    "WARNING"
                )
        except Exception as e:
            if log_callback:
                log_callback(f"Fan restore error (ignored): {str(e)}", "WARNING")
    
    def _collect_from_device(
        self,
        config: DeviceConfig,
        client: IGXWebsocketClient,
        channels: Dict[str, Any],
        model: Any,
        field_to_path: Dict[str, str],
        ip_address: str,
    ) -> None:
        """Collect data from a single device into the shared IODatabase."""
        first_timestamp: Optional[int] = None
        
        try:
            while not self.stop_event.is_set():
                try:
                    client.updateSubscribedFields()
                except Exception as e:
                    print(f"Warning: updateSubscribedFields failed: {e}")
                
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
        all_points: list[tuple[str, Any, int]] = []
        channels_to_add: set[str] = set()
        
        # Collect all data points first (without holding lock)
        potential_channels = set()
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
                
                potential_channels.add(channel_path)
                
                # Process data points
                for data_point in data:
                    if not isinstance(data_point, (list, tuple)) or len(data_point) < 2:
                        continue
                    
                    value = data_point[0]
                    ts_raw = data_point[1]
                    
                    # Convert timestamp to nanoseconds
                    try:
                        if isinstance(ts_raw, float):
                            ts_ns = int(ts_raw * 1e9 if ts_raw < 1e12 else ts_raw)
                        elif isinstance(ts_raw, int):
                            ts_ns = ts_raw
                        else:
                            continue
                    except (ValueError, TypeError, OverflowError):
                        continue
                    
                    if updated_first_timestamp is None:
                        updated_first_timestamp = ts_ns
                    
                    all_points.append((channel_path, value, ts_ns))
                
                channel.clearDatums()
            except Exception:
                continue
        
        # Batch update database (single lock acquisition)
        if potential_channels or all_points:
            with self._lock:
                # Add new channels if needed
                existing_channels = self.io_database.get_all_channels()
                for channel_path in potential_channels:
                    if channel_path not in existing_channels:
                        self.io_database.add_channel(channel_path)
                
                # Add all data points
                for ch_path, val, ts in all_points:
                    self.io_database.add_data_point(ch_path, val, ts)
        
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
        "high_voltage": client.field(IC256_45_PATHS["high_voltage"]["monitor_voltage_internal"]),
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
    device_name="IC256",
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
