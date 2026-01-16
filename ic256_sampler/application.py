"""Application class for IC256 data collection.

This module provides the Application class which manages the application lifecycle,
device connections, data collection, and GUI coordination.
"""
import sys
import threading
import time
import atexit
import signal
from typing import Dict, Optional, Tuple, Any
import requests
from .gui import GUI
from .ic256_model import IC256Model
from .utils import is_valid_device
from .device_paths import IC256_45_PATHS, build_http_url
from .device_manager import (
    DeviceManager,
    IC256_CONFIG,
    TX2_CONFIG,
    get_timestamp_strings,
)
from .model_collector import ModelCollector, collect_data_with_model
from .gui_helpers import (
    safe_gui_update,
    log_message_safe,
    show_message_safe,
    set_button_state_safe,
)
from .igx_client import IGXWebsocketClient

# Application constants
DEFAULT_SAMPLING_RATE: int = 500  # Hz
MIN_SAMPLING_RATE: int = 1
MAX_SAMPLING_RATE: int = 6000
THREAD_JOIN_TIMEOUT: float = 30.0  # seconds
CONFIG_DELAY: float = 0.5  # seconds - delay before starting data collection
TIME_UPDATE_INTERVAL: float = 0.1  # seconds - interval for time display updates
FAN_RESTORE_TIMEOUT: float = 20.0  # seconds - timeout for fan restore HTTP request


class Application:
    """Main application class that manages state and coordinates components."""
    
    def __init__(self):
        """Initialize the application."""
        self.window: Optional[GUI] = None
        self.stop_event = threading.Event()
        self.device_statistics: Dict[str, Dict[str, Any]] = {}  # device_name -> {rows, file_size, file_path}
        self.collector: Optional[ModelCollector] = None
        self.device_manager: Optional[DeviceManager] = None  # Single persistent device manager with connections
        self.collector_thread: Optional[threading.Thread] = None
        self.stats_thread: Optional[threading.Thread] = None  # Statistics update thread
        self._stopping = False  # Flag to track if we're in stopping mode
        self._cleanup_registered = False  # Track if cleanup is registered
    
    def cleanup(self) -> None:
        """Clean up resources and stop data collection gracefully.
        
        This method should be called on shutdown to ensure:
        - Data collection is stopped
        - All threads are properly terminated
        - Files are closed
        - Device connections are closed
        """
        try:
            # Stop data collection if it's running
            if not self.stop_event.is_set():
                self.stop_event.set()
                self._stopping = True
                
                # Stop DeviceManager
                if self.device_manager:
                    try:
                        self.device_manager.stop()
                    except Exception:
                        pass
                
                # Stop collector
                if self.collector:
                    try:
                        self.collector.stop()
                    except Exception:
                        pass
                
                # Wait for collector thread to finish (with timeout)
                if self.collector_thread and self.collector_thread.is_alive():
                    self.collector_thread.join(timeout=5.0)
                
                # Finalize collector (flush and close files)
                if self.collector:
                    try:
                        self.collector.finalize()
                    except Exception:
                        pass
            
            # Close all websocket connections
            if self.device_manager:
                try:
                    self.device_manager.close_all_connections()
                except Exception:
                    pass
            
        except Exception:
            pass  # Ignore errors during cleanup
    
    def _register_cleanup(self) -> None:
        """Register cleanup handlers for graceful shutdown."""
        if self._cleanup_registered:
            return
        
        self._cleanup_registered = True
        
        # Register cleanup with atexit
        atexit.register(self.cleanup)
        
        # Register signal handler for SIGINT (Ctrl+C) on Unix-like systems
        if hasattr(signal, 'SIGINT'):
            def signal_handler(signum, frame):
                print("\nReceived interrupt signal, shutting down gracefully...")
                self.cleanup()
                if self.window:
                    try:
                        self.window.root.quit()
                    except Exception:
                        pass
                sys.exit(0)
            
            signal.signal(signal.SIGINT, signal_handler)
    
    def _get_gui_values(self) -> Tuple[str, str, str, str]:
        """Get values from GUI entries.
        
        Returns:
            Tuple of (ic256_ip, tx2_ip, note, save_folder)
        """
        if not self.window:
            return "", "", "", ""
        return (
            self.window.ix256_a_entry.get(),
            self.window.tx2_entry.get(),
            self.window.note_entry.get(),
            self.window.path_entry.get(),
        )
    
    def _get_sampling_rate(self) -> int:
        """Get sampling rate from GUI, with validation and defaults.
        
        Returns:
            Valid sampling rate in Hz
        """
        if not self.window:
            return DEFAULT_SAMPLING_RATE
        
        try:
            rate = int(self.window.sampling_entry.get())
            if MIN_SAMPLING_RATE <= rate <= MAX_SAMPLING_RATE:
                return rate
            raise ValueError(f"Sampling rate must be between {MIN_SAMPLING_RATE} and {MAX_SAMPLING_RATE} Hz")
        except ValueError:
            log_message_safe(
                self.window,
                f"Invalid sampling rate, using default: {DEFAULT_SAMPLING_RATE} Hz",
                "WARNING"
            )
            return DEFAULT_SAMPLING_RATE
    
    def _log_callback(self, message: str, level: str) -> None:
        """Callback function for logging from device setup."""
        log_message_safe(self.window, message, level)
    
    def _connection_status_callback(self, status_dict: Dict[str, str]) -> None:
        """Callback function for connection status updates from device manager.
        
        Args:
            status_dict: Dictionary mapping device names to status strings
        """
        if self.window:
            safe_gui_update(
                self.window,
                lambda s=status_dict: self.window.update_connection_status(s)
            )
    
    def _ensure_connections(self) -> None:
        """Ensure websocket connections exist for configured devices.
        
        Creates connections if they don't exist, or updates them if IPs changed.
        Connections persist for the entire program lifecycle.
        """
        if not self.window:
            return
        
        # Check if GUI is ready (widgets exist)
        if not hasattr(self.window, 'ix256_a_entry'):
            return  # GUI not fully rendered yet, skip for now
        
        ic256_ip, tx2_ip, _, _ = self._get_gui_values()
        
        # Create device manager if it doesn't exist
        if self.device_manager is None:
            self.device_manager = DeviceManager()
            # Set up connection status callback
            self.device_manager.set_status_callback(self._connection_status_callback)
        
        # Get default sampling rate
        sampling_rate = self._get_sampling_rate()
        
        # Check and update IC256 connection
        if ic256_ip:
            with self.device_manager._lock:
                needs_connection = (
                    IC256_CONFIG.device_name not in self.device_manager.connections or
                    self.device_manager.connections[IC256_CONFIG.device_name].ip_address != ic256_ip
                )
            
            if needs_connection:
                # Close old connection if IP changed
                connection_removed = False
                with self.device_manager._lock:
                    if IC256_CONFIG.device_name in self.device_manager.connections:
                        old_conn = self.device_manager.connections[IC256_CONFIG.device_name]
                        try:
                            if old_conn.thread.is_alive():
                                old_conn.thread.join(timeout=1.0)
                            old_conn.client.close()
                        except Exception:
                            pass
                        del self.device_manager.connections[IC256_CONFIG.device_name]
                        connection_removed = True
                
                # Notify status change after removing connection (outside lock)
                if connection_removed:
                    self.device_manager._notify_status_change()
                
                # Create new connection (but don't start collection yet)
                self.device_manager.add_device(IC256_CONFIG, ic256_ip, sampling_rate, self._log_callback)
        else:
            # IC256 IP is empty - close and remove IC256 connection if it exists
            connection_removed = False
            with self.device_manager._lock:
                if IC256_CONFIG.device_name in self.device_manager.connections:
                    old_conn = self.device_manager.connections[IC256_CONFIG.device_name]
                    try:
                        # Stop any active collection threads
                        if old_conn.thread.is_alive():
                            old_conn.thread.join(timeout=1.0)
                        # Close the websocket connection
                        old_conn.client.close()
                    except Exception:
                        pass
                    # Remove the connection
                    del self.device_manager.connections[IC256_CONFIG.device_name]
                    connection_removed = True
            
            # Notify status change after removing connection (outside lock)
            if connection_removed:
                self.device_manager._notify_status_change()
        
        # Check and update TX2 connection
        if tx2_ip:
            with self.device_manager._lock:
                needs_connection = (
                    TX2_CONFIG.device_name not in self.device_manager.connections or
                    self.device_manager.connections[TX2_CONFIG.device_name].ip_address != tx2_ip
                )
            
            if needs_connection:
                # Close old connection if IP changed
                connection_removed = False
                with self.device_manager._lock:
                    if TX2_CONFIG.device_name in self.device_manager.connections:
                        old_conn = self.device_manager.connections[TX2_CONFIG.device_name]
                        try:
                            if old_conn.thread.is_alive():
                                old_conn.thread.join(timeout=1.0)
                            old_conn.client.close()
                        except Exception:
                            pass
                        del self.device_manager.connections[TX2_CONFIG.device_name]
                        connection_removed = True
                
                # Notify status change after removing connection (outside lock)
                if connection_removed:
                    self.device_manager._notify_status_change()
                
                # Create new connection (but don't start collection yet)
                self.device_manager.add_device(TX2_CONFIG, tx2_ip, sampling_rate, self._log_callback)
        else:
            # TX2 IP is empty - close and remove TX2 connection if it exists
            connection_removed = False
            with self.device_manager._lock:
                if TX2_CONFIG.device_name in self.device_manager.connections:
                    old_conn = self.device_manager.connections[TX2_CONFIG.device_name]
                    try:
                        # Stop any active collection threads
                        if old_conn.thread.is_alive():
                            old_conn.thread.join(timeout=1.0)
                        # Close the websocket connection
                        old_conn.client.close()
                    except Exception:
                        pass
                    # Remove the connection
                    del self.device_manager.connections[TX2_CONFIG.device_name]
                    connection_removed = True
            
            # Notify status change after removing connection (outside lock)
            if connection_removed:
                self.device_manager._notify_status_change()
    
    def _update_elapse_time(self) -> None:
        """Update elapsed time display in GUI."""
        start_time = time.time()
        while not self.stop_event.is_set():
            elapsed_time = time.time() - start_time
            minute_time = f"{int(elapsed_time // 60):02d}"
            second_time = f"{int(elapsed_time % 60):02d}"
            ticks_time = f"{int((elapsed_time - int(elapsed_time)) * 1000):03d}"
            
            safe_gui_update(
                self.window,
                lambda m=minute_time, s=second_time, t=ticks_time: self.window.update_elapse_time(m, s, t)
            )
            time.sleep(TIME_UPDATE_INTERVAL)
    
    def _update_statistics(self) -> None:
        """Update statistics display (rows and file size) in GUI.
        
        Continues updating even after stop_event is set, until collector thread finishes
        AND statistics have stabilized (no changes for a while). This ensures statistics
        are updated while data is being written to CSV after stop, and shows final values.
        """
        consecutive_no_change = 0
        previous_total_rows = 0
        previous_total_size = 0
        
        while True:
            # Continue updating if:
            # 1. Not stopped yet, OR
            # 2. We're in stopping mode and (collector thread is alive OR statistics are still changing)
            collector_thread_alive = (
                self.collector_thread and 
                self.collector_thread.is_alive()
            )
            
            should_continue = (
                not self.stop_event.is_set() or
                (self._stopping and collector_thread_alive)
            )
            
            if self.window and self.device_statistics:
                # Aggregate statistics across all devices
                total_rows = sum(stats.get("rows", 0) for stats in self.device_statistics.values())
                total_size = sum(stats.get("file_size", 0) for stats in self.device_statistics.values())
                
                # Check if statistics have changed
                if self._stopping:
                    if (total_rows == previous_total_rows and 
                        total_size == previous_total_size):
                        consecutive_no_change += 1
                    else:
                        consecutive_no_change = 0
                        previous_total_rows = total_rows
                        previous_total_size = total_size
                    
                    # If collector thread finished and statistics haven't changed for 15 updates (1.5 seconds), we're done
                    if (not collector_thread_alive and 
                        consecutive_no_change >= 15):
                        # Final update before stopping
                        if total_size < 1024:
                            size_str = f"{total_size} B"
                        elif total_size < 1024 * 1024:
                            size_str = f"{total_size / 1024:.1f} KB"
                        else:
                            size_str = f"{total_size / (1024 * 1024):.1f} MB"
                        
                        safe_gui_update(
                            self.window,
                            lambda r=total_rows, s=size_str: self.window.update_statistics(r, s)
                        )
                        break
                else:
                    # Not in stopping mode, update previous values
                    previous_total_rows = total_rows
                    previous_total_size = total_size
                
                # Format file size
                if total_size < 1024:
                    size_str = f"{total_size} B"
                elif total_size < 1024 * 1024:
                    size_str = f"{total_size / 1024:.1f} KB"
                else:
                    size_str = f"{total_size / (1024 * 1024):.1f} MB"
                
                safe_gui_update(
                    self.window,
                    lambda r=total_rows, s=size_str: self.window.update_statistics(r, s)
                )
            
            if not should_continue:
                # Not in stopping mode and stop_event is set, exit
                break
            
            time.sleep(TIME_UPDATE_INTERVAL)
    
    def _device_thread(self) -> None:
        """Main device thread that sets up and starts data collection."""
        if not self.window:
            return
        
        # Stop any previous statistics update thread before starting new acquisition
        # This prevents multiple threads from updating the GUI with stale data
        if self.stats_thread and self.stats_thread.is_alive():
            # Set stop_event to signal old thread to exit
            self.stop_event.set()
            # Wait briefly for thread to exit
            self.stats_thread.join(timeout=0.5)
        
        self.stop_event.clear()
        safe_gui_update(self.window, self.window.reset_elapse_time)
        safe_gui_update(self.window, self.window.reset_statistics)
        set_button_state_safe(self.window, "start_button", "disabled")

        # Get timestamp and configuration
        date, time_str = get_timestamp_strings()
        sampling_rate = self._get_sampling_rate()
        ic256_ip, tx2_ip, note, save_folder = self._get_gui_values()

        # Ensure connections exist (creates or reuses existing connections)
        self._ensure_connections()
        
        # Use the persistent device manager
        device_manager = self.device_manager
        
        # CRITICAL: Set stop_event BEFORE calling stop() so stop() uses the correct event
        device_manager.stop_event = self.stop_event
        
        # CRITICAL: Stop any previous acquisition to reset _running state
        # This ensures start() will actually start the threads
        device_manager.stop()
        
        # Clear the database for new acquisition
        device_manager.clear_database()

        # Check which devices are available (connections already exist)
        devices_added = []
        with device_manager._lock:
            if ic256_ip and IC256_CONFIG.device_name in device_manager.connections:
                devices_added.append(IC256_CONFIG.device_name)
            if tx2_ip and TX2_CONFIG.device_name in device_manager.connections:
                devices_added.append(TX2_CONFIG.device_name)
        
        # Update sampling rate on existing connections and create new threads
        for device_name in devices_added:
            with device_manager._lock:
                connection = device_manager.connections[device_name]
                # Update sampling rate
                # Check if connection is still open before trying to setup
                # The connection should remain open between acquisitions (keep-alive thread maintains it)
                if connection.client.ws == "" or not connection.client.ws.connected:
                    # Connection is actually closed - reconnect is necessary
                    error_msg = f"Connection closed for {device_name}. Attempting reconnect..."
                    log_message_safe(self.window, error_msg, "WARNING")
                    print(f"Warning: {error_msg}")
                    try:
                        connection.client.reconnect()
                    except Exception as reconnect_error:
                        error_msg = f"Failed to reconnect {device_name}: {reconnect_error}"
                        log_message_safe(self.window, error_msg, "ERROR")
                        print(f"Error: {error_msg}")
                        continue  # Skip this device
                
                try:
                    connection.model.setup_device(connection.client, sampling_rate)
                    # CRITICAL: setup_device() calls sendSubscribeFields() which REPLACES
                    # the subscribedFields dictionary with only frequency fields, losing
                    # all data channel subscriptions. We must re-subscribe to data channels.
                    connection.client.sendSubscribeFields({
                        field: True for field in connection.channels.values()
                    })
                except (ConnectionAbortedError, ConnectionResetError, OSError) as e:
                    # Connection error during setup - only reconnect if connection is actually closed
                    # Transient errors should not trigger reconnect (keep-alive thread handles those)
                    error_msg = f"Connection error setting up {device_name}: {e}"
                    log_message_safe(self.window, error_msg, "WARNING")
                    print(f"Warning: {error_msg}")
                    
                    # Connection error during setup
                    # Check if connection is actually closed before reconnecting
                    # During normal stop/start cycles, connections should remain open
                    # Only reconnect if connection is actually closed
                    if connection.client.ws == "" or not connection.client.ws.connected:
                        # Connection is actually closed - reconnect is necessary
                        error_msg = f"Connection closed for {device_name}. Attempting reconnect..."
                        log_message_safe(self.window, error_msg, "WARNING")
                        print(f"Warning: {error_msg}")
                        try:
                            connection.client.reconnect()
                            # Retry setup after reconnect
                            connection.model.setup_device(connection.client, sampling_rate)
                            # CRITICAL: After reconnect, we must re-subscribe to data channels
                            # The reconnect creates a new websocket, so all subscriptions are lost
                            connection.client.sendSubscribeFields({
                                field: True for field in connection.channels.values()
                            })
                        except Exception as retry_error:
                            # Reconnect failed - skip this device
                            error_msg = f"Failed to reconnect {device_name}: {retry_error}"
                            log_message_safe(self.window, error_msg, "ERROR")
                            print(f"Error: {error_msg}")
                            continue  # Skip this device
                    else:
                        # Connection is still open but setup failed - likely transient error
                        # The keep-alive thread should handle reconnection if needed
                        # For now, just log and try to continue with channel re-subscription
                        print(f"Transient connection error for {device_name} during setup (connection still open). "
                              f"Keep-alive thread will handle reconnection if needed.")
                        # Try to re-subscribe to channels even if setup_device failed
                        # This ensures data channels are subscribed even if frequency setup had issues
                        try:
                            connection.client.sendSubscribeFields({
                                field: True for field in connection.channels.values()
                            })
                        except Exception:
                            # If re-subscription also fails, the keep-alive thread will handle reconnection
                            pass
                
                # Ensure old thread is stopped before creating new one
                if connection.thread.is_alive():
                    # Old thread is still running - wait for it to stop
                    connection.thread.join(timeout=1.0)
                
                # Create new data collection thread for this acquisition
                config = connection.config
                thread = threading.Thread(
                    target=device_manager._collect_from_device,
                    name=f"{config.device_type.lower()}_device_{connection.ip_address}",
                    daemon=True,
                    args=(config, connection.client, connection.channels, connection.model, connection.field_to_path, connection.ip_address),
                )
                connection.thread = thread

        # Check if any devices were found
        if len(devices_added) == 0:
            error_msg = "No devices available for data collection. Please configure at least one device and ensure connections are established."
            show_message_safe(self.window, error_msg, "red")
            log_message_safe(self.window, "Data collection start failed: No valid devices found", "ERROR")
            log_message_safe(self.window, f"IC256 IP: {ic256_ip}, TX2 IP: {tx2_ip}", "INFO")
            with device_manager._lock:
                available_connections = list(device_manager.connections.keys())
            log_message_safe(self.window, f"Available connections: {available_connections}", "INFO")
            set_button_state_safe(self.window, "start_button", "normal")
            return

        # Create ModelCollector using the first device's model and reference channel
        # For now, we'll use IC256 if available, otherwise TX2
        primary_device_config = IC256_CONFIG if IC256_CONFIG.device_name in devices_added else TX2_CONFIG
        primary_model = primary_device_config.model_creator()
        reference_channel = primary_model.get_reference_channel()
        
        file_name = f"{primary_device_config.filename_prefix}-{date}-{time_str}.csv"
        file_path = f"{save_folder}/{file_name}"
        
        collector = ModelCollector(
            device_manager=device_manager,
            model=primary_model,
            reference_channel=reference_channel,
            sampling_rate=sampling_rate,
            file_path=file_path,
            device_name=primary_device_config.device_type.lower(),
            note=note,
        )
        
        # Initialize statistics - reset to ensure clean state for new acquisition
        # This ensures file size and row counts start at 0 for the new acquisition
        self.device_statistics = {device: {"rows": 0, "file_size": 0, "file_path": ""} for device in devices_added}
        collector.statistics = self.device_statistics.get(primary_device_config.device_name, {})

        # Show success message
        device_list_str = ", ".join(devices_added)
        verb = "is" if len(devices_added) == 1 else "are"
        
        show_message_safe(self.window, f" {device_list_str} {verb} collecting.", "green")
        log_message_safe(self.window, f"Data collection started: {device_list_str}", "INFO")
        set_button_state_safe(self.window, "stop_button", "normal")

        # Store collector (device_manager is already stored as persistent instance)
        self.collector = collector

        # Start all threads
        time_thread = threading.Thread(
            target=self._update_elapse_time,
            name="elapse_time",
            daemon=True
        )
        time_thread.start()
        
        # Start statistics update thread
        # Store reference so we can stop it before next acquisition
        self.stats_thread = threading.Thread(
            target=self._update_statistics,
            name="statistics_update",
            daemon=True
        )
        self.stats_thread.start()
        
        # Start ModelCollector thread (which starts DeviceManager)
        collector_thread = threading.Thread(
            target=collect_data_with_model,
            name="model_collector",
            daemon=True,
            args=(collector, self.stop_event),
        )
        collector_thread.start()
        self.collector_thread = collector_thread
        
        log_message_safe(self.window, f"Started data collection: {device_list_str}", "INFO")
    
    def _configure_and_start(self) -> None:
        """Configure device and start data collection in a background thread."""
        if not self.window:
            return
        
        try:
            ic256_ip, tx2_ip, _, _ = self._get_gui_values()
            # Check if at least one device IP is provided
            if not ic256_ip and not tx2_ip:
                show_message_safe(self.window, "At least one device IP address (IC256 or TX2) is required.", "red")
                set_button_state_safe(self.window, "start_button", "normal")
                return

            log_message_safe(
                self.window,
                "Pre-config stage disabled - proceeding directly to data collection",
                "INFO"
            )

            time.sleep(CONFIG_DELAY)
            thread = threading.Thread(target=self._device_thread, name="device_thread", daemon=True)
            thread.start()
        except Exception as e:
            error_msg = f"Error starting collection: {str(e)}"
            show_message_safe(self.window, error_msg, "red")
            log_message_safe(self.window, error_msg, "ERROR")
            import traceback
            traceback.print_exc()
            set_button_state_safe(self.window, "start_button", "normal")
    
    def start_collection(self) -> None:
        """Start data collection with proper device configuration (non-blocking)."""
        if not self.window:
            return
        
        set_button_state_safe(self.window, "start_button", "disabled")
        show_message_safe(self.window, "Starting data collection...", "blue")
        
        config_thread = threading.Thread(
            target=self._configure_and_start,
            name="config_and_start",
            daemon=True
        )
        config_thread.start()
    
    def _restore_fan_setting(self, ip_address: str) -> None:
        """Restore IC256 fan setting in a background thread."""
        try:
            url_fan_ic256 = build_http_url(ip_address, IC256_45_PATHS["io"]["fan_out"])
            time.sleep(2.0)  # Delay to let device finish data collection cleanup
            requests.put(url_fan_ic256, json=True, timeout=FAN_RESTORE_TIMEOUT)
            log_message_safe(
                self.window,
                f"Fan setting restored: {IC256_45_PATHS['io']['fan_out']}",
                "INFO"
            )
        except requests.exceptions.Timeout:
            log_message_safe(
                self.window,
                "Fan restore timed out (device may be busy) - this is not critical",
                "WARNING"
            )
        except requests.exceptions.RequestException as e:
            log_message_safe(
                self.window,
                f"Fan restore failed (non-critical): {str(e)}",
                "WARNING"
            )
        except Exception as e:
            log_message_safe(self.window, f"Fan restore error (ignored): {str(e)}", "WARNING")
    
    def _wait_for_threads_and_cleanup(self, device_threads: Dict[str, threading.Thread]) -> None:
        """Wait for data collection threads to finish and perform cleanup."""
        device_names = list(device_threads.keys())
        threads_finished = True
        
        # Wait for threads to finish
        for device_name, thread in device_threads.items():
            thread.join(timeout=THREAD_JOIN_TIMEOUT)
            if thread.is_alive():
                threads_finished = False
                log_message_safe(
                    self.window,
                    f"Warning: {device_name} thread did not finish within timeout",
                    "WARNING"
                )
        
        if not threads_finished:
            log_message_safe(
                self.window,
                "Some threads did not finish cleanly - data may be incomplete",
                "WARNING"
            )

        device_list_str = ", ".join(device_names)
        show_message_safe(self.window, f"Collection completed ({device_list_str})", "green")
        log_message_safe(self.window, f"Data collection stopped: {device_list_str}", "INFO")

        # Restore IC256 fan setting
        ic256_ip, _, _, _ = self._get_gui_values()
        if ic256_ip:
            restore_thread = threading.Thread(
                target=self._restore_fan_setting,
                args=(ic256_ip,),
                name="restore_fan_setting",
                daemon=True
            )
            restore_thread.start()
    
    def stop_collection(self) -> None:
        """Stop data collection and cleanup resources.
        
        This method:
        1. Stops new data collection (sets stop_event, stops DeviceManager)
        2. Uses non-blocking approach to wait for collector thread to finish
        3. Only re-enables start button after all data is written to CSV
        
        Uses GUI after() callbacks to avoid freezing the GUI thread.
        """
        if not self.window:
            return
        
        # Phase 1: Stop new data collection
        self.stop_event.set()
        self._stopping = True  # Mark that we're stopping (allows stats to continue updating)
        
        # Stop DeviceManager (stops collecting new data from devices)
        if self.device_manager:
            try:
                self.device_manager.stop()
            except Exception:
                pass
        
        # Stop collector (marks as stopped, but processing continues)
        if self.collector:
            try:
                self.collector.stop()
            except Exception:
                pass
        
        # Phase 2: Use non-blocking approach to wait for collector thread
        # Update GUI to show we're finishing up
        show_message_safe(self.window, "Finishing data write...", "blue")
        log_message_safe(self.window, "Stopped data collection, finishing CSV write...", "INFO")
        
        # Use after() callback to periodically check if thread is done (non-blocking)
        self._check_collector_thread_finished()
    
    def _check_collector_thread_finished(self) -> None:
        """Periodically check if collector thread is finished (non-blocking).
        
        Uses GUI after() callback to avoid blocking the GUI thread.
        """
        if not self.window:
            return
        
        if self.collector_thread and self.collector_thread.is_alive():
            # Thread still running - check again in 100ms (non-blocking)
            self.window.root.after(100, self._check_collector_thread_finished)
        else:
            # Thread finished - complete cleanup
            self._stopping = False
            # Ensure statistics thread is also stopped
            # Use non-blocking approach - just signal it to stop
            # It will stop naturally when it sees stop_event is set
            if self.stats_thread and self.stats_thread.is_alive():
                self.stop_event.set()
                # Don't wait for it - it will stop naturally
                # If we need to ensure it's stopped, we can check in a callback
            self._finalize_stop()
    
    def _finalize_stop(self) -> None:
        """Finalize stop process after collector thread has finished."""
        if not self.window:
            return
        
        # Phase 3: Reset GUI - only enable start button after all work is done
        set_button_state_safe(self.window, "stop_button", "disabled")
        set_button_state_safe(self.window, "start_button", "normal")
        show_message_safe(self.window, "Data collection stopped.", "blue")
        log_message_safe(self.window, "Data collection stopped and CSV write completed", "INFO")
    
    def _setup_single_device(
        self,
        ip_address: str,
        device_type: str,
        sampling_rate: int,
    ) -> Optional[IGXWebsocketClient]:
        """Set up a single device for configuration.
        
        Returns:
            Client if successful, None otherwise
        """
        if not ip_address or not is_valid_device(ip_address, device_type):
            return None
        
        try:
            log_message_safe(self.window, f"Setting up {device_type} device at {ip_address}", "INFO")
            client = IGXWebsocketClient(ip_address)
            device_name = "ic256_45" if device_type == "IC256" else "tx2"
            # Set up device using model
            if "ic256" in device_name.lower():
                model = IC256Model()
                model.setup_device(client, sampling_rate)
            client.close()
            log_message_safe(self.window, f"{device_type} device setup successful at {ip_address}", "INFO")
            return client
        except Exception as e:
            error_msg = f"Failed to set up {device_type} at {ip_address}: {str(e)}"
            show_message_safe(self.window, error_msg, "red")
            log_message_safe(self.window, error_msg, "ERROR")
            return None
    
    def _setup_thread(self) -> None:
        """Set up device configuration in a separate thread."""
        if not self.window:
            return
        
        set_button_state_safe(self.window, "set_up_button", "disabled", self.window.loading_image)
        
        try:
            sampling_rate = self._get_sampling_rate()
            if not (MIN_SAMPLING_RATE <= sampling_rate <= MAX_SAMPLING_RATE):
                raise ValueError(f"Sampling rate must be between {MIN_SAMPLING_RATE} and {MAX_SAMPLING_RATE} Hz")
            
            log_message_safe(self.window, f"Setting up devices with sampling rate: {sampling_rate} Hz", "INFO")
        except ValueError as e:
            error_msg = f"Invalid sampling rate: {str(e)}"
            show_message_safe(self.window, error_msg, "red")
            log_message_safe(self.window, error_msg, "ERROR")
            set_button_state_safe(self.window, "set_up_button", "normal", self.window.fail_image)
            return

        # Set up devices
        ic256_ip, tx2_ip, _, _ = self._get_gui_values()
        devices_setup = []
        
        if ic256_ip:
            client = self._setup_single_device(ic256_ip, "IC256", sampling_rate)
            if client:
                devices_setup.append("IC256")
        
        if tx2_ip:
            try:
                client = self._setup_single_device(tx2_ip, "TX2", sampling_rate)
                if client:
                    devices_setup.append("TX2")
            except Exception as e:
                log_message_safe(
                    self.window,
                    f"TX2 validation error at {tx2_ip} (skipping): {str(e)}",
                    "WARNING"
                )

        if len(devices_setup) == 0:
            error_msg = "No device found. Please try again."
            show_message_safe(self.window, error_msg, "red")
            log_message_safe(self.window, error_msg, "ERROR")
            set_button_state_safe(self.window, "set_up_button", "normal", self.window.fail_image)
            return

        device_list_str = ", ".join(devices_setup)
        success_msg = f"Update successful. ({device_list_str})"
        show_message_safe(self.window, success_msg, "green")
        log_message_safe(self.window, success_msg, "INFO")
        set_button_state_safe(self.window, "set_up_button", "normal", self.window.pass_image)
    
    def setup_devices(self) -> None:
        """Start device setup in a separate thread."""
        thread = threading.Thread(target=self._setup_thread, name="set_up_device", daemon=True)
        thread.start()
    
    def run(self) -> None:
        """Run the application."""
        # Register cleanup handlers for graceful shutdown
        self._register_cleanup()
        
        try:
            self.window = GUI("IC256-42/35")

            # Override GUI methods with our implementations
            self.window.start = self.start_collection
            self.window.stop = self.stop_collection
            self.window.set_up_device = self.setup_devices
            
            # Set up window close handler to call cleanup
            def on_window_close():
                """Handle window close event."""
                try:
                    # Clean up resources
                    self.cleanup()
                    # Destroy the window
                    if self.window and self.window.root:
                        self.window.root.quit()
                        self.window.root.destroy()
                except Exception as e:
                    # Force exit if cleanup fails
                    print(f"Error during window close: {e}")
                    import sys
                    sys.exit(0)
            
            self.window.on_close = on_window_close

            # Start the GUI (this blocks until window is closed)
            # Connections will be created when needed (when starting collection or when IPs change)
            self.window.render()
        except KeyboardInterrupt:
            # Handle keyboard interrupt gracefully
            print("\nShutting down gracefully...")
            self.cleanup()
            if self.window:
                try:
                    self.window.root.quit()
                    self.window.root.destroy()
                except Exception:
                    pass
            sys.exit(0)
        except Exception as e:
            # Handle any other unexpected errors
            print(f"Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            self.cleanup()
            sys.exit(1)
