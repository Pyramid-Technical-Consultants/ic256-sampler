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
from .gui import GUI
from .device_manager import DeviceManager, IC256_CONFIG, TX2_CONFIG
from .model_collector import ModelCollector
from .statistics_aggregator import StatisticsUpdater
from .gui_helpers import (
    safe_gui_update,
    log_message_safe,
    show_message_safe,
    set_button_state_safe,
)

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
        self.stats_updater: Optional[StatisticsUpdater] = None  # Statistics updater
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
    
    def _ensure_device_manager(self) -> None:
        """Ensure device manager exists and is configured."""
        if self.device_manager is None:
            self.device_manager = DeviceManager()
            self.device_manager.set_status_callback(self._connection_status_callback)
    
    def _handle_collection_error(self, error_msg: str, enable_button: bool = True) -> None:
        """Handle collection errors with consistent messaging.
        
        Args:
            error_msg: Error message to display
            enable_button: Whether to re-enable the start button
        """
        show_message_safe(self.window, error_msg, "red")
        log_message_safe(self.window, error_msg, "ERROR")
        if enable_button:
            set_button_state_safe(self.window, "start_button", "normal")
    
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
        
        # Ensure device manager exists
        self._ensure_device_manager()
        
        # Get default sampling rate
        sampling_rate = self._get_sampling_rate()
        
        # Use DeviceManager to ensure connections
        self.device_manager.ensure_connections(
            ic256_ip,
            tx2_ip,
            sampling_rate,
            self._log_callback
        )
    
    def _update_elapse_time(self) -> None:
        """Update elapsed time display in GUI."""
        if not self.window:
            return
        
        start_time = time.time()
        while not self.stop_event.is_set():
            elapsed_time = time.time() - start_time
            # Format time components efficiently
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)
            ticks = int((elapsed_time - int(elapsed_time)) * 1000)
            
            # Create formatted strings
            minute_time = f"{minutes:02d}"
            second_time = f"{seconds:02d}"
            ticks_time = f"{ticks:03d}"
            
            # Update GUI (lambda captures values correctly with defaults)
            safe_gui_update(
                self.window,
                lambda m=minute_time, s=second_time, t=ticks_time: self.window.update_elapse_time(m, s, t)
            )
            time.sleep(TIME_UPDATE_INTERVAL)
    
    def _create_stats_updater(self) -> StatisticsUpdater:
        """Create or recreate statistics updater with proper callback.
        
        Returns:
            StatisticsUpdater instance
        """
        def update_callback(rows: int, size_str: str):
            if self.window:
                safe_gui_update(
                    self.window,
                    lambda r=rows, s=size_str: self.window.update_statistics(r, s)
                )
        
        return StatisticsUpdater(
            self.device_statistics,
            update_callback,
            TIME_UPDATE_INTERVAL
        )
    
    def _update_statistics(self) -> None:
        """Update statistics display (rows and file size) in GUI.
        
        Continues updating even after stop_event is set, until collector thread finishes
        AND statistics have stabilized (no changes for a while). This ensures statistics
        are updated while data is being written to CSV after stop, and shows final values.
        """
        if not self.window:
            return
        
        # Create or reuse statistics updater
        if not self.stats_updater:
            self.stats_updater = self._create_stats_updater()
        
        # Update collector thread status
        collector_thread_alive = (
            self.collector_thread and 
            self.collector_thread.is_alive()
        )
        self.stats_updater.set_collector_thread_alive(collector_thread_alive)
        self.stats_updater.set_stopping(self._stopping)
        
        # Run update loop
        self.stats_updater.update_loop(self.stop_event)
    
    def _stop_previous_threads(self) -> None:
        """Stop any previous collection threads before starting new acquisition."""
        # Stop device manager to reset _running state before new acquisition
        if self.device_manager:
            self.device_manager.stop()
        
        # Stop previous statistics thread if running
        if self.stats_thread and self.stats_thread.is_alive():
            # Use a separate flag to stop stats thread without affecting collection
            if self.stats_updater:
                self.stats_updater.set_stopping(True)
                self.stats_updater.set_collector_thread_alive(False)
            # Wait briefly for thread to exit
            self.stats_thread.join(timeout=0.5)
            self.stats_thread = None
        
        # Reset stats updater for new collection
        self.stats_updater = None
    
    def _device_thread(self) -> None:
        """Main device thread that sets up and starts data collection."""
        if not self.window:
            return
        
        # Stop any previous threads and reset state
        self._stop_previous_threads()
        
        # Clear stop event and reset GUI
        self.stop_event.clear()
        self._stopping = False
        safe_gui_update(self.window, self.window.reset_elapse_time)
        safe_gui_update(self.window, self.window.reset_statistics)
        set_button_state_safe(self.window, "start_button", "disabled")

        # Get configuration
        sampling_rate = self._get_sampling_rate()
        ic256_ip, tx2_ip, note, save_folder = self._get_gui_values()

        # Ensure connections exist (creates or reuses existing connections)
        self._ensure_connections()
        
        # Get devices that should be added
        devices_added = ModelCollector.get_devices_added(
            self.device_manager,
            ic256_ip,
            tx2_ip
        )
        
        # Check if any devices were found
        if len(devices_added) == 0:
            error_msg = "No devices available for data collection. Please configure at least one device and ensure connections are established."
            self._handle_collection_error(error_msg)
            log_message_safe(self.window, f"IC256 IP: {ic256_ip}, TX2 IP: {tx2_ip}", "INFO")
            if self.device_manager:
                with self.device_manager._lock:
                    available_connections = list(self.device_manager.connections.keys())
                log_message_safe(self.window, f"Available connections: {available_connections}", "INFO")
            return
        
        # Create collector using factory method
        collector = ModelCollector.create_for_collection(
            self.device_manager,
            devices_added,
            sampling_rate,
            save_folder,
            note,
            self.device_statistics,
            log_callback=self._log_callback,
        )
        
        if not collector:
            self._handle_collection_error("Failed to create collector")
            return
        
        # Prepare devices for collection
        if not collector.prepare_devices_for_collection(
            devices_added,
            sampling_rate,
            self.stop_event,
            self._log_callback
        ):
            self._handle_collection_error("Failed to prepare devices for collection")
            return
        
        # Store collector
        self.collector = collector

        # Show success message and update UI
        device_list_str = ", ".join(devices_added)
        verb = "is" if len(devices_added) == 1 else "are"
        success_msg = f" {device_list_str} {verb} collecting."
        
        show_message_safe(self.window, success_msg, "green")
        log_message_safe(self.window, f"Data collection started: {device_list_str}", "INFO")
        set_button_state_safe(self.window, "stop_button", "normal")

        # Start all threads
        # Elapsed time thread
        threading.Thread(
            target=self._update_elapse_time,
            name="elapse_time",
            daemon=True
        ).start()
        
        # Statistics update thread
        self.stats_thread = threading.Thread(
            target=self._update_statistics,
            name="statistics_update",
            daemon=True
        )
        self.stats_thread.start()
        
        # Collection thread
        self.collector_thread = threading.Thread(
            target=collector.run_collection,
            name="model_collector",
            daemon=True,
            args=(self.stop_event,),
        )
        self.collector_thread.start()
        
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
            self._handle_collection_error(error_msg)
            import traceback
            traceback.print_exc()
    
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
        if self.device_manager:
            self.device_manager.restore_fan_setting(
                ip_address,
                self._log_callback
            )
    
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
            # Statistics thread will stop naturally when it sees stop_event is set
            # No need to set stop_event again - it's already set
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

        # Set up devices using DeviceManager
        ic256_ip, tx2_ip, _, _ = self._get_gui_values()
        devices_setup = []
        
        # Ensure device manager exists
        self._ensure_device_manager()
        
        # Setup IC256 device
        if ic256_ip:
            if self.device_manager.setup_single_device(
                ic256_ip, "IC256", sampling_rate, self._log_callback
            ):
                devices_setup.append("IC256")
        
        # Setup TX2 device
        if tx2_ip:
            try:
                if self.device_manager.setup_single_device(
                    tx2_ip, "TX2", sampling_rate, self._log_callback
                ):
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
                    self.cleanup()
                    if self.window and self.window.root:
                        self.window.root.quit()
                        self.window.root.destroy()
                except Exception as e:
                    print(f"Error during window close: {e}")
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
