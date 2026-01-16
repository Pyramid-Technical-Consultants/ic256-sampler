"""Main application entry point for IC256 data collection.

This module handles application initialization, device connection,
thread management, and coordination between GUI and data collection.
"""
import os
import sys
import portalocker
import threading
import time
import atexit
from typing import Dict, Optional, Tuple, Any
import tempfile
import requests
from .gui import GUI
from .data_collection import set_up_device
from .utils import is_valid_device
from .device_paths import IC256_45_PATHS, build_http_url
from .device_manager import (
    IC256_CONFIG,
    TX2_CONFIG,
    setup_device_thread,
    get_timestamp_strings,
)
from .gui_helpers import (
    safe_gui_update,
    log_message_safe,
    show_message_safe,
    set_button_state_safe,
)
from .igx_client import IGXWebsocketClient

# Application constants
LOCK_FILE_NAME: str = "my_app.lock"
DEFAULT_SAMPLING_RATE: int = 500  # Hz
MIN_SAMPLING_RATE: int = 1
MAX_SAMPLING_RATE: int = 6000
THREAD_JOIN_TIMEOUT: float = 30.0  # seconds
CONFIG_DELAY: float = 0.5  # seconds - delay before starting data collection
TIME_UPDATE_INTERVAL: float = 0.1  # seconds - interval for time display updates
FAN_RESTORE_TIMEOUT: float = 20.0  # seconds - timeout for fan restore HTTP request

# Initialize lock file for single instance enforcement
lock_file_path = os.path.join(tempfile.gettempdir(), LOCK_FILE_NAME)
lock_file = open(lock_file_path, "w")


def cleanup_lock_file() -> None:
    """Clean up lock file on program exit."""
    try:
        if lock_file and not lock_file.closed:
            portalocker.unlock(lock_file)
            lock_file.close()
    except Exception:
        pass  # Ignore errors during cleanup


atexit.register(cleanup_lock_file)

# Enforce single instance
try:
    portalocker.lock(lock_file, portalocker.LOCK_EX | portalocker.LOCK_NB)
except portalocker.LockException:
    print("Another instance is already running.")
    lock_file.close()
    sys.exit(1)


class Application:
    """Main application class that manages state and coordinates components."""
    
    def __init__(self):
        """Initialize the application."""
        self.window: Optional[GUI] = None
        self.stop_event = threading.Event()
        self.device_threads: Dict[str, threading.Thread] = {}
        self.device_statistics: Dict[str, Dict[str, Any]] = {}  # device_name -> {rows, file_size, file_path}
    
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
        """Update statistics display (rows and file size) in GUI."""
        while not self.stop_event.is_set():
            if self.window and self.device_statistics:
                # Aggregate statistics across all devices
                total_rows = sum(stats.get("rows", 0) for stats in self.device_statistics.values())
                total_size = sum(stats.get("file_size", 0) for stats in self.device_statistics.values())
                
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
            time.sleep(TIME_UPDATE_INTERVAL)
    
    def _device_thread(self) -> None:
        """Main device thread that sets up and starts data collection."""
        if not self.window:
            return
        
        self.stop_event.clear()
        safe_gui_update(self.window, self.window.reset_elapse_time)
        safe_gui_update(self.window, self.window.reset_statistics)
        set_button_state_safe(self.window, "start_button", "disabled")

        # Get timestamp and configuration
        date, time_str = get_timestamp_strings()
        sampling_rate = self._get_sampling_rate()
        ic256_ip, tx2_ip, note, save_folder = self._get_gui_values()

        # Initialize device threads dictionary and statistics
        self.device_threads = {}
        self.device_statistics = {}

        # Set up IC256 device
        ic256_stats: Dict[str, Any] = {}
        ic256_thread = setup_device_thread(
            IC256_CONFIG,
            ic256_ip,
            sampling_rate,
            date,
            time_str,
            note,
            save_folder,
            self.stop_event,
            self._log_callback,
            ic256_stats,
        )
        if ic256_thread:
            self.device_threads[IC256_CONFIG.device_name] = ic256_thread
            self.device_statistics[IC256_CONFIG.device_name] = ic256_stats

        # Set up TX2 device (optional)
        tx2_stats: Dict[str, Any] = {}
        tx2_thread = setup_device_thread(
            TX2_CONFIG,
            tx2_ip,
            sampling_rate,
            date,
            time_str,
            note,
            save_folder,
            self.stop_event,
            self._log_callback,
            tx2_stats,
        )
        if tx2_thread:
            self.device_threads[TX2_CONFIG.device_name] = tx2_thread
            self.device_statistics[TX2_CONFIG.device_name] = tx2_stats

        # Check if any devices were found
        if len(self.device_threads) == 0:
            show_message_safe(self.window, "No device found. Please try again.", "red")
            log_message_safe(self.window, "Data collection start failed: No valid devices found", "ERROR")
            set_button_state_safe(self.window, "start_button", "normal")
            return

        # Show success message
        device_names = list(self.device_threads.keys())
        device_list_str = ", ".join(device_names)
        verb = "is" if len(device_names) == 1 else "are"
        
        show_message_safe(self.window, f" {device_list_str} {verb} collecting.", "green")
        log_message_safe(self.window, f"Data collection started: {device_list_str}", "INFO")
        set_button_state_safe(self.window, "stop_button", "normal")

        # Start all threads
        time_thread = threading.Thread(
            target=self._update_elapse_time,
            name="elapse_time",
            daemon=True
        )
        time_thread.start()
        
        # Start statistics update thread
        stats_thread = threading.Thread(
            target=self._update_statistics,
            name="statistics_update",
            daemon=True
        )
        stats_thread.start()
        
        for device_name, thread in self.device_threads.items():
            thread.start()
            log_message_safe(self.window, f"Started data collection thread: {device_name}", "INFO")
    
    def _configure_and_start(self) -> None:
        """Configure device and start data collection in a background thread."""
        if not self.window:
            return
        
        try:
            ic256_ip, _, _, _ = self._get_gui_values()
            if not ic256_ip:
                show_message_safe(self.window, "IC256 IP address is required.", "red")
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
        """Stop data collection and restore device settings."""
        if not self.window:
            return
        
        set_button_state_safe(self.window, "start_button", "normal")
        set_button_state_safe(self.window, "stop_button", "disabled")
        self.stop_event.set()
        
        # Show immediate feedback
        device_names = list(self.device_threads.keys())
        device_list_str = ", ".join(device_names)
        show_message_safe(self.window, f"Stopping collection ({device_list_str})...", "blue")
        log_message_safe(self.window, f"Stopping data collection: {device_list_str}", "INFO")

        # Move thread waiting to background thread
        threads_to_wait = dict(self.device_threads)
        cleanup_thread = threading.Thread(
            target=self._wait_for_threads_and_cleanup,
            args=(threads_to_wait,),
            name="thread_cleanup",
            daemon=True
        )
        cleanup_thread.start()
    
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
            set_up_device(client, device_name, sampling_rate)
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
        self.window = GUI("IC256-42/35")

        # Override GUI methods with our implementations
        self.window.start = self.start_collection
        self.window.stop = self.stop_collection
        self.window.set_up_device = self.setup_devices

        # Start the GUI
        self.window.render()


def main() -> None:
    """Main entry point for the IC256 Sampler application."""
    app = Application()
    app.run()


if __name__ == "__main__":
    main()
