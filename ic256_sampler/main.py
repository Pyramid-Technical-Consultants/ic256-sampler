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
from typing import Dict, Optional, Any
from .igx_client import IGXWebsocketClient
from .gui import GUI
from .data_collection import collect_data, set_up_device
from .utils import is_valid_device
from datetime import datetime
import tempfile
import requests
from .device_paths import (
    IC256_45_PATHS,
    TX2_PATHS,
    build_http_url,
)

# Application constants
LOCK_FILE_NAME: str = "my_app.lock"
DEFAULT_SAMPLING_RATE: int = 500  # Hz
MIN_SAMPLING_RATE: int = 1
MAX_SAMPLING_RATE: int = 6000
THREAD_JOIN_TIMEOUT: float = 30.0  # seconds - increased to allow data collection threads to finish writing and cleanup
CONFIG_DELAY: float = 0.5  # seconds - delay before starting data collection
TIME_UPDATE_INTERVAL: float = 0.1  # seconds - interval for time display updates
FAN_RESTORE_TIMEOUT: float = 20.0  # seconds - timeout for fan restore HTTP request (increased)

# Initialize lock file for single instance enforcement
lock_file_path = os.path.join(tempfile.gettempdir(), LOCK_FILE_NAME)
lock_file = open(lock_file_path, "w")

# Global state (will be initialized after GUI creation)
detected_device_thread: Dict[str, threading.Thread] = {}
stop_event: threading.Event = threading.Event()
window: Optional[GUI] = None


def cleanup_lock_file() -> None:
    """Clean up lock file on program exit.
    
    This function is registered with atexit to ensure the lock file
    is properly released when the program terminates.
    """
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


def update_elapse_time() -> None:
    """Update elapsed time display in GUI.
    
    This function runs in a separate thread and updates the GUI
    with the current elapsed time since data collection started.
    """
    start_time = time.time()  # Track when data collection started
    while not stop_event.is_set():
        elapsed_time = time.time() - start_time
        minute_time = f"{int(elapsed_time // 60):02d}"
        second_time = f"{int(elapsed_time % 60):02d}"
        ticks_time = f"{int((elapsed_time - int(elapsed_time)) * 1000):03d}"
        
        # Use root.after for thread-safe GUI updates
        if window:
            window.root.after(
                0,
                lambda m=minute_time, s=second_time, t=ticks_time: window.update_elapse_time(m, s, t)
            )
        time.sleep(TIME_UPDATE_INTERVAL)


def _create_ic256_channels(client: IGXWebsocketClient) -> Dict[str, Any]:
    """Create channel dictionary for IC256 device.
    
    Args:
        client: IGXWebsocketClient instance for the IC256 device
        
    Returns:
        Dictionary mapping channel names to field objects
    """
    return {
        "mean_channel_a": client.field(IC256_45_PATHS["adc"]["gaussian_fit_a_mean"]),
        "sigma_channel_a": client.field(IC256_45_PATHS["adc"]["gaussian_fit_a_sigma"]),
        "mean_channel_b": client.field(IC256_45_PATHS["adc"]["gaussian_fit_b_mean"]),
        "sigma_channel_b": client.field(IC256_45_PATHS["adc"]["gaussian_fit_b_sigma"]),
        "primary_channel": client.field(IC256_45_PATHS["adc"]["primary_dose"]),
        "channel_sum": client.field(IC256_45_PATHS["adc"]["channel_sum"]),
        "external_trigger": client.field(IC256_45_PATHS["adc"]["gate_signal"]),
    }


def _create_ic256_env_channels(client: IGXWebsocketClient) -> Dict[str, Any]:
    """Create environment channel dictionary for IC256 device.
    
    Args:
        client: IGXWebsocketClient instance for the IC256 device
        
    Returns:
        Dictionary mapping environment channel names to field objects
    """
    return {
        "temperature": client.field(IC256_45_PATHS["environmental_sensor"]["temperature"]),
        "humidity": client.field(IC256_45_PATHS["environmental_sensor"]["humidity"]),
        "pressure": client.field(IC256_45_PATHS["environmental_sensor"]["pressure"]),
        "connected": client.field(IC256_45_PATHS["environmental_sensor"]["state"]),
    }


def _create_tx2_channels(client: IGXWebsocketClient) -> Dict[str, Any]:
    """Create channel dictionary for TX2 device.
    
    Args:
        client: IGXWebsocketClient instance for the TX2 device
        
    Returns:
        Dictionary mapping channel names to field objects
    """
    return {
        "probe_a": client.field(TX2_PATHS["adc"]["channel_5"]),
        "probe_b": client.field(TX2_PATHS["adc"]["channel_1"]),
        "fr2": client.field(TX2_PATHS["adc"]["fr2"]),
    }


def _setup_ic256_device(
    ip_address: str,
    sampling_rate: int,
    date: str,
    time_str: str,
) -> Optional[threading.Thread]:
    """Set up IC256 device and create data collection thread.
    
    Args:
        ip_address: IP address of the IC256 device
        sampling_rate: Sampling rate in Hz
        date: Date string for filename (YYYYMMDD)
        time_str: Time string for filename (HHMMSS)
        
    Returns:
        Thread object for data collection, or None if setup failed
    """
    if not ip_address or not is_valid_device(ip_address, "IC256"):
        return None
    
    try:
        if window:
            window.root.after(
                0,
                lambda: window.log_message(f"Connecting to IC256 device at {ip_address}", "INFO")
            )
        
        client = IGXWebsocketClient(ip_address)
        channels = _create_ic256_channels(client)
        env_channels = _create_ic256_env_channels(client)
        
        file_name = f"IC256_42x35-{date}-{time_str}.csv"
        note = window.note_entry.get() if window else ""
        save_folder = window.path_entry.get() if window else ""
        
        thread = threading.Thread(
            target=collect_data,
            name="ic256_device",
            daemon=True,
            args=(
                client,
                channels,
                env_channels,
                file_name,
                "ic256_45",
                note,
                save_folder,
                stop_event,
                sampling_rate,
            ),
        )
        
        if window:
            window.root.after(
                0,
                lambda: window.log_message(f"IC256 device thread created: {file_name}", "INFO")
            )
        
        return thread
    except Exception as e:
        error_msg = f"Failed to set up IC256 device at {ip_address}: {str(e)}"
        if window:
            window.root.after(0, lambda: window.log_message(error_msg, "ERROR"))
        print(error_msg)
        return None


def _setup_tx2_device(
    ip_address: str,
    sampling_rate: int,
    date: str,
    time_str: str,
) -> Optional[threading.Thread]:
    """Set up TX2 device and create data collection thread.
    
    Args:
        ip_address: IP address of the TX2 device
        sampling_rate: Sampling rate in Hz
        date: Date string for filename (YYYYMMDD)
        time_str: Time string for filename (HHMMSS)
        
    Returns:
        Thread object for data collection, or None if setup failed
    """
    if not ip_address:
        return None
    
    try:
        if not is_valid_device(ip_address, "TX2"):
            if window:
                window.root.after(
                    0,
                    lambda: window.log_message(
                        f"TX2 device at {ip_address} validation failed (skipping)", "WARNING"
                    )
                )
            return None
        
        if window:
            window.root.after(
                0,
                lambda: window.log_message(f"Connecting to TX2 device at {ip_address}", "INFO")
            )
        
        client = IGXWebsocketClient(ip_address)
        channels = _create_tx2_channels(client)
        
        file_name = f"TX2-{date}-{time_str}.csv"
        note = window.note_entry.get() if window else ""
        save_folder = window.path_entry.get() if window else ""
        
        thread = threading.Thread(
            target=collect_data,
            name="tx2_device",
            daemon=True,
            args=(
                client,
                channels,
                None,  # TX2 has no environment channels
                file_name,
                "tx2",
                note,
                save_folder,
                stop_event,
                sampling_rate,
            ),
        )
        
        if window:
            window.root.after(
                0,
                lambda: window.log_message(f"TX2 device thread created: {file_name}", "INFO")
            )
        
        return thread
    except Exception as e:
        error_msg = f"TX2 device found but connection failed (skipping): {str(e)}"
        if window:
            window.root.after(0, lambda: window.log_message(error_msg, "WARNING"))
        print(error_msg)
        return None


def device_thread() -> None:
    """Main device thread that sets up and starts data collection.
    
    This function runs in a background thread and handles:
    - Device validation and connection
    - Channel creation
    - Thread creation for data collection
    - Starting all collection threads
    """
    global detected_device_thread
    
    if not window:
        return
    
    stop_event.clear()
    window.root.after(0, window.reset_elapse_time)
    window.root.after(0, lambda: window.start_button.config(state="disabled"))

    # Get timestamp for filenames
    date = datetime.now().strftime("%Y%m%d")
    time_str = datetime.now().strftime("%H%M%S")
    
    # Get sampling rate
    try:
        sampling_rate = int(window.sampling_entry.get())
    except ValueError:
        sampling_rate = DEFAULT_SAMPLING_RATE
        window.root.after(
            0,
            lambda: window.log_message(
                f"Invalid sampling rate, using default: {DEFAULT_SAMPLING_RATE} Hz", "WARNING"
            )
        )

    # Initialize device threads dictionary
    detected_device_thread = {}

    # Set up IC256 device
    ic256_ip = window.ix256_a_entry.get()
    ic256_thread = _setup_ic256_device(ic256_ip, sampling_rate, date, time_str)
    if ic256_thread:
        detected_device_thread["IC256-42/35"] = ic256_thread

    # Set up TX2 device (optional)
    tx2_ip = window.tx2_entry.get()
    tx2_thread = _setup_tx2_device(tx2_ip, sampling_rate, date, time_str)
    if tx2_thread:
        detected_device_thread["TX2"] = tx2_thread

    # Start time update thread
    time_thread = threading.Thread(
        target=update_elapse_time,
        name="elapse_time",
        daemon=True
    )

    # Check if any devices were found
    if len(detected_device_thread) == 0:
        window.root.after(
            0,
            lambda: window.show_message("No device found. Please try again.", "red")
        )
        window.root.after(
            0,
            lambda: window.log_message("Data collection start failed: No valid devices found", "ERROR")
        )
        window.root.after(0, lambda: window.start_button.config(state="normal"))
        return

    # Show success message
    device_names = list(detected_device_thread.keys())
    device_list_str = ", ".join(device_names)
    verb = "is" if len(device_names) == 1 else "are"
    
    window.root.after(
        0,
        lambda: window.show_message(f" {device_list_str} {verb} collecting.", "green")
    )
    window.root.after(
        0,
        lambda: window.log_message(f"Data collection started: {device_list_str}", "INFO")
    )
    window.root.after(0, lambda: window.stop_button.config(state="normal"))

    # Start all threads
    for device_name, thread in detected_device_thread.items():
        thread.start()
        window.root.after(
            0,
            lambda name=device_name: window.log_message(f"Started data collection thread: {name}", "INFO")
        )
    time_thread.start()


def configure_and_start() -> None:
    """Configure device and start data collection in a background thread.
    
    This function handles pre-configuration (currently disabled) and
    then starts the device thread for data collection.
    """
    if not window:
        return
    
    try:
        ip_address = window.ix256_a_entry.get()
        if not ip_address:
            window.root.after(
                0,
                lambda: window.show_message("IC256 IP address is required.", "red")
            )
            window.root.after(0, lambda: window.start_button.config(state="normal"))
            return

        # Pre-config stage disabled for gate signal and fan control
        window.root.after(
            0,
            lambda: window.log_message("Pre-config stage disabled - proceeding directly to data collection", "INFO")
        )

        time.sleep(CONFIG_DELAY)
        thread = threading.Thread(target=device_thread, name="device_thread", daemon=True)
        thread.start()
    except Exception as e:
        error_msg = f"Error starting collection: {str(e)}"
        window.root.after(0, lambda: window.show_message(error_msg, "red"))
        window.root.after(0, lambda: window.log_message(error_msg, "ERROR"))
        window.root.after(0, lambda: window.start_button.config(state="normal"))


def start() -> None:
    """Start data collection with proper device configuration (non-blocking).
    
    This function is called by the GUI when the Start button is clicked.
    It immediately disables the button and starts configuration in a
    background thread to prevent UI freezing.
    """
    if not window:
        return
    
    window.start_button.config(state="disabled")
    window.show_message("Starting data collection...", "blue")
    
    config_thread = threading.Thread(
        target=configure_and_start,
        name="config_and_start",
        daemon=True
    )
    config_thread.start()


def _restore_fan_setting(ip_address: str) -> None:
    """Restore IC256 fan setting in a background thread.
    
    Args:
        ip_address: IP address of the IC256 device
    """
    try:
        url_fan_ic256 = build_http_url(ip_address, IC256_45_PATHS["io"]["fan_out"])
        # Use longer timeout and add a delay to let device finish processing
        time.sleep(2.0)  # Increased delay to let device finish data collection cleanup
        requests.put(url_fan_ic256, json=True, timeout=FAN_RESTORE_TIMEOUT)
        if window:
            window.root.after(
                0,
                lambda: window.log_message(
                    f"Fan setting restored: {IC256_45_PATHS['io']['fan_out']}", "INFO"
                )
            )
    except requests.exceptions.Timeout:
        # Timeout is not critical - just log as warning
        if window:
            window.root.after(
                0,
                lambda: window.log_message(
                    "Fan restore timed out (device may be busy) - this is not critical", "WARNING"
                )
            )
    except requests.exceptions.RequestException as e:
        # Other request errors - log as warning, not error
        if window:
            error_msg = str(e)  # Capture error message before lambda
            window.root.after(
                0,
                lambda msg=error_msg: window.log_message(
                    f"Fan restore failed (non-critical): {msg}", "WARNING"
                )
            )
    except Exception as e:
        if window:
            error_msg = str(e)  # Capture error message before lambda
            window.root.after(
                0,
                lambda msg=error_msg: window.log_message(f"Fan restore error (ignored): {msg}", "WARNING")
            )


def _wait_for_threads_and_cleanup(device_threads: Dict[str, threading.Thread]) -> None:
    """Wait for data collection threads to finish and perform cleanup.
    
    This function runs in a background thread to avoid freezing the GUI.
    
    Args:
        device_threads: Dictionary of device names to thread objects
    """
    device_names = list(device_threads.keys())
    threads_finished = True
    
    # Wait for threads to finish (with timeout to prevent indefinite blocking)
    for device_name, thread in device_threads.items():
        thread.join(timeout=THREAD_JOIN_TIMEOUT)
        if thread.is_alive():
            threads_finished = False
            if window:
                window.root.after(
                    0,
                    lambda name=device_name: window.log_message(
                        f"Warning: {name} thread did not finish within timeout", "WARNING"
                    )
                )
    
    if not threads_finished and window:
        window.root.after(
            0,
            lambda: window.log_message(
                "Some threads did not finish cleanly - data may be incomplete", "WARNING"
            )
        )

    device_list_str = ", ".join(device_names)
    if window:
        window.root.after(
            0,
            lambda: window.show_message(f"Collection completed ({device_list_str})", "green")
        )
        window.root.after(
            0,
            lambda: window.log_message(f"Data collection stopped: {device_list_str}", "INFO")
        )

    # Restore IC256 fan setting in background thread to avoid freezing GUI
    if window:
        ip_address = window.ix256_a_entry.get()
        if ip_address:
            restore_thread = threading.Thread(
                target=_restore_fan_setting,
                args=(ip_address,),
                name="restore_fan_setting",
                daemon=True
            )
            restore_thread.start()


def stop() -> None:
    """Stop data collection and restore device settings.
    
    This function is called by the GUI when the Stop button is clicked.
    It immediately signals threads to stop and moves cleanup to a background
    thread to prevent GUI freezing.
    """
    global stop_event, detected_device_thread
    
    if not window:
        return
    
    window.start_button.config(state="normal")
    window.stop_button.config(state="disabled")
    stop_event.set()
    
    # Show immediate feedback
    device_names = list(detected_device_thread.keys())
    device_list_str = ", ".join(device_names)
    window.show_message(f"Stopping collection ({device_list_str})...", "blue")
    window.log_message(f"Stopping data collection: {device_list_str}", "INFO")

    # Move thread waiting to background thread to prevent GUI freezing
    # Make a copy of the threads dict since it may be modified
    threads_to_wait = dict(detected_device_thread)
    cleanup_thread = threading.Thread(
        target=_wait_for_threads_and_cleanup,
        args=(threads_to_wait,),
        name="thread_cleanup",
        daemon=True
    )
    cleanup_thread.start()


def set_up_thread() -> None:
    """Set up device configuration in a separate thread.
    
    This function validates the sampling rate and configures device
    frequencies for all connected devices.
    """
    if not window:
        return
    
    window.set_up_button.config(state="disabled", image=window.loading_image)
    detected_device: Dict[str, IGXWebsocketClient] = {}
    
    try:
        sampling_rate = int(window.sampling_entry.get())
        if not (MIN_SAMPLING_RATE <= sampling_rate <= MAX_SAMPLING_RATE):
            raise ValueError(f"Sampling rate must be between {MIN_SAMPLING_RATE} and {MAX_SAMPLING_RATE} Hz")
        window.log_message(f"Setting up devices with sampling rate: {sampling_rate} Hz", "INFO")
    except ValueError as e:
        error_msg = f"Invalid sampling rate: {str(e)}"
        window.show_message(error_msg, "red")
        window.log_message(error_msg, "ERROR")
        window.set_up_button.config(state="normal", image=window.fail_image)
        return

    # Set up IC256 device
    ic256_ip = window.ix256_a_entry.get()
    if ic256_ip and is_valid_device(ic256_ip, "IC256"):
        try:
            window.log_message(f"Setting up IC256 device at {ic256_ip}", "INFO")
            ic256_client = IGXWebsocketClient(ic256_ip)
            detected_device["IC256"] = ic256_client
            set_up_device(ic256_client, "ic256_45", sampling_rate)
            ic256_client.close()
            window.log_message(f"IC256 device setup successful at {ic256_ip}", "INFO")
        except Exception as e:
            error_msg = f"Failed to set up IC256 at {ic256_ip}: {str(e)}"
            window.show_message(error_msg, "red")
            window.log_message(error_msg, "ERROR")
            if "IC256" in detected_device:
                del detected_device["IC256"]

    # Set up TX2 device (optional)
    tx2_ip = window.tx2_entry.get()
    if tx2_ip:
        try:
            if is_valid_device(tx2_ip, "TX2"):
                try:
                    window.log_message(f"Setting up TX2 device at {tx2_ip}", "INFO")
                    tx2_client = IGXWebsocketClient(tx2_ip)
                    detected_device["TX2"] = tx2_client
                    set_up_device(tx2_client, "tx2", sampling_rate)
                    tx2_client.close()
                    window.log_message(f"TX2 device setup successful at {tx2_ip}", "INFO")
                except Exception as e:
                    error_msg = f"TX2 setup failed at {tx2_ip} (skipping): {str(e)}"
                    window.log_message(error_msg, "WARNING")
            else:
                window.log_message(
                    f"TX2 device at {tx2_ip} validation failed (skipping)", "WARNING"
                )
        except Exception as e:
            window.log_message(
                f"TX2 validation error at {tx2_ip} (skipping): {str(e)}", "WARNING"
            )

    if len(detected_device) == 0:
        error_msg = "No device found. Please try again."
        window.show_message(error_msg, "red")
        window.log_message(error_msg, "ERROR")
        window.set_up_button.config(state="normal", image=window.fail_image)
        return

    device_list_str = ", ".join(detected_device.keys())
    success_msg = f"Update successful. ({device_list_str})"
    window.show_message(success_msg, "green")
    window.log_message(success_msg, "INFO")
    window.set_up_button.config(state="normal", image=window.pass_image)


def set_up() -> None:
    """Start device setup in a separate thread.
    
    This function is called by the GUI when the Set Up button is clicked.
    """
    thread = threading.Thread(target=set_up_thread, name="set_up_device", daemon=True)
    thread.start()


def main() -> None:
    """Main entry point for the IC256 Sampler application."""
    # Initialize application
    global stop_event, window
    stop_event = threading.Event()
    window = GUI("IC256-42/35")

    # Override GUI methods with our implementations
    window.start = start
    window.stop = stop
    window.set_up_device = set_up

    # Start the GUI
    window.render()


if __name__ == "__main__":
    main()
